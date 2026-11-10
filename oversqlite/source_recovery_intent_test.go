// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Source recovery must never strand local edits. A client whose source the server refuses keeps every
// pending edit, rebuilds under the replacement source and uploads the edits there. Edits that cannot
// apply to the authoritative snapshot block the rebuild until the caller discards them explicitly.
// An edit of a row that another device changed in the meantime keeps the version it was made from, so
// its upload raises an ordinary conflict instead of overwriting that change.

func markSourceRecoveryForTest(t *testing.T, db *sql.DB, reason SourceRecoveryCode, replacementSourceID string) {
	t.Helper()
	ctx := context.Background()
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	attachment.RebuildRequired = true
	require.NoError(t, persistAttachmentState(ctx, db, attachment))
	require.NoError(t, ensureSourceState(ctx, db, replacementSourceID))
	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindSourceRecovery, Reason: string(reason), ReplacementSourceID: replacementSourceID})
}

// recoveryExitTransport refuses every upload operation of the retired source, serves one sealed
// replacement snapshot, and hands the replacement source's uploads to its own push fixture.
type recoveryExitTransport struct {
	t                    *testing.T
	client               *Client
	oldSource, newSource string
	boundary             int64
	rows                 []oversync.SnapshotRow
	replacement          *mockPushServer
	refusedUploads       int
	snapshotCreates      int
}

func (s *recoveryExitTransport) roundTrip(r *http.Request) (*http.Response, error) {
	source := r.Header.Get(oversync.SourceIDHeader)
	switch {
	case strings.HasPrefix(r.URL.Path, "/sync/push"):
		if source == s.oldSource {
			s.refusedUploads++
			return errorJSONResponse(http.StatusConflict, oversync.SourceRetiredResponse{
				Error: "source_retired", Message: "source is retired", SourceID: s.oldSource, ReplacedBySourceID: s.newSource,
			}), nil
		}
		return s.replacement.RoundTrip(r)
	case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
		s.snapshotCreates++
		return snapshotFixtureResponseForGeneration(s.t, s.client.discovery.SchemaGeneration, r, "snapshot-exit", s.boundary, s.rows), nil
	case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/snapshot-exit":
		return jsonResponse(oversync.SnapshotChunkResponse{
			SnapshotID: "snapshot-exit", SnapshotBundleSeq: s.boundary, Rows: s.rows, NextRowOrdinal: int64(len(s.rows)),
		}), nil
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/"):
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
	}
	return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
}

func pendingIntentForTest(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT table_name || '/' || json_extract(key_json,'$.id') || '/' || op || '/' || base_row_version FROM _sync_dirty_rows ORDER BY table_name,key_json`)
	require.NoError(t, err)
	defer rows.Close()
	var pending []string
	for rows.Next() {
		var entry string
		require.NoError(t, rows.Scan(&entry))
		pending = append(pending, entry)
	}
	require.NoError(t, rows.Err())
	return pending
}

func TestSourceRecoveryIntent_FrozenUnsentUploadAndLaterEditsCarryToReplacementSource(t *testing.T) {
	ctx := context.Background()
	c, db := newUsersBundleClient(t)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO users VALUES('synced','Synced','synced@example.com')`)
	require.NoError(t, err)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	oldSource := c.sourceID

	// An upload freezes and its request never reaches the server.
	_, err = db.Exec(`UPDATE users SET name='Edited offline' WHERE id='synced'`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO users VALUES('created','Created offline','created@example.com')`)
	require.NoError(t, err)
	_, err = c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)=0`))
	// Later edits stay dirty behind the frozen upload.
	_, err = db.Exec(`INSERT INTO users VALUES('later','Later offline','later@example.com')`)
	require.NoError(t, err)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))

	// The server retires the source. Neither push nor resolve can ever settle the frozen upload.
	markSourceRecoveryForTest(t, db, SourceRecoveryRetired, "replacement-source")
	replacement := newMockPushServer(t)
	replacement.nextBundleSeq = 2
	transport := &recoveryExitTransport{
		t: t, client: c, oldSource: oldSource, newSource: "replacement-source", boundary: 1, replacement: replacement,
		rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "synced"}, RowVersion: 1,
			Payload: mustJSONPayload(t, map[string]any{"id": "synced", "name": "Synced", "email": "synced@example.com"})}},
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(transport.roundTrip)}
	var refused *SourceRecoveryRequiredError
	_, err = c.PushPending(ctx)
	require.ErrorAs(t, err, &refused)
	require.Equal(t, SourceRecoveryRetired, refused.Code)

	report, err := c.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, "replacement-source", c.sourceID)
	require.Equal(t, 1, transport.snapshotCreates)
	require.Empty(t, replacement.pushRequests, "rebuild itself uploads nothing")
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Equal(t, outboxStateNone, requireOutboxBundle(t, db).State)

	// Every edit is still visible locally and is pending against the installed snapshot versions.
	require.Equal(t, "Edited offline", baselineText(t, db, `SELECT name FROM users WHERE id='synced'`))
	require.Equal(t, "Created offline", baselineText(t, db, `SELECT name FROM users WHERE id='created'`))
	require.Equal(t, "Later offline", baselineText(t, db, `SELECT name FROM users WHERE id='later'`))
	require.Equal(t, []string{"users/created/INSERT/0", "users/later/INSERT/0", "users/synced/UPDATE/1"}, pendingIntentForTest(t, db))

	// Ordinary push uploads all of it as the replacement source's first bundle.
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	require.Len(t, replacement.pushRequests, 1)
	require.EqualValues(t, 1, replacement.pushRequests[0].SourceBundleID)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	committed := replacement.committedBySource[1]
	require.NotNil(t, committed)
	uploaded := map[string]string{}
	for _, row := range committed.Rows {
		uploaded[row.Key["id"].(string)] = row.Op
	}
	require.Equal(t, map[string]string{"synced": oversync.OpUpdate, "created": oversync.OpInsert, "later": oversync.OpInsert}, uploaded)
}

// A retained application rejection describes the retired source's request, which the carry releases
// after requeueing its intent. The rejection goes with it, so the carried edit uploads under the
// replacement source instead of waiting on a resolution that can no longer select anything.
func TestSourceRecoveryIntent_RetainedRejectionIsReleasedWithTheCarriedRequest(t *testing.T) {
	ctx := context.Background()
	c, db := newUsersBundleClient(t)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO users VALUES('synced','Synced','synced@example.com')`)
	require.NoError(t, err)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	oldSource := c.sourceID

	// The server rejects the next upload; the rejection is retained for an explicit decision.
	_, err = db.Exec(`UPDATE users SET name='Rejected offline' WHERE id='synced'`)
	require.NoError(t, err)
	server.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{
			"error": "transition_input_invalid", "message": "input cannot be converted",
		})
	}
	var needsResolution *PushRejectionNeedsResolutionError
	_, err = c.PushPending(ctx)
	require.ErrorAs(t, err, &needsResolution)
	pending, err := c.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)

	// The server then retires the source, and rebuild carries the rejected request's intent.
	markSourceRecoveryForTest(t, db, SourceRecoveryRetired, "replacement-source")
	replacement := newMockPushServer(t)
	replacement.nextBundleSeq = 3
	transport := &recoveryExitTransport{
		t: t, client: c, oldSource: oldSource, newSource: "replacement-source", boundary: 1, replacement: replacement,
		rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "synced"}, RowVersion: 1,
			Payload: mustJSONPayload(t, map[string]any{"id": "synced", "name": "Synced", "email": "synced@example.com"})}},
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(transport.roundTrip)}
	_, err = c.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, "replacement-source", c.sourceID)
	require.Equal(t, []string{"users/synced/UPDATE/1"}, pendingIntentForTest(t, db))
	require.Equal(t, "Rejected offline", baselineText(t, db, `SELECT name FROM users WHERE id='synced'`))
	released, err := c.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.Nil(t, released, "the carry releases the rejection together with the request it described")
	var stale *StalePushRejectionResolutionError
	require.ErrorAs(t, c.RetryPushRejection(ctx, *pending), &stale)

	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	require.Len(t, replacement.pushRequests, 1)
	require.EqualValues(t, 1, replacement.pushRequests[0].SourceBundleID)
	require.Equal(t, []string{"users:UPDATE:synced:1"}, pushedRowIdentities(t, c, replacement.pushRequests[0]))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
}

func TestSourceRecoveryIntent_UnsatisfiableEditBlocksRebuildUntilExplicitDiscard(t *testing.T) {
	ctx := context.Background()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}},
		`CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL,
		parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED)`)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL)`)
	require.NoError(t, err)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	oldSource := c.sourceID

	// A local child is pending while another device deletes its parent on the server.
	_, err = db.Exec(`INSERT INTO nodes VALUES('X','child of a remotely deleted parent','P')`)
	require.NoError(t, err)
	markSourceRecoveryForTest(t, db, SourceRecoverySequenceOutOfOrder, "replacement-source")
	replacement := newMockPushServer(t)
	replacement.nextBundleSeq = 3
	transport := &recoveryExitTransport{
		t: t, client: c, oldSource: oldSource, newSource: "replacement-source", boundary: 2, replacement: replacement,
		rows: []oversync.SnapshotRow{{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": "Q"}, RowVersion: 1,
			Payload: mustJSONPayload(t, map[string]any{"id": "Q", "name": "other", "parent_id": nil})}},
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(transport.roundTrip)}
	before := recoveryVector(t, db, "nodes", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state")

	_, err = c.Rebuild(ctx)
	var blocked *PendingIntentUnsatisfiableError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, []string{"nodes"}, blocked.Tables)
	require.Equal(t, oldSource, c.sourceID)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, before, recoveryVector(t, db, "nodes", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state"),
		"a blocked rebuild changes nothing")

	dropped, err := c.DiscardPendingIntentForRecovery(ctx)
	require.NoError(t, err)
	require.Equal(t, []DiscardedIntentRow{{Schema: "main", Table: "nodes", KeyJSON: `{"id":"X"}`, Op: oversync.OpInsert}}, dropped.Rows)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))

	_, err = c.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, "replacement-source", c.sourceID)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, "Q", baselineText(t, db, `SELECT group_concat(id) FROM nodes`))
	require.Empty(t, replacement.pushRequests, "the dropped edit never reached the server")
}

func TestSourceRecoveryIntent_EditCollidingWithSnapshotUniqueValueBlocksRebuild(t *testing.T) {
	ctx := context.Background()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}},
		`CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE, email TEXT NOT NULL)`)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO users VALUES('a','Alpha','a@example.com')`)
	require.NoError(t, err)
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	oldSource := c.sourceID

	// The same unique name is pending locally and reached the server from another device.
	_, err = db.Exec(`INSERT INTO users VALUES('b','Taken','b@example.com')`)
	require.NoError(t, err)
	markSourceRecoveryForTest(t, db, SourceRecoveryRetired, "replacement-source")
	replacement := newMockPushServer(t)
	replacement.nextBundleSeq = 3
	transport := &recoveryExitTransport{
		t: t, client: c, oldSource: oldSource, newSource: "replacement-source", boundary: 2, replacement: replacement,
		rows: []oversync.SnapshotRow{
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "a"}, RowVersion: 1,
				Payload: mustJSONPayload(t, map[string]any{"id": "a", "name": "Alpha", "email": "a@example.com"})},
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "c"}, RowVersion: 2,
				Payload: mustJSONPayload(t, map[string]any{"id": "c", "name": "Taken", "email": "c@example.com"})},
		},
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(transport.roundTrip)}
	before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state")

	_, err = c.Rebuild(ctx)
	var blocked *PendingIntentUnsatisfiableError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, []string{"users"}, blocked.Tables)
	require.Equal(t, oldSource, c.sourceID)
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state"))
	require.Equal(t, []string{"users/b/INSERT/0"}, pendingIntentForTest(t, db))
}

func TestSourceRecoveryIntent_DiscardNeedsSourceRecoveryAndAnAcceptedReplacement(t *testing.T) {
	ctx := context.Background()
	c, db := newUsersBundleClient(t)
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO users VALUES('pending','Pending','pending@example.com')`)
	require.NoError(t, err)

	var unavailable *PendingIntentDiscardUnavailableError
	_, err = c.DiscardPendingIntentForRecovery(ctx)
	require.ErrorAs(t, err, &unavailable)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))

	// A frozen request may still have an outcome on the server until the replacement is accepted.
	_, err = c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	markSourceRecoveryForTest(t, db, SourceRecoveryRetired, "replacement-source")
	before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal")
	_, err = c.DiscardPendingIntentForRecovery(ctx)
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal"))
}

type recordingConflictResolver struct {
	result    MergeResult
	conflicts []ConflictContext
}

func (r *recordingConflictResolver) Resolve(conflict ConflictContext) MergeResult {
	r.conflicts = append(r.conflicts, conflict)
	return r.result
}

func TestSourceRecoveryIntent_EditOfRowChangedByPeerKeepsOriginalBaseAndReachesResolver(t *testing.T) {
	userRow := func(t *testing.T, id, name string, version int64) oversync.SnapshotRow {
		return oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: version,
			Payload: mustJSONPayload(t, map[string]any{"id": id, "name": name, "email": id + "@example.com"})}
	}
	peerRow := func(id, name string) map[string]any {
		return map[string]any{"id": id, "name": name, "email": id + "@example.com"}
	}
	cases := []struct {
		name          string
		localEdit     string
		freeze        bool
		snapshot      func(t *testing.T) []oversync.SnapshotRow
		id            string
		pending       string
		localCarried  string
		uploadedOp    string
		uploadedBase  int64
		serverDeleted bool
		serverRow     map[string]any
		localResolved string
	}{
		{
			name: "update of a row a peer updated", localEdit: `UPDATE users SET name='Edited offline' WHERE id='synced'`,
			snapshot: func(t *testing.T) []oversync.SnapshotRow {
				return []oversync.SnapshotRow{userRow(t, "synced", "Edited by peer", 2)}
			},
			id: "synced", pending: "users/synced/UPDATE/1", localCarried: "Edited offline",
			uploadedOp: oversync.OpUpdate, uploadedBase: 1, serverRow: peerRow("synced", "Edited by peer"), localResolved: "Edited by peer",
		},
		{
			name: "frozen unsent update of a row a peer updated", localEdit: `UPDATE users SET name='Edited offline' WHERE id='synced'`, freeze: true,
			snapshot: func(t *testing.T) []oversync.SnapshotRow {
				return []oversync.SnapshotRow{userRow(t, "synced", "Edited by peer", 2)}
			},
			id: "synced", pending: "users/synced/UPDATE/1", localCarried: "Edited offline",
			uploadedOp: oversync.OpUpdate, uploadedBase: 1, serverRow: peerRow("synced", "Edited by peer"), localResolved: "Edited by peer",
		},
		{
			name: "update of a row a peer deleted", localEdit: `UPDATE users SET name='Edited offline' WHERE id='synced'`,
			snapshot: func(t *testing.T) []oversync.SnapshotRow { return nil },
			id:       "synced", pending: "users/synced/UPDATE/1", localCarried: "Edited offline",
			uploadedOp: oversync.OpInsert, uploadedBase: 1, serverDeleted: true, localResolved: "",
		},
		{
			name: "delete of a row a peer updated", localEdit: `DELETE FROM users WHERE id='synced'`,
			snapshot: func(t *testing.T) []oversync.SnapshotRow {
				return []oversync.SnapshotRow{userRow(t, "synced", "Edited by peer", 2)}
			},
			id: "synced", pending: "users/synced/DELETE/1", localCarried: "",
			uploadedOp: oversync.OpDelete, uploadedBase: 1, serverRow: peerRow("synced", "Edited by peer"), localResolved: "Edited by peer",
		},
		{
			name: "insert of a key a peer created", localEdit: `INSERT INTO users VALUES('created','Created offline','created@example.com')`,
			snapshot: func(t *testing.T) []oversync.SnapshotRow {
				return []oversync.SnapshotRow{userRow(t, "created", "Created by peer", 2), userRow(t, "synced", "Synced", 1)}
			},
			id: "created", pending: "users/created/INSERT/0", localCarried: "Created offline",
			uploadedOp: oversync.OpUpdate, uploadedBase: 0, serverRow: peerRow("created", "Created by peer"), localResolved: "Created by peer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, db := newUsersBundleClient(t)
			c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
			c.HTTP = &http.Client{Transport: newMockPushServer(t)}
			_, err := db.Exec(`INSERT INTO users VALUES('synced','Synced','synced@example.com')`)
			require.NoError(t, err)
			_, err = c.PushPending(ctx)
			require.NoError(t, err)
			oldSource := c.sourceID

			_, err = db.Exec(tc.localEdit)
			require.NoError(t, err)
			if tc.freeze {
				_, err = c.ensurePushOutboundSnapshot(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			}
			markSourceRecoveryForTest(t, db, SourceRecoveryRetired, "replacement-source")
			replacement := newMockPushServer(t)
			replacement.nextBundleSeq = 3
			transport := &recoveryExitTransport{
				t: t, client: c, oldSource: oldSource, newSource: "replacement-source", boundary: 2, replacement: replacement,
				rows: tc.snapshot(t),
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(transport.roundTrip)}
			localName := `SELECT COALESCE((SELECT name FROM users WHERE id='` + tc.id + `'),'')`

			_, err = c.Rebuild(ctx)
			require.NoError(t, err)
			require.Equal(t, "replacement-source", c.sourceID)
			require.Empty(t, replacement.pushRequests, "rebuild itself uploads nothing")
			// The application still sees its edit, pending against the version it was made from.
			require.Equal(t, []string{tc.pending}, pendingIntentForTest(t, db))
			require.Equal(t, tc.localCarried, baselineText(t, db, localName))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))

			resolver := &recordingConflictResolver{result: AcceptServer{}}
			c.Resolver = resolver
			var uploaded []oversync.PushRequestRow
			replacement.pushHook = func(session *mockDirectPush) *http.Response {
				uploaded = append(uploaded, session.Rows...)
				return structuredConflictResponse(session.Rows[0], 2, tc.serverDeleted, tc.serverRow)
			}
			_, err = c.PushPending(ctx)
			require.NoError(t, err)
			require.Len(t, uploaded, 1)
			require.Equal(t, tc.uploadedOp, uploaded[0].Op)
			require.Equal(t, tc.uploadedBase, uploaded[0].BaseRowVersion)
			require.Len(t, resolver.conflicts, 1)
			require.Equal(t, tc.uploadedBase, resolver.conflicts[0].BaseRowVersion)
			require.Equal(t, tc.serverDeleted, resolver.conflicts[0].ServerRowDeleted)
			require.Equal(t, tc.localResolved, baselineText(t, db, localName))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
		})
	}
}
