package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func sealedTestSnapshot(t *testing.T, c *Client, id string, boundary int64, rows []oversync.SnapshotRow, recovery *oversync.SnapshotRecovery) *oversync.SnapshotSession {
	return sealedTestSnapshotForSource(t, c, c.sourceID, id, boundary, rows, recovery)
}

func sealedTestSnapshotForSource(t *testing.T, c *Client, source, id string, boundary int64, rows []oversync.SnapshotRow, recovery *oversync.SnapshotRecovery) *oversync.SnapshotSession {
	t.Helper()
	require.NotNil(t, c.discovery)
	return sealedTestSnapshotForGeneration(t, c.discovery.SchemaGeneration, source, id, boundary, rows, recovery)
}

func sealedTestSnapshotForGeneration(t *testing.T, generation oversync.SchemaGenerationCapabilities, source, id string, boundary int64, rows []oversync.SnapshotRow, recovery *oversync.SnapshotRecovery) *oversync.SnapshotSession {
	t.Helper()
	session := &oversync.SnapshotSession{SnapshotID: id, SnapshotBundleSeq: boundary, RowCount: int64(len(rows)), ExpiresAt: "2030-01-01T00:00:00Z", Recovery: recovery}
	_, size, err := oversync.CompactGroupsForSnapshotRowsAt(generation, rows, 1)
	require.NoError(t, err)
	session.ByteCount = size
	h, err := oversync.NewSnapshotHashWriter(source, generation, id, boundary, recovery)
	require.NoError(t, err)
	for _, row := range rows {
		groups, _, err := oversync.CompactGroupsForSnapshotRowsAt(generation, []oversync.SnapshotRow{row}, 1)
		require.NoError(t, err)
		index := groups[0].TableIndex
		keys := make([]any, len(generation.Tables[index].SyncKeyColumns))
		for i, name := range generation.Tables[index].SyncKeyColumns {
			keys[i] = row.Key[name]
		}
		require.NoError(t, h.Add(index, keys, row.RowVersion, row.Payload))
	}
	session.SnapshotHash, err = h.Sum(session.RowCount, session.ByteCount)
	require.NoError(t, err)
	return session
}

func snapshotFixtureResponse(t *testing.T, c *Client, r *http.Request, id string, boundary int64, rows []oversync.SnapshotRow) *http.Response {
	t.Helper()
	var request oversync.SnapshotSessionCreateRequest
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if len(raw) > 0 {
			require.NoError(t, json.Unmarshal(raw, &request))
		}
	}
	source := c.sourceID
	if request.SourceReplacement != nil {
		source = request.SourceReplacement.NewSourceID
	}
	return jsonResponse(sealedTestSnapshotForSource(t, c, source, id, boundary, rows, request.Recovery))
}

func stageRecoverySnapshot(t *testing.T, c *Client, id string, boundary int64, rows []oversync.SnapshotRow) *oversync.SnapshotSession {
	t.Helper()
	session := sealedTestSnapshot(t, c, id, boundary, rows, nil)
	require.NoError(t, c.stageSnapshotChunk(context.Background(), &oversync.SnapshotChunkResponse{SnapshotID: id, SnapshotBundleSeq: boundary, Rows: rows, NextRowOrdinal: int64(len(rows)), ByteCount: session.ByteCount}, 0))
	require.NoError(t, c.markCheckpointRecoveryRequiredLocked(context.Background(), "explicit_rebuild"))
	return session
}

func recoveryUsers(t *testing.T, c *Client, names, emails []string) []oversync.SnapshotRow {
	t.Helper()
	rows := make([]oversync.SnapshotRow, len(names))
	for i, name := range names {
		id := string(rune('A' + i))
		rows[i] = oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": name, "email": emails[i]})}
	}
	return rows
}

func seedRecoveryUsers(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	for _, id := range []string{"A", "B"} {
		seedSyncedUserRow(t, c, db, id, "same", 1)
	}
	_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX users_email_unique ON users(email)`)
	require.NoError(t, err)
	return c, db
}

func TestFinalStateRecovery_CleanAdmission(t *testing.T) {
	for _, state := range []string{"dirty_anywhere", "prepared", "stored_outcome", "rejected", "ack_pending"} {
		t.Run(state, func(t *testing.T) {
			c, db, server := recoveryJournalClient(t)
			if state == "rejected" {
				server.pushHook = func(p *mockDirectPush) *http.Response {
					return server.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "application_rejected"}}, p.Generation)
				}
			}
			if state == "stored_outcome" {
				c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if state == "prepared" && r.URL.Path == "/sync/push" || state == "ack_pending" && r.URL.Path == "/sync/push-outcomes/ack" {
					return nil, context.Canceled
				}
				return server.RoundTrip(r)
			})}
			if state != "dirty_anywhere" {
				_, err := c.PushPending(context.Background())
				require.Error(t, err)
			}
			var requests atomic.Int64
			c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) { requests.Add(1); return nil, context.Canceled })}
			before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_bundle", "_sync_outbox_rows", "_sync_upload_journal", "_sync_row_state", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state")
			_, err := c.Rebuild(context.Background())
			require.Error(t, err)
			require.Zero(t, requests.Load())
			require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_bundle", "_sync_outbox_rows", "_sync_upload_journal", "_sync_row_state", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state"))
		})
	}
}

func logicalRecoveryVector(t *testing.T, db *sql.DB) string {
	t.Helper()
	var identities string
	require.NoError(t, db.QueryRow(`SELECT json_group_array(json_array(rid,id)) FROM (SELECT rowid AS rid,id FROM users ORDER BY id)`).Scan(&identities))
	return identities + recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_bundle", "_sync_outbox_rows", "_sync_upload_journal", "_sync_attachment_state", "_sync_source_state")
}

func TestFinalStateRecovery_SnapshotPreservesLogicalAudit(t *testing.T) {
	for _, kind := range []string{"main", "temp", "sdk_name_spoof", "unchanged"} {
		t.Run(kind, func(t *testing.T) {
			c, db := seedRecoveryUsers(t)
			prefix := ""
			if kind == "temp" {
				prefix = "TEMP "
			}
			trigger := "audit_delete"
			if kind == "sdk_name_spoof" {
				_, err := db.Exec(`DROP TRIGGER trg_users_ad`)
				require.NoError(t, err)
				trigger = "trg_users_ad"
			}
			_, err := db.Exec(`CREATE TABLE audit(value TEXT); CREATE ` + prefix + `TRIGGER ` + trigger + ` AFTER DELETE ON users BEGIN INSERT INTO audit VALUES(old.id); END`)
			require.NoError(t, err)
			names := []string{"changed", "same"}
			emails := []string{"b", "a"}
			if kind == "unchanged" {
				names = []string{"same", "same"}
				emails = []string{"a", "b"}
			}
			session := stageRecoverySnapshot(t, c, "audit-"+kind, 2, recoveryUsers(t, c, names, emails))
			before := logicalRecoveryVector(t, db)
			err = c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{})
			if kind == "unchanged" {
				require.NoError(t, err)
			} else {
				var blocked *FinalStateApplyBlockedError
				require.ErrorAs(t, err, &blocked)
				require.Equal(t, trigger, blocked.Trigger)
				require.Equal(t, before, logicalRecoveryVector(t, db))
			}
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM audit`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestFinalStateRecovery_SnapshotPreservesLocalOnlyClosure(t *testing.T) {
	for _, action := range []string{"CASCADE", "NO ACTION", "empty"} {
		t.Run(action, func(t *testing.T) {
			c, db := seedRecoveryUsers(t)
			rule := action
			if action == "empty" {
				rule = "CASCADE"
			}
			_, err := db.Exec(`CREATE TABLE notes(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(id) ON DELETE ` + rule + `,body BLOB NOT NULL); CREATE TABLE app_settings(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO app_settings VALUES(41,'local settings')`)
			require.NoError(t, err)
			if action != "empty" {
				_, err = db.Exec(`INSERT INTO notes VALUES(77,'A',x'00aaff')`)
				require.NoError(t, err)
			}
			session := stageRecoverySnapshot(t, c, "closure-"+action, 2, recoveryUsers(t, c, []string{"same", "same"}, []string{"b", "a"}))
			var rowA, rowB int64
			require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id='A'`).Scan(&rowA))
			require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id='B'`).Scan(&rowB))
			before := logicalRecoveryVector(t, db)
			err = c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{})
			if action == "CASCADE" {
				var blocked *FinalStateApplyBlockedError
				require.ErrorAs(t, err, &blocked)
				require.Equal(t, before, logicalRecoveryVector(t, db))
			} else {
				require.NoError(t, err)
			}
			var actual int64
			require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id='A'`).Scan(&actual))
			require.Equal(t, rowA, actual)
			require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id='B'`).Scan(&actual))
			require.Equal(t, rowB, actual)
			if action != "empty" {
				var id int64
				var parent, body string
				require.NoError(t, db.QueryRow(`SELECT id,user_id,hex(body) FROM notes`).Scan(&id, &parent, &body))
				require.Equal(t, int64(77), id)
				require.Equal(t, "A", parent)
				require.Equal(t, "00AAFF", body)
			}
			var setting string
			require.NoError(t, db.QueryRow(`SELECT value FROM app_settings WHERE id=41`).Scan(&setting))
			require.Equal(t, "local settings", setting)
		})
	}
}

func TestFinalStateRecovery_DependencyMembership(t *testing.T) {
	c, db := seedRecoveryUsers(t)
	_, err := db.Exec(`CREATE TABLE notes(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(id) ON DELETE CASCADE,body TEXT);`)
	require.NoError(t, err)
	session := stageRecoverySnapshot(t, c, "late-child", 2, recoveryUsers(t, c, []string{"same", "same"}, []string{"b", "a"}))
	guard, err := c.pinSnapshotApplyGuard(context.Background(), snapshotApplyOptions{})
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO notes VALUES(91,'A','late child')`)
	require.NoError(t, err)
	before := logicalRecoveryVector(t, db)
	err = c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{PinnedGuard: guard})
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, before, logicalRecoveryVector(t, db))
	var body string
	require.NoError(t, db.QueryRow(`SELECT body FROM notes WHERE id=91`).Scan(&body))
	require.Equal(t, "late child", body)
}

func TestFinalStateRecovery_SnapshotDeltaRollback(t *testing.T) {
	for _, failure := range []string{"constraint", "cancellation", "hash", "schema"} {
		t.Run(failure, func(t *testing.T) {
			ddl := strings.Replace(usersTestDDL, "name TEXT NOT NULL", "name TEXT NOT NULL CHECK(name <> 'reject')", 1)
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, ddl)
			for _, id := range []string{"A", "B"} {
				seedSyncedUserRow(t, c, db, id, "same", 1)
			}
			_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX users_email_unique ON users(email)`)
			require.NoError(t, err)
			names := []string{"changed", "same"}
			if failure == "constraint" {
				names[0] = "reject"
			}
			rows := recoveryUsers(t, c, names, []string{"b", "c"})
			session := stageRecoverySnapshot(t, c, "rollback-"+failure, 2, rows)
			options := snapshotApplyOptions{}
			options.PinnedGuard, err = c.pinSnapshotApplyGuard(context.Background(), options)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch failure {
			case "cancellation":
				c.afterSnapshotRowsHook = cancel
			case "hash":
				session.SnapshotHash = strings.Repeat("0", 64)
			case "schema":
				_, err = db.Exec(`CREATE TABLE late_schema(value TEXT)`)
				require.NoError(t, err)
			}
			before := logicalRecoveryVector(t, db)
			err = c.applyStagedSnapshotLocked(ctx, session, options)
			require.Error(t, err)
			if failure == "constraint" {
				require.ErrorContains(t, err, "CHECK constraint")
			}
			if failure == "cancellation" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if failure == "hash" {
				require.ErrorContains(t, err, "hash mismatch")
			}
			require.Equal(t, before, logicalRecoveryVector(t, db))
			require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
		})
	}
}

func installSealedSnapshotFixture(t *testing.T, c *Client, rows []oversync.SnapshotRow, behavior func(*http.Request, int) error) *atomic.Int64 {
	t.Helper()
	var creates atomic.Int64
	var session *oversync.SnapshotSession
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if behavior != nil {
			if err := behavior(r, int(creates.Load())); err != nil {
				return nil, err
			}
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			creates.Add(1)
			var req oversync.SnapshotSessionCreateRequest
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &req))
			if req.Recovery != nil && req.Recovery.MinimumBundleSeq > 3 {
				return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{Error: "recovery_floor_ahead"}), nil
			}
			session = sealedTestSnapshot(t, c, "transfer-"+strconv.FormatInt(creates.Load(), 10), 3, rows, req.Recovery)
			return jsonResponse(session), nil
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/"):
			require.NotNil(t, session)
			after, err := strconv.ParseInt(r.URL.Query().Get("after_row_ordinal"), 10, 64)
			require.NoError(t, err)
			end := min(int64(len(rows)), after+1)
			groups, size, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, rows[after:end], after+1)
			require.NoError(t, err)
			if groups == nil {
				groups = []oversync.CompactRowGroup{}
			}
			return jsonResponse(oversync.CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: end, HasMore: end < int64(len(rows)), LogicalByteCount: size}), nil
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/"):
			return &http.Response{StatusCode: 204, Header: http.Header{}, Body: http.NoBody}, nil
		}
		return nil, io.EOF
	})}
	return &creates
}

func TestFinalStateRecovery_SealedTransfer(t *testing.T) {
	for _, cut := range []string{"partial", "complete", "floor"} {
		t.Run(cut, func(t *testing.T) {
			c, db := seedRecoveryUsers(t)
			rows := recoveryUsers(t, c, []string{"remote", "same"}, []string{"b", "a"})
			interrupted := true
			creates := installSealedSnapshotFixture(t, c, rows, func(r *http.Request, _ int) error {
				if interrupted && cut == "partial" && r.Method == http.MethodGet && r.URL.Query().Get("after_row_ordinal") == "1" {
					return context.Canceled
				}
				return nil
			})
			if cut == "complete" || cut == "floor" {
				c.beforeSnapshotApplyHook = func(context.Context) error { return context.Canceled }
			}
			before := recoveryVector(t, db, "users", "_sync_row_state", "_sync_source_state")
			_, err := c.Rebuild(context.Background())
			require.Error(t, err)
			require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_source_state"))
			require.EqualValues(t, 1, creates.Load())
			var transfer string
			require.NoError(t, db.QueryRow(`SELECT snapshot_transfer FROM _sync_operation_state`).Scan(&transfer))
			var state snapshotTransfer
			require.NoError(t, json.Unmarshal([]byte(transfer), &state))
			if cut == "partial" {
				require.EqualValues(t, 1, state.Rows)
				require.False(t, state.Complete)
			} else {
				require.True(t, state.Complete)
			}
			interrupted = false
			c.beforeSnapshotApplyHook = nil
			if cut == "complete" {
				// No GET/create is allowed after the complete verified local stage.
				c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					require.Equal(t, http.MethodDelete, r.Method)
					return &http.Response{StatusCode: 204, Body: http.NoBody, Header: http.Header{}}, nil
				})}
			}
			if cut == "floor" {
				_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=4`)
				require.NoError(t, err)
				_, err = c.Rebuild(context.Background())
				require.ErrorContains(t, err, "recovery_floor_ahead")
				require.EqualValues(t, 2, creates.Load())
				return
			}
			_, err = c.Rebuild(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, 1, creates.Load())
			requireLastBundleSeqSeen(t, c, context.Background(), 3)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
			var saved string
			require.NoError(t, db.QueryRow(`SELECT snapshot_transfer FROM _sync_operation_state`).Scan(&saved))
			require.Empty(t, saved)
		})
	}
}

func TestFinalStateRecovery_ExactSpecialKeys(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	ids := []string{"<A>&", "B\u2028\u2029", "absent<&\u2028"}
	rows := make([]oversync.SnapshotRow, 0, 2)
	for i, id := range ids {
		_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, id, "before", fmt.Sprint(i))
		require.NoError(t, err)
		tx, err := db.Begin()
		require.NoError(t, err)
		key, err := c.syncKeyJSONForPKInTx(tx, "users", id)
		require.NoError(t, err)
		require.NoError(t, c.seedFixtureRowStateInTx(context.Background(), tx, "main", "users", key, 1, false))
		require.NoError(t, tx.Commit())
		if i < 2 {
			rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "after", "email": fmt.Sprint(i)})})
		}
	}
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows; UPDATE _sync_attachment_state SET last_bundle_seq_seen=1`)
	require.NoError(t, err)
	session := stageRecoverySnapshot(t, c, "exact-keys", 2, rows)
	var oldRowID int64
	require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id=?`, ids[0]).Scan(&oldRowID))
	require.NoError(t, c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{}))
	require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM users`))
	var newRowID int64
	require.NoError(t, db.QueryRow(`SELECT rowid FROM users WHERE id=?`, ids[0]).Scan(&newRowID))
	require.Equal(t, oldRowID, newRowID)
	for _, id := range ids[:2] {
		_, err = db.Exec(`UPDATE users SET name='local after snapshot' WHERE id=?`, id)
		require.NoError(t, err)
		var base int64
		var observations int
		require.NoError(t, db.QueryRow(`SELECT base_row_version FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id')=?`, id).Scan(&base))
		require.EqualValues(t, 2, base)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_row_state WHERE json_extract(key_json,'$.id')=?`, id).Scan(&observations))
		require.Equal(t, 1, observations)
	}
}

func TestFinalStateRecovery_AuthorityAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.sqlite")
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(usersTestDDL)
	require.NoError(t, err)
	config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	c, err := newOwnedAdmittedFixtureClient(db, "http://first.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	attachTestClient(t, c, "bundle-user", "bundle-device")
	seedSyncedUserRow(t, c, db, "A", "before", 1)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1`)
	require.NoError(t, err)
	rows := recoveryUsers(t, c, []string{"old authority"}, []string{"a"})
	installSealedSnapshotFixture(t, c, rows, nil)
	c.beforeSnapshotApplyHook = func(context.Context) error { return context.Canceled }
	_, err = c.Rebuild(context.Background())
	require.Error(t, err)
	var encoded string
	require.NoError(t, db.QueryRow(`SELECT snapshot_transfer FROM _sync_operation_state`).Scan(&encoded))
	var saved snapshotTransfer
	require.NoError(t, json.Unmarshal([]byte(encoded), &saved))
	require.True(t, saved.Complete)
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	db, err = sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	c, err = newOwnedAdmittedFixtureClient(db, "http://second.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	defer c.Close()
	attachTestClient(t, c, "bundle-user", "bundle-device")
	var creates int
	c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "second.invalid", r.URL.Host)
		require.Equal(t, http.MethodPost, r.Method)
		creates++
		return nil, context.Canceled
	})}
	_, err = c.Rebuild(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, creates, "a verified stage belongs to its exact authority")
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id='A'`).Scan(&name))
	require.Equal(t, "before", name)
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
}

func TestFinalStateRecovery_PreservationGraphRemainder(t *testing.T) {
	for _, cyclic := range []bool{false, true} {
		t.Run(fmt.Sprint(cyclic), func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			const n = 64
			var rows []oversync.SnapshotRow
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("%03d", i)
				seedSyncedUserRow(t, c, db, id, "same", 1)
				_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=? WHERE id=?`, id, id)
				require.NoError(t, err)
				_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=0`)
				require.NoError(t, err)
				desired := fmt.Sprintf("%03d", i+1)
				if cyclic && i < 4 {
					desired = fmt.Sprintf("%03d", i^1)
				}
				rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "same", "email": desired})})
			}
			_, err := db.Exec(`CREATE UNIQUE INDEX unique_email ON users(email); UPDATE _sync_attachment_state SET last_bundle_seq_seen=1`)
			require.NoError(t, err)
			session := stageRecoverySnapshot(t, c, "graph-remainder", 2, rows)
			ctx := withUniqueApplyContext(context.Background())
			uniqueInvocation(ctx).snapshot = true
			require.NoError(t, c.completeWork(ctx, "prepare graph witness", func(ctx context.Context) error {
				tx, err := beginImmediateTx(ctx, db)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if err := c.prepareSnapshotApply(ctx, tx, session); err != nil {
					return commitApplyProgress(ctx, tx, err)
				}
				return tx.Commit(ctx)
			}))
			wantCycles := int64(0)
			if cyclic {
				wantCycles = 4
			}
			require.EqualValues(t, n-wantCycles, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows WHERE pending=6`))
			for repeat := 0; repeat < 2; repeat++ {
				tx, err := beginImmediateTx(ctx, db)
				require.NoError(t, err)
				require.NoError(t, c.validateSnapshotPreservation(ctx, tx))
				require.NoError(t, tx.Commit(ctx))
				require.EqualValues(t, wantCycles, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows WHERE pending=7`))
			}
			require.NoError(t, c.applyStagedSnapshotLocked(ctx, session, snapshotApplyOptions{}))
			requireLastBundleSeqSeen(t, c, ctx, 2)
		})
	}
}

func TestFinalStateRecovery_UnindexedEmptyMembershipWork(t *testing.T) {
	c, db := seedRecoveryUsers(t)
	// NULL references make every affected incoming set empty, while each proof
	// still has to inspect all 1,000 unindexed local rows.
	_, err := db.Exec(`CREATE TABLE notes(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(id) ON DELETE CASCADE,body BLOB); WITH RECURSIVE n(i) AS(VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000) INSERT INTO notes SELECT i,NULL,x'001122aaff' FROM n`)
	require.NoError(t, err)
	before := recoveryVector(t, db, "notes")
	rows := recoveryUsers(t, c, []string{"same", "same"}, []string{"b", "a"})
	installSealedSnapshotFixture(t, c, rows, nil)
	_, err = c.Rebuild(context.Background())
	require.NoError(t, err)
	require.Equal(t, before, recoveryVector(t, db, "notes"))
	require.GreaterOrEqual(t, c.a3Stats.totalWork, int64(2*2*1000*48), "both actual mutation and SCC vacate scans are charged")
}

func TestFinalStateRecovery_VacateRequiresWriterProof(t *testing.T) {
	c, db := seedRecoveryUsers(t)
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	defer tx.Rollback()
	err = c.validateUniqueVacate(withUniqueApplyContext(context.Background()), tx,
		&applyPlan{kind: "snapshot"}, applySurvivor{table: "users", pk: "A"})
	require.ErrorContains(t, err, "final-writer preservation proof")
}

func TestFinalStateRecovery_InvalidMetadataCleanupBudget(t *testing.T) {
	for _, used := range []int{62, 63} {
		t.Run(fmt.Sprint(used), func(t *testing.T) {
			c, _ := seedRecoveryUsers(t)
			var creates, deletes int
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodDelete {
					deletes++
					return &http.Response{StatusCode: 204, Header: http.Header{}, Body: http.NoBody}, nil
				}
				creates++
				session := sealedTestSnapshot(t, c, "bad-expiry", 2, nil, nil)
				session.ExpiresAt = ""
				return jsonResponse(session), nil
			})}
			invocation := &syncInvocation{recoveryExchanges: used}
			ctx := context.WithValue(context.Background(), syncInvocationKey{}, invocation)
			_, err := c.createSnapshotSession(ctx, nil)
			require.Error(t, err)
			require.Equal(t, 1, creates)
			require.Equal(t, 63-used, deletes)
			require.Equal(t, 64, invocation.recoveryExchanges)
		})
	}
}

// Once a replacement receipt commits locally, an expired partial download is
// refreshed under that source. The immutable replacement tuple is not replayed.
func TestFinalStateRecovery_ExpiredReplacement(t *testing.T) {
	for _, status := range []int{http.StatusGone, http.StatusNotFound} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			c, db := seedRecoveryUsers(t)
			old := c.sourceID
			require.NoError(t, c.markSourceRecoveryRequiredLocked(context.Background(), SourceRecoveryRetired, "", "replacement-source"))
			rows := recoveryUsers(t, c, []string{"remote", "same"}, []string{"b", "a"})
			var session *oversync.SnapshotSession
			creates, pages := 0, 0
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch r.Method {
				case http.MethodPost:
					creates++
					var request oversync.SnapshotSessionCreateRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					if creates == 1 {
						require.Equal(t, old, r.Header.Get(oversync.SourceIDHeader))
						require.NotNil(t, request.SourceReplacement)
						require.Equal(t, "replacement-source", request.SourceReplacement.NewSourceID)
						require.Nil(t, request.Recovery)
					} else {
						require.Equal(t, 2, creates)
						require.Equal(t, "replacement-source", r.Header.Get(oversync.SourceIDHeader))
						require.Nil(t, request.SourceReplacement)
						require.Equal(t, &oversync.SnapshotRecovery{MinimumBundleSeq: 1}, request.Recovery)
						require.Equal(t, old, c.sourceID, "local identity only changes at final commit")
						require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
					}
					session = sealedTestSnapshotForSource(t, c, "replacement-source", fmt.Sprintf("replacement-%d", creates), 2, rows, request.Recovery)
					return jsonResponse(session), nil
				case http.MethodGet:
					pages++
					require.Equal(t, "replacement-source", r.Header.Get(oversync.SourceIDHeader))
					after, err := strconv.ParseInt(r.URL.Query().Get("after_row_ordinal"), 10, 64)
					require.NoError(t, err)
					if creates == 1 && after == 1 {
						code := "snapshot_session_expired"
						if status == http.StatusNotFound {
							code = "snapshot_session_not_found"
						}
						return errorJSONResponse(status, oversync.ErrorResponse{Error: code}), nil
					}
					end := min(int64(len(rows)), after+1)
					groups, bytes, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, rows[after:end], after+1)
					require.NoError(t, err)
					return jsonResponse(oversync.CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: end, HasMore: end < int64(len(rows)), LogicalByteCount: bytes}), nil
				case http.MethodDelete:
					return &http.Response{StatusCode: 204, Header: http.Header{}, Body: http.NoBody}, nil
				}
				return nil, fmt.Errorf("unexpected replacement snapshot request")
			})}
			_, err := c.Rebuild(context.Background())
			require.NoError(t, err)
			require.Equal(t, 2, creates)
			require.Equal(t, 4, pages)
			require.Equal(t, "replacement-source", c.sourceID)
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
			require.Equal(t, "remote", baselineText(t, db, `SELECT name FROM users WHERE id='A'`))
		})
	}
}

func TestFinalStateRecovery_ContextChangePreventsDispatch(t *testing.T) {
	for _, phase := range []string{"page_main", "page_temp", "refresh_main", "refresh_temp"} {
		t.Run(phase, func(t *testing.T) {
			c, db := seedRecoveryUsers(t)
			ddl := `CREATE TABLE context_change(value TEXT)`
			if strings.HasSuffix(phase, "temp") {
				ddl = `CREATE TEMP TABLE context_change(value TEXT)`
			}
			if strings.HasPrefix(phase, "page") {
				connection, err := db.Conn(context.Background())
				require.NoError(t, err)
				require.NoError(t, connection.Raw(func(raw any) error {
					native := raw.(*sqlite3.SQLiteConn)
					return native.RegisterFunc("snapshot_change_schema", func() (int, error) { _, err := native.Exec(ddl, nil); return 1, err }, false)
				}))
				require.NoError(t, connection.Close())
				_, err = db.Exec(`CREATE TEMP TRIGGER snapshot_context_change AFTER UPDATE OF snapshot_transfer ON _sync_operation_state
     WHEN CASE WHEN json_valid(new.snapshot_transfer) AND json_valid(old.snapshot_transfer)
      THEN json_extract(new.snapshot_transfer,'$.rows')=1 AND json_extract(old.snapshot_transfer,'$.rows')=0 ELSE 0 END
     BEGIN SELECT snapshot_change_schema();END`)
				require.NoError(t, err)
			}
			rows := recoveryUsers(t, c, []string{"remote", "same"}, []string{"b", "a"})
			creates := installSealedSnapshotFixture(t, c, rows, nil)
			before := recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_source_state")
			delegate := c.HTTP.Transport
			pages := 0
			c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") {
					pages++
					if strings.HasPrefix(phase, "refresh") {
						_, err := db.Exec(ddl)
						require.NoError(t, err)
						return errorJSONResponse(http.StatusGone, oversync.ErrorResponse{Error: "snapshot_session_expired"}), nil
					}
				}
				return delegate.RoundTrip(r)
			})
			_, err := c.Rebuild(context.Background())
			require.Error(t, err)
			require.ErrorContains(t, err, "snapshot context or schema changed")
			require.EqualValues(t, 1, creates.Load())
			require.Equal(t, 1, pages)
			require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_source_state"))
			requireLastBundleSeqSeen(t, c, context.Background(), 1)
		})
	}
}

func TestFinalStateRecovery_RetryAdmission(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		for _, phase := range []string{"create", "page"} {
			for _, change := range []string{"dirty", "main", "temp"} {
				t.Run(fmt.Sprintf("%d_%s_%s", status, phase, change), func(t *testing.T) {
					c, db := seedRecoveryUsers(t)
					rows := recoveryUsers(t, c, []string{"remote"}, []string{"b"})
					installSealedSnapshotFixture(t, c, rows, nil)
					c.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
					c.config.SnapshotCapacityRetryPolicy = &SnapshotCapacityRetryPolicy{Enabled: true, MaxWait: time.Second, FallbackDelay: time.Millisecond}
					delegate := c.HTTP.Transport
					creates, pages := 0, 0
					var changed string
					c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
						selected := false
						if r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions" {
							creates++
							selected = phase == "create"
						}
						if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") {
							pages++
							selected = phase == "page"
						}
						if selected {
							ddl := `UPDATE users SET name='later' WHERE id='A'`
							if change != "dirty" {
								ddl = `CREATE TABLE ` + change + `.changed_retry(id INTEGER)`
							}
							_, err := db.Exec(ddl)
							require.NoError(t, err)
							changed = recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_source_state", "_sync_attachment_state")
							code := "synthetic_transient"
							if status == http.StatusTooManyRequests {
								code = "snapshot_build_capacity"
								if phase == "page" {
									code = "snapshot_chunk_capacity"
								}
							}
							return errorJSONResponse(status, oversync.ErrorResponse{Error: code}), nil
						}
						return delegate.RoundTrip(r)
					})
					_, err := c.Rebuild(context.Background())
					require.Error(t, err)
					require.Equal(t, 1, creates)
					if phase == "page" {
						require.Equal(t, 1, pages)
					} else {
						require.Zero(t, pages)
					}
					require.NotEmpty(t, changed)
					require.Equal(t, changed, recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_source_state", "_sync_attachment_state"))
					requireLastBundleSeqSeen(t, c, context.Background(), 1)
				})
			}
		}
	}
}
