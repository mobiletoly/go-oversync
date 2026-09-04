// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSync_DrainsBoundedBacklogInOnePublicCall(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	client.config.UploadLimit = 2
	for i := 0; i < 5; i++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, fmt.Sprintf("user-%d", i), "Local", "local@example.com")
		require.NoError(t, err)
	}
	server := newMockPushServer(t)
	pulls := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" {
			return server.RoundTrip(r)
		}
		require.Equal(t, "/sync/pull", r.URL.Path)
		pulls++
		require.Equal(t, "3", r.URL.Query().Get("after_bundle_seq"))
		return jsonResponse(oversync.PullResponse{StableBundleSeq: 3}), nil
	})}

	report, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
	require.Equal(t, RemoteSyncOutcomeAlreadyAtTarget, report.RemoteOutcome)
	require.Len(t, server.pushRequests, 3)
	require.Equal(t, []int{2, 2, 1}, []int{
		len(expandCapturedPushRequest(t, client, server.pushRequests[0])),
		len(expandCapturedPushRequest(t, client, server.pushRequests[1])),
		len(expandCapturedPushRequest(t, client, server.pushRequests[2])),
	})
	require.Equal(t, 1, pulls)
	dirty, err := client.pendingChangeCount(context.Background())
	require.NoError(t, err)
	require.Zero(t, dirty)
}

func TestSync_FinalWorkSnapshotDrainsWriteAtReadBoundary(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" {
			return server.RoundTrip(r)
		}
		require.Equal(t, "/sync/pull", r.URL.Path)
		return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
	})}
	readCount := 0
	client.beforeFinalWorkReadHook = func() {
		readCount++
		if readCount == 1 {
			_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('boundary', 'Late', 'late@example.com')`)
			require.NoError(t, err)
		}
	}

	report, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
	require.GreaterOrEqual(t, readCount, 2)
	require.Len(t, server.pushRequests, 1)
	dirty, outbound, err := client.pendingPushWorkCounts(context.Background())
	require.NoError(t, err)
	require.Zero(t, dirty)
	require.Zero(t, outbound)
}

func TestSync_StopsBeforePushAttempt65(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	client.config.UploadLimit = 1
	client.config.MaxSyncWorkUnitsPerInvocation = 100_000_000 // isolate the existing 64 push-attempt allowance
	for i := 0; i < 65; i++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, fmt.Sprintf("user-%03d", i), "Local", "local@example.com")
		require.NoError(t, err)
	}
	server := newMockPushServer(t)
	pulls := 0
	ambiguousFirstReply := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" {
			if ambiguousFirstReply {
				ambiguousFirstReply = false
				response, err := server.RoundTrip(r)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				return nil, io.ErrUnexpectedEOF
			}
			return server.RoundTrip(r)
		}
		pulls++
		return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
	})}

	_, err := client.Sync(context.Background())
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompletePushAttemptLimit, incomplete.Reason)
	require.Equal(t, 64, incomplete.PushAttempts)
	require.Equal(t, 64, incomplete.ConfirmedBundleCount)
	require.Equal(t, 1, incomplete.DirtyCount)
	require.Len(t, server.pushRequests, 65)
	require.Equal(t, server.pushRequests[0].SourceBundleID, server.pushRequests[1].SourceBundleID)
	require.Zero(t, pulls)

	report, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
	require.Len(t, server.pushRequests, 66)
	require.Equal(t, 1, pulls)
}

func TestSync_Attempt64MayFinishAndPull(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	client.config.UploadLimit = 1
	client.config.MaxSyncWorkUnitsPerInvocation = 100_000_000 // isolate the existing 64 push-attempt allowance
	for i := 0; i < 64; i++ {
		_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`,
			fmt.Sprintf("user-%03d", i), "Local", "local@example.com")
		require.NoError(t, err)
	}
	server := newMockPushServer(t)
	pulls := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" {
			return server.RoundTrip(r)
		}
		pulls++
		return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
	})}

	report, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
	require.Len(t, server.pushRequests, 64)
	require.Equal(t, 1, pulls)
	dirty, err := client.pendingChangeCount(context.Background())
	require.NoError(t, err)
	require.Zero(t, dirty)
}

func TestSync_RecoveryDrainsBatchesAndLateSnapshotGateWrite(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	client.config.UploadLimit = 1
	for i := 0; i < 3; i++ {
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			fmt.Sprintf("recovery-%d", i), "Local", "local@example.com")
		require.NoError(t, err)
	}
	_, err := db.Exec(`UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1`)
	require.NoError(t, err)
	originalSourceID := client.sourceID
	server := newMockPushServer(t)
	snapshots := 0
	var sessionRows []oversync.SnapshotRow
	var sessionSeq int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/push":
			return server.RoundTrip(r)
		case r.URL.Path == "/sync/snapshot-sessions" && r.Method == http.MethodPost:
			snapshots++
			sessionRows = nil
			for seq := int64(1); seq < server.nextBundleSeq; seq++ {
				for _, row := range server.committedBySeq[seq].Rows {
					sessionRows = append(sessionRows, oversync.SnapshotRow{
						Schema: row.Schema, Table: row.Table, Key: row.Key,
						RowVersion: row.RowVersion, Payload: row.Payload,
					})
				}
			}
			sessionSeq = server.nextBundleSeq - 1
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID: fmt.Sprintf("snapshot-%d", snapshots), SnapshotBundleSeq: sessionSeq,
				RowCount: int64(len(sessionRows)), ByteCount: testSnapshotRowsByteCount(sessionRows),
				ExpiresAt: "2030-01-01T00:00:00Z",
			}), nil
		case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodGet:
			if snapshots == 1 {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('recovery-late', 'Late', 'late@example.com')`)
				require.NoError(t, err)
			}
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: fmt.Sprintf("snapshot-%d", snapshots), SnapshotBundleSeq: sessionSeq,
				Rows: sessionRows, NextRowOrdinal: int64(len(sessionRows)), HasMore: false,
			}), nil
		case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodDelete:
			return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	report, err := client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
	require.Len(t, server.committedBySource, 4)
	require.Equal(t, 2, snapshots)
	require.Equal(t, originalSourceID, client.sourceID)
	dirty, err := client.pendingChangeCount(ctx)
	require.NoError(t, err)
	require.Zero(t, dirty)
	name, _, exists := loadUserForKey(t, db, "recovery-late")
	require.True(t, exists)
	require.Equal(t, "Late", name)
}

func TestSync_DrainsWritesAfterPullDiscoversRecovery(t *testing.T) {
	for _, reason := range []string{"history_pruned", "checkpoint_ahead"} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
			`)
			client.config.UploadLimit = 1
			server := newMockPushServer(t)
			client.HTTP = &http.Client{Transport: roundTripFunc(server.RoundTrip)}
			_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('seed','Remote seed','seed@example.com')`)
			require.NoError(t, err)
			_, err = client.PushPending(ctx)
			require.NoError(t, err)
			initialRecovery, err := client.rebuildRequired(ctx)
			require.NoError(t, err)
			require.False(t, initialRecovery, "recovery must first be discovered by pull")
			if reason == "checkpoint_ahead" {
				_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen = 99 WHERE singleton_key = 1`)
				require.NoError(t, err)
			}
			originalSourceID := client.sourceID
			pushesBefore := len(server.pushRequests)
			pulls := 0
			snapshots := 0
			lateWriteRan := false
			var sessionRows []oversync.SnapshotRow
			var sessionSeq int64
			client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch {
				case r.URL.Path == "/sync/push":
					return server.RoundTrip(r)
				case r.URL.Path == "/sync/pull":
					pulls++
					if !lateWriteRan {
						lateWriteRan = true
						_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES
							('late-a','Local A','a@example.com'),('late-b','Local B','b@example.com')`)
						require.NoError(t, err)
					}
					return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
						Error: reason, Message: "recovery required",
					}), nil
				case r.URL.Path == "/sync/snapshot-sessions" && r.Method == http.MethodPost:
					snapshots++
					sessionRows = nil
					for seq := int64(1); seq < server.nextBundleSeq; seq++ {
						for _, row := range server.committedBySeq[seq].Rows {
							sessionRows = append(sessionRows, oversync.SnapshotRow{
								Schema: row.Schema, Table: row.Table, Key: row.Key,
								RowVersion: row.RowVersion, Payload: row.Payload,
							})
						}
					}
					sessionSeq = server.nextBundleSeq - 1
					return jsonResponse(oversync.SnapshotSession{
						SnapshotID: fmt.Sprintf("snapshot-%d", snapshots), SnapshotBundleSeq: sessionSeq,
						RowCount: int64(len(sessionRows)), ByteCount: testSnapshotRowsByteCount(sessionRows),
						ExpiresAt: "2030-01-01T00:00:00Z",
					}), nil
				case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodGet:
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID: fmt.Sprintf("snapshot-%d", snapshots), SnapshotBundleSeq: sessionSeq,
						Rows: sessionRows, NextRowOrdinal: int64(len(sessionRows)), HasMore: false,
					}), nil
				case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodDelete:
					return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
				}
				return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			})}

			report, err := client.Sync(ctx)
			require.NoError(t, err)
			require.True(t, lateWriteRan)
			require.Equal(t, PushOutcomeCommitted, report.PushOutcome)
			require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
			require.Equal(t, 2, len(server.pushRequests)-pushesBefore)
			require.Equal(t, 1, pulls)
			require.Equal(t, 1, snapshots)
			require.Equal(t, originalSourceID, client.sourceID)
			dirty, err := client.pendingChangeCount(ctx)
			require.NoError(t, err)
			require.Zero(t, dirty)
			outbound, err := client.pendingPushOutboundCount(ctx)
			require.NoError(t, err)
			require.Zero(t, outbound)
			require.Equal(t, outboxStateNone, requireOutboxBundle(t, db).State)
			var nextSourceBundleID int64
			require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state`).Scan(&nextSourceBundleID))
			require.Equal(t, int64(4), nextSourceBundleID)
			checkpoint, err := client.LastBundleSeqSeen(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(3), checkpoint)
			rebuildRequired, err := client.rebuildRequired(ctx)
			require.NoError(t, err)
			require.False(t, rebuildRequired)
			for id, expected := range map[string]string{"seed": "Remote seed", "late-a": "Local A", "late-b": "Local B"} {
				name, _, exists := loadUserForKey(t, db, id)
				require.True(t, exists)
				require.Equal(t, expected, name)
			}
		})
	}
}

func TestSync_ConflictReentryAtAttempt65IsRefused(t *testing.T) {
	client, db := newUsersBundleClient(t)
	client.config.UploadLimit = 1
	client.config.MaxSyncWorkUnitsPerInvocation = 100_000_000 // all planner pages remain charged
	client.Resolver = &ClientWinsResolver{}
	conflictID := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	seedSyncedUserRow(t, client, db, conflictID, "Original", 1)
	for i := 1; i <= 63; i++ {
		_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`,
			fmt.Sprintf("00000000-0000-4000-8000-%012d", i), "Backlog", "backlog@example.com")
		require.NoError(t, err)
	}
	updateConflictUserName(t, db, conflictID, "Local") // conflict is dirty after the 63 independent rows
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		if session.Rows[0].Key["id"] == conflictID {
			return structuredConflictResponse(session.Rows[0], 2, false,
				serverConflictUserRow(conflictID, "Server", "server@example.com"))
		}
		return server.defaultPushResponse(session)
	}
	pulls := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" {
			return server.RoundTrip(r)
		}
		pulls++
		return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
	})}

	_, err := client.Sync(context.Background())
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompletePushAttemptLimit, incomplete.Reason)
	require.Equal(t, 64, incomplete.PushAttempts)
	require.Equal(t, 63, incomplete.ConfirmedBundleCount)
	require.Len(t, server.pushRequests, 64)
	require.Zero(t, pulls)
	require.Equal(t, 1, incomplete.DirtyCount)
	require.Equal(t, 0, incomplete.OutboundCount)
}
