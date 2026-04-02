package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestResultContract_OpenStates(t *testing.T) {
	t.Run("ready anonymous", func(t *testing.T) {
		client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})
		openResult := mustOpen(t, client, context.Background())
		require.Equal(t, OpenStateReadyAnonymous, openResult.State)
		require.Empty(t, openResult.AttachedUserID)
		require.Empty(t, openResult.TargetUserID)
	})

	t.Run("ready attached after restart", func(t *testing.T) {
		ctx := context.Background()
		client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})
		_, err := client.Attach(ctx, "result-user")
		require.NoError(t, err)
		require.NoError(t, client.Close())

		restarted, err := NewClient(db, "http://example.invalid", tokenProviderForTests, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, restarted.Close()) })

		openResult := mustOpen(t, restarted, ctx)
		require.Equal(t, OpenStateReadyAttached, openResult.State)
		require.Equal(t, "result-user", openResult.AttachedUserID)
		require.Empty(t, openResult.TargetUserID)
	})

	t.Run("attach recovery required", func(t *testing.T) {
		client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})
		setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "result-user"})

		openResult := mustOpen(t, client, context.Background())
		require.Equal(t, OpenStateAttachRecoveryRequired, openResult.State)
		require.Equal(t, "result-user", openResult.TargetUserID)
	})
}

func TestResultContract_AttachAndSyncStatus(t *testing.T) {
	t.Run("retry later result shape", func(t *testing.T) {
		client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{
			Resolution:    "retry_later",
			RetryAfterSec: 7,
		})

		result, err := client.Attach(context.Background(), "result-user")
		require.NoError(t, err)
		require.Equal(t, AttachStatusRetryLater, result.Status)
		require.Equal(t, 7*time.Second, result.RetryAfter)
		require.Nil(t, result.Restore)
	})

	t.Run("restore result shape", func(t *testing.T) {
		client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/sync/capabilities":
				return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"connect_lifecycle": true}}), nil
			case "/sync/connect":
				return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
			case "/sync/snapshot-sessions":
				return jsonResponse(&oversync.SnapshotSession{
					SnapshotID:        "snapshot-result-contract",
					SnapshotBundleSeq: 9,
					RowCount:          1,
					ExpiresAt:         time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
				}), nil
			case "/sync/snapshot-sessions/snapshot-result-contract":
				switch r.Method {
				case http.MethodGet:
					return jsonResponse(&oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-result-contract",
						SnapshotBundleSeq: 9,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        oversync.SyncKey{"id": "remote-1"},
							RowVersion: 9,
							Payload:    mustJSONPayload(t, map[string]any{"id": "remote-1", "name": "Remote", "email": "remote@example.com"}),
						}},
						NextRowOrdinal: 1,
						HasMore:        false,
					}), nil
				case http.MethodDelete:
					return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
				}
			}
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found", Message: "unexpected path"}), nil
		})

		result, err := client.Attach(context.Background(), "result-user")
		require.NoError(t, err)
		require.Equal(t, AttachStatusConnected, result.Status)
		require.Equal(t, AttachOutcomeUsedRemote, result.Outcome)
		require.NotNil(t, result.Restore)
		require.Equal(t, int64(9), result.Restore.BundleSeq)
		require.Equal(t, int64(1), result.Restore.RowCount)
		require.Equal(t, AuthorityStatusAuthoritativeMaterialized, result.SyncStatus.Authority)

		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'remote-1'`).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("sync status preconditions and connected state", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = db.Close() })
		_, err = db.Exec(lifecycleUsersDDL)
		require.NoError(t, err)

		client, err := NewClient(db, "http://example.invalid", tokenProviderForTests, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })

		_, err = client.SyncStatus(context.Background())
		var openErr *OpenRequiredError
		require.ErrorAs(t, err, &openErr)

		mustOpen(t, client, context.Background())
		_, err = client.SyncStatus(context.Background())
		var attachErr *AttachRequiredError
		require.ErrorAs(t, err, &attachErr)

		attachTestClient(t, client, "result-user", "device-a")
		_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
		require.NoError(t, err)

		status, err := client.SyncStatus(context.Background())
		require.NoError(t, err)
		require.Equal(t, AuthorityStatusAuthoritativeEmpty, status.Authority)
		require.True(t, status.Pending.HasPendingSyncData)
		require.EqualValues(t, 1, status.Pending.PendingRowCount)
		require.True(t, status.Pending.BlocksDetach)
		require.EqualValues(t, 0, status.LastBundleSeqSeen)
	})
}

func TestResultContract_PushOutcomes(t *testing.T) {
	ctx := context.Background()

	t.Run("no change", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		report := mustPushPending(t, client, ctx)
		require.Equal(t, PushOutcomeNoChange, report.Outcome)
		require.False(t, report.Status.Pending.HasPendingSyncData)
	})

	t.Run("skipped paused", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.PauseUploads()
		report := mustPushPending(t, client, ctx)
		require.Equal(t, PushOutcomeSkippedPaused, report.Outcome)
	})

	t.Run("committed", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
		require.NoError(t, err)
		client.HTTP = &http.Client{Transport: newMockPushSessionServer(t)}

		report := mustPushPending(t, client, ctx)
		require.Equal(t, PushOutcomeCommitted, report.Outcome)
		require.False(t, report.Status.Pending.HasPendingSyncData)
	})
}

func TestResultContract_RemoteSyncAndRotationOutcomes(t *testing.T) {
	ctx := context.Background()

	t.Run("pull already at target", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "/sync/pull", r.URL.Path)
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		})}

		report := mustPullToStable(t, client, ctx)
		require.Equal(t, RemoteSyncOutcomeAlreadyAtTarget, report.Outcome)
	})

	t.Run("pull applied incremental", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "/sync/pull", r.URL.Path)
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 1,
				HasMore:         false,
				Bundles: []oversync.Bundle{{
					BundleSeq:      1,
					SourceID:       "peer-a",
					SourceBundleID: 11,
					Rows: []oversync.BundleRow{{
						Schema:     "main",
						Table:      "users",
						Key:        oversync.SyncKey{"id": "user-1"},
						Op:         oversync.OpInsert,
						RowVersion: 1,
						Payload:    mustJSONPayload(t, map[string]any{"id": "user-1", "name": "Ada", "email": "ada@example.com"}),
					}},
				}},
			}), nil
		})}

		report := mustPullToStable(t, client, ctx)
		require.Equal(t, RemoteSyncOutcomeAppliedIncremental, report.Outcome)
		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("pull skipped paused", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.PauseDownloads()
		report := mustPullToStable(t, client, ctx)
		require.Equal(t, RemoteSyncOutcomeSkippedPaused, report.Outcome)
	})

	t.Run("rebuild snapshot keeps managed source outside source recovery", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/sync/snapshot-sessions":
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "snapshot-result-contract",
					SnapshotBundleSeq: 5,
					RowCount:          1,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			case "/sync/snapshot-sessions/snapshot-result-contract":
				switch r.Method {
				case http.MethodGet:
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-result-contract",
						SnapshotBundleSeq: 5,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        oversync.SyncKey{"id": "user-1"},
							RowVersion: 5,
							Payload:    mustJSONPayload(t, map[string]any{"id": "user-1", "name": "Ada", "email": "ada@example.com"}),
						}},
						NextRowOrdinal: 1,
						HasMore:        false,
					}), nil
				case http.MethodDelete:
					return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
				}
			}
			return nil, fmt.Errorf("unexpected path: %s", r.URL.Path)
		})}

		report := mustRebuild(t, client, ctx)
		require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
		require.NotNil(t, report.Restore)
		require.Equal(t, "bundle-device", client.sourceID)

		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
		require.Equal(t, 1, count)
	})
}

func TestResultContract_SourceInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("diagnostic surface exposes opaque source and recovery flags", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)

		info, err := client.SourceInfo(ctx)
		require.NoError(t, err)
		require.Equal(t, "bundle-device", info.CurrentSourceID)
		require.False(t, info.RebuildRequired)
		require.False(t, info.SourceRecoveryRequired)
		require.Equal(t, SourceRecoveryCode(""), info.SourceRecoveryReason)

		_, err = db.Exec(`UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1`)
		require.NoError(t, err)
		require.NoError(t, persistOperationState(ctx, db, &operationStateRecord{
			Kind:   operationKindSourceRecovery,
			Reason: string(SourceRecoverySequenceChanged),
		}))

		info, err = client.SourceInfo(ctx)
		require.NoError(t, err)
		require.Equal(t, "bundle-device", info.CurrentSourceID)
		require.True(t, info.RebuildRequired)
		require.True(t, info.SourceRecoveryRequired)
		require.Equal(t, SourceRecoverySequenceChanged, info.SourceRecoveryReason)
	})
}

func TestResultContract_SyncAndSyncThenDetach(t *testing.T) {
	ctx := context.Background()

	t.Run("sync reports paused directions", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.PauseUploads()
		client.PauseDownloads()

		report, err := client.Sync(ctx)
		require.NoError(t, err)
		require.Equal(t, PushOutcomeSkippedPaused, report.PushOutcome)
		require.Equal(t, RemoteSyncOutcomeSkippedPaused, report.RemoteOutcome)
	})

	t.Run("sync then detach blocked result", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
		require.NoError(t, err)
		client.PauseUploads()
		client.PauseDownloads()

		result := mustSyncThenDetach(t, client, ctx)
		require.False(t, result.IsSuccess())
		require.Equal(t, DetachOutcomeBlockedUnsyncedData, result.Detach.Outcome)
		require.Greater(t, result.RemainingPendingRowCount, int64(0))
	})

	t.Run("sync then detach success", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, lifecycleUsersDDL)
		client.PauseDownloads()

		result := mustSyncThenDetach(t, client, ctx)
		require.True(t, result.IsSuccess())
		require.Equal(t, DetachOutcomeDetached, result.Detach.Outcome)
	})
}
