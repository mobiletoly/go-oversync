package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func mustBundleKey(id string) oversync.SyncKey {
	return oversync.SyncKey{"id": id}
}

func mustBundlePayload(t *testing.T, id, name, email string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":    id,
		"name":  name,
		"email": email,
	})
	require.NoError(t, err)
	return raw
}

func snapshotStageCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count))
	return count
}

func readBundleClientState(t *testing.T, db *sql.DB, userID string) (string, int64, int64) {
	t.Helper()
	return requireBundleState(t, db)
}

func TestPullToStable_AppliesMultipleBundlesToFrozenCeiling(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/sync/pull", r.URL.Path)
		requests++

		switch requests {
		case 1:
			require.Equal(t, "0", r.URL.Query().Get("after_bundle_seq"))
			require.Equal(t, "", r.URL.Query().Get("target_bundle_seq"))
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 3,
				HasMore:         true,
				Bundles: []oversync.Bundle{
					{
						BundleSeq:      1,
						SourceID:       "peer-a",
						SourceBundleID: 11,
						Rows: []oversync.BundleRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							Op:         oversync.OpInsert,
							RowVersion: 1,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
					},
					{
						BundleSeq:      2,
						SourceID:       "peer-a",
						SourceBundleID: 12,
						Rows: []oversync.BundleRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							Op:         oversync.OpUpdate,
							RowVersion: 2,
							Payload:    mustBundlePayload(t, "user-1", "Ada Lovelace", "ada@example.com"),
						}},
					},
				},
			}), nil
		case 2:
			require.Equal(t, "2", r.URL.Query().Get("after_bundle_seq"))
			require.Equal(t, "3", r.URL.Query().Get("target_bundle_seq"))
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 3,
				HasMore:         false,
				Bundles: []oversync.Bundle{{
					BundleSeq:      3,
					SourceID:       "peer-b",
					SourceBundleID: 21,
					Rows: []oversync.BundleRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-2"),
						Op:         oversync.OpInsert,
						RowVersion: 3,
						Payload:    mustBundlePayload(t, "user-2", "Grace", "grace@example.com"),
					}},
				}},
			}), nil
		default:
			return nil, io.EOF
		}
	})}

	mustPullToStable(t, client, ctx)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "user-1").Scan(&name))
	require.Equal(t, "Ada Lovelace", name)
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "user-2").Scan(&name))
	require.Equal(t, "Grace", name)

	var lastBundleSeq int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&lastBundleSeq))
	require.Equal(t, int64(3), lastBundleSeq)
}

func TestPullToStable_RetriesTransientFailureAndSucceeds(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/sync/pull", r.URL.Path)
		requests++
		if requests == 1 {
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
				Error:   "temporarily_unavailable",
				Message: "retry me",
			}), nil
		}
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
					Key:        mustBundleKey("user-1"),
					Op:         oversync.OpInsert,
					RowVersion: 1,
					Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
				}},
			}},
		}), nil
	})}

	mustPullToStable(t, client, ctx)
	require.Equal(t, 2, requests)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "user-1").Scan(&name))
	require.Equal(t, "Ada", name)
}

func TestPullToStable_RetryExhaustionReturnsTypedError(t *testing.T) {
	ctx := context.Background()
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/sync/pull", r.URL.Path)
		requests++
		return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
			Error:   "temporarily_unavailable",
			Message: "retry me",
		}), nil
	})}

	_, err := client.PullToStable(ctx)
	var retryErr *RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, "pull_request", retryErr.Operation)
	require.Equal(t, 3, retryErr.Attempts)
	require.Equal(t, 3, requests)
}

func TestPullToStable_AppliesTextSyncKeyBundleRows(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "docs", SyncKeyColumnName: "doc_id"}}, `
		CREATE TABLE docs (
			doc_id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
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
					Table:      "docs",
					Key:        oversync.SyncKey{"doc_id": "doc-1"},
					Op:         oversync.OpInsert,
					RowVersion: 1,
					Payload:    []byte(`{"doc_id":"doc-1","body":"Hello text key"}`),
				}},
			}},
		}), nil
	})}

	mustPullToStable(t, client, ctx)

	var body string
	require.NoError(t, db.QueryRow(`SELECT body FROM docs WHERE doc_id = ?`, "doc-1").Scan(&body))
	require.Equal(t, "Hello text key", body)

	var rowVersion int64
	var deleted int
	require.NoError(t, db.QueryRow(`
		SELECT row_version, deleted
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'docs' AND key_json = ?
	`, `{"doc_id":"doc-1"}`).Scan(&rowVersion, &deleted))
	require.Equal(t, int64(1), rowVersion)
	require.Equal(t, 0, deleted)
}

func TestPullToStable_RejectsDirtyRows(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "user-1", "Ada", "ada@example.com")
	require.NoError(t, err)

	_, err = client.PullToStable(ctx)
	var dirtyErr *DirtyStateRejectedError
	require.ErrorAs(t, err, &dirtyErr)
	require.Equal(t, 1, dirtyErr.DirtyCount)
}

func TestPullAndRebuild_RejectWhilePushOutboundReplayIsPending(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		op   func(*Client, context.Context) error
	}{
		{
			name: "pull rejects pending outbound replay",
			op: func(client *Client, ctx context.Context) error {
				_, err := client.PullToStable(ctx)
				return err
			},
		},
		{
			name: "hydrate rejects pending outbound replay",
			op: func(client *Client, ctx context.Context) error {
				_, err := client.Rebuild(ctx, RebuildKeepSource, "")
				return err
			},
		},
		{
			name: "recover rejects pending outbound replay",
			op: func(client *Client, ctx context.Context) error {
				_, err := client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			_, err := db.Exec(`
				UPDATE _sync_outbox_bundle
				SET state = 'prepared', source_id = ?, source_bundle_id = 1, row_count = 1, canonical_request_hash = 'hash'
				WHERE singleton_key = 1
			`, client.SourceID)
			require.NoError(t, err)
			_, err = db.Exec(`
				INSERT INTO _sync_outbox_rows (
					source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload
				) VALUES (1, 0, 'main', 'users', ?, ?, 'INSERT', 0, ?, ?)
			`, rowKeyJSON("user-1"), rowKeyJSON("user-1"), `{"id":"user-1","name":"Ada","email":"ada@example.com"}`, `{"id":"user-1","name":"Ada","email":"ada@example.com"}`)
			require.NoError(t, err)

			requests := 0
			client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				return nil, io.EOF
			})}

			err = tc.op(client, ctx)
			var replayErr *PendingPushReplayError
			require.ErrorAs(t, err, &replayErr)
			require.Equal(t, 1, replayErr.OutboundCount)
			require.Equal(t, 0, requests)
		})
	}
}

func TestApplyStagedSnapshot_FailsClosedWhenAttachedStateIsMissing(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "local-1", "Local", "local@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-missing-state', 1, 'main', 'users', ?, 7, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		UPDATE _sync_attachment_state
		SET binding_state = 'anonymous', attached_user_id = '', pending_initialization_id = ''
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	err = client.applyStagedSnapshotLocked(ctx, &oversync.SnapshotSession{
		SnapshotID:        "snapshot-missing-state",
		SnapshotBundleSeq: 7,
		RowCount:          1,
	}, false, "")
	var connectErr *AttachRequiredError
	require.ErrorAs(t, err, &connectErr)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "local-1").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "remote-1").Scan(&count))
	require.Equal(t, 0, count)
}

func TestPullToStable_FailedBundleApplyLeavesCheckpointUnchanged(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(oversync.PullResponse{
			StableBundleSeq: 1,
			HasMore:         false,
			Bundles: []oversync.Bundle{{
				BundleSeq:      1,
				SourceID:       "peer-a",
				SourceBundleID: 9,
				Rows: []oversync.BundleRow{
					{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						Op:         oversync.OpInsert,
						RowVersion: 1,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					},
					{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-2"),
						Op:         oversync.OpInsert,
						RowVersion: 1,
						Payload:    []byte(`{"id":"user-2","name":"Broken"}`),
					},
				},
			}},
		}), nil
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 0, count)

	var lastBundleSeq int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&lastBundleSeq))
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestPullToStable_HistoryPrunedFallsBackToSnapshot(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch r.URL.Path {
		case "/sync/pull":
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "history_pruned",
				Message: "after_bundle_seq 0 is below retained floor 5",
			}), nil
		case "/sync/snapshot-sessions":
			if r.Method == http.MethodPost {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "snapshot-pruned",
					SnapshotBundleSeq: 9,
					RowCount:          1,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
		case "/sync/snapshot-sessions/snapshot-pruned":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-pruned",
					SnapshotBundleSeq: 9,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 9,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		default:
			return nil, io.EOF
		}
		return nil, io.EOF
	})}

	mustPullToStable(t, client, ctx)
	require.GreaterOrEqual(t, requests, 2)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "user-1").Scan(&name))
	require.Equal(t, "Ada", name)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(9), lastBundleSeq)
}

func TestPullToStable_FailureBetweenBundlesKeepsEarlierCheckpoint(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(oversync.PullResponse{
			StableBundleSeq: 2,
			HasMore:         false,
			Bundles: []oversync.Bundle{
				{
					BundleSeq:      1,
					SourceID:       "peer-a",
					SourceBundleID: 11,
					Rows: []oversync.BundleRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						Op:         oversync.OpInsert,
						RowVersion: 1,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
				},
				{
					BundleSeq:      2,
					SourceID:       "peer-b",
					SourceBundleID: 12,
					Rows: []oversync.BundleRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-2"),
						Op:         oversync.OpInsert,
						RowVersion: 2,
						Payload:    []byte(`{"id":"user-2","name":"Broken"}`),
					}},
				},
			},
		}), nil
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-2'`).Scan(&count))
	require.Equal(t, 0, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), lastBundleSeq)
}

func TestPullToStable_RejectsChangingStableBundleSeq(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 2,
				HasMore:         true,
				Bundles: []oversync.Bundle{{
					BundleSeq:      1,
					SourceID:       "peer-a",
					SourceBundleID: 11,
					Rows: []oversync.BundleRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						Op:         oversync.OpInsert,
						RowVersion: 1,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
				}},
			}), nil
		case 2:
			return jsonResponse(oversync.PullResponse{
				StableBundleSeq: 3,
				HasMore:         false,
				Bundles:         nil,
			}), nil
		default:
			return nil, io.EOF
		}
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stable bundle seq changed")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), lastBundleSeq)
}

func TestPullToStable_RejectsMalformedPullResponse(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"stable_bundle_seq":1,"bundles":[`))),
		}, nil
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode pull response")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 0, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestPullToStable_RejectsPullResponseMissingStableBundleSeq(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(map[string]any{
			"has_more": false,
			"bundles": []map[string]any{{
				"bundle_seq":       1,
				"source_id":        "peer-a",
				"source_bundle_id": 11,
				"rows": []map[string]any{{
					"schema":      "main",
					"table":       "users",
					"key":         map[string]any{"id": "user-1"},
					"op":          oversync.OpInsert,
					"row_version": 1,
					"payload": map[string]any{
						"id":    "user-1",
						"name":  "Ada",
						"email": "ada@example.com",
					},
				}},
			}},
		}), nil
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing stable_bundle_seq")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 0, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestPullToStable_RejectsIncompletePullBeforeFrozenCeiling(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(oversync.PullResponse{
			StableBundleSeq: 2,
			HasMore:         false,
			Bundles: []oversync.Bundle{{
				BundleSeq:      1,
				SourceID:       "peer-a",
				SourceBundleID: 11,
				Rows: []oversync.BundleRow{{
					Schema:     "main",
					Table:      "users",
					Key:        mustBundleKey("user-1"),
					Op:         oversync.OpInsert,
					RowVersion: 1,
					Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
				}},
			}},
		}), nil
	})}

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pull ended early")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), lastBundleSeq)
}

func TestRebuildKeepSource_RebuildsFromSnapshotWithDeferredFKs(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id)
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-hydrate",
				SnapshotBundleSeq: 7,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-hydrate":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-hydrate",
					SnapshotBundleSeq: 7,
					Rows: []oversync.SnapshotRow{
						{
							Schema:     "main",
							Table:      "posts",
							Key:        mustBundleKey("post-1"),
							RowVersion: 7,
							Payload:    []byte(`{"id":"post-1","title":"Hello","author_id":"user-1"}`),
						},
						{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 6,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						},
					},
					NextRowOrdinal: 2,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM posts WHERE id = 'post-1'`).Scan(&count))
	require.Equal(t, 1, count)

	var lastBundleSeq int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&lastBundleSeq))
	require.Equal(t, int64(7), lastBundleSeq)
}

func TestRebuildKeepSource_RejectsMalformedSnapshotResponse(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-malformed",
				SnapshotBundleSeq: 7,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-malformed":
			switch r.Method {
			case http.MethodGet:
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"snapshot_bundle_seq":`))),
				}, nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode snapshot chunk response")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestRebuildKeepSource_RejectsSnapshotResponseMissingBundleSeq(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(map[string]any{
				"snapshot_id": "snapshot-missing-bundle-seq",
				"row_count":   1,
				"expires_at":  "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-missing-bundle-seq":
			switch r.Method {
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_bundle_seq")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestRebuildRotateSource_RotatesSourceIDAndResetsBundleState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)

	originalSourceID := client.SourceID
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "stale", "Stale", "stale@example.com")
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, client.updateStructuredRowStateInTx(ctx, tx, "main", "users", rowKeyJSON("stale"), 4, false))
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`
		INSERT INTO _sync_push_stage (
			bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		) VALUES (99, 0, 'main', 'users', ?, 'UPDATE', 4, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Stage","email":"stage@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-stale', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Snapshot","email":"snapshot@example.com"}`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)
	_, err = db.Exec(`INSERT INTO legacy_docs (id, body) VALUES (?, ?)`, "legacy-1", "stale body")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_managed_tables (schema_name, table_name)
		VALUES ('main', 'legacy_docs')
		ON CONFLICT(schema_name, table_name) DO NOTHING
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_row_state (schema_name, table_name, key_json, row_version, deleted)
		VALUES ('main', 'legacy_docs', ?, 9, 0)
	`, rowKeyJSON("legacy-1"))
	require.NoError(t, err)

	client.config.SnapshotChunkRows = 1
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			var rebuildRequired int
			require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
			require.Equal(t, 1, rebuildRequired)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-recover",
				SnapshotBundleSeq: 9,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-recover":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-recover",
						SnapshotBundleSeq: 9,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 8,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-recover",
						SnapshotBundleSeq: 9,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-2"),
							RowVersion: 9,
							Payload:    mustBundlePayload(t, "user-2", "Grace", "grace@example.com"),
						}},
						NextRowOrdinal: 2,
						HasMore:        false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildRotateSource, newTestSourceID())
	require.NotEqual(t, originalSourceID, client.SourceID)

	sourceID, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	var rebuildRequired int
	require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&rebuildRequired))
	require.Equal(t, client.SourceID, sourceID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(9), lastBundleSeq)
	require.Equal(t, 0, rebuildRequired)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-2'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`
		SELECT COUNT(*)
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'users' AND key_json = ?
	`, rowKeyJSON("stale")).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'legacy_docs'`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestRebuildRequired_NormalSyncRejectsUntilRebuildClearsIt(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name           string
		rebuild        func(*Client, context.Context) error
		expectRotation bool
	}{
		{
			name: "rebuild keep_source clears rebuild_required",
			rebuild: func(client *Client, ctx context.Context) error {
				_, err := client.Rebuild(ctx, RebuildKeepSource, "")
				return err
			},
		},
		{
			name: "rebuild rotate_source clears rebuild_required",
			rebuild: func(client *Client, ctx context.Context) error {
				_, err := client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
				return err
			},
			expectRotation: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)
			originalSourceID := client.SourceID

			_, err := db.Exec(`UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID)
			require.NoError(t, err)

			_, err = client.Sync(ctx)
			var rebuildErr *RebuildRequiredError
			require.ErrorAs(t, err, &rebuildErr)

			snapshotRequests := 0
			pullRequests := 0
			client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/sync/snapshot-sessions":
					require.Equal(t, http.MethodPost, r.Method)
					snapshotRequests++
					var rebuildRequired int
					require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
					require.Equal(t, 1, rebuildRequired)
					return jsonResponse(oversync.SnapshotSession{
						SnapshotID:        "snapshot-rebuild-required",
						SnapshotBundleSeq: 7,
						RowCount:          1,
						ExpiresAt:         "2030-01-01T00:00:00Z",
					}), nil
				case "/sync/snapshot-sessions/snapshot-rebuild-required":
					switch r.Method {
					case http.MethodGet:
						return jsonResponse(oversync.SnapshotChunkResponse{
							SnapshotID:        "snapshot-rebuild-required",
							SnapshotBundleSeq: 7,
							Rows: []oversync.SnapshotRow{{
								Schema:     "main",
								Table:      "users",
								Key:        mustBundleKey("user-1"),
								RowVersion: 7,
								Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
							}},
							NextRowOrdinal: 1,
							HasMore:        false,
						}), nil
					case http.MethodDelete:
						return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
					}
				case "/sync/pull":
					require.Equal(t, http.MethodGet, r.Method)
					pullRequests++
					require.Equal(t, "7", r.URL.Query().Get("after_bundle_seq"))
					return jsonResponse(oversync.PullResponse{
						StableBundleSeq: 7,
						HasMore:         false,
						Bundles:         nil,
					}), nil
				}
				return nil, io.EOF
			})}

			require.NoError(t, tc.rebuild(client, ctx))
			require.Equal(t, 1, snapshotRequests)

			var rebuildRequired int
			require.NoError(t, db.QueryRow(`SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1 AND ? IS NOT NULL`, client.UserID).Scan(&rebuildRequired))
			require.Equal(t, 0, rebuildRequired)

			if tc.expectRotation {
				require.NotEqual(t, originalSourceID, client.SourceID)
			} else {
				require.Equal(t, originalSourceID, client.SourceID)
			}

			_, err = client.Sync(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, pullRequests)

			name, email, exists := loadUserForKey(t, db, "user-1")
			require.True(t, exists)
			require.Equal(t, "Ada", name)
			require.Equal(t, "ada@example.com", email)
		})
	}
}

func TestRebuildKeepSource_MultiChunkDownloadStagesRowsBeforeFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	requests := 0
	client.config.SnapshotChunkRows = 1
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-multi",
				SnapshotBundleSeq: 8,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-multi":
			switch r.Method {
			case http.MethodGet:
				requests++
				switch requests {
				case 1:
					require.Equal(t, "0", r.URL.Query().Get("after_row_ordinal"))
					require.Equal(t, "1", r.URL.Query().Get("max_rows"))
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-multi",
						SnapshotBundleSeq: 8,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 7,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case 2:
					require.Equal(t, 1, snapshotStageCount(t, db))
					var count int
					require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
					require.Equal(t, 1, count)
					require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
					require.Equal(t, 0, count)
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-multi",
						SnapshotBundleSeq: 8,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-2"),
							RowVersion: 8,
							Payload:    mustBundlePayload(t, "user-2", "Grace", "grace@example.com"),
						}},
						NextRowOrdinal: 2,
						HasMore:        false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")
	require.Equal(t, 0, snapshotStageCount(t, db))

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id IN ('user-1', 'user-2')`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestRebuildKeepSource_OneChunkStillStagesBeforeFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	chunk := &oversync.SnapshotChunkResponse{
		SnapshotID:        "snapshot-stage-one",
		SnapshotBundleSeq: 5,
		Rows: []oversync.SnapshotRow{{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey("user-1"),
			RowVersion: 5,
			Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
		}},
		NextRowOrdinal: 1,
		HasMore:        false,
	}
	session := &oversync.SnapshotSession{
		SnapshotID:        "snapshot-stage-one",
		SnapshotBundleSeq: 5,
		RowCount:          1,
		ExpiresAt:         "2030-01-01T00:00:00Z",
	}

	require.NoError(t, client.stageSnapshotChunk(ctx, chunk, 0))
	require.Equal(t, 1, snapshotStageCount(t, db))
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	require.NoError(t, client.applyStagedSnapshotLocked(ctx, session, false, ""))
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestRebuildKeepSource_ClearsStaleSnapshotStageBeforeNewAttempt(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "snapshot-old", 1, "main", "users", rowKeyJSON("stale-user"), 1, string(mustBundlePayload(t, "stale-user", "Stale", "stale@example.com")))
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-fresh",
				SnapshotBundleSeq: 6,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-fresh":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-fresh",
					SnapshotBundleSeq: 6,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 6,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")
	require.Equal(t, 0, snapshotStageCount(t, db))
}

func TestRebuildKeepSource_PartialDownloadRestartClearsStageAndStartsFromZero(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.config.SnapshotChunkRows = 1
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	firstAttemptRequests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-restart-a",
				SnapshotBundleSeq: 7,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-restart-a":
			switch r.Method {
			case http.MethodGet:
				firstAttemptRequests++
				if firstAttemptRequests == 1 {
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-restart-a",
						SnapshotBundleSeq: 7,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 6,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				}
				return nil, io.EOF
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Equal(t, 1, snapshotStageCount(t, db))

	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-restart-b",
				SnapshotBundleSeq: 7,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-restart-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-restart-b",
					SnapshotBundleSeq: 7,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 7,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
}

func TestRebuildKeepSource_SnapshotSessionExpiredRetryRestoresData(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	client.config.SnapshotChunkRows = 1

	sessionCreates := 0
	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "snapshot-expired-a",
					SnapshotBundleSeq: 8,
					RowCount:          2,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-expired-b",
				SnapshotBundleSeq: 8,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-expired-a":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-expired-a",
						SnapshotBundleSeq: 8,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 7,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return errorJSONResponse(http.StatusGone, oversync.ErrorResponse{
						Error:   "snapshot_session_expired",
						Message: "snapshot session snapshot-expired-a has expired; start a new snapshot session",
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/snapshot-expired-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-expired-b",
					SnapshotBundleSeq: 8,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 8,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_session_expired")
	require.Equal(t, 1, snapshotStageCount(t, db))

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)

	mustRebuild(t, client, ctx, RebuildKeepSource, "")
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err = client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), lastBundleSeq)
}

func TestRebuildKeepSource_SnapshotSessionNotFoundRetryRestoresData(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	client.config.SnapshotChunkRows = 1

	sessionCreates := 0
	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "snapshot-not-found-a",
					SnapshotBundleSeq: 6,
					RowCount:          2,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-not-found-b",
				SnapshotBundleSeq: 6,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-not-found-a":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-not-found-a",
						SnapshotBundleSeq: 6,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 5,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
						Error:   "snapshot_session_not_found",
						Message: "snapshot session snapshot-not-found-a was not found",
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/snapshot-not-found-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-not-found-b",
					SnapshotBundleSeq: 6,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 6,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_session_not_found")
	require.Equal(t, 1, snapshotStageCount(t, db))

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)

	mustRebuild(t, client, ctx, RebuildKeepSource, "")
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err = client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(6), lastBundleSeq)
}

func TestRebuildRotateSource_SnapshotSessionExpiredRetryRestoresData(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalSourceID := client.SourceID
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)
	client.config.SnapshotChunkRows = 1

	sessionCreates := 0
	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "recover-expired-a",
					SnapshotBundleSeq: 9,
					RowCount:          2,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "recover-expired-b",
				SnapshotBundleSeq: 9,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/recover-expired-a":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "recover-expired-a",
						SnapshotBundleSeq: 9,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 8,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return errorJSONResponse(http.StatusGone, oversync.ErrorResponse{
						Error:   "snapshot_session_expired",
						Message: "snapshot session recover-expired-a has expired; start a new snapshot session",
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/recover-expired-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "recover-expired-b",
					SnapshotBundleSeq: 9,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 9,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_session_expired")
	require.Equal(t, 1, snapshotStageCount(t, db))
	require.Equal(t, originalSourceID, client.SourceID)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	sourceID, nextSourceBundleID, lastBundleSeq := readBundleClientState(t, db, client.UserID)
	require.Equal(t, originalSourceID, sourceID)
	require.Equal(t, int64(5), nextSourceBundleID)
	require.Equal(t, int64(4), lastBundleSeq)

	mustRebuild(t, client, ctx, RebuildRotateSource, newTestSourceID())
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NotEqual(t, originalSourceID, client.SourceID)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	sourceID, nextSourceBundleID, lastBundleSeq = readBundleClientState(t, db, client.UserID)
	require.Equal(t, client.SourceID, sourceID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(9), lastBundleSeq)
}

func TestRebuildRotateSource_SnapshotSessionNotFoundRetryRestoresData(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalSourceID := client.SourceID
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)
	client.config.SnapshotChunkRows = 1

	sessionCreates := 0
	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        "recover-not-found-a",
					SnapshotBundleSeq: 10,
					RowCount:          2,
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "recover-not-found-b",
				SnapshotBundleSeq: 10,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/recover-not-found-a":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "recover-not-found-a",
						SnapshotBundleSeq: 10,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "users",
							Key:        mustBundleKey("user-1"),
							RowVersion: 9,
							Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
						Error:   "snapshot_session_not_found",
						Message: "snapshot session recover-not-found-a was not found",
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/recover-not-found-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "recover-not-found-b",
					SnapshotBundleSeq: 10,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 10,
						Payload:    mustBundlePayload(t, "user-1", "Ada", "ada@example.com"),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_session_not_found")
	require.Equal(t, 1, snapshotStageCount(t, db))
	require.Equal(t, originalSourceID, client.SourceID)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	sourceID, nextSourceBundleID, lastBundleSeq := readBundleClientState(t, db, client.UserID)
	require.Equal(t, originalSourceID, sourceID)
	require.Equal(t, int64(5), nextSourceBundleID)
	require.Equal(t, int64(4), lastBundleSeq)

	mustRebuild(t, client, ctx, RebuildRotateSource, newTestSourceID())
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NotEqual(t, originalSourceID, client.SourceID)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	sourceID, nextSourceBundleID, lastBundleSeq = readBundleClientState(t, db, client.UserID)
	require.Equal(t, client.SourceID, sourceID)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(10), lastBundleSeq)
}

func TestRebuildKeepSource_CheckpointUnchangedOnFailedFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-bad-final",
				SnapshotBundleSeq: 4,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-bad-final":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-bad-final",
					SnapshotBundleSeq: 4,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 4,
						Payload:    []byte(`{"id":"user-1","name":"Broken"}`),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildKeepSource, "")
	require.Error(t, err)
	require.Equal(t, 1, snapshotStageCount(t, db))

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestRebuildRotateSource_SourceIDDoesNotRotateOnFailedFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalSourceID := client.SourceID
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 5, 4)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-bad-recover",
				SnapshotBundleSeq: 9,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-bad-recover":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-bad-recover",
					SnapshotBundleSeq: 9,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        mustBundleKey("user-1"),
						RowVersion: 9,
						Payload:    []byte(`{"id":"user-1","name":"Broken"}`),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx, RebuildRotateSource, newTestSourceID())
	require.Error(t, err)
	require.Equal(t, originalSourceID, client.SourceID)

	sourceID, nextSourceBundleID, lastBundleSeq := requireBundleState(t, db)
	require.Equal(t, originalSourceID, sourceID)
	require.Equal(t, int64(5), nextSourceBundleID)
	require.Equal(t, int64(4), lastBundleSeq)
}

func TestRebuildKeepSource_SelfReferentialRowsApplyAcrossChunkBoundaries(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}}, `
		CREATE TABLE categories (
			id TEXT PRIMARY KEY,
			parent_id TEXT,
			name TEXT NOT NULL,
			FOREIGN KEY (parent_id) REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED
		)
	`)

	client.config.SnapshotChunkRows = 1
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-self-ref",
				SnapshotBundleSeq: 3,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-self-ref":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-self-ref",
						SnapshotBundleSeq: 3,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "categories",
							Key:        mustBundleKey("child"),
							RowVersion: 2,
							Payload:    []byte(`{"id":"child","parent_id":"root","name":"Child"}`),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-self-ref",
						SnapshotBundleSeq: 3,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "categories",
							Key:        mustBundleKey("root"),
							RowVersion: 1,
							Payload:    []byte(`{"id":"root","parent_id":null,"name":"Root"}`),
						}},
						NextRowOrdinal: 2,
						HasMore:        false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&count))
	require.Equal(t, 2, count)
	var parentID string
	require.NoError(t, db.QueryRow(`SELECT parent_id FROM categories WHERE id = 'child'`).Scan(&parentID))
	require.Equal(t, "root", parentID)
}

func TestRebuildKeepSource_CyclicRowsApplyAcrossChunkBoundaries(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "teams", SyncKeyColumnName: "id"},
		{TableName: "team_members", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE teams (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			captain_member_id TEXT,
			FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
		)
	`, `
		CREATE TABLE team_members (
			id TEXT PRIMARY KEY,
			team_id TEXT NOT NULL,
			name TEXT NOT NULL,
			FOREIGN KEY (team_id) REFERENCES teams(id) DEFERRABLE INITIALLY DEFERRED
		)
	`)

	client.config.SnapshotChunkRows = 1
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-cycle",
				SnapshotBundleSeq: 5,
				RowCount:          2,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-cycle":
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-cycle",
						SnapshotBundleSeq: 5,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "team_members",
							Key:        mustBundleKey("member-1"),
							RowVersion: 4,
							Payload:    []byte(`{"id":"member-1","team_id":"team-1","name":"Captain"}`),
						}},
						NextRowOrdinal: 1,
						HasMore:        true,
					}), nil
				case "1":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        "snapshot-cycle",
						SnapshotBundleSeq: 5,
						Rows: []oversync.SnapshotRow{{
							Schema:     "main",
							Table:      "teams",
							Key:        mustBundleKey("team-1"),
							RowVersion: 5,
							Payload:    []byte(`{"id":"team-1","name":"Ops","captain_member_id":"member-1"}`),
						}},
						NextRowOrdinal: 2,
						HasMore:        false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx, RebuildKeepSource, "")

	var captainID string
	require.NoError(t, db.QueryRow(`SELECT captain_member_id FROM teams WHERE id = 'team-1'`).Scan(&captainID))
	require.Equal(t, "member-1", captainID)
	var teamID string
	require.NoError(t, db.QueryRow(`SELECT team_id FROM team_members WHERE id = 'member-1'`).Scan(&teamID))
	require.Equal(t, "team-1", teamID)
}

func TestSnapshotStageRows_AreIsolatedBySnapshotID(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	require.NoError(t, client.stageSnapshotChunk(ctx, &oversync.SnapshotChunkResponse{
		SnapshotID:        "snapshot-a",
		SnapshotBundleSeq: 1,
		Rows: []oversync.SnapshotRow{{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey("user-a"),
			RowVersion: 1,
			Payload:    mustBundlePayload(t, "user-a", "Ada", "ada@example.com"),
		}},
		NextRowOrdinal: 1,
		HasMore:        false,
	}, 0))
	require.NoError(t, client.stageSnapshotChunk(ctx, &oversync.SnapshotChunkResponse{
		SnapshotID:        "snapshot-b",
		SnapshotBundleSeq: 1,
		Rows: []oversync.SnapshotRow{{
			Schema:     "main",
			Table:      "users",
			Key:        mustBundleKey("user-b"),
			RowVersion: 1,
			Payload:    mustBundlePayload(t, "user-b", "Grace", "grace@example.com"),
		}},
		NextRowOrdinal: 1,
		HasMore:        false,
	}, 0))

	var countA, countB int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = 'snapshot-a'`).Scan(&countA))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = 'snapshot-b'`).Scan(&countB))
	require.Equal(t, 1, countA)
	require.Equal(t, 1, countB)
}
