// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPullToStable_AppliesMultipleBundlesToFrozenCeiling(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
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
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
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
			id TEXT PRIMARY KEY NOT NULL,
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
			doc_id TEXT PRIMARY KEY NOT NULL,
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
		SELECT row_version, server_kind='deleted'
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
			id TEXT PRIMARY KEY NOT NULL,
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

func TestPullToStable_RejectsWriteCommittedDuringResponseBeforeBundleApply(t *testing.T) {
	for _, localID := range []string{"user-1", "user-2"} {
		t.Run(localID, func(t *testing.T) {
			ctx := context.Background()
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
			`)
			requests := 0
			client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				require.Equal(t, "/sync/pull", r.URL.Path)
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, 'Local', 'local@example.com')`, localID)
				require.NoError(t, err)
				return jsonResponse(oversync.PullResponse{
					StableBundleSeq: 1,
					Bundles: []oversync.Bundle{pullBundle(1, "peer-a", 1,
						validUserInsertRow(t, "user-1", 1, "Remote", "remote@example.com"))},
				}), nil
			})}

			_, err := client.PullToStable(ctx)
			var dirtyErr *DirtyStateRejectedError
			require.ErrorAs(t, err, &dirtyErr)
			require.Equal(t, 1, dirtyErr.DirtyCount)
			require.Equal(t, 1, requests)
			var name string
			require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, localID).Scan(&name))
			require.Equal(t, "Local", name)
			if localID != "user-1" {
				require.ErrorIs(t, db.QueryRow(`SELECT name FROM users WHERE id = 'user-1'`).Scan(&name), sql.ErrNoRows)
			}
			var checkpoint, dirtyCount int
			require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&checkpoint))
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
			require.Zero(t, checkpoint)
			require.Equal(t, 1, dirtyCount)
		})
	}
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
				_, err := client.Rebuild(ctx)
				return err
			},
		},
		{
			name: "recover rejects pending outbound replay",
			op: func(client *Client, ctx context.Context) error {
				_, err := client.Rebuild(ctx)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('user-1','Ada','ada@example.com')`)
			require.NoError(t, err)
			frozen, err := client.ensurePushOutboundSnapshot(ctx)
			require.NoError(t, err)
			require.Len(t, frozen.Rows, 1)

			requests := 0
			client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				return nil, io.EOF
			})}

			err = tc.op(client, ctx)
			if tc.name == "pull rejects pending outbound replay" {
				var replayErr *PendingPushReplayError
				require.ErrorAs(t, err, &replayErr)
				require.Equal(t, 1, replayErr.OutboundCount)
			} else {
				var pending *UploadJournalIntegrityError
				require.ErrorAs(t, err, &pending)
			}
			require.Equal(t, 0, requests)
		})
	}
}

func TestPullToStable_FailedBundleApplyCheckpointBehavior(t *testing.T) {
	tests := []struct {
		name                  string
		response              oversync.PullResponse
		expectedLastBundleSeq int64
		requireUsers          func(*testing.T, *sql.DB)
	}{
		{
			name: "leaves checkpoint unchanged when first bundle fails",
			response: oversync.PullResponse{
				StableBundleSeq: 1,
				HasMore:         false,
				Bundles: []oversync.Bundle{
					pullBundle(
						1,
						"peer-a",
						9,
						validUserInsertRow(t, "user-1", 1, "Ada", "ada@example.com"),
						rawUserInsertRow("user-2", 1, []byte(`{"id":"user-2","name":"Broken"}`)),
					),
				},
			},
			expectedLastBundleSeq: 0,
			requireUsers: func(t *testing.T, db *sql.DB) {
				requireTotalUserCount(t, db, 0)
			},
		},
		{
			name: "keeps earlier checkpoint when later bundle fails",
			response: oversync.PullResponse{
				StableBundleSeq: 2,
				HasMore:         false,
				Bundles: []oversync.Bundle{
					pullBundle(1, "peer-a", 11, validUserInsertRow(t, "user-1", 1, "Ada", "ada@example.com")),
					pullBundle(2, "peer-b", 12, rawUserInsertRow("user-2", 2, []byte(`{"id":"user-2","name":"Broken"}`))),
				},
			},
			expectedLastBundleSeq: 1,
			requireUsers: func(t *testing.T, db *sql.DB) {
				requireUserCount(t, db, "user-1", 1)
				requireUserCount(t, db, "user-2", 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireFailedBundleApplyCheckpoint(t, tt.response, tt.expectedLastBundleSeq, tt.requireUsers)
		})
	}
}

func TestPullToStable_RejectsMalformedPullResponse(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
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
	client, db := newUsersBundleClient(t)
	setPullResponses(t, client, map[string]any{
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
	})

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required stable_bundle_seq")

	requireTotalUserCount(t, db, 0)
	requireLastBundleSeqSeen(t, client, ctx, 0)
}

func TestPullToStable_RejectsInvalidPullResponsesAfterApplyingBundle(t *testing.T) {
	tests := []struct {
		name          string
		expectedError string
		responses     []any
	}{
		{
			name:          "stable bundle seq changed",
			expectedError: "stable bundle seq changed",
			responses: []any{
				oversync.PullResponse{
					StableBundleSeq: 2,
					HasMore:         true,
					Bundles: []oversync.Bundle{
						pullBundle(1, "peer-a", 11, validUserInsertRow(t, "user-1", 1, "Ada", "ada@example.com")),
					},
				},
				oversync.PullResponse{
					StableBundleSeq: 3,
					HasMore:         false,
					Bundles:         []oversync.Bundle{},
				},
			},
		},
		{
			name:          "pull ended before frozen ceiling",
			expectedError: "pull ended early",
			responses: []any{
				oversync.PullResponse{
					StableBundleSeq: 2,
					HasMore:         false,
					Bundles: []oversync.Bundle{
						pullBundle(1, "peer-a", 11, validUserInsertRow(t, "user-1", 1, "Ada", "ada@example.com")),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requirePullToStableErrorAfterUserApplied(t, tt.expectedError, tt.responses...)
		})
	}
}
