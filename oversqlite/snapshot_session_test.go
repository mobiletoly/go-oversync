// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRebuildKeepSource_RebuildsFromSnapshotWithDeferredFKs(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY NOT NULL,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	snapshotRows := []oversync.SnapshotRow{
		{Schema: "main", Table: "posts", Key: mustBundleKey("post-1"), RowVersion: 7, Payload: []byte(`{"id":"post-1","title":"Hello","author_id":"user-1"}`)},
		{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 6, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")},
	}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-hydrate",
				SnapshotBundleSeq: 7,
				RowCount:          2,
				ByteCount:         snapshotBytes,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-hydrate":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-hydrate",
					SnapshotBundleSeq: 7,
					Rows:              snapshotRows,
					NextRowOrdinal:    2,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx)

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
			id TEXT PRIMARY KEY NOT NULL,
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

	_, err = client.Rebuild(ctx)
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
			id TEXT PRIMARY KEY NOT NULL,
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
				"byte_count":  1,
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

	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_bundle_seq")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)

	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lastBundleSeq)
}

func TestRebuildKeepSource_MultiChunkDownloadStagesRowsBeforeFinalApply(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	snapshotRows := []oversync.SnapshotRow{
		{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 7, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")},
		{Schema: "main", Table: "users", Key: mustBundleKey("user-2"), RowVersion: 8, Payload: mustBundlePayload(t, "user-2", "Grace", "grace@example.com")},
	}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)

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
				ByteCount:         snapshotBytes,
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
						Rows:              snapshotRows[:1],
						NextRowOrdinal:    1,
						HasMore:           true,
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
						Rows:              snapshotRows[1:],
						NextRowOrdinal:    2,
						HasMore:           false,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx)
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
			id TEXT PRIMARY KEY NOT NULL,
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
		ByteCount:         testSnapshotRowsByteCount(chunk.Rows),
		ExpiresAt:         "2030-01-01T00:00:00Z",
	}

	require.NoError(t, client.stageSnapshotChunk(ctx, chunk, 0))
	require.Equal(t, 1, snapshotStageCount(t, db))
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
	require.NoError(t, client.applyStagedSnapshotLocked(ctx, session, snapshotApplyOptions{}))
	require.Equal(t, 0, snapshotStageCount(t, db))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestRebuildKeepSource_ClearsStaleSnapshotStageBeforeNewAttempt(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
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
	snapshotRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 6, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}
	snapshotBytes := testSnapshotRowsByteCount(snapshotRows)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-fresh",
				SnapshotBundleSeq: 6,
				RowCount:          1,
				ByteCount:         snapshotBytes,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-fresh":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-fresh",
					SnapshotBundleSeq: 6,
					Rows:              snapshotRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx)
	require.Equal(t, 0, snapshotStageCount(t, db))
}

func TestRebuildKeepSource_PartialDownloadRestartClearsStageAndStartsFromZero(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	client.config.SnapshotChunkRows = 1
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	firstRows := []oversync.SnapshotRow{
		{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 6, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")},
		{Schema: "main", Table: "users", Key: mustBundleKey("never-fetched"), RowVersion: 7, Payload: mustBundlePayload(t, "never-fetched", "Never", "never@example.com")},
	}
	secondRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: 7, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}

	firstAttemptRequests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-restart-a",
				SnapshotBundleSeq: 7,
				RowCount:          2,
				ByteCount:         testSnapshotRowsByteCount(firstRows),
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
						Rows:              firstRows[:1],
						NextRowOrdinal:    1,
						HasMore:           true,
					}), nil
				}
				return nil, io.EOF
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx)
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
				ByteCount:         testSnapshotRowsByteCount(secondRows),
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-restart-b":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-restart-b",
					SnapshotBundleSeq: 7,
					Rows:              secondRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	mustRebuild(t, client, ctx)
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
}

type snapshotSessionRetryCase struct {
	snapshotPrefix   string
	bundleSeq        int64
	firstRowVersion  int64
	secondRowVersion int64
	failureStatus    int
	failureCode      string
	failureMessage   string
	keepSource       bool
}

func runSnapshotSessionRetryRestoresData(t *testing.T, tc snapshotSessionRetryCase) {
	t.Helper()

	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	originalSourceID := client.sourceID
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('stale', 'Stale', 'stale@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	if !tc.keepSource {
		setCurrentSourceBundleState(t, db, 5, 4)
	}
	client.config.SnapshotChunkRows = 1

	firstSnapshotID := tc.snapshotPrefix + "-a"
	secondSnapshotID := tc.snapshotPrefix + "-b"
	firstRows := []oversync.SnapshotRow{
		{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: tc.firstRowVersion, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")},
		{Schema: "main", Table: "users", Key: mustBundleKey("never-fetched"), RowVersion: tc.bundleSeq, Payload: mustBundlePayload(t, "never-fetched", "Never", "never@example.com")},
	}
	secondRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("user-1"), RowVersion: tc.secondRowVersion, Payload: mustBundlePayload(t, "user-1", "Ada", "ada@example.com")}}
	sessionCreates := 0
	secondAttemptStarted := false
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			sessionCreates++
			if sessionCreates == 1 {
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID:        firstSnapshotID,
					SnapshotBundleSeq: tc.bundleSeq,
					RowCount:          2,
					ByteCount:         testSnapshotRowsByteCount(firstRows),
					ExpiresAt:         "2030-01-01T00:00:00Z",
				}), nil
			}
			secondAttemptStarted = true
			require.Equal(t, 0, snapshotStageCount(t, db))
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        secondSnapshotID,
				SnapshotBundleSeq: tc.bundleSeq,
				RowCount:          1,
				ByteCount:         testSnapshotRowsByteCount(secondRows),
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/" + firstSnapshotID:
			switch r.Method {
			case http.MethodGet:
				switch r.URL.Query().Get("after_row_ordinal") {
				case "0":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID:        firstSnapshotID,
						SnapshotBundleSeq: tc.bundleSeq,
						Rows:              firstRows[:1],
						NextRowOrdinal:    1,
						HasMore:           true,
					}), nil
				case "1":
					return errorJSONResponse(tc.failureStatus, oversync.ErrorResponse{
						Error:   tc.failureCode,
						Message: tc.failureMessage,
					}), nil
				}
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		case "/sync/snapshot-sessions/" + secondSnapshotID:
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        secondSnapshotID,
					SnapshotBundleSeq: tc.bundleSeq,
					Rows:              secondRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), tc.failureCode)
	require.Equal(t, 1, snapshotStageCount(t, db))
	if !tc.keepSource {
		require.Equal(t, originalSourceID, client.sourceID)
	}

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 0, count)

	if tc.keepSource {
		lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(0), lastBundleSeq)
	} else {
		sourceID, nextSourceBundleID, lastBundleSeq := readBundleClientState(t, db, client.UserID)
		require.Equal(t, originalSourceID, sourceID)
		require.Equal(t, int64(5), nextSourceBundleID)
		require.Equal(t, int64(4), lastBundleSeq)
	}

	mustRebuild(t, client, ctx)
	require.True(t, secondAttemptStarted)
	require.Equal(t, 0, snapshotStageCount(t, db))
	if !tc.keepSource {
		require.Equal(t, originalSourceID, client.sourceID)
	}
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'stale'`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = 'user-1'`).Scan(&count))
	require.Equal(t, 1, count)

	if tc.keepSource {
		lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
		require.NoError(t, err)
		require.Equal(t, tc.bundleSeq, lastBundleSeq)
	} else {
		sourceID, nextSourceBundleID, lastBundleSeq := readBundleClientState(t, db, client.UserID)
		require.Equal(t, client.sourceID, sourceID)
		require.Equal(t, int64(5), nextSourceBundleID)
		require.Equal(t, tc.bundleSeq, lastBundleSeq)
	}
}

func TestRebuildKeepSource_SnapshotSessionExpiredRetryRestoresData(t *testing.T) {
	runSnapshotSessionRetryRestoresData(t, snapshotSessionRetryCase{
		snapshotPrefix:   "snapshot-expired",
		bundleSeq:        8,
		firstRowVersion:  7,
		secondRowVersion: 8,
		failureStatus:    http.StatusGone,
		failureCode:      "snapshot_session_expired",
		failureMessage:   "snapshot session snapshot-expired-a has expired; start a new snapshot session",
		keepSource:       true,
	})
}

func TestRebuildKeepSource_SnapshotSessionNotFoundRetryRestoresData(t *testing.T) {
	runSnapshotSessionRetryRestoresData(t, snapshotSessionRetryCase{
		snapshotPrefix:   "snapshot-not-found",
		bundleSeq:        6,
		firstRowVersion:  5,
		secondRowVersion: 6,
		failureStatus:    http.StatusNotFound,
		failureCode:      "snapshot_session_not_found",
		failureMessage:   "snapshot session snapshot-not-found-a was not found",
		keepSource:       true,
	})
}

func TestRebuild_SnapshotSessionExpiredRetryRestoresData(t *testing.T) {
	runSnapshotSessionRetryRestoresData(t, snapshotSessionRetryCase{
		snapshotPrefix:   "recover-expired",
		bundleSeq:        9,
		firstRowVersion:  8,
		secondRowVersion: 9,
		failureStatus:    http.StatusGone,
		failureCode:      "snapshot_session_expired",
		failureMessage:   "snapshot session recover-expired-a has expired; start a new snapshot session",
	})
}

func TestRebuild_SnapshotSessionNotFoundRetryRestoresData(t *testing.T) {
	runSnapshotSessionRetryRestoresData(t, snapshotSessionRetryCase{
		snapshotPrefix:   "recover-not-found",
		bundleSeq:        10,
		firstRowVersion:  9,
		secondRowVersion: 10,
		failureStatus:    http.StatusNotFound,
		failureCode:      "snapshot_session_not_found",
		failureMessage:   "snapshot session recover-not-found-a was not found",
	})
}

func setChunkedSnapshotSession(t *testing.T, client *Client, snapshotID string, bundleSeq int64, chunks [][]oversync.SnapshotRow) {
	t.Helper()
	sessionPath := "/sync/snapshot-sessions/" + snapshotID
	var rowCount int64
	var byteCount int64
	var firstRowOrdinal int64
	for _, rows := range chunks {
		rowCount += int64(len(rows))
		_, logicalBytes, err := oversync.CompactGroupsForSnapshotRowsAt(client.discovery.SchemaGeneration, rows, firstRowOrdinal)
		require.NoError(t, err)
		byteCount += logicalBytes
		firstRowOrdinal += int64(len(rows))
	}
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			require.Equal(t, http.MethodPost, r.Method)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        snapshotID,
				SnapshotBundleSeq: bundleSeq,
				RowCount:          rowCount,
				ByteCount:         byteCount,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case sessionPath:
			switch r.Method {
			case http.MethodGet:
				afterOrdinal, err := strconv.Atoi(r.URL.Query().Get("after_row_ordinal"))
				require.NoError(t, err)

				nextOrdinal := 0
				for i, rows := range chunks {
					currentOrdinal := nextOrdinal
					nextOrdinal += len(rows)
					if currentOrdinal == afterOrdinal {
						groups, logicalBytes, err := oversync.CompactGroupsForSnapshotRowsAt(client.discovery.SchemaGeneration, rows, int64(currentOrdinal))
						require.NoError(t, err)
						return jsonResponse(oversync.CompactSnapshotChunkResponse{
							Rows: groups, NextRowOrdinal: int64(nextOrdinal), HasMore: i < len(chunks)-1, LogicalByteCount: logicalBytes,
						}), nil
					}
				}
				return nil, io.EOF
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}
}
