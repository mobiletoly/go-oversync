// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPushPending_HistoryProjectionCapacityHonorsRetryAfter(t *testing.T) {
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

	client.config.PushCapacityRetryPolicy = &PushCapacityRetryPolicy{
		Enabled: true, MaxWait: 2 * time.Second, FallbackDelay: time.Millisecond, JitterFraction: 0,
	}
	base := newMockPushServer(t)
	createRequests := 0
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
			createRequests++
			if createRequests == 1 {
				resp := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
					Error: "history_projection_capacity_exceeded", Message: "internal details must not escape",
				})
				resp.Header.Set("Retry-After", "1")
				return resp, nil
			}
		}
		return base.RoundTrip(r)
	})}

	started := time.Now()
	report := mustPushPending(t, client, ctx)
	elapsed := time.Since(started)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.GreaterOrEqual(t, elapsed, time.Second)
	require.Equal(t, 2, createRequests)
	stats := client.PushTransferDiagnostics()
	require.Equal(t, int64(1), stats.CapacityResponses)
	require.Equal(t, int64(1), stats.CapacityRetries)
	require.GreaterOrEqual(t, stats.CapacityWait, time.Second)
}

func TestPushPending_HistoryProjectionCapacityBudgetIsBounded(t *testing.T) {
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

	client.config.PushCapacityRetryPolicy = &PushCapacityRetryPolicy{
		Enabled: true, MaxWait: 10 * time.Millisecond, FallbackDelay: time.Millisecond, JitterFraction: 0,
	}
	requests := 0
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		resp := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
			Error: "history_projection_capacity_exceeded", Message: "internal details must not escape",
		})
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})}

	_, err = client.PushPending(ctx)
	var exhausted *PushCapacityRetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, "push", exhausted.Operation)
	require.Equal(t, "history_projection_capacity_exceeded", exhausted.ErrorCode)
	require.Equal(t, 1, requests)
	require.NotContains(t, err.Error(), "internal details")
	stats := client.PushTransferDiagnostics()
	require.Equal(t, int64(1), stats.CapacityResponses)
	require.Zero(t, stats.CapacityRetries)
}

func TestPushPending_CommitOutcomeUnknownRestartsCreateWithStableSourceTuple(t *testing.T) {
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

	server := newMockPushServer(t)
	commitResponseLost := false
	server.pushHook = func(session *mockDirectPush) *http.Response {
		committedResponse := server.defaultPushResponse(session)
		if !commitResponseLost {
			commitResponseLost = true
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
				Error:   "commit_outcome_unknown",
				Message: "Push transaction commit outcome is unknown; retry the same complete push with the same source tuple",
			})
		}
		return committedResponse
	}
	client.HTTP = &http.Client{Transport: server}

	report := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.True(t, commitResponseLost)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, server.pushRequests[0].SourceBundleID, server.pushRequests[1].SourceBundleID)
	require.Equal(t, server.pushRequests[0].CanonicalRequestHash, server.pushRequests[1].CanonicalRequestHash)
	require.Equal(t, int64(1), server.pushRequests[0].SourceBundleID)
	require.NotEmpty(t, server.pushRequests[0].CanonicalRequestHash)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)
}

func TestPushPending_TextSyncKeyRoundTripsThroughPush(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "docs", SyncKeyColumnName: "doc_id"}}, `
		CREATE TABLE docs (
			doc_id TEXT PRIMARY KEY NOT NULL,
			body TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO docs (doc_id, body) VALUES (?, ?)`, "doc-1", "Hello text key")
	require.NoError(t, err)

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	require.Len(t, server.pushRequests, 1)
	uploaded := expandCapturedPushRequest(t, client, server.pushRequests[0])
	require.Len(t, uploaded, 1)
	require.Equal(t, oversync.SyncKey{"doc_id": "doc-1"}, uploaded[0].Key)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(uploaded[0].Payload, &payload))
	require.Equal(t, "doc-1", payload["doc_id"])
	require.Equal(t, "Hello text key", payload["body"])

	keyJSONBytes, err := json.Marshal(map[string]any{"doc_id": "doc-1"})
	require.NoError(t, err)
	var rowVersion int64
	var deleted int
	require.NoError(t, db.QueryRow(`
		SELECT row_version, server_kind='deleted'
		FROM _sync_row_state
		WHERE schema_name = 'main' AND table_name = 'docs' AND key_json = ?
	`, string(keyJSONBytes)).Scan(&rowVersion, &deleted))
	require.Equal(t, int64(1), rowVersion)
	require.Equal(t, 0, deleted)
}

func TestPushPending_RejectsNonUUIDBlobSyncKey(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id BLOB PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (x'0102', ?, ?)`, "Blob", "blob@example.com")
	require.NoError(t, err)

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	before := vectorState(t, client)
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid blob UUID pk")
	require.Contains(t, err.Error(), "want 16")
	require.Equal(t, before, vectorState(t, client))
	require.Empty(t, server.pushRequests)
}
