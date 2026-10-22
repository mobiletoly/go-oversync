// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
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

func setPullResponses(t *testing.T, client *Client, responses ...any) {
	t.Helper()
	// This ordinary replay fixture supplies only pull units, with no sealed
	// snapshot authority. Preserve its original blocker/retry assertions in
	// disabled mode. Public automatic recovery has its own complete authority.
	client.config.RecoveryMode = RecoveryDisabled
	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if requests >= len(responses) {
			return nil, io.EOF
		}
		response := responses[requests]
		requests++
		return jsonResponse(response), nil
	})}
}

func pullBundle(seq int64, sourceID string, sourceBundleID int64, rows ...oversync.BundleRow) oversync.Bundle {
	return oversync.Bundle{
		BundleSeq:      seq,
		SourceID:       sourceID,
		SourceBundleID: sourceBundleID,
		Rows:           rows,
	}
}

func validUserInsertRow(t *testing.T, id string, rowVersion int64, name, email string) oversync.BundleRow {
	t.Helper()
	return rawUserInsertRow(id, rowVersion, mustBundlePayload(t, id, name, email))
}

func rawUserInsertRow(id string, rowVersion int64, payload []byte) oversync.BundleRow {
	return oversync.BundleRow{
		Schema:     "main",
		Table:      "users",
		Key:        mustBundleKey(id),
		Op:         oversync.OpInsert,
		RowVersion: rowVersion,
		Payload:    payload,
	}
}

func requireTotalUserCount(t *testing.T, db *sql.DB, expected int) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, expected, count)
}

func requireUserCount(t *testing.T, db *sql.DB, id string, expected int) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, id).Scan(&count))
	require.Equal(t, expected, count)
}

func requireLastBundleSeqSeen(t *testing.T, client *Client, ctx context.Context, expected int64) {
	t.Helper()
	lastBundleSeq, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, expected, lastBundleSeq)
}

func requireFailedBundleApplyCheckpoint(t *testing.T, response oversync.PullResponse, expectedLastBundleSeq int64, requireUsers func(*testing.T, *sql.DB)) {
	t.Helper()
	ctx := context.Background()
	client, db := newUsersBundleClient(t)
	setPullResponses(t, client, response)

	_, err := client.PullToStable(ctx)
	require.Error(t, err)

	requireUsers(t, db)
	requireLastBundleSeqSeen(t, client, ctx, expectedLastBundleSeq)
}

func requirePullToStableErrorAfterUserApplied(t *testing.T, expectedError string, responses ...any) {
	t.Helper()
	ctx := context.Background()
	client, db := newUsersBundleClient(t)
	setPullResponses(t, client, responses...)

	_, err := client.PullToStable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), expectedError)

	requireUserCount(t, db, "user-1", 1)
	requireLastBundleSeqSeen(t, client, ctx, 1)
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
