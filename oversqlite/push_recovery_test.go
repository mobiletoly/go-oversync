// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPushPending_CreateHistoryPrunedRequiresSourceRecoveryAndRotatePreservesOutbox(t *testing.T) {
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
	client.sourceIDGenerator = func() string { return "rotated-device" }

	base := newMockPushServer(t)
	createPruned := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sync/push" && createPruned:
			createPruned = false
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "history_pruned",
				Message: "source tuple is below retained history floor",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			var req oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.NotNil(t, req.SourceReplacement)
			require.Equal(t, "bundle-device", req.SourceReplacement.PreviousSourceID)
			require.Equal(t, "rotated-device", req.SourceReplacement.NewSourceID)
			require.Equal(t, "history_pruned", req.SourceReplacement.Reason)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "source-recovery-snapshot",
				SnapshotBundleSeq: 9,
				RowCount:          0,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/source-recovery-snapshot":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID:        "source-recovery-snapshot",
				SnapshotBundleSeq: 9,
				Rows:              nil,
				NextRowOrdinal:    0,
				HasMore:           false,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/source-recovery-snapshot":
			return jsonResponse(map[string]any{"status": "deleted"}), nil
		default:
			return base.RoundTrip(r)
		}
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, "bundle-device", outbox.SourceID)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, int64(1), outbox.RowCount)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoveryHistoryPruned), requireOperationReason(t, db))
	require.Equal(t, "rotated-device", requireOperationReplacementSourceID(t, db))

	_, err = client.PullToStable(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	_, err = client.Sync(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryHistoryPruned, recoveryErr.Code)

	report, err := client.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, "rotated-device", client.sourceID)

	outbox = requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, "rotated-device", outbox.SourceID)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
	require.Equal(t, "", requireOperationReason(t, db))

	var localCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 1, localCount)

	pushReport := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, pushReport.Outcome)
	require.Len(t, base.pushRequests, 1)

	sourceID, nextSourceBundleID, lastBundleSeqSeen := requireBundleState(t, db)
	require.Equal(t, "rotated-device", sourceID)
	require.Equal(t, int64(2), nextSourceBundleID)
	require.GreaterOrEqual(t, lastBundleSeqSeen, int64(9))

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&localCount))
	require.Equal(t, 0, localCount)
	name, _, exists := loadUserForKey(t, db, "user-1")
	require.True(t, exists)
	require.Equal(t, "Ada", name)

	var replacedBy string
	require.NoError(t, db.QueryRow(`SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = ?`, "bundle-device").Scan(&replacedBy))
	require.Equal(t, "rotated-device", replacedBy)
}

func TestPushPending_CreateSourceSequenceOutOfOrderPersistsSourceRecoveryAcrossRestart(t *testing.T) {
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
	client.sourceIDGenerator = func() string { return "restarted-rotated-device" }

	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && r.URL.Path == "/sync/generations/1" {
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		}
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "source_sequence_out_of_order",
				Message: "expected source_bundle_id 2",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoverySequenceOutOfOrder), requireOperationReason(t, db))
	require.Equal(t, "restarted-rotated-device", requireOperationReplacementSourceID(t, db))

	require.NoError(t, client.Close())

	restarted, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	restarted.HTTP = client.HTTP
	mustOpen(t, restarted, ctx)
	result, err := restarted.Attach(ctx, "bundle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = restarted.PushPending(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	_, err = restarted.PullToStable(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	_, err = restarted.Sync(ctx)
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceOutOfOrder, recoveryErr.Code)

	restarted.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			var req oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.NotNil(t, req.SourceReplacement)
			require.Equal(t, "bundle-device", req.SourceReplacement.PreviousSourceID)
			require.Equal(t, "restarted-rotated-device", req.SourceReplacement.NewSourceID)
			require.Equal(t, "source_sequence_out_of_order", req.SourceReplacement.Reason)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "sequence-out-of-order-recovery",
				SnapshotBundleSeq: 4,
				RowCount:          0,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/sequence-out-of-order-recovery":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID:        "sequence-out-of-order-recovery",
				SnapshotBundleSeq: 4,
				Rows:              nil,
				NextRowOrdinal:    0,
				HasMore:           false,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/sequence-out-of-order-recovery":
			return jsonResponse(map[string]any{"status": "deleted"}), nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})}

	report, err := restarted.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, "restarted-rotated-device", restarted.sourceID)
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
}

func TestPushPending_CommitSourceSequenceChangedRequiresSourceRecovery(t *testing.T) {
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
	client.sourceIDGenerator = func() string { return "commit-rotated-device" }

	base := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error:   "source_sequence_changed",
				Message: "expected source_bundle_id 2 at commit time",
			}), nil
		}
		return base.RoundTrip(r)
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoverySequenceChanged, recoveryErr.Code)

	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.Equal(t, int64(1), outbox.SourceBundleID)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoverySequenceChanged), requireOperationReason(t, db))
	require.Equal(t, "commit-rotated-device", requireOperationReplacementSourceID(t, db))
}

func TestPushPending_CreateSourceRetiredEntersSourceRecoveryAndAdoptsReplacement(t *testing.T) {
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

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
			return errorJSONResponse(http.StatusConflict, oversync.SourceRetiredResponse{
				Error:              "source_retired",
				Message:            "source bundle-device was retired for user bundle-user and replaced by server-rotated-device",
				SourceID:           "bundle-device",
				ReplacedBySourceID: "server-rotated-device",
			}), nil
		}
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	})}

	_, err = client.PushPending(ctx)
	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryRetired, recoveryErr.Code)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, string(SourceRecoveryRetired), requireOperationReason(t, db))
	require.Equal(t, "server-rotated-device", requireOperationReplacementSourceID(t, db))
}
