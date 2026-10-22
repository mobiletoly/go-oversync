// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// These replace prepared-source-rebind/snapshot success expectations. A
// nonterminal source error cannot dispose of F or authorize its retargeting.
func TestPushPending_CreateHistoryPrunedRetainsOriginalAndBlocksSnapshot(t *testing.T) {
	testFrozenSourceErrorPreservation(t, SourceRecoveryHistoryPruned, false)
}
func TestPushPending_CreateSourceSequenceOutOfOrderRetainsOriginalAcrossRestart(t *testing.T) {
	testFrozenSourceErrorPreservation(t, SourceRecoverySequenceOutOfOrder, true)
}
func testFrozenSourceErrorPreservation(t *testing.T, code SourceRecoveryCode, reopen bool) {
	ctx := context.Background()
	client, db, _ := recoveryJournalClient(t)
	client.sourceIDGenerator = func() string { return "reserved-successor" }
	var pushes, resolves, snapshots int
	transport := &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/push":
			pushes++
		case "/sync/push-outcomes/resolve":
			resolves++
		case "/sync/snapshot-sessions":
			snapshots++
			return nil, fmt.Errorf("snapshot must not bypass F")
		default:
			return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
		}
		return errorJSONResponse(409, oversync.ErrorResponse{Error: string(code), Message: "original operation admission unavailable"}), nil
	})}
	client.HTTP = transport
	_, err := client.PushPending(ctx)
	var recovery *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recovery)
	require.Equal(t, code, recovery.Code)
	require.Equal(t, operationKindSourceRecovery, requireOperationKind(t, db))
	require.Equal(t, "reserved-successor", requireOperationReplacementSourceID(t, db))
	originalSource := client.sourceID
	if reopen {
		require.NoError(t, client.Close())
		client, err = newTestClient(t, db, "http://example.invalid", tokenProviderForTests, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
		require.NoError(t, err)
		client.HTTP = transport
		mustOpen(t, client, ctx)
		_, err = client.Attach(ctx, "bundle-user")
		require.NoError(t, err)
		defer client.Close()
	}
	before := recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state")
	_, err = client.PushPending(ctx)
	require.ErrorAs(t, err, &recovery)
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state"))
	_, err = client.Rebuild(ctx)
	require.Error(t, err)
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state"))
	require.Equal(t, originalSource, client.sourceID)
	require.Equal(t, originalSource, requireOutboxBundle(t, db).SourceID)
	require.Equal(t, 1, pushes)
	require.Equal(t, 1, resolves)
	require.Zero(t, snapshots)
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
