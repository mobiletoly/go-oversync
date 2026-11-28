package oversqlite

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func rejectedUsersBundle(t *testing.T) (*Client, PendingPushRejection) {
	t.Helper()
	ctx := context.Background()
	client, db := newUsersBundleClient(t)
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('user-1','Ada','ada@example.com')`)
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{
			"error": "transition_input_invalid", "message": "input cannot be converted",
		})
	}
	client.HTTP = &http.Client{Transport: server}
	_, err = client.PushPending(ctx)
	var needsResolution *PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &needsResolution)
	pending, err := client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	return client, *pending
}

func TestPushApplicationRejection_ResolutionBindsCompleteSelectedIdentity(t *testing.T) {
	client, pending := rejectedUsersBundle(t)
	tests := map[string]func(*PendingPushRejection){
		"id":               func(p *PendingPushRejection) { p.ID += "-changed" },
		"revision":         func(p *PendingPushRejection) { p.Revision++ },
		"source":           func(p *PendingPushRejection) { p.SourceID += "-changed" },
		"source bundle":    func(p *PendingPushRejection) { p.SourceBundleID++ },
		"request hash":     func(p *PendingPushRejection) { p.RequestHash = strings.Repeat("f", 64) },
		"generation id":    func(p *PendingPushRejection) { p.Generation.ID++ },
		"generation proof": func(p *PendingPushRejection) { p.Generation.Fingerprint = strings.Repeat("e", 64) },
		"operation":        func(p *PendingPushRejection) { p.Operation += "-changed" },
		"status":           func(p *PendingPushRejection) { p.Status = http.StatusConflict },
		"category":         func(p *PendingPushRejection) { p.Category += "_changed" },
		"message":          func(p *PendingPushRejection) { p.Message += " changed" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			selected := pending
			mutate(&selected)
			var stale *StalePushRejectionResolutionError
			require.ErrorAs(t, client.RetryPushRejection(context.Background(), selected), &stale)
		})
	}
	require.NoError(t, client.RetryPushRejection(context.Background(), pending))
}

func TestPushApplicationRejection_RetryPreservesExactFrozenRequest(t *testing.T) {
	ctx := context.Background()
	client, db := newUsersBundleClient(t)
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('user-1','Ada','ada@example.com')`)
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{
			"error": "transition_input_invalid", "message": "input cannot be converted",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	var needsResolution *PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &needsResolution)
	pending, err := client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.Equal(t, "transition_input_invalid", pending.Category)
	originalHash := pending.RequestHash

	require.NoError(t, client.RetryPushRejection(ctx, *pending))
	pending, err = client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.Nil(t, pending)
	server.pushHook = nil
	report, err := client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, originalHash, server.pushRequests[0].CanonicalRequestHash)
	require.Equal(t, originalHash, server.pushRequests[1].CanonicalRequestHash)
}

func TestPushApplicationRejection_DiscardPreservesNewerLocalIntent(t *testing.T) {
	ctx := context.Background()
	client, db := newUsersBundleClient(t)
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('user-1','Ada','ada@example.com')`)
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{
			"error": "transition_input_invalid", "message": "input cannot be converted",
		})
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	var needsResolution *PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &needsResolution)
	pending, err := client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	_, err = db.Exec(`UPDATE users SET name='Ada Newer' WHERE id='user-1'`)
	require.NoError(t, err)

	var stale *StalePushRejectionResolutionError
	require.ErrorAs(t, client.DiscardPushRejection(ctx, *pending), &stale)
	pending, err = client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.NoError(t, client.DiscardPushRejection(ctx, *pending))
	// Discarding an application rejection is local-only in the direct-push
	// protocol; it must not issue another network request.
	require.Len(t, server.pushRequests, 1)
	pending, err = client.PendingPushRejection(ctx)
	require.NoError(t, err)
	require.Nil(t, pending)
	var frozen, dirty int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&frozen))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Zero(t, frozen)
	require.Equal(t, 1, dirty)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id='user-1'`).Scan(&name))
	require.Equal(t, "Ada Newer", name)
}

func TestAdmittedFixture_RequiresLocalContractBeforeCreatingRuntimeState(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests,
		admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.ErrorContains(t, err, "config.localContract is required")
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&count))
	require.Zero(t, count)
}

func TestUpgradeClassification_PushApplicationRejectionNeedsResolution(t *testing.T) {
	err := &PushRejectionNeedsResolutionError{Pending: PendingPushRejection{
		Operation: "push", Status: http.StatusUnprocessableEntity, Category: "transition_input_invalid",
	}}
	require.Equal(t, UpgradeOutcomeNeedsResolution, classifyUpgradeCause(err, false))
}
