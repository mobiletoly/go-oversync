package oversqlite_e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_PushPending_RetriesInjectedTransientFailure(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_retry_push_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-retry-push-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", newRetryingConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Retry", "retry@example.com")
	require.NoError(t, err)

	server.InjectFailure(exampleserver.FailureEndpointPushSessionCreate, 503, 1)

	mustPushPendingE2E(t, client, ctx)

	var count int
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM `+schema+`.users
		WHERE id = $1
	`, rowID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEndToEnd_PushPending_RetryExhaustionReturnsTypedError(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_retry_push_exhaust_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-retry-push-exhaust-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", newRetryingConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Retry", "retry@example.com")
	require.NoError(t, err)

	server.InjectFailure(exampleserver.FailureEndpointPushSessionCreate, 503, 3)

	_, err = client.PushPending(ctx)
	var retryErr *oversqlite.RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, "push_pending", retryErr.Operation)
}

func TestEndToEnd_PullToStable_RetriesInjectedTransientFailure(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_retry_pull_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-retry-pull-user-" + uuid.NewString()
	config := newRetryingConfig(schema, syncTables("users"))
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", config, usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", config, usersDDL)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)
	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)

	rowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	server.InjectFailure(exampleserver.FailureEndpointPull, 503, 1)

	mustPullToStableE2E(t, clientB, ctx)

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, rowID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEndToEnd_PullToStable_RetryExhaustionReturnsTypedError(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_retry_pull_exhaust_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-retry-pull-exhaust-user-" + uuid.NewString()
	config := newRetryingConfig(schema, syncTables("users"))
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", config, usersDDL)
	clientB, _ := newSQLiteClientWithoutConnect(t, server, userID, "device-b", config, usersDDL)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)
	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)

	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	server.InjectFailure(exampleserver.FailureEndpointPull, 503, 3)

	_, err = clientB.PullToStable(ctx)
	var retryErr *oversqlite.RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, "pull_request", retryErr.Operation)
}
