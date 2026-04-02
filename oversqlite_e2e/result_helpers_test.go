package oversqlite_e2e

import (
	"context"
	"testing"

	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/stretchr/testify/require"
)

func mustOpenE2E(t *testing.T, client *oversqlite.Client, ctx context.Context, sourceID string) oversqlite.OpenResult {
	t.Helper()
	_ = sourceID
	result, err := client.Open(ctx)
	require.NoError(t, err)
	return result
}

func mustPushPendingE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.PushReport {
	t.Helper()
	report, err := client.PushPending(ctx)
	require.NoError(t, err)
	return report
}

func mustPullToStableE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.RemoteSyncReport {
	t.Helper()
	report, err := client.PullToStable(ctx)
	require.NoError(t, err)
	return report
}

func mustRebuildE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.RemoteSyncReport {
	t.Helper()
	report, err := client.Rebuild(ctx)
	require.NoError(t, err)
	return report
}

func mustDetachE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.DetachResult {
	t.Helper()
	result, err := client.Detach(ctx)
	require.NoError(t, err)
	return result
}

func requireCurrentSourceIDE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) string {
	t.Helper()
	info := mustSourceInfoE2E(t, client, ctx)
	require.NotEmpty(t, info.CurrentSourceID)
	return info.CurrentSourceID
}

func mustSourceInfoE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.SourceInfo {
	t.Helper()
	info, err := client.SourceInfo(ctx)
	require.NoError(t, err)
	return info
}

func mustSyncThenDetachE2E(t *testing.T, client *oversqlite.Client, ctx context.Context) oversqlite.SyncThenDetachResult {
	t.Helper()
	result, err := client.SyncThenDetach(ctx)
	require.NoError(t, err)
	return result
}
