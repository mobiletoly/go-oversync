package oversqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustOpen(t *testing.T, client *Client, ctx context.Context) {
	t.Helper()
	err := client.Open(ctx)
	require.NoError(t, err)
}

func mustAttach(t *testing.T, client *Client, ctx context.Context, userID string) AttachResult {
	t.Helper()
	result, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	return result
}

func mustDetach(t *testing.T, client *Client, ctx context.Context) DetachResult {
	t.Helper()
	result, err := client.Detach(ctx)
	require.NoError(t, err)
	return result
}

func mustPushPending(t *testing.T, client *Client, ctx context.Context) PushReport {
	t.Helper()
	report, err := client.PushPending(ctx)
	require.NoError(t, err)
	return report
}

func mustPullToStable(t *testing.T, client *Client, ctx context.Context) RemoteSyncReport {
	t.Helper()
	report, err := client.PullToStable(ctx)
	require.NoError(t, err)
	return report
}

func mustSyncThenDetach(t *testing.T, client *Client, ctx context.Context) SyncThenDetachResult {
	t.Helper()
	result, err := client.SyncThenDetach(ctx)
	require.NoError(t, err)
	return result
}

func mustRebuild(t *testing.T, client *Client, ctx context.Context) RemoteSyncReport {
	t.Helper()
	report, err := client.Rebuild(ctx)
	require.NoError(t, err)
	return report
}
