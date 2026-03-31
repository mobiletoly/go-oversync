package oversqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustOpen(t *testing.T, client *Client, ctx context.Context, sourceID string) OpenResult {
	t.Helper()
	result, err := client.Open(ctx, sourceID)
	require.NoError(t, err)
	return result
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

func mustRebuild(t *testing.T, client *Client, ctx context.Context, mode RebuildMode, newSourceID string) RemoteSyncReport {
	t.Helper()
	report, err := client.Rebuild(ctx, mode, newSourceID)
	require.NoError(t, err)
	return report
}

func mustRotateSource(t *testing.T, client *Client, ctx context.Context, sourceID string) SourceRotationResult {
	t.Helper()
	result, err := client.RotateSource(ctx, sourceID)
	require.NoError(t, err)
	return result
}
