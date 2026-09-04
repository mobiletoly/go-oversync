// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestClient_RejectsOverlappingSyncOperations(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)

	userID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, "Ada", "ada@example.com")
	require.NoError(t, err)

	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}

	entered := make(chan struct{})
	release := make(chan struct{})
	client.SetBeforePushCommitHook(func(context.Context) error {
		close(entered)
		<-release
		return nil
	})

	firstErr := make(chan error, 1)
	go func() {
		_, err := client.PushPending(ctx)
		firstErr <- err
	}()

	<-entered

	cases := []struct {
		name string
		op   func() error
	}{
		{
			name: "push rejects overlap",
			op: func() error {
				_, err := client.PushPending(ctx)
				return err
			},
		},
		{
			name: "pull rejects overlap",
			op: func() error {
				_, err := client.PullToStable(ctx)
				return err
			},
		},
		{
			name: "hydrate rejects overlap",
			op: func() error {
				_, err := client.Rebuild(ctx)
				return err
			},
		},
		{
			name: "recover rejects overlap",
			op: func() error {
				_, err := client.Rebuild(ctx)
				return err
			},
		},
		{
			name: "sync rejects overlap",
			op: func() error {
				_, err := client.Sync(ctx)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op()
			var inProgressErr *SyncOperationInProgressError
			require.ErrorAs(t, err, &inProgressErr)
		})
	}

	close(release)
	require.NoError(t, <-firstErr)
}

func TestIsExpectedSyncContention(t *testing.T) {
	t.Parallel()

	require.False(t, IsExpectedSyncContention(nil))
	require.True(t, IsExpectedSyncContention(&SyncOperationInProgressError{}))
	require.True(t, IsExpectedSyncContention(fmt.Errorf("wrapped: %w", &SyncOperationInProgressError{})))
	require.False(t, IsExpectedSyncContention(&PendingPushReplayError{OutboundCount: 3}))
	require.False(t, IsExpectedSyncContention(context.Canceled))
}
