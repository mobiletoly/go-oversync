package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// The first retained bundle is valid server state but violates an admitted local
// unique index. A later authoritative state is installable without changing that
// index or any application values to make the historical bundle pass.
func TestServerWriteRecovery_CleanLaterHistoricalState(t *testing.T) {
	authority, err := NewTestServer(&ServerConfig{SchemaGenerations: scopeClientGeneration("users")})
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	ctx := context.Background()
	scope, left, right := uuid.NewString(), uuid.NewString(), uuid.NewString()
	db, client := newUsersClientForTest(t, authority, scope)
	insertRecoveryUser(t, db, left, "left", "left@example.invalid")
	insertRecoveryUser(t, db, right, "right", "right@example.invalid")
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE UNIQUE INDEX local_user_name ON users(name)`)
	require.NoError(t, err)
	sourceProgress := recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`)
	bad := reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `UPDATE business.users SET name='collision' WHERE _sync_scope_id=$1`, scope)
		return err
	})
	good := reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `UPDATE business.users SET name=CASE WHEN id=$2 THEN 'right' ELSE 'left' END WHERE _sync_scope_id=$1`, scope, left)
		return err
	})
	require.Greater(t, good.Bundle.BundleSeq, bad.Bundle.BundleSeq)
	var creates, acks, pushes int
	var installed oversync.SnapshotSession
	var afterSnapshot int64
	client.HTTP = &http.Client{Transport: recoveryHTTP(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "v1", request.Header.Get("Oversync-Protocol-Version"))
		t.Logf("HTTP identity route=%s source=%s generation=%s fingerprint=%s", request.URL.Path,
			request.Header.Get("Oversync-Source-ID"), request.Header.Get("Oversync-Schema-Generation-ID"), request.Header.Get("Oversync-Schema-Generation-Fingerprint"))
		switch request.URL.Path {
		case "/sync/push":
			pushes++
		case "/sync/push-outcomes/ack":
			acks++
		}
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil || request.Method != http.MethodPost || request.URL.Path != "/sync/snapshot-sessions" {
			return response, err
		}
		creates++
		require.NoError(t, json.Unmarshal(readSnapshotFixtureResponse(t, response), &installed))
		afterSnapshot = reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, `UPDATE business.users SET name='post-T' WHERE _sync_scope_id=$1 AND id=$2`, scope, left)
			return err
		}).Bundle.BundleSeq
		return response, nil
	})}
	report, err := client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
	require.Equal(t, good.Bundle.BundleSeq, installed.SnapshotBundleSeq)
	require.Len(t, installed.SnapshotHash, 64)
	require.NotNil(t, installed.Recovery)
	require.GreaterOrEqual(t, installed.SnapshotBundleSeq, installed.Recovery.MinimumBundleSeq)
	checkpoint, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, afterSnapshot, checkpoint, "ordinary pull consumes the write after sealed T")
	status, err := client.RecoveryStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, "completed", status.Phase)
	require.Equal(t, installed.SnapshotBundleSeq, status.ObservedBoundary)
	require.Equal(t, sourceProgress, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Equal(t, 1, creates)
	require.Zero(t, acks)
	require.Zero(t, pushes)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, left).Scan(&name))
	require.Equal(t, "post-T", name)
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, right).Scan(&name))
	require.Equal(t, "left", name)
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_upload_journal", "_sync_snapshot_stage"} {
		require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM `+table), table)
	}
	require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
	t.Logf("RECOVERY_HTTP_COMPLETE bad=%d snapshot_T=%d snapshot_hash=%s post_T=%d vector=%s", bad.Bundle.BundleSeq,
		installed.SnapshotBundleSeq, installed.SnapshotHash, afterSnapshot, recoveryLocalVector(t, db))
}
