//go:build oversync_audit

package oversync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAuditTransactionFault_CancelDuringDirectPushFinalizationRollsBackAndRetries(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	row := fixture.userRow(uuid.New(), "MetadataGate")
	const gateKey int64 = 732026071004
	_, err := fixture.pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION sync.audit_wait_before_bundle_log_insert() RETURNS trigger LANGUAGE plpgsql AS $function$
		BEGIN PERFORM pg_advisory_xact_lock(732026071004); RETURN NEW; END $function$;
		CREATE TRIGGER audit_wait_before_bundle_log_insert BEFORE INSERT ON sync.bundle_log FOR EACH ROW EXECUTE FUNCTION sync.audit_wait_before_bundle_log_insert()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS audit_wait_before_bundle_log_insert ON sync.bundle_log; DROP FUNCTION IF EXISTS sync.audit_wait_before_bundle_log_insert()`)
	})

	gate, err := fixture.pool.Acquire(ctx)
	require.NoError(t, err)
	defer gate.Release()
	_, err = gate.Exec(ctx, `SELECT pg_advisory_lock($1)`, gateKey)
	require.NoError(t, err)
	defer func() { _, _ = gate.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, gateKey) }()

	pushCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, pushErr := fixture.push(t, pushCtx, fixture.writer, 1, []PushRequestRow{row})
		done <- pushErr
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := fixture.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case pushErr := <-done:
		require.Error(t, pushErr)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled direct push did not return")
	}
	require.Zero(t, fixture.businessUserCount(t, ctx))
	var bundles, rows int
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundles))
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rows))
	require.Zero(t, bundles)
	require.Zero(t, rows)

	var unlocked bool
	require.NoError(t, gate.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, gateKey).Scan(&unlocked))
	require.True(t, unlocked)
	committed, err := fixture.push(t, ctx, fixture.writer, 1, []PushRequestRow{row})
	require.NoError(t, err)
	require.Equal(t, int64(1), committed.Bundle.BundleSeq)
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}
