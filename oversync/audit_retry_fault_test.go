//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestAuditRetryFault_PoolExhaustionBeforeDirectPushLeavesRetryableState(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "pool_exhaustion", 1, 0)
	actor := fixture.actor("pool-exhaustion", "writer")
	mustInitializeEmptyScope(t, ctx, fixture.services[0], actor.UserID, actor.SourceID)
	row := fixture.userRow(t, uuid.New(), "Pool Retry")
	request := fixture.directPushRequest(t, ctx, fixture.services[0], actor, row)
	held, err := fixture.servicePools[0].Acquire(ctx)
	require.NoError(t, err)
	heldReleased := false
	t.Cleanup(func() {
		if !heldReleased {
			held.Release()
		}
	})
	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = fixture.services[0].PushOneShot(waitCtx, actor, testSchemaGenerationProofForService(fixture.services[0]), request)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	fixture.requireBusinessRowCount(t, ctx, actor.UserID, 0)
	held.Release()
	heldReleased = true
	response, err := fixture.services[0].PushOneShot(ctx, actor, testSchemaGenerationProofForService(fixture.services[0]), request)
	require.NoError(t, err)
	require.NotNil(t, response)
	bundle := response.Bundle
	fixture.requireBusinessRowCount(t, ctx, actor.UserID, 1)
	requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actor, 1, bundle.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditRetryFault_ServerReportedRetryableTransactionFailureCommitsDirectPushOnce(t *testing.T) {
	for _, testCase := range []struct{ name, code, message string }{
		{name: "serialization_failure", code: "40001", message: "serialization failure"},
		{name: "deadlock_detected", code: "40P01", message: "deadlock detected"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newAuditMultiServiceFixture(t, ctx, "retry_"+testCase.code, 2, 0)
			actor := fixture.actor("retryable-"+testCase.code, "writer")
			mustInitializeEmptyScope(t, ctx, fixture.services[0], actor.UserID, actor.SourceID)
			sequence := installAuditRetryOnceTrigger(t, ctx, fixture, testCase.code, testCase.message)
			bundle, err := pushRowsOneShot(t, ctx, fixture.services[0], actor, 1, []PushRequestRow{fixture.userRow(t, uuid.New(), "Retry "+testCase.code)})
			require.NoError(t, err)
			require.Equal(t, int64(1), bundle.BundleSeq)
			var lastValue int64
			var called bool
			require.NoError(t, fixture.controlPool.QueryRow(ctx, fmt.Sprintf(`SELECT last_value, is_called FROM %s`, sequence)).Scan(&lastValue, &called))
			require.True(t, called)
			require.Equal(t, int64(2), lastValue)
			fixture.requireBusinessRowCount(t, ctx, actor.UserID, 1)
			requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actor, 1, bundle.BundleHash)
			requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
		})
	}
}

func installAuditRetryOnceTrigger(t *testing.T, ctx context.Context, fixture *auditMultiServiceFixture, code, message string) string {
	t.Helper()
	sequence := pgx.Identifier{fixture.schemaName, "audit_retry_sequence_" + code}.Sanitize()
	function := pgx.Identifier{fixture.schemaName, "audit_retry_function_" + code}.Sanitize()
	table := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
	trigger := pgx.Identifier{"audit_retry_trigger_" + code}.Sanitize()
	_, err := fixture.controlPool.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE %s START WITH 1`, sequence))
	require.NoError(t, err)
	_, err = fixture.controlPool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $audit$
		BEGIN
			IF nextval('%s'::regclass) = 1 THEN RAISE EXCEPTION 'audit injected %s' USING ERRCODE = '%s'; END IF;
			RETURN NEW;
		END
		$audit$`, function, sequence, message, code))
	require.NoError(t, err)
	_, err = fixture.controlPool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()`, trigger, table, function))
	require.NoError(t, err)
	return sequence
}
