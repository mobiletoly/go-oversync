package oversync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadiness_ActualExecutionAndAdmissionSessionsRequireOrigin(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, f.svc.Close(ctx))
	pc := f.pool.Config().Copy()
	pc.MaxConns = 1
	pair, err := NewReadinessPoolPair(ctx, pc, 1)
	require.NoError(t, err)
	defer pair.Close()
	execution := pair.ExecutionPool()
	admission := pair.AdmissionPool()
	sc := *f.svc.config
	rc := *sc.Readiness
	sc.Readiness = &rc
	rc.Adapter = progressAdmission{rc.Adapter}
	rc.PoolPair = pair
	svc, err := NewRuntimeService(execution, &sc, nil)
	require.NoError(t, err)
	defer svc.Close(ctx)
	require.NoError(t, svc.Bootstrap(ctx))
	_, err = execution.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS attestation_shadow; CREATE OR REPLACE FUNCTION attestation_shadow.current_setting(text) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT ''origin''::text'; SET search_path=attestation_shadow,pg_catalog`)
	require.NoError(t, err)
	_, err = admission.Exec(ctx, `SET session_replication_role=replica`)
	require.NoError(t, err)
	_, err = svc.beginOperation(ctx)
	require.Error(t, err)
	require.Zero(t, admission.Stat().AcquiredConns())
	_, err = admission.Exec(ctx, `SET session_replication_role=origin`)
	require.NoError(t, err)
	_, err = execution.Exec(ctx, `SET session_replication_role=replica`)
	require.NoError(t, err)
	_, _, err = svc.acquireUserUploadConn(ctx, "attestation")
	require.ErrorContains(t, err, "requires session_replication_role=origin")
	require.Zero(t, execution.Stat().AcquiredConns())
	// A fresh readiness inspection on the same initialized pool also fails closed.
	require.Error(t, svc.Bootstrap(ctx))
	_, err = execution.Exec(ctx, `SET session_replication_role=origin`)
	require.NoError(t, err)
	_, err = execution.Exec(ctx, `SET search_path=pg_catalog; DROP SCHEMA attestation_shadow CASCADE`)
	require.NoError(t, err)
	require.NoError(t, svc.Close(ctx))
}
