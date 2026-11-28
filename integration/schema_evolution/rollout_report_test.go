package schemaevolution

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRolloutReport_FenceAndDurableIntent(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, service := tableBaseline(t)
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	cfg, err := TableEvolutionConfiguration(TablesBaseline)
	require.NoError(t, err)
	require.NoError(t, a.RequirePreservingClients(ctx, "preserving-v1"))
	ledger, err := a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)

	role := "m8_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` NOLOGIN`)
	require.NoError(t, err)
	defer func() { _, _ = pool.Exec(ctx, `DROP ROLE `+pgx.Identifier{role}.Sanitize()) }()
	now := time.Now().UTC()
	targetHash, err := RolloutTargetDigest(cfg)
	require.NoError(t, err)
	p := RolloutPlan{Version: 1, ID: uuid.NewString(), Action: "prepare", AuthorityID: cfg.Readiness.Expected.AuthorityID,
		DeploymentRevision: cfg.Readiness.Expected.Revision, LedgerRevision: ledger.Revision, RegistryFingerprint: cfg.Readiness.Expected.RegistryFingerprint,
		TargetSHA256: targetHash, Deadline: now.Add(time.Minute), MaxRows: 2, MaxBatches: 100, MaxAttempts: 100, QueryTimeout: 10 * time.Second, MaxEvidenceBytes: 1 << 20,
		Members: []RolloutMember{{Name: "server", Role: role, ProcessStartID: "stopped-instance", ApplicationCodeSHA256: strings.Repeat("a", 64)}},
		Policy:  RetirementPolicy{Revision: "v1", OwnerApproval: "test-owner", ClientAdmissionPolicy: "preserving-v1", ReplacementAvailableAt: now.Add(-time.Hour), NoticeAt: now.Add(-time.Hour), SupportDuration: time.Minute, NoticeDuration: time.Minute, ChannelEvidence: []string{"test-installed"}, OfflineIntentDisposition: "preserve_unresolved_no_retired_upload", BackupEvidence: strings.Repeat("b", 64)}}
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT current_database(),epoch FROM app_evolution.maintenance WHERE singleton`).Scan(&p.Database, &p.Epoch))
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT catalog_revision,catalog_fingerprint FROM sync.meta WHERE singleton_key`).Scan(&p.CatalogRevision, &p.CatalogFingerprint))
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.True(t, report.Complete)
	require.Empty(t, report.Blockers)
	require.Equal(t, []string{"prepare"}, report.TargetOperations)
	require.True(t, report.PolicyEligible)
	require.NotEmpty(t, report.BackupEvidenceSHA256)
	require.GreaterOrEqual(t, len(report.Predicates), 8)
	for _, predicate := range report.Predicates {
		require.True(t, predicate.Complete, predicate.Name)
		require.NotEmpty(t, predicate.Source, predicate.Name)
	}
	var n int
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT count(*) FROM app_evolution.rollout_operations`).Scan(&n))
	require.Zero(t, n)
	// A writer omitted from the process list is still discovered from actual grants.
	writer := "m8_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{writer}.Sanitize()+` LOGIN`)
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(ctx, `DROP OWNED BY `+pgx.Identifier{writer}.Sanitize())
		_, _ = pool.Exec(ctx, `DROP ROLE `+pgx.Identifier{writer}.Sanitize())
	}()
	_, err = a.conn.Exec(ctx, `GRANT UPDATE ON main.people TO `+pgx.Identifier{writer}.Sanitize())
	require.NoError(t, err)
	blocked, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Contains(t, blocked.Blockers, "unfenced_database_writer")
	_, err = a.conn.Exec(ctx, `ALTER ROLE `+pgx.Identifier{writer}.Sanitize()+` NOLOGIN`)
	require.NoError(t, err)
	fresh, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, fresh.Blockers)
	require.NotEqual(t, report.StateDigest, fresh.StateDigest)
	planHash, err := RolloutPlanDigest(p)
	require.NoError(t, err)
	var result RolloutResult
	_, err = a.reserveRollout(ctx, p, planHash, report, &result)
	require.ErrorContains(t, err, "stale")
	completed, err := a.reserveRollout(ctx, p, planHash, fresh, &result)
	require.NoError(t, err)
	require.False(t, completed)
	// A failed phase does not erase its already committed identity or deadline.
	tx, err := a.conn.Begin(ctx)
	require.NoError(t, err)
	_, _, err = loadRolloutAttempt(ctx, tx, p, planHash, &result)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))
	completed, err = a.reserveRollout(ctx, p, planHash, fresh, &result)
	require.NoError(t, err)
	require.False(t, completed)
	changed := p
	changed.Deadline = changed.Deadline.Add(time.Minute)
	changedHash, err := RolloutPlanDigest(changed)
	require.NoError(t, err)
	_, err = a.reserveRollout(ctx, changed, changedHash, fresh, &result)
	require.ErrorContains(t, err, "identity_conflict")
	tampered := fresh
	tampered.Predicates = slices.Clone(fresh.Predicates)
	tampered.Predicates[0].Source = ""
	_, err = a.ApplyRollout(ctx, p, tampered, cfg, nil)
	require.ErrorContains(t, err, "report")
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.rollout_operations SET plan='{}' WHERE operation_id=$1`, p.ID)
	require.ErrorContains(t, err, "immutable")
	_, err = a.conn.Exec(ctx, `DELETE FROM app_evolution.rollout_operations WHERE operation_id=$1`, p.ID)
	require.ErrorContains(t, err, "retained")
}
