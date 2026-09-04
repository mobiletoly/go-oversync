package schemaevolution

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func rolloutServingFixture(t *testing.T) (*pgxpool.Pool, DrainProof, *Authority, *oversync.GenerationLedger) {
	return rolloutServingFixtureShape(t, 3, 3)
}
func rolloutServingFixtureShape(t *testing.T, scopes, notes int, beforeDrain ...func(*pgxpool.Pool, *oversync.SyncService)) (*pgxpool.Pool, DrainProof, *Authority, *oversync.GenerationLedger) {
	t.Helper()
	ctx := context.Background()
	pool, old := tableBaseline(t)
	for i := 0; i < scopes; i++ {
		scope := fmt.Sprintf("retirement-scope-%d", i)
		_, err := CreateScope(ctx, pool, scope)
		require.NoError(t, err)
		require.NoError(t, fixtureWrite(t, old, scope, `WITH notes AS (INSERT INTO main.legacy_notes(_sync_scope_id,id,body) SELECT $1,gen_random_uuid(),'retained note' FROM generate_series(1,$2::int) RETURNING id) INSERT INTO main.people(_sync_scope_id,id,name) VALUES($1,$3,'Survivor')`, scope, notes, uuid.New()))

	}

	bounds := tableTestBounds()
	bounds.Rows = 100
	bounds.Deadline = time.Now().Add(5 * time.Minute)
	bounds.MaxBundles = 1000
	if scopes*notes >= 100000 {
		// Separate successful scale fixture: reconcile the previous failed fixture
		// independently; no retry mutates an existing run's finite capacity.
		bounds.MaxHistoryBytes = 128 << 20
		bounds.MaxEvidenceBytes = 16 << 20
		bounds.MaxBundles = 10000
	}
	proof, cleanup := tableCleanupStart(t, pool, old, bounds)
	backfill := tableBackfillStart(t, pool, proof, cleanup)
	worker := TableWorker{Pool: pool, Service: backfill, Capacity: tableTestCapacity}
	require.NoError(t, worker.Populate(ctx))
	require.NoError(t, backfill.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	tablePublish(t, a, TablesServing)
	servingConfig, err := TableEvolutionConfiguration(TablesServing)
	require.NoError(t, err)
	if len(beforeDrain) > 0 {
		servingConfig.SnapshotSessionTTL = 5 * time.Second
	}
	serving := rolloutTestRuntime(t, pool, servingConfig)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{serving}))
	for _, before := range beforeDrain {
		before(pool, serving)
	}
	proof, err = Drain(ctx, pool, []*oversync.SyncService{serving}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	cfg, err := TableEvolutionConfiguration(TablesServing)
	require.NoError(t, err)
	ledger, err := a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)
	return pool, proof, a, ledger
}

func rolloutTestRuntime(t *testing.T, pool *pgxpool.Pool, cfg *oversync.ServiceConfig) *oversync.SyncService {
	t.Helper()
	ctx := context.Background()
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, s.Bootstrap(ctx))
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func rolloutTestPlan(t *testing.T, a *Authority, target *oversync.ServiceConfig, action string) RolloutPlan {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	role := "m8_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err := a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` NOLOGIN`)
	require.NoError(t, err)
	// Cleanup uses the pool because authority may be closed before test teardown.
	pool := a.conn.Conn().Config().Copy()
	t.Cleanup(func() {
		c, err := pgx.ConnectConfig(ctx, pool)
		if err == nil {
			_, _ = c.Exec(ctx, `DROP ROLE `+pgx.Identifier{role}.Sanitize())
			_ = c.Close(ctx)
		}
	})
	hash, err := RolloutTargetDigest(target)
	require.NoError(t, err)
	p := RolloutPlan{Version: 1, ID: uuid.NewString(), Action: action, TargetSHA256: hash, Deadline: now.Add(90 * time.Second), MaxRows: 2, MaxBatches: 100, MaxAttempts: 100, QueryTimeout: 10 * time.Second, MaxEvidenceBytes: 1 << 20,
		Members: []RolloutMember{{Name: "server", Role: role, ProcessStartID: uuid.NewString(), ApplicationCodeSHA256: rolloutSourceDigest()}},
		Policy:  RetirementPolicy{Revision: "test-v1", OwnerApproval: "test-owner", ClientAdmissionPolicy: "table_evolution-preserving-v1", ReplacementAvailableAt: now.Add(-time.Hour), NoticeAt: now.Add(-time.Hour), SupportDuration: time.Minute, NoticeDuration: time.Minute, ChannelEvidence: []string{"installed-test-channel"}, OfflineIntentDisposition: "preserve_unresolved_no_retired_upload", BackupEvidence: digest([]byte("test-backup"))}}
	var policy string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT client_policy FROM app_evolution.maintenance WHERE singleton`).Scan(&policy))
	if policy == "" {
		policy = "rollout-test-preserving-v1"
		require.NoError(t, a.RequirePreservingClients(ctx, policy))
	}
	p.Policy.ClientAdmissionPolicy = policy
	if action == "retire_tables" {
		p.TableWritePolicy = "deny_and_revoke_atomically"
	}
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT current_database(),m.epoch,d.contract->>'AuthorityID',d.revision,d.contract->>'RegistryFingerprint',l.revision,s.catalog_revision,s.catalog_fingerprint FROM app_evolution.maintenance m CROSS JOIN app_evolution.deployment d CROSS JOIN app_evolution.generation_ledger_meta l CROSS JOIN sync.meta s WHERE m.singleton AND d.singleton AND l.singleton AND s.singleton_key`).Scan(&p.Database, &p.Epoch, &p.AuthorityID, &p.DeploymentRevision, &p.RegistryFingerprint, &p.LedgerRevision, &p.CatalogRevision, &p.CatalogFingerprint))
	return p
}

func TestRolloutTables_RetirementContractAndLostResponse(t *testing.T) {
	requireRolloutQualification(t)
	runRolloutTableJourney(t, 3, 3, 2, false)
}
func runRolloutTableJourney(t *testing.T, scopes, notes, batchRows int, fleet bool) {
	journeyStart := time.Now()
	ctx := context.Background()
	pool, proof, a, ledger := rolloutServingFixtureShape(t, scopes, notes)
	defer a.Close()
	qualification := digest([]byte("TestRolloutTables_RetirementContractAndLostResponse"))
	cfg, err := RolloutTableConfiguration(RolloutTableLedger, ledger, qualification)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	if fleet {
		proof = qualifyRolloutFleet(t, pool, a, cfg)
		a, err = OpenAuthority(ctx, proof)
		require.NoError(t, err)
	}
	a.Close()
	service := rolloutTestRuntime(t, pool, cfg)
	require.NoError(t, service.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	retired := *ledger
	retired.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	retired.Revision++
	retired.Entries[0].State = "retired"
	retired.Entries[0].RetiredRevision = retired.Revision
	cfg7, err := RolloutTableConfiguration(RolloutTableGenerationRetired, &retired, qualification)
	require.NoError(t, err)
	p := rolloutTestPlan(t, a, cfg7, "retire_generation")
	p.Retiring = ledger.Entries[0].Proof
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	result, err := a.ApplyRollout(ctx, p, report, cfg7, nil)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	replay, err := a.ApplyRollout(ctx, p, report, cfg7, nil)
	require.NoError(t, err)
	require.Equal(t, result, replay)
	// The tombstone is durable and can never become supported again.
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.generation_ledger SET state='supported',retired_revision=NULL WHERE generation_id=1`)
	require.ErrorContains(t, err, "immutable")
	cfg8, err := RolloutTableConfiguration(RolloutTableIdentityRetired, &retired, qualification)
	require.NoError(t, err)
	retiredWriter := "m8_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{retiredWriter}.Sanitize()+` NOLOGIN`)
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(ctx, `DROP OWNED BY `+pgx.Identifier{retiredWriter}.Sanitize())
		_, _ = pool.Exec(ctx, `DROP ROLE `+pgx.Identifier{retiredWriter}.Sanitize())
	}()
	_, err = a.conn.Exec(ctx, `GRANT INSERT(id),UPDATE(body),DELETE ON main.legacy_notes TO `+pgx.Identifier{retiredWriter}.Sanitize())
	require.NoError(t, err)
	tables := rolloutTestPlan(t, a, cfg8, "retire_tables")
	tables.TableIDs = []int32{1}
	missingWritePolicy := tables
	missingWritePolicy.TableWritePolicy = ""
	_, err = RolloutPlanDigest(missingWritePolicy)
	require.ErrorContains(t, err, "write_policy_required")
	cfg7.Readiness.DisableBackgroundWorkers = true
	require.NoError(t, WithCatalogRetirement(cfg7, tables))
	a.Close()
	private := rolloutTestRuntime(t, pool, cfg7)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	report, err = RolloutReportReadOnly(ctx, pool, tables)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	require.Contains(t, report.DependencyInventory, RolloutDependencyEvidence{Kind: "writer_role", Identity: retiredWriter, Disposition: "revoke_dml_atomically"})
	result, err = a.ApplyRollout(ctx, tables, report, cfg8, private)
	require.NoError(t, err)
	require.Equal(t, retiredNotesCatalogFingerprint(), result.Catalog.CatalogFingerprint)
	var retiredWritable bool
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT has_table_privilege($1,'main.legacy_notes','INSERT') OR has_table_privilege($1,'main.legacy_notes','UPDATE') OR has_table_privilege($1,'main.legacy_notes','DELETE')`, retiredWriter).Scan(&retiredWritable))
	require.False(t, retiredWritable)
	require.NoError(t, private.Close(ctx))
	a.Close()
	active := rolloutTestRuntime(t, pool, cfg8)
	require.NoError(t, active.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()

	// History references block row-state pruning before any deletion. Source state
	// is invariant across both explicit whole-prefix expiry and row-state pruning.
	var sourcesBefore string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY user_pk,source_id),'[]')::text FROM sync.source_state s`).Scan(&sourcesBefore))
	telemetry, err := ReadRolloutTelemetry(ctx, pool, nil)
	require.NoError(t, err)
	require.True(t, telemetry.Complete)
	require.NotNil(t, telemetry.HistoryBytes)
	require.Nil(t, telemetry.Process)
	t.Logf("rollout population rows=%d scopes=%d history_bytes=%d telemetry_query_duration=%s", scopes*notes, scopes, *telemetry.HistoryBytes, telemetry.QueryDuration)
	var mixed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log b WHERE EXISTS(SELECT 1 FROM sync.bundle_rows r WHERE r.user_pk=b.user_pk AND r.bundle_seq=b.bundle_seq AND r.table_id=1) AND EXISTS(SELECT 1 FROM sync.bundle_rows r WHERE r.user_pk=b.user_pk AND r.bundle_seq=b.bundle_seq AND r.table_id<>1)`).Scan(&mixed))
	require.Equal(t, scopes, mixed, "retention blocker must include whole mixed-table bundles")
	prune := rolloutTestPlan(t, a, cfg8, "prune_table_state")
	prune.TableIDs = []int32{1}
	prune.MaxRows = batchRows
	prune.MaxBatches = 1000
	prune.MaxAttempts = 1000
	prune.Deadline = time.Now().Add(5 * time.Minute)
	blocked, err := RolloutReportReadOnly(ctx, pool, prune)
	require.NoError(t, err)
	require.Contains(t, blocked.Blockers, "retained_history_requires_table_state")
	var retainedBefore string
	const survivingHistory = `SELECT COALESCE(jsonb_agg(to_jsonb(b) ORDER BY user_pk,bundle_seq),'[]')::text FROM sync.bundle_log b WHERE bundle_seq=(SELECT max(b2.bundle_seq) FROM sync.bundle_log b2 WHERE b2.user_pk=b.user_pk)`
	require.NoError(t, pool.QueryRow(ctx, survivingHistory).Scan(&retainedBefore))
	expiry := rolloutTestPlan(t, a, cfg8, "expire_history")
	expiry.TableIDs = []int32{1}
	expiry.RetentionWindow = 1
	expiry.MaxRows = max(8, batchRows)
	expiry.MaxBatches = 1000
	expiry.MaxAttempts = 1000
	expiry.Deadline = time.Now().Add(5 * time.Minute)
	expiryReport, err := RolloutReportReadOnly(ctx, pool, expiry)
	require.NoError(t, err)
	require.Empty(t, expiryReport.Blockers)
	a.Close()
	expiryInput := RolloutExecution{Plan: expiry, Report: expiryReport, TargetStage: RolloutTableIdentityRetired, TargetLedger: &retired, QualificationSHA256: qualification}
	runRolloutBatches(t, pool, expiryInput)
	var retainedAfter string
	require.NoError(t, pool.QueryRow(ctx, survivingHistory).Scan(&retainedAfter))
	require.Equal(t, retainedBefore, retainedAfter, "remaining canonical descriptors, hashes and counts are immutable")
	pruneReport, err := RolloutReportReadOnly(ctx, pool, prune)
	require.NoError(t, err)
	require.Empty(t, pruneReport.Blockers)
	pruneInput := RolloutExecution{Plan: prune, Report: pruneReport, TargetStage: RolloutTableIdentityRetired, TargetLedger: &retired, QualificationSHA256: qualification}
	pruned := runRolloutBatches(t, pool, pruneInput)
	require.EqualValues(t, scopes*notes, pruned.DeletedRows)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	var sourcesAfter string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY user_pk,source_id),'[]')::text FROM sync.source_state s`).Scan(&sourcesAfter))
	require.Equal(t, sourcesBefore, sourcesAfter)
	var maxRows, prunedScopes int
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT max((outcome->'Catalog'->>'DeletedRows')::int),count(DISTINCT outcome->'Catalog'->>'ScopeUserPK') FROM app_evolution.rollout_batches WHERE operation_id=$1`, prune.ID).Scan(&maxRows, &prunedScopes))
	require.LessOrEqual(t, maxRows, batchRows)
	require.Equal(t, scopes, prunedScopes)
	t.Logf("rollout scale rows=%d scopes=%d maximum_prune_batch=%d", scopes*notes, prunedScopes, maxRows)
	cfg9, err := RolloutTableConfiguration(RolloutTableContracted, &retired, qualification)
	require.NoError(t, err)
	contract := rolloutTestPlan(t, a, cfg9, "contract")
	identity := notesContractIdentity()
	contract.Contract = &identity
	contract.TableIDs = []int32{1}
	report, err = RolloutReportReadOnly(ctx, pool, contract)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	result, err = a.ApplyRollout(ctx, contract, report, cfg9, nil)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	var exists bool
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT to_regclass('main.legacy_notes') IS NOT NULL`).Scan(&exists))
	require.False(t, exists)
	var next int
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT next_table_id FROM sync.meta WHERE singleton_key`).Scan(&next))
	require.Equal(t, 5, next)
	a.Close()
	final := rolloutTestRuntime(t, pool, cfg9)
	require.EqualValues(t, 3, final.Readiness().CatalogRevision)
	require.NoError(t, final.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	nextGeneration := cfg9.SchemaGenerations[0]
	nextGeneration.ID = 3
	allocated, err := a.AllocateGeneration(ctx, retired.Revision, cfg9.RegisteredTables, nextGeneration)
	require.NoError(t, err)
	enabled := *allocated
	enabled.Entries = append([]oversync.GenerationLifecycle(nil), allocated.Entries...)
	enabled.Revision++
	enabled.Entries[len(enabled.Entries)-1].State = "supported"
	successor, err := RolloutSuccessorConfiguration(cfg9, nextGeneration, &enabled, 10, qualification)
	require.NoError(t, err)
	activate := rolloutTestPlan(t, a, successor, "enable_generation")
	activate.Activating = enabled.Entries[len(enabled.Entries)-1].Proof
	activationReport, err := RolloutReportReadOnly(ctx, pool, activate)
	require.NoError(t, err)
	require.Empty(t, activationReport.Blockers)
	_, err = a.ApplyRollout(ctx, activate, activationReport, successor, nil)
	require.NoError(t, err)
	a.Close()
	later := rolloutTestRuntime(t, pool, successor)
	require.Equal(t, enabled.Revision, later.Readiness().GenerationLedgerRevision)

	telemetry, err = ReadRolloutTelemetry(ctx, pool, later)
	require.NoError(t, err)
	require.NotNil(t, telemetry.PhaseDuration)
	t.Logf("rollout completed total=%s phase_query_duration_ns=%d authority_wait_ns=%d prune_rows=%d evidence_bytes=%d", time.Since(journeyStart), *telemetry.PhaseDuration, *telemetry.AuthorityLockWait, *telemetry.PrunedRows, *telemetry.EvidenceBytes)

}

func runRolloutBatches(t *testing.T, pool *pgxpool.Pool, input RolloutExecution) RolloutResult {
	t.Helper()
	var result RolloutResult
	for i := 0; i < input.Plan.MaxBatches; i++ {
		var err error
		result, err = ExecuteTableRollout(context.Background(), pool, input, i > 0)
		require.NoError(t, err)
		if result.State == "completed" {
			return result
		}
	}
	t.Fatal("rollout did not finish within its declared batch budget")
	return result
}
