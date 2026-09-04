package schemaevolution

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRolloutRecovery_FencedArchiveAndReturningReceipts(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	earlierFile, earlierCut, earlierDigest := captureEarlierArchive(t, pool, old)
	old = tableTestService(t, pool, TablesBaseline)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{old}))
	staged := tablePushRequest(t, old, 1)
	committed, err := old.PushOneShot(ctx, staged.actor, staged.proof, &staged.request)
	require.NoError(t, err)
	// The simulated lost reply is deliberately not settled by a replacement source.
	observer := staged.actor
	observer.SourceID = "checkpoint-observer"
	_, err = old.Connect(ctx, observer, staged.proof, &oversync.ConnectRequest{})
	require.NoError(t, err)
	checkpoint, err := old.ProcessPull(ctx, observer, staged.proof, 0, 100, 0)
	require.NoError(t, err)
	proof, err := Drain(ctx, pool, []*oversync.SyncService{old}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	cfg, err := TableEvolutionConfiguration(TablesBaseline)
	require.NoError(t, err)
	ledger, err := a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)
	p := rolloutTestPlan(t, a, cfg, "restore")
	require.NotEmpty(t, fixtureServer.container, "Docker reference archive lane requires its managed PostgreSQL container")
	driver := DockerPostgresArchive{Container: fixtureServer.container, Database: p.Database, User: "postgres"}
	archive, err := os.CreateTemp(t.TempDir(), "cut-*.dump")
	require.NoError(t, err)
	cut, err := a.CaptureRecoveryCut(ctx, p, "terminal-cut", 64<<20, archive, driver)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	externalDigest, err := RecoveryCutDigest(cut)
	require.NoError(t, err)
	a.Close()
	// A real earlier archive has the same generation and deployment, but lacks
	// this upload's receipt and the client's observed checkpoint.
	require.Equal(t, cut.Ledger, earlierCut.Ledger)
	restoreFixtureArchive(t, pool, driver, earlierFile, earlierCut, earlierDigest)
	var earlierDeployment int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT revision FROM app_evolution.deployment`).Scan(&earlierDeployment))
	require.Equal(t, p.DeploymentRevision, earlierDeployment)
	var absent int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.source_state WHERE source_id=$1`, staged.actor.SourceID).Scan(&absent))
	require.Zero(t, absent)
	require.ErrorContains(t, ReopenTableServing(ctx, pool, nil), "recovery_verification_required")
	for attempt := 0; attempt < 2; attempt++ {
		_, err = VerifyRecoveredCut(ctx, pool, cut, externalDigest, earlierCut.ArchiveSHA256, ledger, p)
		require.Error(t, err)
		// Even supplying the latest archive's label cannot make old DB state pass.
		_, err = VerifyRecoveredCut(ctx, pool, cut, externalDigest, cut.ArchiveSHA256, ledger, p)
		require.Error(t, err)
	}
	var stillAbsent int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.source_state WHERE source_id=$1`, staged.actor.SourceID).Scan(&stillAbsent))
	require.Equal(t, absent, stillAbsent)
	restoreFixtureArchive(t, pool, driver, archive.Name(), cut, externalDigest)
	require.NoError(t, PrepareRecoveredCatalog(ctx, pool, cut, externalDigest, p))
	verification, err := VerifyRecoveredCut(ctx, pool, cut, externalDigest, cut.ArchiveSHA256, ledger, p)
	require.NoError(t, err)
	require.Equal(t, externalDigest, verification.CutSHA256)
	verificationDigest, err := RecoveryVerificationDigest(verification)
	require.NoError(t, err)
	replayedVerification, err := VerifyRecoveredCut(ctx, pool, cut, externalDigest, cut.ArchiveSHA256, ledger, p)
	require.NoError(t, err)
	require.Equal(t, verification, replayedVerification)
	var durableDigest string
	require.NoError(t, pool.QueryRow(ctx, `SELECT verification_sha256 FROM app_evolution.recovery_gate WHERE singleton AND state='verified'`).Scan(&durableDigest))
	require.Equal(t, verificationDigest, durableDigest)
	reopenPlan := p
	reopenPlan.Action = "reopen"
	reopenPlan.RecoveryCutSHA256 = externalDigest
	reopenPlan.RecoveryResultSHA256 = verificationDigest
	require.NoError(t, validateRecoveryAdmission(ctx, pool, &reopenPlan))
	wrongReopen := reopenPlan
	wrongReopen.RecoveryResultSHA256 = strings.Repeat("0", 64)
	require.ErrorContains(t, validateRecoveryAdmission(ctx, pool, &wrongReopen), "identity_mismatch")
	fresh := tableTestService(t, pool, TablesBaseline)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{fresh}))
	replay, err := fresh.PushOneShot(ctx, staged.actor, staged.proof, &staged.request)
	require.NoError(t, err)
	require.Equal(t, committed.Bundle.BundleSeq, replay.Bundle.BundleSeq)
	resumed, err := fresh.ProcessPull(ctx, observer, staged.proof, checkpoint.StableBundleSeq, 100, 0)
	require.NoError(t, err)
	require.Equal(t, checkpoint.StableBundleSeq, resumed.StableBundleSeq)
}

func captureEarlierArchive(t *testing.T, pool *pgxpool.Pool, old *oversync.SyncService) (string, RecoveryCut, string) {
	t.Helper()
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{old}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	cfg, err := TableEvolutionConfiguration(TablesBaseline)
	require.NoError(t, err)
	_, err = a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)
	p := rolloutTestPlan(t, a, cfg, "restore")
	archive, err := os.CreateTemp(t.TempDir(), "earlier-*.dump")
	require.NoError(t, err)
	driver := DockerPostgresArchive{Container: fixtureServer.container, Database: p.Database, User: "postgres"}
	cut, err := a.CaptureRecoveryCut(ctx, p, "earlier-cut", 64<<20, archive, driver)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	hash, err := RecoveryCutDigest(cut)
	require.NoError(t, err)
	return archive.Name(), cut, hash
}
func restoreFixtureArchive(t *testing.T, pool *pgxpool.Pool, driver DockerPostgresArchive, path string, cut RecoveryCut, hash string) {
	t.Helper()
	ctx := context.Background()
	pool.Reset()
	admin, err := pgx.Connect(ctx, fixtureDSN(t))
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `DROP DATABASE `+pgx.Identifier{cut.Database}.Sanitize()+` WITH (FORCE)`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{cut.Database}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, admin.Close(ctx))
	input, err := os.Open(path)
	require.NoError(t, err)
	require.NoError(t, RestoreRecoveryArchive(ctx, driver, input, cut, hash))
	require.NoError(t, input.Close())
}
