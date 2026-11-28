package schemaevolution

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRolloutControllerProcessHelper(t *testing.T) {
	path := os.Getenv("ROLLOUT_OPERATION")
	if path == "" {
		t.Skip("controller process entry")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("ROLLOUT_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var input RolloutExecution
	require.NoError(t, json.Unmarshal(raw, &input))
	_, err = ExecuteTableRollout(ctx, pool, input, false)
	require.NoError(t, err)
	// The transaction has committed, but the caller never receives the result.
	os.Exit(97)
}

func TestRolloutController_DeathBeforeAndAfterCommit(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, _, a, ledger := rolloutServingFixture(t)
	defer a.Close()
	qualification := digest([]byte(t.Name()))
	cfg, err := RolloutTableConfiguration(RolloutTableLedger, ledger, qualification)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	retired := *ledger
	retired.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	retired.Revision++
	retired.Entries[0].State = "retired"
	retired.Entries[0].RetiredRevision = retired.Revision
	target, err := RolloutTableConfiguration(RolloutTableGenerationRetired, &retired, qualification)
	require.NoError(t, err)
	p := rolloutTestPlan(t, a, target, "retire_generation")
	p.Retiring = ledger.Entries[0].Proof
	p.TargetSelection = &RolloutTargetSelection{TargetStage: RolloutTableGenerationRetired, TargetLedger: &retired, QualificationSHA256: qualification}
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	input := RolloutExecution{Plan: p, Report: report, TargetStage: RolloutTableGenerationRetired, TargetLedger: &retired, QualificationSHA256: qualification}
	file := filepath.Join(t.TempDir(), "operation.json")
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(file, raw, 0600))
	binary := filepath.Join(t.TempDir(), "rollout")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/rollout")
	build.Env = append(os.Environ(), "GOTOOLCHAIN=go1.25.12")
	built, buildErr := build.CombinedOutput()
	require.NoError(t, buildErr, string(built))
	cli := func(args ...string) []byte {
		t.Helper()
		c := exec.CommandContext(ctx, binary, args...)
		c.Env = append(os.Environ(), "DATABASE_URL="+pool.Config().ConnString())
		output, err := c.CombinedOutput()
		require.NoError(t, err, string(output))
		return output
	}
	var observed RolloutReport
	require.NoError(t, json.Unmarshal(cli("report", "--plan", file, "--format", "json"), &observed))
	require.Equal(t, report.StateDigest, observed.StateDigest)
	require.Equal(t, report.PlanDigest, observed.PlanDigest)
	reportFile := filepath.Join(t.TempDir(), "report.json")
	reportBytes, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(reportFile, reportBytes, 0600))
	// A database-side test latch stops the real controller inside its transaction.
	_, err = a.conn.Exec(ctx, `CREATE FUNCTION app_evolution.m8_crash_latch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(1869833075,12);RETURN NEW;END $$;
 CREATE TRIGGER m8_crash_latch BEFORE UPDATE ON app_evolution.generation_ledger FOR EACH ROW EXECUTE FUNCTION app_evolution.m8_crash_latch()`)
	require.NoError(t, err)
	a.Close()
	latch, err := pgx.Connect(ctx, pool.Config().ConnString())
	require.NoError(t, err)
	defer latch.Close(ctx)
	_, err = latch.Exec(ctx, `SELECT pg_advisory_lock(1869833075,12)`)
	require.NoError(t, err)
	command := func() *exec.Cmd {
		c := exec.Command(os.Args[0], "-test.run=^TestRolloutControllerProcessHelper$")
		c.Env = append(os.Environ(), "ROLLOUT_OPERATION="+file, "ROLLOUT_DATABASE="+pool.Config().ConnString())
		return c
	}
	child := command()
	require.NoError(t, child.Start())
	t.Cleanup(func() {
		if child.Process != nil {
			_ = child.Process.Kill()
		}
	})
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=1869833075 AND objid=12 AND NOT granted)`).Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	_, err = latch.Exec(ctx, `SELECT pg_advisory_unlock(1869833075,12)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DROP TRIGGER m8_crash_latch ON app_evolution.generation_ledger;DROP FUNCTION app_evolution.m8_crash_latch()`)
	require.NoError(t, err)
	_, _, running, err := RolloutStatus(ctx, pool, p.ID)
	require.NoError(t, err)
	require.Equal(t, "running", running.State)
	actual, err := ReadRolloutLedger(ctx, pool)
	require.NoError(t, err)
	require.Equal(t, ledger, actual)
	output, err := command().CombinedOutput()
	require.Error(t, err, string(output))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 97, exit.ExitCode(), string(output))
	_, _, committed, err := RolloutStatus(ctx, pool, p.ID)
	require.NoError(t, err)
	require.Equal(t, "completed", committed.State)
	replay, err := ExecuteTableRollout(ctx, pool, input, true)
	require.NoError(t, err)
	require.Equal(t, committed, replay)
	for _, args := range [][]string{{"apply", "--plan", file, "--report", reportFile}, {"resume", "--operation-id", p.ID}} {
		var result RolloutResult
		require.NoError(t, json.Unmarshal(cli(args...), &result))
		require.Equal(t, committed, result)
	}
	require.Contains(t, string(cli("status", "--operation-id", p.ID)), "completed")
	changed := input
	changed.Plan.MaxAttempts++
	_, err = ExecuteTableRollout(ctx, pool, changed, true)
	require.Error(t, err)
	var closed bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT closed FROM app_evolution.barrier`).Scan(&closed))
	require.True(t, closed)
}
