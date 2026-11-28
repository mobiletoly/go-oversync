package schemaevolution

import (
	"context"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRolloutColumn_RetirementAndRestrictedContraction(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, service, id := fixtureMaintaining(t)
	require.NoError(t, fixtureWorker(pool, service).Run(ctx))
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Certify(ctx, 3))
	require.NoError(t, a.Apply(ctx, 4))
	require.NoError(t, a.Certify(ctx, 4))
	coexist, err := Configuration(4, 3, true)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, coexist))
	a.Close()
	service = bootstrapFixture(t, pool, coexist)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	proof, err = Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	ledger, err := a.PrepareGenerationLedger(ctx, coexist)
	require.NoError(t, err)
	qualification := digest([]byte("TestRolloutColumn_RetirementAndRestrictedContraction"))
	cfg, err := RolloutColumnConfiguration(RolloutColumnLedger, ledger, qualification)
	require.NoError(t, err)
	prepare := rolloutTestPlan(t, a, cfg, "prepare")
	preparationReport, err := RolloutReportReadOnly(ctx, pool, prepare)
	require.NoError(t, err)
	require.Empty(t, preparationReport.Blockers)
	_, err = a.ApplyRollout(ctx, prepare, preparationReport, cfg, nil)
	require.NoError(t, err)
	a.Close()
	service = rolloutTestRuntime(t, pool, cfg)
	require.NoError(t, service.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	retired := *ledger
	retired.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	retired.Revision++
	retired.Entries[0].State = "retired"
	retired.Entries[0].RetiredRevision = retired.Revision
	target, err := RolloutColumnConfiguration(RolloutColumnGenerationRetired, &retired, qualification)
	require.NoError(t, err)
	p := rolloutTestPlan(t, a, target, "retire_generation")
	p.Retiring = ledger.Entries[0].Proof
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	_, err = a.ApplyRollout(ctx, p, report, target, nil)
	require.NoError(t, err)
	contracted, err := RolloutColumnConfiguration(RolloutColumnContracted, &retired, qualification)
	require.NoError(t, err)
	contract := rolloutTestPlan(t, a, contracted, "contract")
	identity := personContractIdentity()
	contract.Contract = &identity
	contract.Columns = []string{"main.upgrade_people.legacy_name"}
	report, err = RolloutReportReadOnly(ctx, pool, contract)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	result, err := a.ApplyRollout(ctx, contract, report, contracted, nil)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	var name, label string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT name,new_label FROM main.upgrade_people WHERE id=$1`, id).Scan(&name, &label))
	require.Equal(t, []string{"A", "B"}, []string{name, label})
	a.Close()
	fresh := rolloutTestRuntime(t, pool, contracted)
	require.True(t, fresh.Readiness().Ready)
}
