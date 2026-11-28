package schemaevolution

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRolloutPolicy_EligibilityAndFinitePause(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool, _, a, ledger := rolloutServingFixture(t)
	defer a.Close()
	cfg, err := RolloutTableConfiguration(RolloutTableLedger, ledger, digest([]byte(t.Name())))
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	base := rolloutTestPlan(t, a, cfg, "prepare")
	for _, tc := range []struct {
		name string
		edit func(*RetirementPolicy)
	}{
		{"missing", func(p *RetirementPolicy) { *p = RetirementPolicy{} }},
		{"no channels", func(p *RetirementPolicy) { p.ChannelEvidence = nil }},
		{"unavailable channel", func(p *RetirementPolicy) { p.ChannelEvidence = []string{""} }},
		{"support window", func(p *RetirementPolicy) { p.SupportDuration = 24 * time.Hour }},
		{"notice window", func(p *RetirementPolicy) { p.NoticeDuration = 24 * time.Hour }},
		{"delayed replacement", func(p *RetirementPolicy) { p.ReplacementAvailableAt = time.Now().Add(time.Hour) }},
		{"offline acknowledgment", func(p *RetirementPolicy) { p.OfflineIntentDisposition = "discard" }},
		{"rollback hold", func(p *RetirementPolicy) { p.RollbackHolds = []string{"retained-compatible-binary"} }},
		{"invalid backup identity", func(p *RetirementPolicy) { p.BackupEvidence = "unverified-label" }},
		{"independent admission", func(p *RetirementPolicy) { p.ClientAdmissionPolicy = "self-reported-header" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			p.ID = uuid.NewString()
			tc.edit(&p.Policy)
			report, err := RolloutReportReadOnly(ctx, pool, p)
			require.NoError(t, err)
			require.NotEmpty(t, report.Blockers)
			_, err = a.ApplyRollout(ctx, p, report, cfg, nil)
			require.Error(t, err)
		})
	}
	// A reserved operation cannot obtain fresh attempts or a later deadline.
	p := base
	p.ID = uuid.NewString()
	p.MaxAttempts = 1
	report, err := RolloutReportReadOnly(ctx, pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	hash, err := RolloutPlanDigest(p)
	require.NoError(t, err)
	var result RolloutResult
	_, err = a.reserveRollout(ctx, p, hash, report, &result)
	require.NoError(t, err)
	_, err = a.reserveRollout(ctx, p, hash, report, &result)
	require.ErrorContains(t, err, "budget")
	stored, _, _, err := RolloutStatus(ctx, pool, p.ID)
	require.NoError(t, err)
	require.Equal(t, p.Deadline, stored.Deadline)
	require.Equal(t, 1, stored.MaxAttempts)
	tampered := report
	tampered.Counts.RowState++
	_, err = a.ApplyRollout(ctx, p, tampered, cfg, nil)
	require.ErrorContains(t, err, "report")
	expired := base
	expired.ID = uuid.NewString()
	expired.Deadline = time.Now().Add(-time.Second)
	blocked, err := RolloutReportReadOnly(ctx, pool, expired)
	require.NoError(t, err)
	require.Contains(t, blocked.Blockers, "plan_deadline_exceeded")
	tiny := base
	tiny.MaxEvidenceBytes = 1024
	_, err = RolloutReportReadOnly(ctx, pool, tiny)
	require.ErrorContains(t, err, "capacity")
	var closed bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT closed FROM app_evolution.barrier`).Scan(&closed))
	require.True(t, closed)
	actual, err := ReadRolloutLedger(ctx, pool)
	require.NoError(t, err)
	require.Equal(t, ledger, actual)
}
