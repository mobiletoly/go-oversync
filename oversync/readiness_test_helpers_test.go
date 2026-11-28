package oversync

import (
	"context"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type readinessAdapterFunc func(context.Context, DatabaseReadinessReader) (ReadinessEvidence, error)

func (f readinessAdapterFunc) Inspect(ctx context.Context, r DatabaseReadinessReader) (ReadinessEvidence, error) {
	return f(ctx, r)
}

// Older owning-package fixtures exercise the managed contract in isolation.
// Their explicit test adapter supplies baseline application evidence; transition
// tests use NewRuntimeService directly and real application ledgers.
func newTestRuntimeService(pool *pgxpool.Pool, c *ServiceConfig, l *slog.Logger) (*SyncService, error) {
	s, err := NewRuntimeService(pool, c, l)
	if err != nil {
		return nil, err
	}
	if s.config.Readiness == nil {
		s.config.Readiness = baselineTestReadiness(s.schemaGenerationRegistry)
	}
	return s, nil
}

func baselineTestReadiness(r *SchemaGenerationRegistry) *ReadinessConfig {
	d := DeploymentContract{AuthorityID: "owning-package-fixture", Revision: 1, RegistryFingerprint: r.Fingerprint(), TransitionRevision: "baseline", TransitionPhase: "baseline", ApplicationCodeSHA256: strings.Repeat("0", 64), QualificationSHA256: strings.Repeat("1", 64)}
	c := &ReadinessConfig{AuthorityLock: [2]int32{1869833075, 6}, Expected: d}
	e := ReadinessEvidence{Deployment: d}
	for _, id := range r.GenerationIDs() {
		contract, _ := r.Generation(id)
		g := GenerationReadiness{Proof: SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}}
		c.Generations = append(c.Generations, g)
		e.Generations = append(e.Generations, GenerationEnablement{Requirement: g, DeploymentRevision: 1, Enabled: true})
	}
	c.Adapter = readinessAdapterFunc(func(context.Context, DatabaseReadinessReader) (ReadinessEvidence, error) { return e, nil })
	return c
}
