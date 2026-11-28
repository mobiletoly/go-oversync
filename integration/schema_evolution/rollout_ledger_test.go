package schemaevolution

import (
	"context"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRolloutLedger_DurableAllocationAndGuards(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	pool := fixturePool(t)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Apply(ctx, 1))
	cfg, err := Configuration(1, 1, false)
	require.NoError(t, err)
	ledger, err := a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(1), ledger.Revision)
	require.NoError(t, WithGenerationLedger(cfg, ledger))
	require.NoError(t, a.Publish(ctx, cfg))
	next := cfg.SchemaGenerations[0]
	next.ID = 3
	allocated, err := a.AllocateGeneration(ctx, 1, cfg.RegisteredTables, next)
	require.NoError(t, err)
	require.Equal(t, "allocated", allocated.Entries[1].State)
	_, err = a.AllocateGeneration(ctx, 2, cfg.RegisteredTables, next)
	require.Error(t, err)
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.generation_ledger SET fingerprint=repeat('f',64) WHERE generation_id=1`)
	require.ErrorContains(t, err, "immutable")
	_, err = a.conn.Exec(ctx, `DELETE FROM app_evolution.generation_ledger WHERE generation_id=1`)
	require.ErrorContains(t, err, "retained")
	// A different connection lacks the migration authority even with database-owner privileges.
	_, err = pool.Exec(ctx, `UPDATE app_evolution.generation_ledger SET state='retired',retired_revision=3 WHERE generation_id=1`)
	require.ErrorContains(t, err, "authority")
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=1,maximum_id=1 WHERE singleton`)
	require.ErrorContains(t, err, "immutable")
	_, err = pool.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=revision+1 WHERE singleton`)
	require.ErrorContains(t, err, "authority")
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	require.NoError(t, err)
	require.NoError(t, oversync.ValidateGenerationLedger(allocated, registry))
	abandoned, err := a.AbandonGeneration(ctx, allocated.Revision, allocated.Entries[1].Proof)
	require.NoError(t, err)
	require.Equal(t, "abandoned", abandoned.Entries[1].State)
	_, err = a.AllocateGeneration(ctx, abandoned.Revision, cfg.RegisteredTables, next)
	require.Error(t, err)
	next.ID = 4
	later, err := a.AllocateGeneration(ctx, abandoned.Revision, cfg.RegisteredTables, next)
	require.NoError(t, err)
	require.EqualValues(t, 4, later.Entries[2].Proof.ID)
	tx, err := a.conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `ALTER TABLE app_evolution.generation_ledger DISABLE TRIGGER generation_identity_guard`)
	require.NoError(t, err)
	_, err = readGenerationLedger(ctx, tx)
	require.ErrorContains(t, err, "guard_invalid")
	require.NoError(t, tx.Rollback(ctx))
	tx, err = a.conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `ALTER TABLE app_evolution.generation_ledger DISABLE TRIGGER generation_identity_guard;UPDATE app_evolution.generation_ledger SET declaration='[]' WHERE generation_id=1`)
	require.NoError(t, err)
	_, err = readGenerationLedger(ctx, tx)
	require.ErrorContains(t, err, "guard_invalid")
	require.NoError(t, tx.Rollback(ctx))
	_, err = a.PrepareGenerationLedger(ctx, cfg)
	require.NoError(t, err)
}
