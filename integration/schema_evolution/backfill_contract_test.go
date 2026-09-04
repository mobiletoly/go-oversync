package schemaevolution

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_DescriptorReplayAndReceiptPruning(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	worker := fixtureWorker(pool, service)
	require.NoError(t, worker.Run(ctx))
	var d BatchDescriptor
	var stored []byte
	var hash string
	require.NoError(t, pool.QueryRow(ctx, `SELECT spec,descriptor,hash FROM app_evolution.batches ORDER BY ordinal LIMIT 1`).Scan(&d, &stored, &hash))
	encoded, err := d.CanonicalBytes()
	require.NoError(t, err)
	require.Equal(t, stored, encoded)
	require.Equal(t, digest(encoded), hash)
	opts, err := d.WriteOptions()
	require.NoError(t, err)
	ctx = fixtureWorkerContext(t, pool)
	called := false
	result, err := worker.Manager.ExecWrite(ctx, d.Scope, opts, func(context.Context, oversync.DatabaseWriteTx) error { called = true; return nil })
	require.NoError(t, err)
	require.False(t, called)
	require.Positive(t, result.Bundle.RowCount)
	d.Rows[0].InputDigest = digest([]byte("changed"))
	changed, err := d.WriteOptions()
	require.NoError(t, err)
	_, err = worker.Manager.ExecWrite(ctx, d.Scope, changed, func(context.Context, oversync.DatabaseWriteTx) error { return nil })
	var replay *oversync.OperationReplayChangedError
	require.ErrorAs(t, err, &replay)
	// Receipt/history expiry does not erase committed application outcomes or
	// recorded full canonical-byte telemetry. Resume uses those durable facts.
	_, err = pool.Exec(ctx, `DELETE FROM sync.scope_write_receipts WHERE operation_id=$1`, opts.OperationID)
	require.NoError(t, err)
	require.NoError(t, worker.reconcileExpired(ctx, d))
	stats, err := worker.Telemetry(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Changed)
	require.EqualValues(t, 1, stats.Bundles)
	require.Positive(t, stats.HistoryBytes)
}

func TestEvolution_ExpiredPendingIdentityGetsReselected(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	worker := fixtureWorker(pool, service)
	d := fixturePrepareNext(t, worker)
	// Install a historical, never-applied descriptor as an expired restore fixture.
	d.ValidUntil = time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	encoded, err := d.CanonicalBytes()
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE app_evolution.batches SET spec=$2,descriptor=$3,hash=$4 WHERE operation_id=$1`, d.OperationID, d, encoded, digest(encoded))
	require.NoError(t, err)
	require.NoError(t, worker.Run(ctx))
	var ids int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(DISTINCT operation_id) FROM app_evolution.batches`).Scan(&ids))
	require.Equal(t, 2, ids)
	var state string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1`, d.OperationID).Scan(&state))
	require.Equal(t, "expired_reselect", state)
}

func TestEvolution_ScopeIncarnationAndPostWatermarkCoverage(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	worker := fixtureWorker(pool, service)
	descriptor := fixturePrepareNext(t, worker)
	opts := oversync.ScopeWriteOptions{WriterID: "schema-evolution-writer", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{9}, OperationValidUntil: time.Now().Add(time.Hour)}}
	require.Error(t, DeleteScope(ctx, pool, worker.Manager, "scope", descriptor.Incarnation, opts))
	_, err := CreateScope(ctx, pool, "scope")
	require.Error(t, err)
	require.NoError(t, worker.Run(ctx))
	// A privileged out-of-protocol inventory mutation is detected, never skipped.
	_, err = pool.Exec(ctx, `ALTER TABLE app_evolution.scope_inventory DISABLE TRIGGER aa_app_scope_guard; UPDATE app_evolution.scope_inventory SET deleted=true`)
	require.NoError(t, err)
	require.ErrorContains(t, worker.Run(ctx), "census_changed")

}

func TestEvolution_CapacityCancellationAndOversizedInput(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	ctx := context.Background()
	worker := fixtureWorker(pool, service)
	worker.Capacity = func(context.Context) (Capacity, error) { return Capacity{}, nil }
	require.ErrorContains(t, worker.Run(ctx), "capacity")
	var batches int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches`).Scan(&batches))
	require.Zero(t, batches)
	worker = fixtureWorker(pool, service)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, worker.Run(cancelled), context.Canceled)
	worker.Bounds.Bytes = 600
	// Bounds are immutable after activation; a different runtime cannot silently
	// widen limits to make an oversized attempt pass.
	require.ErrorContains(t, worker.Run(ctx), "immutable job bounds")
	worker = fixtureWorker(pool, service)
	require.NoError(t, fixtureWorker(pool, service).Run(ctx))
	var name string
	require.NoError(t, pool.QueryRow(ctx, `SELECT name FROM main.upgrade_people WHERE id=$1`, id).Scan(&name))
	require.Equal(t, "A", name)
}

func TestEvolution_OperationEncodingVector(t *testing.T) {
	data, err := migrationFiles.ReadFile("operation-encoding-v3.json")
	require.NoError(t, err)
	var vector struct {
		Descriptor  BatchDescriptor
		Hex, SHA256 string
	}
	require.NoError(t, json.Unmarshal(data, &vector))
	encoded, err := vector.Descriptor.CanonicalBytes()
	require.NoError(t, err)
	require.Equal(t, vector.Hex, hex.EncodeToString(encoded))
	require.Equal(t, vector.SHA256, digest(encoded))
}
