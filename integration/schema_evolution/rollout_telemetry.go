package schemaevolution

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"time"
)

// Nullable values distinguish unavailable measurements from observed zero.
// This bounded snapshot never supplies mutation authority or retirement policy.
type RolloutTelemetry struct {
	Complete                                     bool
	ObservedAt                                   time.Time
	QueryDuration                                time.Duration
	DeploymentRevision                           *int64
	LedgerRevision                               *int64
	Phase                                        string
	HistoryBytes                                 *int64
	Sessions                                     *RolloutCounts
	Supported                                    []RolloutGenerationTelemetry
	Attempts, Batches, PrunedRows, EvidenceBytes *int64
	PhaseDuration, AuthorityLockWait             *int64
	Process                                      *oversync.GenerationObservation
}
type RolloutGenerationTelemetry struct {
	GenerationID    int32
	DurableBindings int64
}

func ReadRolloutTelemetry(ctx context.Context, pool *pgxpool.Pool, process *oversync.SyncService) (RolloutTelemetry, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := RolloutTelemetry{}
	start := time.Now()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		var deployment, ledger, history, attempts, batches, pruned, bytes, duration, wait int64
		if err := tx.QueryRow(ctx, `SELECT transaction_timestamp(),d.revision,l.revision,d.contract->>'TransitionPhase' FROM app_evolution.deployment d CROSS JOIN app_evolution.generation_ledger_meta l`).Scan(&result.ObservedAt, &deployment, &ledger, &result.Phase); err != nil {
			return err
		}
		result.DeploymentRevision, result.LedgerRevision = &deployment, &ledger
		if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(byte_count),0) FROM sync.bundle_log`).Scan(&history); err != nil {
			return err
		}
		result.HistoryBytes = &history
		counts := RolloutCounts{}
		if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM sync.snapshot_sessions),(SELECT count(*) FROM sync.bundle_capture_stage),(SELECT count(*) FROM sync.bundle_rows),(SELECT count(*) FROM sync.row_state)`).Scan(&counts.SnapshotSessions, &counts.CaptureRows, &counts.RetainedRows, &counts.RowState); err != nil {
			return err
		}
		result.Sessions = &counts
		rows, err := tx.Query(ctx, `SELECT g.generation_id,(SELECT count(*) FROM sync.source_generation_bindings b WHERE b.schema_generation_id=g.generation_id) FROM app_evolution.generation_ledger g WHERE g.state='supported' ORDER BY g.generation_id LIMIT 3`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var value RolloutGenerationTelemetry
			if err = rows.Scan(&value.GenerationID, &value.DurableBindings); err != nil {
				rows.Close()
				return err
			}
			result.Supported = append(result.Supported, value)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(attempts),0),COALESCE(sum(batches),0),COALESCE(sum(deleted_rows),0),COALESCE(sum(evidence_bytes),0) FROM app_evolution.rollout_operations`).Scan(&attempts, &batches, &pruned, &bytes); err != nil {
			return err
		}
		if err = tx.QueryRow(ctx, `SELECT COALESCE(sum((outcome->>'Duration')::bigint),0),COALESCE(sum((outcome->>'AuthorityLockWait')::bigint),0) FROM app_evolution.rollout_batches`).Scan(&duration, &wait); err != nil {
			return err
		}
		result.Attempts, result.Batches, result.PrunedRows, result.EvidenceBytes = &attempts, &batches, &pruned, &bytes
		result.PhaseDuration, result.AuthorityLockWait = &duration, &wait
		return nil
	})
	result.QueryDuration = time.Since(start)
	if err != nil {
		return result, err
	}
	if process != nil {
		metrics := process.GenerationObservations()
		result.Process = &metrics
	}
	result.Complete = true
	return result, nil
}
