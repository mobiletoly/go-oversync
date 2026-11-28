package schemaevolution

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

func minDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Evidence bytes are retained PostgreSQL row-value bytes (including both descriptor
// representations), plus reserved outcome/error space. Physical free disk is a
// separate required application measurement and includes indexes, WAL and overhead.
func checkEvidenceCapacity(ctx context.Context, tx oversync.DatabaseReadinessReader, b Bounds, extra int64) error {
	var used int64
	err := tx.QueryRow(ctx, `SELECT
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.repair_evidence x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_progress x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.certificates x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_rows x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.census_scopes x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.jobs x),0)+
 COALESCE((SELECT sum(pg_column_size(x)+evidence_reserve) FROM app_evolution.batches x),0)+
 COALESCE((SELECT sum(pg_column_size(x)) FROM app_evolution.row_work x),0)`).Scan(&used)
	if err != nil {
		return err
	}
	if used > b.MaxEvidenceBytes || extra > b.MaxEvidenceBytes-used {
		return fmt.Errorf("evidence_capacity_exceeded")
	}
	return nil
}

func (w *Worker) persistReservedDescriptor(ctx context.Context, d BatchDescriptor, encoded []byte) error {
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return persistReservedDescriptorTx(ctx, tx, d, encoded)
	})
}

// checkJobEvidenceCapacity applies the persisted limit to a completed evidence
// mutation before its transaction commits. Reservations remain conservatively
// counted after completion; they are never silently spent or renewed.
func checkJobEvidenceCapacity(ctx context.Context, tx oversync.DatabaseReadinessReader) error {
	var bounds Bounds
	if err := tx.QueryRow(ctx, `SELECT bounds FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&bounds); err != nil {
		return err
	}
	return checkEvidenceCapacity(ctx, tx, bounds, 0)
}

// updateEvidence makes a single worker metadata update and its final capacity
// check atomic. The worker coordinator serializes these transactions.
func (w *Worker) updateEvidence(ctx context.Context, statement string, args ...any) error {
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, statement, args...); err != nil {
			return err
		}
		return checkJobEvidenceCapacity(ctx, tx)
	})
}

func persistReservedDescriptorTx(ctx context.Context, tx pgx.Tx, d BatchDescriptor, encoded []byte) error {
	if err := fenceWorkerMetadata(ctx, tx); err != nil {
		return err
	}
	var bounds Bounds
	if err := tx.QueryRow(ctx, `SELECT bounds FROM app_evolution.jobs WHERE job=$1 FOR UPDATE`, JobID).Scan(&bounds); err != nil {
		return err
	}
	if digest(encodeJSON(bounds)) != d.BoundsDigest || !time.Now().Before(bounds.Deadline) {
		return fmt.Errorf("immutable bounds or run deadline changed")
	}
	var history, bundles int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(CASE WHEN metrics_complete THEN history_bytes ELSE history_reserve END),0),COALESCE(sum(CASE WHEN metrics_complete THEN bundles ELSE 1 END),0) FROM app_evolution.batches WHERE job=$1`, JobID).Scan(&history, &bundles); err != nil {
		return err
	}
	if history > bounds.MaxHistoryBytes-int64(bounds.Bytes) || bundles >= bounds.MaxBundles {
		return fmt.Errorf("history_capacity_exceeded")
	}
	// Keep this allowance even after completion: conservative accounting cannot
	// lose diagnostic capacity to a later retry or unexpectedly larger outcome.
	reserve := int64(4096 + len(d.Rows)*(bounds.ErrorBytes+256))
	extra := int64(len(encoded)+len(encodeJSON(d))) + reserve + 1024
	if err := checkEvidenceCapacity(ctx, tx, bounds, extra); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO app_evolution.batches(operation_id,job,ordinal,descriptor,spec,hash,evidence_reserve,history_reserve) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, d.OperationID, d.Job, d.Ordinal, encoded, d, digest(encoded), reserve, bounds.Bytes)
	if err != nil {
		return err
	}
	return checkEvidenceCapacity(ctx, tx, bounds, 0)
}
