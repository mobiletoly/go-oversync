package schemaevolution

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

const TableProjectID = "33333333-3333-4333-8333-333333333333"

type tableDescriptor struct {
	Format                                                    string
	Operation, Plan, Incarnation                              uuid.UUID
	Epoch                                                     int64
	Scope                                                     string
	Keys                                                      []uuid.UUID
	Source, BoundsDigest, MigrationDigest, CatalogFingerprint string
	Created, Deadline                                         time.Time
}

// Populate creates independent project entities and persists relationship values
// using SQL. It does not move person identity or project sync rows across tables.
func (w *TableWorker) Populate(ctx context.Context) error {
	return w.withWorker(ctx, "cleaned", func(ctx context.Context, run tableRun) error {
		if run.Phase == "populated" || run.Validation.Pages > 0 {
			return w.certifyTables(ctx)
		}
		if err := w.captureTableCensus(ctx); err != nil {
			return err
		}
		for {
			d, done, err := w.prepareTableBatch(ctx)
			if err != nil {
				return err
			}
			if done {
				return w.certifyTables(ctx)
			}
			w.signal("population_after_prepare")
			if err := w.reserveTableAttempt(ctx, d); err != nil {
				return err
			}
			if err := w.applyTableBatch(ctx, d); err != nil {
				return err
			}
			w.signal("population_after_commit")
		}
	})
}

func (w *TableWorker) captureTableCensus(ctx context.Context) error {
	for {
		done := false
		err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
			return captureTableCensusPage(ctx, tx, run, &done)
		})
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func captureTableCensusPage(ctx context.Context, tx pgx.Tx, run tableRun, done *bool) error {
	if err := validateTableWatermark(ctx, tx, run); err != nil {
		return err
	}
	if run.CensusComplete {
		*done = true
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT sequence,scope_id,incarnation FROM app_evolution.scope_inventory WHERE NOT deleted AND sequence>$1 AND sequence<=$2 ORDER BY sequence LIMIT $3`, run.Cursor, run.Watermark, run.Bounds.ScopePage)
	if err != nil {
		return err
	}
	type entry struct {
		sequence    int64
		scope       string
		incarnation uuid.UUID
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.sequence, &e.scope, &e.incarnation); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	cursor := run.Cursor
	for _, e := range entries {
		if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.table_scopes(epoch,sequence,scope_id,incarnation) VALUES($1,$2,$3,$4)`, run.Epoch, e.sequence, e.scope, e.incarnation); err != nil {
			return err
		}
		cursor = e.sequence
	}
	*done = len(entries) < run.Bounds.ScopePage
	_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET census_cursor=$1,census_complete=$2 WHERE singleton`, cursor, *done)
	return err
}

func validateTableWatermark(ctx context.Context, r oversync.DatabaseReadinessReader, run tableRun) error {
	var watermark int64
	var changed bool
	if err := r.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM app_evolution.scope_inventory`).Scan(&watermark); err != nil {
		return err
	}
	if watermark != run.Watermark {
		return fmt.Errorf("table scope watermark changed")
	}
	if err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.table_scopes s LEFT JOIN app_evolution.scope_inventory i ON i.incarnation=s.incarnation AND i.scope_id=s.scope_id AND i.sequence=s.sequence AND NOT i.deleted WHERE s.epoch=$1 AND i.incarnation IS NULL)`, run.Epoch).Scan(&changed); err != nil {
		return err
	}
	if changed {
		return fmt.Errorf("table scope incarnation changed")
	}
	return nil
}

func (w *TableWorker) prepareTableBatch(ctx context.Context) (tableDescriptor, bool, error) {
	var d tableDescriptor
	done := false
	err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
		if err := validateTableWatermark(ctx, tx, run); err != nil {
			return err
		}
		var failed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.table_batches WHERE epoch=$1 AND state='failed')`, run.Epoch).Scan(&failed); err != nil {
			return err
		}
		if failed {
			return fmt.Errorf("unresolved table population failure")
		}
		err := tx.QueryRow(ctx, `SELECT descriptor FROM app_evolution.table_batches WHERE epoch=$1 AND state='pending'`, run.Epoch).Scan(&d)
		if err == nil {
			return nil
		}
		if err != pgx.ErrNoRows {
			return err
		}
		var cursor *uuid.UUID
		err = tx.QueryRow(ctx, `SELECT scope_id,incarnation,cursor_key FROM app_evolution.table_scopes WHERE epoch=$1 AND NOT populated ORDER BY sequence LIMIT 1 FOR UPDATE`, run.Epoch).Scan(&d.Scope, &d.Incarnation, &cursor)
		if err == pgx.ErrNoRows {
			done = true
			return nil
		}
		if err != nil {
			return err
		}
		d.Keys, err = selectTablePeople(ctx, tx, d.Scope, cursor, run.Bounds.Rows)
		if err != nil {
			return err
		}
		d.Format = "table-population-v1"
		d.Operation = uuid.New()
		d.Plan = run.ID
		d.Epoch = run.Epoch
		d.Created = time.Now().UTC()
		d.Deadline = minTableDeadline(d.Created.Add(7*24*time.Hour), run.Bounds.Deadline)
		d.Source = run.Source
		d.BoundsDigest = digest(encodeJSON(run.Bounds))
		d.MigrationDigest = oversync.MigrationManifestDigest(tableManifest())
		d.CatalogFingerprint = tableCatalogFingerprint(2)
		if int64(len(tableJSON(d))) > run.Bounds.PendingBytes || len(tableJSON(d)) > run.Bounds.Bytes {
			return fmt.Errorf("table descriptor capacity exhausted")
		}
		if run.History+int64(run.Bounds.Bytes) > run.Bounds.MaxHistoryBytes || run.Bundles+1 > run.Bounds.MaxBundles {
			return fmt.Errorf("table cumulative history capacity exhausted")
		}
		_, err = tx.Exec(ctx, `INSERT INTO app_evolution.table_batches(operation_id,epoch,descriptor,hash,evidence_reserve,history_reserve) VALUES($1,$2,$3,$4,$5,$6)`, d.Operation, run.Epoch, d, digest(tableJSON(d)), len(tableJSON(d))+1024, run.Bounds.Bytes)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET history_bytes=history_bytes+$1,bundles=bundles+1 WHERE singleton`, run.Bounds.Bytes)
		return err
	})
	return d, done, err
}

func selectTablePeople(ctx context.Context, r oversync.DatabaseReadinessReader, scope string, cursor *uuid.UUID, limit int) ([]uuid.UUID, error) {
	rows, err := r.Query(ctx, `SELECT id FROM main.people WHERE _sync_scope_id=$1 AND ($2::uuid IS NULL OR id>$2) ORDER BY id LIMIT $3`, scope, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []uuid.UUID
	for rows.Next() {
		var key uuid.UUID
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func minTableDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (w *TableWorker) applyTableBatch(ctx context.Context, d tableDescriptor) error {
	run, err := loadTableRun(ctx, w.Pool)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, d.Deadline)
	defer cancel()
	ctx, stop := context.WithTimeout(ctx, run.Bounds.QueryTimeout)
	defer stop()
	hash := sha256.Sum256(tableJSON(d))
	_, err = oversync.NewScopeManager(w.Service, oversync.ScopeManagerConfig{}).ExecWrite(ctx, d.Scope, oversync.ScopeWriteOptions{WriterID: "schema-evolution-backfill", MaxBytesPerBundle: run.Bounds.Bytes, RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: d.Operation, OperationHash: hash, OperationValidUntil: d.Deadline}}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if err := verifyTableDescriptor(ctx, tx, d, run); err != nil {
			return err
		}
		if err := permitWorkerTransaction(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO main.projects(_sync_scope_id,id,title) VALUES($1,$2,'Shared project') ON CONFLICT DO NOTHING`, d.Scope, TableProjectID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE main.people SET project_id=$1::uuid WHERE _sync_scope_id=$2 AND id=ANY($3::uuid[]) AND project_id IS DISTINCT FROM $1::uuid`, TableProjectID, d.Scope, d.Keys); err != nil {
			return err
		}
		if err := validateTableBatchRows(ctx, tx, d); err != nil {
			return err
		}
		w.signal("population_inside_apply")
		return checkpointTableBatch(ctx, tx, d, run)
	})
	var noop *oversync.ScopeWriteNoCapturedChangesError
	if errors.As(err, &noop) {
		return w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
			if err := verifyTableDescriptor(ctx, tx, d, run); err != nil {
				return err
			}
			if err := validateTableBatchRows(ctx, tx, d); err != nil {
				return err
			}
			w.signal("population_inside_noop")
			return checkpointTableBatch(ctx, tx, d, run)
		})
	}
	return w.recordTableFailure(ctx, d, err)
}

func verifyTableDescriptor(ctx context.Context, r oversync.DatabaseReadinessReader, d tableDescriptor, run tableRun) error {
	if err := lockAdmission(ctx, r, false); err != nil {
		return err
	}
	c, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok {
		return fmt.Errorf("table worker capability missing")
	}
	if err := verifyWorker(ctx, r, c); err != nil {
		return err
	}
	current, err := loadTableRun(ctx, r)
	if err != nil {
		return err
	}
	if current.ID != run.ID || digest(encodeJSON(current.Bounds)) != digest(encodeJSON(run.Bounds)) {
		return fmt.Errorf("immutable table plan changed")
	}
	run = current
	if err := run.check(c, "cleaned"); err != nil {
		return err
	}
	if d.Format != "table-population-v1" || d.Plan != run.ID || d.Epoch != run.Epoch || d.Source != run.Source || d.BoundsDigest != digest(encodeJSON(run.Bounds)) || d.MigrationDigest != oversync.MigrationManifestDigest(tableManifest()) || d.CatalogFingerprint != tableCatalogFingerprint(2) || d.Deadline.After(run.Bounds.Deadline) {
		return fmt.Errorf("table descriptor identity changed")
	}
	var hash, phase string
	if err := r.QueryRow(ctx, `SELECT hash FROM app_evolution.table_batches WHERE operation_id=$1 AND state='pending' FOR UPDATE`, d.Operation).Scan(&hash); err != nil {
		return err
	}
	if hash != digest(tableJSON(d)) {
		return fmt.Errorf("table descriptor hash changed")
	}
	if err := r.QueryRow(ctx, `SELECT contract->>'TransitionPhase' FROM app_evolution.deployment WHERE singleton`).Scan(&phase); err != nil {
		return err
	}
	if phase != tablePhaseName(TablesBackfill) {
		return fmt.Errorf("post-FK runtime required")
	}
	return validateTableWatermark(ctx, r, run)
}

func validateTableBatchRows(ctx context.Context, r oversync.DatabaseReadinessReader, d tableDescriptor) error {
	var project bool
	var people int
	if err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM main.projects WHERE _sync_scope_id=$1 AND id=$2::uuid AND title='Shared project'),(SELECT count(*) FROM main.people WHERE _sync_scope_id=$1 AND id=ANY($3::uuid[]) AND project_id=$2::uuid)`, d.Scope, TableProjectID, d.Keys).Scan(&project, &people); err != nil {
		return err
	}
	if !project || people != len(d.Keys) {
		return fmt.Errorf("table batch coverage invalid")
	}
	return nil
}

func checkpointTableBatch(ctx context.Context, tx oversync.DatabaseWriteTx, d tableDescriptor, run tableRun) error {
	var cursor any
	if len(d.Keys) > 0 {
		cursor = d.Keys[len(d.Keys)-1]
	}
	if tag, err := tx.Exec(ctx, `UPDATE app_evolution.table_scopes SET cursor_key=COALESCE($1::uuid,cursor_key),populated=$2 WHERE epoch=$3 AND scope_id=$4 AND incarnation=$5`, cursor, len(d.Keys) < run.Bounds.Rows, d.Epoch, d.Scope, d.Incarnation); err != nil {
		return err
	} else if tag.RowsAffected() != 1 {
		return fmt.Errorf("table scope checkpoint missing")
	}
	if tag, err := tx.Exec(ctx, `UPDATE app_evolution.table_batches SET state='applied',outcome=$1,evidence_reserve=0 WHERE operation_id=$2 AND state='pending'`, map[string]any{"keys": len(d.Keys), "project": TableProjectID}, d.Operation); err != nil {
		return err
	} else if tag.RowsAffected() != 1 {
		return fmt.Errorf("table batch checkpoint missing")
	}
	return checkTableCapacity(ctx, tx, run)
}

func (w *TableWorker) certifyTables(ctx context.Context) error {
	for {
		done, err := w.certifyTablePage(ctx)
		if err != nil {
			return err
		}
		w.signal("validation_after_checkpoint")
		if done {
			break
		}
	}
	return w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
		if run.Validation.Stage != tableValidationDone {
			return fmt.Errorf("table validation incomplete")
		}
		validation := tableValidationResult(run)
		cert := oversync.BackfillCertificate{Requirement: tableBackfillRequirement(), MigrationDigest: oversync.MigrationManifestDigest(tableManifest()), TransitionRevision: "additive-tables-v1", TransitionPhase: tablePhaseName(TablesServing), CoverageWatermark: run.Watermark, ValidatedAt: time.Now().UTC(), ResultSHA256: validation.ResultSHA256}
		_, err := tx.Exec(ctx, `UPDATE app_evolution.table_run SET phase='populated',certificate=$1 WHERE singleton`, cert)
		return err
	})
}
