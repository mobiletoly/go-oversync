package schemaevolution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

var errScopePageYield = errors.New("scope_page_yield")

func (w *Worker) pendingDescriptor(ctx context.Context) (*BatchDescriptor, error) {
	var d BatchDescriptor
	var encoded []byte
	var hash string
	err := w.Pool.QueryRow(ctx, `SELECT spec,descriptor,hash FROM app_evolution.batches WHERE job=$1 AND state='pending'`, JobID).Scan(&d, &encoded, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actual, err := d.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(actual, encoded) || digest(actual) != hash {
		return nil, fmt.Errorf("immutable descriptor drift")
	}
	return &d, nil
}

type inventoryScope struct {
	Scope       string
	Incarnation uuid.UUID
	Sequence    int64
	Deleted     bool
}
type preparationRow struct {
	Key             uuid.UUID
	Legacy, Version string
	InputDigest     string
	Name, Label     *string
	Bytes           int
}

func (w *Worker) prepareNext(ctx context.Context) (*BatchDescriptor, error) {
	for page := 0; page < w.Bounds.ScopePage; page++ {
		var watermark, sequence int64
		var cursor *uuid.UUID
		var state string
		err := w.Pool.QueryRow(ctx, `SELECT watermark,cursor_sequence,cursor_key,state FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&watermark, &sequence, &cursor, &state)
		if err != nil {
			return nil, err
		}
		if state != "running" {
			return nil, fmt.Errorf("job is not running")
		}
		var scope inventoryScope
		err = w.Pool.QueryRow(ctx, `SELECT scope_id,incarnation,sequence,deleted FROM app_evolution.census_scopes WHERE epoch=(SELECT epoch FROM app_evolution.jobs WHERE job='person-encoding') AND sequence >= $1 AND sequence <= $2 ORDER BY sequence LIMIT 1`, sequence, watermark).Scan(&scope.Scope, &scope.Incarnation, &scope.Sequence, &scope.Deleted)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, w.finishTraversal(ctx)
		}
		if err != nil {
			return nil, err
		}
		if scope.Sequence != sequence {
			cursor = nil
		}
		if scope.Deleted {
			if err := w.advanceEmptyScope(ctx, scope); err != nil {
				return nil, err
			}
			continue
		}
		if err := requireSettledScope(ctx, w.Pool, scope.Scope); err != nil {
			return nil, err
		}
		rows, err := w.selectRows(ctx, scope, cursor)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			if err := w.advanceEmptyScope(ctx, scope); err != nil {
				return nil, err
			}
			continue
		}
		return w.prepareDescriptor(ctx, scope, rows)
	}
	// Bound empty-scope traversal per scheduling unit without declaring completion.
	return nil, errScopePageYield
}

func (w *Worker) selectRows(ctx context.Context, scope inventoryScope, cursor *uuid.UUID) ([]preparationRow, error) {
	rows, err := w.Pool.Query(ctx, `WITH candidates AS (SELECT p.*,xmin::text AS input_version,octet_length(to_jsonb(p)::text)+512 AS size FROM main.upgrade_people p WHERE _sync_scope_id=$1 AND ($2::uuid IS NULL OR id>$2) ORDER BY id LIMIT $3)
 SELECT id,CASE WHEN size<=$4 THEN legacy_name ELSE '' END,CASE WHEN size<=$4 THEN name END,CASE WHEN size<=$4 THEN new_label END,input_version,size,encode(sha256(convert_to(legacy_name,'UTF8')),'hex') FROM candidates ORDER BY id`, scope.Scope, cursor, w.Bounds.Rows, w.Bounds.Bytes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []preparationRow
	total := 0
	for rows.Next() {
		var row preparationRow
		if err := rows.Scan(&row.Key, &row.Legacy, &row.Name, &row.Label, &row.Version, &row.Bytes, &row.InputDigest); err != nil {
			return nil, err
		}
		if row.Bytes > w.Bounds.Bytes {
			return nil, w.recordRowError(ctx, scope, row, "oversized_row")
		}
		if total+row.Bytes > w.Bounds.Bytes {
			break
		}
		total += row.Bytes
		result = append(result, row)
	}
	return result, rows.Err()
}

func (w *Worker) prepareDescriptor(ctx context.Context, scope inventoryScope, rows []preparationRow) (*BatchDescriptor, error) {
	d := BatchDescriptor{Authority: "mobile-flow-postgresql", Job: JobID, Revision: JobRevision, Algorithm: backfillRequirement.AlgorithmSHA256, TransitionRevision: TransitionRevision, Phase: "maintaining", Scope: scope.Scope, Incarnation: scope.Incarnation, ScopeSequence: scope.Sequence, WriterID: "schema-evolution-backfill", OperationID: uuid.New(), ValidUntil: w.descriptorValidUntil()}
	if err := w.Pool.QueryRow(ctx, `SELECT COALESCE(max(ordinal),0)+1 FROM app_evolution.batches`).Scan(&d.Ordinal); err != nil {
		return nil, err
	}
	if err := w.Pool.QueryRow(ctx, `SELECT epoch,census FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&d.Epoch, &d.Census); err != nil {
		return nil, err
	}
	d.SQLDigest = digest([]byte(backfillSQL))
	d.BoundsDigest = digest(encodeJSON(w.Bounds))
	for _, row := range rows {
		d.Rows = append(d.Rows, SelectedRow{Key: row.Key, Version: row.Version, InputDigest: row.InputDigest})
	}
	encoded, err := d.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	if len(encoded) > w.Bounds.Bytes {
		return nil, fmt.Errorf("descriptor_bytes_exceeded")
	}
	var stored int64
	if err := w.Pool.QueryRow(ctx, `SELECT COALESCE(sum(octet_length(descriptor)),0) FROM app_evolution.batches WHERE state='pending'`).Scan(&stored); err != nil {
		return nil, err
	}
	if stored+int64(len(encoded)) > w.Bounds.PendingBytes {
		return nil, fmt.Errorf("descriptor_storage_exceeded")
	}
	w.signal("before_descriptor")
	err = w.persistReservedDescriptor(ctx, d, encoded)
	w.signal("after_descriptor")
	return &d, err
}

func (w *Worker) descriptorValidUntil() time.Time {
	return minDeadline(time.Now().UTC().Add(7*24*time.Hour), w.Bounds.Deadline).Truncate(time.Microsecond)
}

func (w *Worker) recordRowError(ctx context.Context, scope inventoryScope, row preparationRow, category string) error {
	err := pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(ctx, tx); err != nil {
			return err
		}
		if err := checkEvidenceCapacity(ctx, tx, w.Bounds, int64(w.Bounds.ErrorBytes+256)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO app_evolution.row_work(job,incarnation,row_key,input_digest,category) VALUES($1,$2,$3,$4,$5) ON CONFLICT(job,incarnation,row_key) DO UPDATE SET input_digest=excluded.input_digest,category=excluded.category`, JobID, scope.Incarnation, row.Key, row.InputDigest, category)
		if err != nil {
			return err
		}
		return checkJobEvidenceCapacity(ctx, tx)
	})
	if err != nil {
		return err
	}
	return fmt.Errorf("%s", category)
}

func (w *Worker) advanceEmptyScope(ctx context.Context, scope inventoryScope) error {
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(ctx, tx); err != nil {
			return err
		}
		var deleted bool
		if err := tx.QueryRow(ctx, `SELECT deleted FROM app_evolution.scope_inventory WHERE incarnation=$1 FOR SHARE`, scope.Incarnation).Scan(&deleted); err != nil {
			return err
		}
		var rowsRemain bool
		if !deleted {
			if err := requireSettledScope(ctx, tx, scope.Scope); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM main.upgrade_people WHERE _sync_scope_id=$1 AND (name IS NULL OR new_label IS NULL OR legacy_name IS DISTINCT FROM name||'|'||new_label))`, scope.Scope).Scan(&rowsRemain); err != nil {
				return err
			}
			if rowsRemain {
				return fmt.Errorf("scope_has_unresolved_rows")
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM app_evolution.row_work WHERE job=$1 AND incarnation=$2`, JobID, scope.Incarnation); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE app_evolution.jobs SET cursor_sequence=$2,cursor_key=NULL WHERE job=$1`, JobID, scope.Sequence+1)
		if err != nil {
			return err
		}
		return checkJobEvidenceCapacity(ctx, tx)
	})
}

func (w *Worker) finishTraversal(ctx context.Context) error {
	var unresolved int64
	if err := w.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.row_work WHERE job=$1)+(SELECT count(*) FROM app_evolution.batches WHERE job=$1 AND state='pending')`, JobID).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved != 0 {
		return fmt.Errorf("unresolved_work")
	}
	return w.updateEvidence(ctx, `UPDATE app_evolution.jobs SET state='traversed' WHERE job=$1`, JobID)
}

// A live initialization is unfinished work even when it has no physical rows.
// This read is also repeated inside the application-only checkpoint transaction.
func requireSettledScope(ctx context.Context, r oversync.DatabaseReadinessReader, scope string) error {
	var state int16
	err := r.QueryRow(ctx, `SELECT ss.state_code FROM sync.scope_state ss JOIN sync.user_state us USING(user_pk) WHERE us.user_id=$1 FOR SHARE OF ss`, scope).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state == 1 {
		return &oversync.ScopeInitializingError{UserID: scope}
	}
	return nil
}
