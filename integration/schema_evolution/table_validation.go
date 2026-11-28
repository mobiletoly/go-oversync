package schemaevolution

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

const tableValidationDone = 6

// One fixed-size checkpoint covers all relations; it commits with each valid page.
// Source, epoch, plan and immutable bounds are bound by the enclosing table_run.
type tableValidationCursor struct {
	Stage    int
	Sequence int64
	Scope    *string
	Key      *uuid.UUID
	Pages    int64
}

func tableValidationResult(run tableRun) oversync.InvariantValidation {
	return oversync.InvariantValidation{Requirement: tableBackfillRequirement(), CoverageWatermark: run.Watermark, ResultSHA256: digest([]byte("table-population-v2:complete"))}
}

func validateTablePopulation(ctx context.Context, r oversync.DatabaseReadinessReader, initial bool) (oversync.InvariantValidation, error) {
	run, err := loadTableRun(ctx, r)
	result := tableValidationResult(run)
	if err != nil {
		return result, err
	}
	if !run.CensusComplete {
		return result, fmt.Errorf("table census incomplete")
	}
	// Readiness deliberately starts afresh, even after a saved certification pass.
	// Read-only adapters cannot grant themselves authority to advance the worker ledger.
	cursor := tableValidationCursor{}
	for cursor.Stage < tableValidationDone {
		bounded, cancel := context.WithTimeout(ctx, run.Bounds.QueryTimeout)
		next, valid, err := validateTablePage(bounded, r, run, cursor, initial)
		cancel()
		if err != nil {
			return result, err
		}
		if !valid {
			result.Violations++
			return result, nil
		}
		cursor = next
	}
	return result, nil
}

func (w *TableWorker) certifyTablePage(ctx context.Context) (bool, error) {
	done := false
	err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
		if !run.CensusComplete {
			return fmt.Errorf("table census incomplete")
		}
		next, valid, err := validateTablePage(ctx, tx, run, run.Validation, true)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("table population validation failed")
		}
		w.signal("validation_before_checkpoint")
		_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET validation_cursor=$1 WHERE singleton`, next)
		done = next.Stage == tableValidationDone
		return err
	})
	return done, err
}

// LIMIT is applied before invariant/history joins, never after filtering failures.
// Cursor advancement and checking are separate so a rejected page cannot skip rows.
func validateTablePage(ctx context.Context, r oversync.DatabaseReadinessReader, run tableRun, c tableValidationCursor, initial bool) (tableValidationCursor, bool, error) {
	if c.Stage < 0 || c.Stage > tableValidationDone {
		return c, false, fmt.Errorf("invalid table validation stage")
	}
	if c.Stage == tableValidationDone {
		return c, true, nil
	}
	if initial && c.Stage == 0 {
		var watermark int64
		if err := r.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM app_evolution.scope_inventory`).Scan(&watermark); err != nil {
			return c, false, err
		}
		if watermark != run.Watermark {
			return c, false, nil
		}
	}
	query, args := tableValidationQuery(run, c, initial)
	rows, err := r.Query(ctx, query, args...)
	if err != nil {
		return c, false, err
	}
	defer rows.Close()
	next := c
	count := 0
	for rows.Next() {
		var valid bool
		if err := rows.Scan(&next.Sequence, &next.Scope, &next.Key, &valid); err != nil {
			return c, false, err
		}
		if !valid {
			return c, false, nil
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return c, false, err
	}
	if count == 0 {
		next = tableValidationCursor{Stage: c.Stage + 1, Pages: c.Pages}
	}
	next.Pages++
	return next, true, nil
}

func tableValidationQuery(run tableRun, c tableValidationCursor, initial bool) (string, []any) {
	switch c.Stage {
	case 0:
		// Include deleted entries in the page bound; a sparse inventory cannot expand a page.
		return `WITH page AS MATERIALIZED (SELECT * FROM app_evolution.scope_inventory WHERE sequence>$1 ORDER BY sequence LIMIT $2)
SELECT i.sequence,NULL::text,NULL::uuid,NOT $3 OR (i.sequence<=$4 AND (i.deleted OR EXISTS(SELECT 1 FROM app_evolution.table_scopes s WHERE s.epoch=$5 AND s.scope_id=i.scope_id AND s.incarnation=i.incarnation AND s.sequence=i.sequence))) FROM page i ORDER BY i.sequence`, []any{c.Sequence, run.Bounds.ScopePage, initial, run.Watermark, run.Epoch}
	case 1:
		seek := "$2::text IS NULL"
		if c.Scope != nil {
			seek = "scope_id>$2"
		}
		return `WITH page AS MATERIALIZED (SELECT * FROM app_evolution.table_scopes WHERE epoch=$1 AND ` + seek + ` ORDER BY scope_id LIMIT $3)
SELECT 0::bigint,s.scope_id,NULL::uuid,s.populated AND (NOT $4 OR (EXISTS(SELECT 1 FROM app_evolution.scope_inventory i WHERE i.incarnation=s.incarnation AND i.scope_id=s.scope_id AND i.sequence=s.sequence AND NOT i.deleted) AND EXISTS(SELECT 1 FROM main.projects p WHERE p._sync_scope_id=s.scope_id AND p.id=$5::uuid))) FROM page s ORDER BY s.scope_id`, []any{run.Epoch, c.Scope, run.Bounds.ScopePage, initial, TableProjectID}
	case 2:
		seek := "$1::uuid IS NULL"
		if c.Key != nil {
			seek = "operation_id>$1"
		}
		return `WITH page AS MATERIALIZED (SELECT operation_id,state FROM app_evolution.table_batches WHERE ` + seek + ` ORDER BY operation_id LIMIT $2)
SELECT 0::bigint,NULL::text,operation_id,state IN ('applied','replaced') FROM page ORDER BY operation_id`, []any{c.Key, run.Bounds.Rows}
	default:
		return tableBusinessValidationQuery(run, c, initial)
	}
}

func tableBusinessValidationQuery(run tableRun, c tableValidationCursor, initial bool) (string, []any) {
	table := "people"
	valid := `(p.project_id IS NULL OR EXISTS(SELECT 1 FROM main.projects t WHERE t._sync_scope_id=p._sync_scope_id AND t.id=p.project_id))`
	if initial {
		valid += ` AND p.project_id IS NOT DISTINCT FROM $4::uuid AND ` + tableCapturePredicate(2, "b.payload_wire->>'project_id'=p.project_id::text")
	}
	if c.Stage == 4 {
		table = "projects"
		valid = "true"
		if initial {
			valid = tableCapturePredicate(4, "b.payload_wire->>'title'=p.title")
		}
	}
	if c.Stage == 5 {
		table = "attachments"
		valid = "NOT $5"
	}
	seek := "$1::text IS NULL AND $2::uuid IS NULL"
	if c.Scope != nil {
		seek = "(_sync_scope_id,id)>($1,$2::uuid)"
	}
	// Bind the same typed parameters in every stage, including relaxed checks.
	query := `WITH page AS MATERIALIZED (SELECT * FROM main.` + table + ` WHERE ` + seek + ` ORDER BY _sync_scope_id,id LIMIT $3)
SELECT 0::bigint,p._sync_scope_id,p.id,(` + valid + `) AND $4::uuid IS NOT NULL AND $5::boolean IS NOT NULL FROM page p ORDER BY p._sync_scope_id,p.id`
	return query, []any{c.Scope, c.Key, run.Bounds.Rows, TableProjectID, initial}
}

func tableCapturePredicate(tableID int, payload string) string {
	return fmt.Sprintf(`EXISTS(SELECT 1 FROM sync.user_state u JOIN sync.row_state s USING(user_pk) JOIN sync.bundle_rows b ON b.user_pk=s.user_pk AND b.bundle_seq=s.bundle_seq AND b.table_id=s.table_id AND b.key_bytes=s.key_bytes WHERE u.user_id=p._sync_scope_id AND s.table_id=%d AND s.key_bytes=uuid_send(p.id) AND NOT s.deleted AND %s)`, tableID, payload)
}
