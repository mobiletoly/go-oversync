package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Admission runs on the final writer, before any business DML. It checks both
// actual delta mutations and the additional rows a uniqueness cycle may vacate.
func (c *Client) validateSnapshotPreservation(ctx context.Context, tx sqliteTransaction) error {
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return err
	}
	work, err := c.snapshotPreservationWork(ctx, tx, p)
	if err != nil {
		return err
	}
	if err := c.reserveReadySyncWork(ctx, p.unit, work); err != nil {
		return err
	}
	if !p.preservationComplete && p.visit > 0 {
		// The charged unresolved remainder is analysed once. Materialize arcs
		// before marking the FK closure, which can include acyclic descendants.
		if _, err := tx.ExecContext(ctx, snapshotCycleClosure+`UPDATE _sync_apply_rows SET pending=7 WHERE ordinal IN(SELECT id FROM closure)`); err != nil {
			return err
		}
	}
	if !p.preservationComplete {
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_apply_plan SET preservation_complete=1 WHERE singleton_key=1`); err != nil {
			return err
		}
		p.preservationComplete = true
	}
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		row, size, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		matches, err := c.applySurvivorMatches(ctx, tx, p, row, size)
		if err != nil {
			return err
		}
		if !matches {
			if err := c.validateSnapshotMutation(ctx, tx, p, row, false); err != nil {
				return err
			}
		}
		var vacated bool
		err = tx.QueryRowContext(ctx, `SELECT pending=7 FROM _sync_apply_rows WHERE ordinal=?`, ordinal).Scan(&vacated)
		if err != nil {
			return err
		}
		if vacated {
			if err := c.validateSnapshotMutation(ctx, tx, p, row, true); err != nil {
				return err
			}
		}
	}
	if invocation := uniqueInvocation(ctx); invocation != nil {
		invocation.snapshotPreserved = true
	}
	return nil
}

const snapshotCycleClosure = `WITH RECURSIVE arcs(a,b) AS MATERIALIZED (
 SELECT a.ordinal,b.ordinal FROM _sync_apply_edges e
 JOIN _sync_apply_rows a ON a.table_name=e.parent_table AND a.local_pk=e.parent_pk
 JOIN _sync_apply_rows b ON b.table_name=e.child_table AND b.local_pk=e.child_pk
 WHERE e.edge_kind='u' AND a.pending NOT IN(3,6) AND b.pending NOT IN(3,6)),
 reach(a,b) AS (SELECT a,b FROM arcs UNION SELECT r.a,e.b FROM reach r JOIN arcs e ON e.a=r.b),
 closure(id) AS (SELECT a FROM reach WHERE a=b UNION SELECT child.ordinal FROM closure c
 JOIN _sync_apply_rows parent ON parent.ordinal=c.id
 JOIN _sync_apply_edges e ON e.edge_kind='f' AND e.parent_table=parent.table_name AND e.parent_pk=parent.local_pk
 JOIN _sync_apply_rows child ON child.table_name=e.child_table AND child.local_pk=e.child_pk) `

// Account for complete final-writer proof work, including unindexed empty sets.
// No local-only row is copied or mutated. COUNT establishes the scan bound;
// membership is then evaluated on this same writer with that bound reserved.
func (c *Client) snapshotPreservationWork(ctx context.Context, tx sqliteTransaction, p *applyPlan) (int64, error) {
	var tables, metadataBytes, triggers, baselines int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM sqlite_schema WHERE type='table'`).Scan(&tables, &metadataBytes); err != nil {
		return 0, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT name FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name FROM sqlite_temp_schema WHERE type='trigger')`).Scan(&triggers); err != nil {
		return 0, err
	}
	bytes, err := c.workspaceScale(p.unit, metadataBytes, 16)
	if err != nil {
		return 0, err
	}
	if err := c.checkApplyWorkspace(p, saturatingWorkAdd(16, saturatingWorkMultiply(tables, 8)), saturatingWorkAdd(4096, bytes)); err != nil {
		return 0, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_row_state`).Scan(&baselines); err != nil {
		return 0, err
	}
	work := saturatingWorkAdd(saturatingWorkMultiply(baselines, 128), saturatingWorkMultiply(p.entries, saturatingWorkAdd(4096, saturatingWorkAdd(saturatingWorkMultiply(tables, 128), saturatingWorkMultiply(triggers, 64)))))
	var last string
	for {
		var table string
		err := tx.QueryRowContext(ctx, `SELECT name FROM sqlite_schema WHERE type='table' AND name>? AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1`, last).Scan(&table)
		if errors.Is(err, sql.ErrNoRows) {
			return work, nil
		}
		if err != nil {
			return 0, err
		}
		last = table
		if _, managed := c.pkByTable[strings.ToLower(table)]; managed || strings.HasPrefix(table, "_sync_") {
			continue
		}
		bound, err := c.snapshotIncomingScanWork(ctx, tx, table, p.entries)
		if err != nil {
			return 0, err
		}
		work = saturatingWorkAdd(work, bound)
	}
}

func (c *Client) snapshotIncomingScanWork(ctx context.Context, tx sqliteTransaction, table string, parents int64) (int64, error) {
	var columns, groups int64
	marks := make([]string, len(c.config.Tables))
	args := []any{table}
	for i, managed := range c.config.Tables {
		marks[i] = "?"
		args = append(args, strings.ToLower(managed.TableName))
	}
	if len(marks) == 0 {
		return 0, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*),count(DISTINCT id) FROM pragma_foreign_key_list(?) WHERE lower("table") IN (`+strings.Join(marks, ",")+`)`, args...).Scan(&columns, &groups); err != nil {
		return 0, err
	}
	if groups == 0 {
		return 0, nil
	}
	var rows int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent(table)).Scan(&rows); err != nil {
		return 0, err
	}
	perScan := saturatingWorkAdd(256, saturatingWorkMultiply(rows, saturatingWorkAdd(32, saturatingWorkMultiply(columns, 16))))
	// A changed row in an SCC needs both its logical mutation and its temporary
	// vacate proved. Each proof can scan the complete unindexed child set.
	proofs := saturatingWorkMultiply(saturatingWorkMultiply(parents, groups), 2)
	return saturatingWorkAdd(rows, saturatingWorkMultiply(proofs, perScan)), nil
}

func (c *Client) validateSnapshotMutation(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, vacating bool) error {
	if err := c.validateSnapshotTriggers(ctx, tx, p, row); err != nil {
		return err
	}
	return c.validateSnapshotIncoming(ctx, tx, p, row, vacating)
}

func (c *Client) validateSnapshotTriggers(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor) error {
	info, err := c.getTableInfoTx(tx, row.table)
	if err != nil {
		return err
	}
	var config SyncTable
	for _, table := range c.config.Tables {
		if strings.EqualFold(table.TableName, row.table) {
			config = table
			break
		}
	}
	expected, err := expectedTriggerSQLByName(c.schemaForTable(row.table), config, info)
	if err != nil {
		return err
	}
	var total, count int64
	query := `SELECT name,sql,temporary FROM (SELECT name,sql,tbl_name,0 AS temporary FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name,sql,tbl_name,1 AS temporary FROM sqlite_temp_schema WHERE type='trigger') WHERE lower(tbl_name)=?`
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST(sql AS BLOB))+length(CAST(name AS BLOB))),0) FROM (`+query+`)`, row.table).Scan(&count, &total); err != nil {
		return err
	}
	if err := c.checkApplyWorkspace(p, 16+count*4, 4096+total*16); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, query, row.table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, sqlText string
		var temporary bool
		if err := rows.Scan(&name, &sqlText, &temporary); err != nil {
			return err
		}
		known, exists := expected[name]
		if temporary || !exists || !sameTriggerSQL(sqlText, known) {
			e := uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
			e.Trigger = name
			return e
		}
	}
	return rows.Err()
}

// Unmanaged children are never rewritten by recovery. Empty incoming sets are
// safe for any action. Nonempty NO ACTION sets are safe only if every referenced
// parent value survives unchanged; the writer retains membership through commit.
func (c *Client) validateSnapshotIncoming(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, vacating bool) error {
	var last string
	for {
		var child string
		err := tx.QueryRowContext(ctx, `SELECT name FROM sqlite_schema WHERE type='table' AND name>? AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1`, last).Scan(&child)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		last = child
		if _, managed := c.pkByTable[strings.ToLower(child)]; managed || strings.HasPrefix(child, "_sync_") {
			continue
		}
		if err := c.validateSnapshotIncomingTable(ctx, tx, p, row, child, vacating); err != nil {
			return err
		}
	}
}

func (c *Client) validateSnapshotIncomingTable(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, child string, vacating bool) error {
	var last int64 = -1
	for {
		var id int64
		err := tx.QueryRowContext(ctx, `SELECT id FROM pragma_foreign_key_list(?) WHERE lower("table")=? AND id>? ORDER BY id LIMIT 1`, child, row.table, last).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		last = id
		if err := c.validateSnapshotIncomingKey(ctx, tx, p, row, child, id, vacating); err != nil {
			return err
		}
	}
}

func (c *Client) validateSnapshotIncomingKey(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, child string, id int64, vacating bool) error {
	var count, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length("from")+length(COALESCE("to",''))+length(on_delete)+length(on_update)),0) FROM pragma_foreign_key_list(?) WHERE id=?`, child, id).Scan(&count, &bytes); err != nil {
		return err
	}
	if err := c.checkApplyWorkspace(p, 16+count*8, 4096+bytes*16); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT "from","to",on_delete,on_update FROM pragma_foreign_key_list(?) WHERE id=? ORDER BY seq`, child, id)
	if err != nil {
		return err
	}
	var joins, refs []string
	var deleteAction, updateAction string
	for rows.Next() {
		var from string
		var to sql.NullString
		if err := rows.Scan(&from, &to, &deleteAction, &updateAction); err != nil {
			rows.Close()
			return err
		}
		if !to.Valid {
			to.String = c.pkByTable[row.table]
		}
		joins = append(joins, `p.`+quoteIdent(to.String)+`=c.`+quoteIdent(from))
		refs = append(refs, to.String)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	value, err := c.convertPKForQueryInTx(tx, row.table, row.pk)
	if err != nil {
		return err
	}
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoteIdent(child)+` c JOIN `+quoteIdent(row.table)+` p ON `+strings.Join(joins, " AND ")+` WHERE p.`+quoteIdent(c.pkByTable[row.table])+`=?)`, value).Scan(&present); err != nil {
		return err
	}
	if !present {
		return nil
	}
	blocked := uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
	blocked.Index = child
	if !row.payload.Valid {
		return blocked
	}
	// An ordinary update never changes its primary key. Other reference keys
	// require exact value proof; use admitted bindings, not textual JSON equality.
	if vacating && deleteAction != "NO ACTION" {
		return blocked
	}
	if !vacating && updateAction != "NO ACTION" && updateAction != "RESTRICT" {
		return blocked
	}
	if err := c.snapshotReferencedValuesSurvive(ctx, tx, row, refs); err != nil {
		return fmt.Errorf("%w: %v", blocked, err)
	}
	return nil
}

func (c *Client) snapshotReferencedValuesSurvive(ctx context.Context, tx sqliteTransaction, row applySurvivor, columns []string) error {
	index := &applyUniqueIndex{}
	for _, column := range columns {
		index.columns = append(index.columns, applyUniqueColumn{name: column, collation: "BINARY"})
	}
	values, err := c.applyUniqueValues(tx, row, index)
	if err != nil {
		return err
	}
	if values == nil {
		return fmt.Errorf("referenced values become null")
	}
	terms := make([]string, len(columns))
	for i, column := range columns {
		terms[i] = quoteIdent(column) + " IS ?"
	}
	key, err := c.convertPKForQueryInTx(tx, row.table, row.pk)
	if err != nil {
		return err
	}
	values = append(values, key)
	var same bool
	err = tx.QueryRowContext(ctx, `SELECT `+strings.Join(terms, " AND ")+` FROM `+quoteIdent(row.table)+` WHERE `+quoteIdent(c.pkByTable[row.table])+`=?`, values...).Scan(&same)
	if err != nil {
		return err
	}
	if !same {
		return fmt.Errorf("referenced parent values change")
	}
	return nil
}
