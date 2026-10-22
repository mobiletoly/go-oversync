package oversqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

// FinalStateApplyBlockedError describes an application correction required before
// ordinary Sync can retry. Keys are exact local identities, never payloads. A
// changed schema/dirty revision invalidates the derived plan through its guard.
type FinalStateApplyBlockedError struct {
	Reason    string
	Operation string
	Table     string
	Index     string
	Trigger   string
	Keys      []string
	HasMore   bool
}

func (e *FinalStateApplyBlockedError) Error() string {
	return "final state apply blocked: " + e.Reason + " (" + e.Table + ")"
}

type applyUniqueColumn struct{ name, collation string }
type applyUniqueIndex struct {
	name    string
	columns []applyUniqueColumn
}

func uniqueBlocked(p *applyPlan, row applySurvivor, reason string) *FinalStateApplyBlockedError {
	return &FinalStateApplyBlockedError{Reason: reason, Operation: p.kind, Table: row.table, Keys: []string{row.pk}}
}

// One index per durable preparation step; metadata and decoded row copies are
// admitted before materialization. Edges share the existing charged apply owner.
func (c *Client) prepareUniqueStep(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	if (p.kind == "settlement" || p.kind == "conflict") && !p.preservationComplete {
		if err := c.prepareSettlementIntent(ctx, tx, p); err != nil {
			return err
		}
	}
	if p.root >= p.entries {
		p.phase, p.visit = "unique_schedule", 0
		return nil
	}
	row, _, err := c.loadApplySurvivor(ctx, tx, p, p.root+1)
	if err != nil {
		return err
	}
	if !row.payload.Valid {
		p.root++
		p.fk = 0
		p.cursor = sql.NullString{}
		return nil
	}
	index, err := c.nextApplyUniqueIndex(ctx, tx, p, row)
	if err != nil {
		return err
	}
	if index == nil {
		p.root++
		p.fk = 0
		p.cursor = sql.NullString{}
		return nil
	}
	values, err := c.applyUniqueValues(tx, row, index)
	if err != nil {
		return err
	}
	if values != nil && p.fk == 0 {
		if err = c.prepareUniqueDependency(ctx, tx, p, row, index, values); err != nil {
			return err
		}
	}
	if values != nil {
		complete, err := c.checkPriorUniqueDesired(ctx, tx, p, row, index, values)
		if err != nil {
			return err
		}
		if !complete {
			return nil
		}
	}
	p.cursor = sql.NullString{String: index.name, Valid: true}
	p.fk = 0
	return nil
}

func (c *Client) nextApplyUniqueIndex(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor) (*applyUniqueIndex, error) {
	var name string
	var partial int
	cursor := ""
	if p.cursor.Valid {
		cursor = p.cursor.String
	}
	// Primary-key uniqueness is already handled by the authoritative key mapping.
	err := scanTransactionRow(ctx, tx, `SELECT name,partial FROM pragma_index_list(?) WHERE "unique"=1 AND origin<>'pk' AND name>? ORDER BY name LIMIT 1`, []any{row.table, cursor}, &name, &partial)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = c.checkApplyWorkspace(p, 16, 4096+12*int64(len(name))); err != nil {
		return nil, err
	}
	blocked := uniqueBlocked(p, row, "unsupported_uniqueness")
	blocked.Index = name
	if partial != 0 {
		return nil, blocked
	}
	info, err := c.getTableInfoTx(tx, row.table)
	if err != nil {
		return nil, err
	}
	if err = c.checkApplyWorkspace(p, int64(8*len(info.Columns)), int64(1024*len(info.Columns))); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT name,coll,cid FROM pragma_index_xinfo(?) WHERE "key"=1 ORDER BY seqno`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	index := &applyUniqueIndex{name: name}
	for rows.Next() {
		var column, coll sql.NullString
		var cid int
		if err = rows.Scan(&column, &coll, &cid); err != nil {
			return nil, err
		}
		if cid < 0 || !column.Valid {
			return nil, blocked
		}
		index.columns = append(index.columns, applyUniqueColumn{column.String, coll.String})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func (c *Client) applyUniqueValues(tx tableInfoQueryer, row applySurvivor, index *applyUniqueIndex) ([]any, error) {
	if !row.payload.Valid {
		return nil, nil
	}
	payload, err := jcs.DecodeObject([]byte(row.payload.String))
	if err != nil {
		return nil, err
	}
	if !row.wire {
		if err = c.validateAndUnwrapCapturedPayload(row.table, payload); err != nil {
			return nil, err
		}
	}
	values := make([]any, 0, len(index.columns))
	for _, col := range index.columns {
		value, ok := payload[strings.ToLower(col.name)]
		if !ok {
			return nil, fmt.Errorf("unique column is absent from desired state")
		}
		if value == nil {
			return nil, nil
		}
		if row.wire {
			value, err = c.sqliteValueWithDeclaredCodec(row.table, ColumnInfo{Name: col.name}, value)
		} else {
			codec, _, ok := c.declaredColumn(row.table, col.name)
			if !ok {
				return nil, fmt.Errorf("unique column is absent from contract")
			}
			value, err = sqliteValueFromLocalCodec(codec, value)
		}
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
func uniqueValuesEqual(ctx context.Context, tx sqliteTransaction, index *applyUniqueIndex, left, right []any) (bool, error) {
	if left == nil || right == nil {
		return false, nil
	}
	terms := make([]string, len(left))
	args := make([]any, 0, 2*len(left))
	for i, col := range index.columns {
		terms[i] = "? = ? COLLATE " + quoteIdent(col.collation)
		args = append(args, left[i], right[i])
	}
	var same bool
	err := scanTransactionRow(ctx, tx, "SELECT "+strings.Join(terms, " AND "), args, &same)
	return same, err
}
func (c *Client) uniqueCurrentHolder(ctx context.Context, tx sqliteTransaction, row applySurvivor, index *applyUniqueIndex, values []any) (string, bool, error) {
	terms := make([]string, len(values))
	for i, col := range index.columns {
		terms[i] = quoteIdent(col.name) + " COLLATE " + quoteIdent(col.collation) + "=?"
	}
	pk := c.pkByTable[row.table]
	info, err := c.getTableInfoTx(tx, row.table)
	if err != nil {
		return "", false, err
	}
	key, err := c.convertPKForQueryInTx(tx, row.table, row.pk)
	if err != nil {
		return "", false, err
	}
	args := append(append([]any{}, values...), key)
	var holder string
	err = scanTransactionRow(ctx, tx, "SELECT "+applyKeyExpression(pk, info.PrimaryKeyIsBlob)+" FROM "+quoteIdent(row.table)+" WHERE "+strings.Join(terms, " AND ")+" AND "+quoteIdent(pk)+"<>? LIMIT 1", args, &holder)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return holder, err == nil, err
}
func (c *Client) putUniqueEdge(ctx context.Context, tx sqliteTransaction, p *applyPlan, table, waiter, holder string) error {
	size := int64(64 + 12*len(table) + 6*(len(waiter)+len(holder)))
	if err := c.checkApplyWorkspace(p, 1, size); err != nil {
		return err
	}
	tag, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO _sync_apply_edges(ordinal,parent_table,parent_pk,child_table,child_pk,bytes,edge_kind) VALUES(?,?,?,?,?,?,'u')`, p.edgeEntries+1, table, waiter, table, holder, size)
	if err != nil {
		return err
	}
	n, err := tag.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		p.edgeEntries++
		p.edgeBytes += size
	}
	return nil
}

// Once preparation is complete, all graph processing and business writes belong
// to this one preadmitted atomic unit. No cursor containing business changes may
// be published as a work yield.
func (c *Client) installUniqueFinalState(ctx context.Context, tx sqliteTransaction, cache *txStmtCache) error {
	invocation := uniqueInvocation(ctx)
	if invocation == nil || !invocation.required {
		return nil
	}
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return err
	}
	if p.phase == "installed" {
		invocation.installed = true
		return nil
	}
	var edges int64
	if err = scanTransactionRow(ctx, tx, `SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'`, nil, &edges); err != nil {
		return err
	}
	if edges == 0 && p.kind != "snapshot" {
		return nil
	}
	if p.phase != "ready" {
		return fmt.Errorf("unique application requires a ready plan")
	}
	for steps := int64(0); ; steps++ {
		if steps > p.entries*p.entries+p.entries {
			return fmt.Errorf("unique apply planner exceeded its admitted progress bound")
		}
		done, err := c.applyUniqueReadyNode(ctx, tx, cache, p)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	if _, err = c.restoreApplySurvivors(ctx, tx, cache, true); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_apply_plan SET phase='installed' WHERE singleton_key=1`)
	if err == nil {
		invocation.installed = true
	}
	return err
}
func nextUniqueReady(ctx context.Context, tx sqliteTransaction, _ bool) (int64, error) {
	query := `SELECT r.ordinal FROM _sync_apply_rows r WHERE pending<>3`
	query += ` AND NOT EXISTS(SELECT 1 FROM _sync_apply_edges e JOIN _sync_apply_rows h ON h.table_name=e.child_table AND h.local_pk=e.child_pk WHERE e.edge_kind='u' AND e.parent_table=r.table_name AND e.parent_pk=r.local_pk AND h.pending<>3)`
	query += ` ORDER BY r.ordinal LIMIT 1`
	var ordinal int64
	err := scanTransactionRow(ctx, tx, query, nil, &ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return ordinal, err
}

// Mark the smallest unresolved SCC and its indexed incoming FK closure. The
// recursive set remains in SQLite; only one admitted row is materialized at a time.
const uniqueCycleCTE = `WITH RECURSIVE arcs(a,b) AS (
 SELECT a.ordinal,b.ordinal FROM _sync_apply_edges e
 JOIN _sync_apply_rows a ON a.table_name=e.parent_table AND a.local_pk=e.parent_pk
 JOIN _sync_apply_rows b ON b.table_name=e.child_table AND b.local_pk=e.child_pk
 WHERE e.edge_kind='u' AND a.pending<>3 AND b.pending<>3
), reach(a,b) AS (SELECT a,b FROM arcs UNION SELECT r.a,e.b FROM reach r JOIN arcs e ON e.a=r.b),
 anchor(id) AS (SELECT min(a) FROM reach WHERE a=b),
 component(id) AS (SELECT a FROM reach,anchor WHERE b=anchor.id AND a IN(SELECT b FROM reach WHERE a=anchor.id)),
 closure(id) AS (SELECT id FROM component UNION SELECT child.ordinal FROM closure c
 JOIN _sync_apply_rows parent ON parent.ordinal=c.id
 JOIN _sync_apply_edges e ON e.edge_kind='f' AND e.parent_table=parent.table_name AND e.parent_pk=parent.local_pk
 JOIN _sync_apply_rows child ON child.table_name=e.child_table AND child.local_pk=e.child_pk) `

func (c *Client) vacateUniqueComponent(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, p *applyPlan) error {
	if _, err := tx.ExecContext(ctx, uniqueCycleCTE+`UPDATE _sync_apply_rows SET pending=4 WHERE ordinal IN(SELECT id FROM closure)`); err != nil {
		return err
	}
	var count int64
	if err := scanTransactionRow(ctx, tx, `SELECT count(*) FROM _sync_apply_rows WHERE pending=4`, nil, &count); err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("unique apply planner found no cyclic component")
	}
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		var selected bool
		if err := scanTransactionRow(ctx, tx, `SELECT pending=4 FROM _sync_apply_rows WHERE ordinal=?`, []any{ordinal}, &selected); err != nil {
			return err
		}
		if !selected {
			continue
		}
		row, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		if err = c.validateUniqueVacate(ctx, tx, p, row); err != nil {
			return err
		}
	}
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		var selected bool
		if err := scanTransactionRow(ctx, tx, `SELECT pending=4 FROM _sync_apply_rows WHERE ordinal=?`, []any{ordinal}, &selected); err != nil {
			return err
		}
		if !selected {
			continue
		}
		row, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		row.payload = sql.NullString{}
		if err = c.restoreApplySurvivor(ctx, tx, cache, row); err != nil {
			return err
		}
	}
	// Freed holders no longer constrain installation. This is derived graph state;
	// update the authoritative charged counters before any further admission.
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_apply_edges WHERE edge_kind='u' AND EXISTS(SELECT 1 FROM _sync_apply_rows r WHERE r.pending=4 AND r.table_name=child_table AND r.local_pk=child_pk)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_apply_rows SET pending=2 WHERE pending=4`); err != nil {
		return err
	}
	if err := scanTransactionRow(ctx, tx, `SELECT count(*),COALESCE(sum(bytes),0) FROM _sync_apply_edges`, nil, &p.edgeEntries, &p.edgeBytes); err != nil {
		return err
	}
	return saveApplyPlanWithWork(ctx, tx, p, 0)
}
func (c *Client) validateUniqueVacate(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor) error {
	if p.kind == "snapshot" {
		// The final writer already proved every possible vacate, including FK
		// closure, before any business mutation. Reuse that writer-owned proof;
		// repeating it here would scan unmanaged sets once per SCC restoration.
		invocation := uniqueInvocation(ctx)
		if invocation == nil || !invocation.snapshotPreserved {
			return fmt.Errorf("snapshot vacate requires final-writer preservation proof")
		}
		return nil
	}
	unknown, err := c.unknownIncomingPushCoverage(ctx, tx, &pushNode{table: row.table})
	if err != nil {
		return err
	}
	if unknown {
		return uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
	}
	args := []any{row.table}
	marksKnown := make([]string, 0, len(c.config.Tables))
	for _, t := range c.config.Tables {
		args = append(args, strings.ToLower(t.TableName))
		marksKnown = append(marksKnown, "?")
	}
	var table string
	err = scanTransactionRow(ctx, tx, `SELECT s.name FROM sqlite_schema s JOIN pragma_foreign_key_list(s.name) f WHERE s.type='table' AND lower(f."table")=? AND s.name NOT IN (`+strings.Join(marksKnown, ",")+`) LIMIT 1`, args, &table)
	if err == nil {
		return uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	names := []any{row.table}
	marks := make([]string, 0, 6)
	for _, suffix := range []string{"bi_guard", "bu_guard", "bd_guard", "ai", "au", "ad"} {
		names = append(names, "trg_"+row.table+"_"+suffix)
		marks = append(marks, "?")
	}
	var trigger string
	query := `SELECT name FROM (SELECT name,tbl_name FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name,tbl_name FROM sqlite_temp_schema WHERE type='trigger') WHERE tbl_name=? AND name NOT IN (` + strings.Join(marks, ",") + `) LIMIT 1`
	err = scanTransactionRow(ctx, tx, query, names, &trigger)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	e := uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
	e.Trigger = trigger
	return e
}

// Suppress a second execution of already installed business effects while the
// original authoritative/settlement code publishes its metadata decisions.
func (c *Client) installedUniqueRow(ctx context.Context, tx sqliteTransaction, table, pk string) (applySurvivor, bool, error) {
	var ordinal int64
	err := scanTransactionRow(ctx, tx, `SELECT r.ordinal FROM _sync_apply_rows r JOIN _sync_apply_plan p ON p.singleton_key=1 AND p.phase='installed' WHERE r.table_name=? AND r.local_pk=?`, []any{table, pk}, &ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return applySurvivor{}, false, nil
	}
	if err != nil {
		return applySurvivor{}, false, err
	}
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return applySurvivor{}, false, err
	}
	row, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
	return row, true, err
}

type uniqueRawContextKey struct{}
type uniqueApplyContextKey struct{}
type uniqueApplyInvocation struct{ required, installed, snapshot, snapshotPreserved bool }

func withUniqueApplyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, uniqueApplyContextKey{}, &uniqueApplyInvocation{})
}
func uniqueInvocation(ctx context.Context) *uniqueApplyInvocation {
	v, _ := ctx.Value(uniqueApplyContextKey{}).(*uniqueApplyInvocation)
	return v
}

// Installed contract identity does not change when an application changes an
// index or trigger. Both SQLite schemas must invalidate derived ordering too.
func uniqueSchemaGuard(ctx context.Context, tx sqliteTransaction, guard string) (string, error) {
	var main, temporary int64
	if err := scanTransactionRow(ctx, tx, `PRAGMA main.schema_version`, nil, &main); err != nil {
		return "", err
	}
	if err := scanTransactionRow(ctx, tx, `PRAGMA temp.schema_version`, nil, &temporary); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d:%d", guard, main, temporary), nil
}

func (c *Client) hasApplyUniqueness(ctx context.Context, tx sqliteTransaction) (bool, error) {
	args := make([]any, 0, len(c.config.Tables))
	marks := make([]string, 0, len(c.config.Tables))
	for _, t := range c.config.Tables {
		args = append(args, strings.ToLower(t.TableName))
		marks = append(marks, "?")
	}
	var found bool
	err := scanTransactionRow(ctx, tx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema s JOIN pragma_index_list(s.name) i WHERE s.type='table' AND s.name IN (`+strings.Join(marks, ",")+`) AND i."unique"=1 AND i.origin<>'pk')`, args, &found)
	return found, err
}

func (c *Client) reuseInstalledUniqueRow(ctx context.Context, tx sqliteTransaction, table, key string) (bool, error) {
	invocation := uniqueInvocation(ctx)
	if invocation == nil || !invocation.installed {
		return false, nil
	}
	row, ok, err := c.installedUniqueRow(ctx, tx, table, key)
	if err != nil || !ok {
		return ok, err
	}
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return true, err
	}
	var size int64
	if err = scanTransactionRow(ctx, tx, `SELECT bytes FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{table, key}, &size); err != nil {
		return true, err
	}
	matches, err := c.applySurvivorMatches(ctx, tx, p, row, size)
	if err != nil || matches {
		return true, err
	}
	cache := newTxStmtCache(tx)
	defer cache.Close()
	return true, c.restoreApplySurvivor(context.WithValue(ctx, uniqueRawContextKey{}, true), tx, cache, row)
}

func (c *Client) preflightUniqueApply(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	var n int64
	if err := scanTransactionRow(ctx, tx, `SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'`, nil, &n); err != nil {
		return err
	}
	if n == 0 && p.kind != "snapshot" {
		return nil
	}
	// Recursive reachability is SQLite-owned but still charged, including its
	// worst-case pair set, query workspace and atomic statement visits.
	pairs := saturatingWorkAdd(p.entries, saturatingWorkMultiply(p.visit, max(p.visit, 1)))
	if err := c.checkApplyWorkspace(p, saturatingWorkMultiply(pairs, 4), saturatingWorkAdd(4096, saturatingWorkMultiply(pairs, 128))); err != nil {
		return err
	}
	return c.reserveReadySyncWork(ctx, p.unit, saturatingWorkAdd(saturatingWorkMultiply(pairs, 8), saturatingWorkMultiply(p.entries, 256)))
}

// Prove the acyclic prefix using staging alone, one vertex per durable step.
// Only the unresolved remainder needs quadratic SCC reachability workspace.
func (c *Client) prepareUniqueScheduleStep(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	query := `SELECT r.ordinal FROM _sync_apply_rows r WHERE pending<>5`
	query += ` AND NOT EXISTS(SELECT 1 FROM _sync_apply_edges e JOIN _sync_apply_rows h ON h.table_name=e.child_table AND h.local_pk=e.child_pk WHERE e.edge_kind='u' AND e.parent_table=r.table_name AND e.parent_pk=r.local_pk AND h.pending<>5)`
	var ordinal int64
	err := scanTransactionRow(ctx, tx, query+` ORDER BY r.ordinal LIMIT 1`, nil, &ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		if err = scanTransactionRow(ctx, tx, `SELECT count(*) FROM _sync_apply_rows WHERE pending<>5`, nil, &p.visit); err != nil {
			return err
		}
		// Retain the admitted acyclic prefix for snapshot preservation analysis.
		if _, err = tx.ExecContext(ctx, `UPDATE _sync_apply_rows SET pending=6 WHERE pending=5`); err != nil {
			return err
		}
		p.phase = "ready"
		return nil
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_apply_rows SET pending=5 WHERE ordinal=?`, ordinal)
	return err
}
func (c *Client) installUniqueFinalStateOwned(ctx context.Context, tx sqliteTransaction) (err error) {
	cache := newTxStmtCache(tx)
	defer func() { err = errors.Join(err, cache.Close()) }()
	return c.installUniqueFinalState(ctx, tx, cache)
}

func (c *Client) checkPriorUniqueDesired(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, index *applyUniqueIndex, values []any) (bool, error) {
	var ordinal int64
	err := scanTransactionRow(ctx, tx, `SELECT ordinal FROM _sync_apply_rows WHERE table_name=? AND ordinal>? AND ordinal<? AND payload IS NOT NULL ORDER BY ordinal LIMIT 1`, []any{row.table, p.fk, p.root + 1}, &ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var size int64
	if err = scanTransactionRow(ctx, tx, `SELECT bytes FROM _sync_apply_rows WHERE ordinal=?`, []any{ordinal}, &size); err != nil {
		return false, err
	}
	if err = c.checkApplyWorkspace(p, 16, 12*(size+int64(len(row.payload.String))+4096)); err != nil {
		return false, err
	}
	other, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
	if err != nil {
		return false, err
	}
	otherValues, err := c.applyUniqueValues(tx, other, index)
	if err != nil {
		return false, err
	}
	same, err := uniqueValuesEqual(ctx, tx, index, values, otherValues)
	if err != nil {
		return false, err
	}
	if same {
		e := uniqueBlocked(p, row, "conflicting_desired_state")
		e.Index = index.name
		e.Keys = append(e.Keys, other.pk)
		return false, e
	}
	p.fk = ordinal
	return false, nil
}

func (c *Client) prepareUniqueDependency(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, index *applyUniqueIndex, values []any) error {
	holder, found, err := c.uniqueCurrentHolder(ctx, tx, row, index, values)
	if err != nil {
		return err
	}
	if found {
		var ordinal int64
		if err = scanTransactionRow(ctx, tx, `SELECT ordinal FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{row.table, holder}, &ordinal); errors.Is(err, sql.ErrNoRows) {
			e := uniqueBlocked(p, row, "conflicting_desired_state")
			e.Index = index.name
			e.Keys = append(e.Keys, holder)
			return e
		} else if err != nil {
			return err
		}
		other, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		otherValues, err := c.applyUniqueValues(tx, other, index)
		if err != nil {
			return err
		}
		same, err := uniqueValuesEqual(ctx, tx, index, values, otherValues)
		if err != nil {
			return err
		}
		if same {
			e := uniqueBlocked(p, row, "conflicting_desired_state")
			e.Index = index.name
			e.Keys = append(e.Keys, holder)
			return e
		}
		if err = c.putUniqueEdge(ctx, tx, p, row.table, row.pk, holder); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) applyUniqueReadyNode(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, p *applyPlan) (bool, error) {
	var remaining bool
	if err := scanTransactionRow(ctx, tx, `SELECT EXISTS(SELECT 1 FROM _sync_apply_rows WHERE pending<>3)`, nil, &remaining); err != nil {
		return false, err
	}
	if !remaining {
		return true, nil
	}
	ordinal, err := nextUniqueReady(ctx, tx, p.kind == "snapshot")
	if err != nil {
		return false, err
	}
	if ordinal == 0 {
		if err = c.vacateUniqueComponent(ctx, tx, cache, p); err != nil {
			return false, err
		}
		ordinal, err = nextUniqueReady(ctx, tx, false)
		if err != nil {
			return false, err
		}
		if ordinal == 0 {
			return false, fmt.Errorf("unique apply planner made no progress")
		}
	}
	row, size, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
	if err != nil {
		return false, err
	}
	matches, err := c.applySurvivorMatches(ctx, tx, p, row, size)
	if err != nil {
		return false, err
	}
	if !matches {
		if err = c.restoreApplySurvivor(ctx, tx, cache, row); err != nil {
			return false, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE _sync_apply_rows SET pending=3 WHERE ordinal=?`, ordinal); err != nil {
		return false, err
	}
	return false, nil
}

// This phase receives already decoded physical bindings. It suppresses only the
// duplicate business mutation; the original caller still publishes metadata.
func (c *Client) reusePreparedUniqueUpsert(ctx context.Context, query tableInfoQueryer, table string, info *TableInfo, pkColumn string, values []interface{}) (bool, error) {
	if ctx.Value(uniqueRawContextKey{}) != nil {
		return false, nil
	}
	tx, ok := query.(sqliteTransaction)
	if !ok {
		return false, nil
	}
	var key string
	for i, col := range info.Columns {
		if strings.EqualFold(col.Name, pkColumn) {
			switch value := values[i].(type) {
			case string:
				key = value
			case []byte:
				key = hex.EncodeToString(value)
			}
			break
		}
	}
	return c.reuseInstalledUniqueRow(ctx, tx, table, key)
}

// Discovery and schema binding precede loading any retained positional cursor.
func (c *Client) applyPreparationGuard(ctx context.Context, tx sqliteTransaction, unit string) (string, error) {
	invocation := uniqueInvocation(ctx)
	if invocation != nil {
		required, err := c.hasApplyUniqueness(ctx, tx)
		if err != nil {
			return "", err
		}
		invocation.required = required || invocation.snapshot
	}
	guard, err := c.applyGuard(ctx, tx, unit)
	if err != nil {
		return "", err
	}
	if invocation != nil && invocation.required {
		return uniqueSchemaGuard(ctx, tx, guard)
	}
	return guard, nil
}
