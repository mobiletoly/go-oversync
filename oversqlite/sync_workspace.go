package oversqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

type workspaceReservation struct {
	Token   string `json:"token"`
	Unit    string `json:"unit"`
	Context string `json:"context"`
	Entries int64  `json:"entries"`
	Bytes   int64  `json:"bytes"`
}

func workspaceIdentityValid(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, b := range value {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

// Scalar counters and the finite allocation records are one writer-owned
// snapshot. Opening a Client or starting a transaction never reclaims it.
func (c *Client) loadWorkspaceReservations(ctx context.Context, tx sqliteTransaction) ([]workspaceReservation, error) {
	if err := c.validateWorkspaceReservationMetadata(ctx, tx); err != nil {
		return nil, err
	}
	var text string
	var count, bytes int64
	if err := scanTransactionRow(ctx, tx, `SELECT reserved_entries,reserved_bytes,workspace_context FROM _sync_operation_state WHERE singleton_key=1`, nil, &count, &bytes, &text); err != nil {
		return nil, err
	}
	if text == "" {
		if count != 0 || bytes != 0 {
			return nil, fmt.Errorf("invalid empty reservation ledger")
		}
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	var records []workspaceReservation
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("invalid workspace reservation ledger: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid trailing reservation ledger")
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("empty reservation ledger must use empty TEXT")
	}
	seen := map[string]bool{}
	total := workspaceCharge{}
	for _, record := range records {
		if !workspaceIdentityValid(record.Token) || !workspaceIdentityValid(record.Unit) || !workspaceIdentityValid(record.Context) || seen[record.Token] {
			return nil, fmt.Errorf("invalid reservation identity")
		}
		seen[record.Token] = true
		var err error
		total, err = c.addWorkspaceCharge(record.Unit, total, workspaceCharge{record.Entries, record.Bytes})
		if err != nil {
			return nil, err
		}
	}
	if total.entries != count || total.bytes != bytes {
		return nil, fmt.Errorf("reservation totals differ from allocation records")
	}
	return records, nil
}

func storeWorkspaceReservations(ctx context.Context, tx sqliteTransaction, records []workspaceReservation) error {
	var entries, bytes int64
	for _, record := range records {
		if record.Entries < 0 || record.Bytes < 0 || record.Entries > math.MaxInt64-entries || record.Bytes > math.MaxInt64-bytes {
			return fmt.Errorf("invalid reservation totals")
		}
		entries += record.Entries
		bytes += record.Bytes
	}
	text := ""
	if len(records) > 0 {
		data, err := json.Marshal(records)
		if err != nil {
			return err
		}
		text = string(data)
	}
	if accounting, _ := ctx.Value(statementAccountingKey{}).(bool); accounting {
		return executeSingleStatement(ctx, tx, `UPDATE _sync_operation_state SET workspace_context=?,reserved_entries=?,reserved_bytes=? WHERE singleton_key=1`, text, entries, bytes)
	}
	_, err := tx.ExecContext(ctx, `UPDATE _sync_operation_state SET workspace_context=?,reserved_entries=?,reserved_bytes=? WHERE singleton_key=1`, text, entries, bytes)
	return err
}

func (c *Client) reserveWorkspace(ctx context.Context, unit string, requested workspaceCharge) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !workspaceIdentityValid(unit) {
		return "", fmt.Errorf("invalid reservation unit")
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	binding, err := c.applyGuard(ctx, tx, unit)
	if err != nil {
		return "", err
	}
	// Include the new ledger record, its parse/encode copies and a metadata slot
	// before reading or growing the ledger. Each live buffer has its own charge.
	charge, err := c.addWorkspaceCharge(unit, requested, workspaceCharge{1, 4096})
	if err != nil {
		return "", err
	}
	if err = c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, charge); err != nil {
		return "", err
	}
	records, err := c.loadWorkspaceReservations(ctx, tx)
	if err != nil {
		return "", err
	}
	var tokenBytes [32]byte
	if _, err = rand.Read(tokenBytes[:]); err != nil {
		return "", err
	}
	token := fmt.Sprintf("%x", tokenBytes)
	records = append(records, workspaceReservation{token, unit, binding, charge.entries, charge.bytes})
	if err = storeWorkspaceReservations(ctx, tx, records); err != nil {
		return "", err
	}
	if err = c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{}); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}

func (c *Client) releaseWorkspace(ctx context.Context, token string) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	records, err := c.loadWorkspaceReservations(ctx, tx)
	if err != nil {
		return err
	}
	for i, record := range records {
		if record.Token == token {
			copy(records[i:], records[i+1:])
			records = records[:len(records)-1]
			if err = storeWorkspaceReservations(ctx, tx, records); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
	}
	return nil // Once-only release is idempotent; it cannot release another slot.
}

// An existing invocation carries allocation identity through preparation,
// HTTP and settlement. The ledger covers every simultaneous supported handle.
type freshLifetimeAdmissionKey struct{}
type workspaceBufferKey struct{}
type workspaceBufferScope struct {
	journalBodyBytes int64
	token, unit      string
	client           *Client
	base             workspaceCharge
	statementWork    int64
}

func (c *Client) resizeWorkspaceInTx(ctx context.Context, tx sqliteTransaction, token string, requested workspaceCharge) error {
	if cache := statementCacheFrom(ctx, tx); cache != nil && cache.pool != nil && !cache.pool.released && cache.pool.scope.token == token {
		var err error
		requested, err = c.addWorkspaceCharge(cache.pool.scope.unit, requested, cache.pool.capacity)
		if err != nil {
			return err
		}
	}
	if err := c.checkAggregateWorkspace(ctx, tx, applyUnit("reservation", 0, ""), nil, nil, workspaceCharge{}); err != nil {
		return err
	}
	records, err := c.loadWorkspaceReservations(ctx, tx)
	if err != nil {
		return err
	}
	for i, record := range records {
		if record.Token != token {
			continue
		}
		next, err := c.addWorkspaceCharge(record.Unit, requested, workspaceCharge{1, 4096})
		if err != nil {
			return err
		}
		next.entries = max(next.entries, record.Entries)
		next.bytes = max(next.bytes, record.Bytes)
		if err = c.checkAggregateWorkspace(ctx, tx, record.Unit, nil, nil, workspaceCharge{next.entries - record.Entries, next.bytes - record.Bytes + 6*max(int64(0), decimalWidth(next.entries)+decimalWidth(next.bytes)-decimalWidth(record.Entries)-decimalWidth(record.Bytes))}); err != nil {
			return err
		}
		binding, err := c.applyGuard(ctx, tx, record.Unit)
		if err != nil {
			return err
		}
		records[i] = workspaceReservation{record.Token, record.Unit, binding, next.entries, next.bytes}
		if err = storeWorkspaceReservations(ctx, tx, records); err != nil {
			return err
		}
		return c.checkAggregateWorkspace(ctx, tx, record.Unit, nil, nil, workspaceCharge{})
	}
	return fmt.Errorf("live workspace reservation is missing")
}

func (c *Client) admitPushBufferCopies(ctx context.Context, tx sqliteTransaction, charge workspaceCharge) error {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return nil
	} // Unconverted conflict callers belong to A3-03.
	if err := c.resizeWorkspaceInTx(ctx, tx, scope.token, charge); err != nil {
		return err
	}
	scope.base.entries = max(scope.base.entries, charge.entries)
	scope.base.bytes = max(scope.base.bytes, charge.bytes)
	return nil
}

func (c *Client) growPushHTTPBuffers(ctx context.Context, bytes, entries int64) error {
	return c.completeWork(ctx, "HTTP buffer admission", func(ctx context.Context) error {
		return c.growPushHTTPBuffersSlice(ctx, bytes, entries)
	})
}

func (c *Client) growPushHTTPBuffersSlice(ctx context.Context, bytes, entries int64) error {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return nil
	}
	if err := c.reserveSyncWork(ctx, scope.unit, 16); err != nil {
		return err
	}
	copyBytes, err := c.workspaceScale(scope.unit, bytes, 64)
	if err != nil {
		return err
	}
	charge, err := c.addWorkspaceCharge(scope.unit, scope.base, workspaceCharge{entries, copyBytes})
	if err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = c.resizeWorkspaceInTx(ctx, tx, scope.token, charge); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Admission metadata is reconciled only on opening, never by a payload SUM on
// allocation. Each indexed step reads scalar lengths/charges, not payload TEXT.
func reconcileWorkspaceOwners(ctx context.Context, q upgradeQueryer, config *Config) error {
	c := &Client{config: config}
	unit := applyUnit("retained-reconciliation", 0, "")
	for _, owner := range []struct{ table, charge, columns, counter string }{
		{"_sync_snapshot_stage", "charge", "64+6*(length(CAST(snapshot_id AS BLOB))+length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))", "SELECT snapshot_stage_entries,snapshot_stage_bytes FROM _sync_operation_state WHERE singleton_key=1"},
		{"_sync_push_stage", "charge", "64+6*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(op AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))", "SELECT push_stage_entries,push_stage_bytes FROM _sync_operation_state WHERE singleton_key=1"},
		{"_sync_apply_rows", "bytes", "96+6*(length(CAST(table_name AS BLOB))+length(CAST(local_pk AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))", "SELECT entries,bytes FROM _sync_apply_plan WHERE singleton_key=1"},
		{"_sync_apply_edges", "bytes", "64+6*(length(CAST(parent_table AS BLOB))+length(CAST(parent_pk AS BLOB))+length(CAST(child_table AS BLOB))+length(CAST(child_pk AS BLOB)))", "SELECT edge_entries,edge_bytes FROM _sync_apply_plan WHERE singleton_key=1"},
	} {
		cursor := int64(math.MinInt64)
		total := workspaceCharge{}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			var ordinal, stored, expected int64
			var kind string
			err := scanTransactionRow(ctx, q, `SELECT rowid,`+owner.charge+`,`+owner.columns+`,typeof(`+owner.charge+`) FROM `+owner.table+` WHERE rowid>? ORDER BY rowid LIMIT 1`, []any{cursor}, &ordinal, &stored, &expected, &kind)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return err
			}
			if ordinal <= 0 || kind != "integer" || stored != expected || stored < 0 {
				return fmt.Errorf("invalid retained row charge in %s", owner.table)
			}
			total, err = c.addWorkspaceCharge(unit, total, workspaceCharge{1, stored})
			if err != nil {
				return err
			}
			cursor = ordinal
		}
		var recorded workspaceCharge
		err := scanTransactionRow(ctx, q, owner.counter, nil, &recorded.entries, &recorded.bytes)
		if errors.Is(err, sql.ErrNoRows) && total.entries == 0 {
			continue
		}
		if err != nil {
			return err
		}
		if recorded != total {
			return fmt.Errorf("retained totals differ in %s", owner.table)
		}
	}
	return nil
}

// Called solely after fresh acquisition of the existing database lifetime
// lease. No retained stage/plan/input or authoritative owner is discarded.
func reclaimWorkspaceReservations(ctx context.Context, tx sqliteTransaction, config *Config) error {
	c := &Client{config: config}
	if err := c.validateWorkspaceReservationMetadata(ctx, tx); err != nil {
		return err
	}
	return storeWorkspaceReservations(ctx, tx, nil)
}

// Reclamation needs no host copy of an abandoned JSON ledger. Inspect bounded
// scalar records using a single streaming cursor, with work/cancellation checks.
func (c *Client) validateWorkspaceReservationMetadata(ctx context.Context, tx sqliteTransaction) (resultErr error) {
	var entries, bytes, empty, valid int64
	err := scanTransactionRow(ctx, tx, `SELECT reserved_entries,reserved_bytes,workspace_context='',CASE WHEN workspace_context='' THEN 1 ELSE json_valid(workspace_context) END FROM _sync_operation_state WHERE singleton_key=1`, nil, &entries, &bytes, &empty, &valid)
	if err != nil {
		return err
	}
	if empty == 1 {
		if entries != 0 || bytes != 0 {
			return fmt.Errorf("invalid empty reservation totals")
		}
		return nil
	}
	if valid != 1 {
		return fmt.Errorf("invalid reservation JSON")
	}
	var array string
	if err = scanTransactionRow(ctx, tx, `SELECT json_type(workspace_context) FROM _sync_operation_state WHERE singleton_key=1`, nil, &array); err != nil {
		return err
	}
	if array != "array" {
		return fmt.Errorf("reservation ledger must be an array")
	}
	rows, err := tx.QueryContext(ctx, `SELECT
        json_type(j.value), (SELECT count(*) FROM json_each(j.value)),
        length(json_extract(j.value,'$.token')),length(json_extract(j.value,'$.unit')),length(json_extract(j.value,'$.context')),
        json_type(j.value,'$.entries'),json_type(j.value,'$.bytes'),json_extract(j.value,'$.entries'),json_extract(j.value,'$.bytes'),
        json_extract(j.value,'$.token') NOT GLOB '*[^0-9a-f]*',json_extract(j.value,'$.unit') NOT GLOB '*[^0-9a-f]*',json_extract(j.value,'$.context') NOT GLOB '*[^0-9a-f]*'
        FROM _sync_operation_state o,json_each(o.workspace_context) j WHERE singleton_key=1`)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	total := workspaceCharge{}
	count := 0
	for rows.Next() {
		if err = ctx.Err(); err != nil {
			return err
		}
		var kind, entryKind, byteKind string
		var fields, a, b, d, hexA, hexB, hexD int64
		var charge workspaceCharge
		if err = rows.Scan(&kind, &fields, &a, &b, &d, &entryKind, &byteKind, &charge.entries, &charge.bytes, &hexA, &hexB, &hexD); err != nil {
			return err
		}
		if kind != "object" || fields != 5 || a != 64 || b != 64 || d != 64 || hexA != 1 || hexB != 1 || hexD != 1 || entryKind != "integer" || byteKind != "integer" {
			return fmt.Errorf("invalid reservation record")
		}
		total, err = c.addWorkspaceCharge(applyUnit("reservation-reconciliation", 0, ""), total, charge)
		if err != nil {
			return err
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	var records, unique int64
	if err = scanTransactionRow(ctx, tx, `SELECT count(*),count(DISTINCT json_extract(j.value,'$.token')) FROM _sync_operation_state o,json_each(o.workspace_context) j WHERE singleton_key=1`, nil, &records, &unique); err != nil {
		return err
	}
	if records != unique {
		return fmt.Errorf("duplicate reservation token")
	}
	if count == 0 || total.entries != entries || total.bytes != bytes {
		return fmt.Errorf("reservation scalar totals differ")
	}
	return nil
}

const (
	defaultSyncWorkspaceEntries = 131072
	defaultSyncWorkspaceBytes   = 256 << 20
	defaultSyncWorkUnits        = 1000000
)

// SyncWorkLimitError is the internal bounded-slice signal. Ordinary completion
// operations resume its durable phase inside the library. Retained for decoding
// historical results and bounded-step diagnostics; it is not a transport failure.
type SyncWorkLimitError struct {
	Unit        string
	Limit, Used int64
	Progress    SyncProgress
}

func (e *SyncWorkLimitError) Error() string {
	return fmt.Sprintf("sync_work_limit: unit=%s limit=%d used=%d", e.Unit, e.Limit, e.Used)
}

// SyncWorkspaceLimitError requires more capacity or application action. Neither
// it nor SyncWorkLimitError consumes a checkpoint or discards pending intent.
type SyncWorkspaceLimitError struct {
	Unit, Dimension       string
	Limit, Used, Reserved int64
	Resumable             bool
	Progress              SyncProgress
}

func (e *SyncWorkspaceLimitError) Error() string {
	return fmt.Sprintf("sync_workspace_limit: unit=%s dimension=%s limit=%d used=%d reserved=%d", e.Unit, e.Dimension, e.Limit, e.Used, e.Reserved)
}

func (c *Client) reserveSyncWork(ctx context.Context, unit string, work int64) error {
	return c.reserveSyncWorkKind(ctx, unit, work, false)
}

func (c *Client) reserveReadySyncWork(ctx context.Context, unit string, work int64) error {
	return c.reserveSyncWorkKind(ctx, unit, work, true)
}

func saturatingWorkAdd(used, work int64) int64 {
	if work > math.MaxInt64-used {
		return math.MaxInt64
	}
	return used + work
}

func saturatingWorkMultiply(count, weight int64) int64 {
	if count > math.MaxInt64/weight {
		return math.MaxInt64
	}
	return count * weight
}

func (c *Client) reserveSyncWorkKind(ctx context.Context, unit string, work int64, readyAtomic bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	limit := c.config.MaxSyncWorkUnitsPerInvocation
	if work < 0 || c.syncWorkUsed < 0 {
		return fmt.Errorf("invalid sync work accounting")
	}
	if work == 0 {
		return nil
	}
	slice := workSliceFrom(ctx)
	firstUsefulStep := slice != nil && !slice.checkpointed
	if c.syncWorkUsed >= limit && !firstUsefulStep && (!readyAtomic || c.readyAtomicUnitDispatched) {
		return &SyncWorkLimitError{Unit: unit, Limit: limit, Used: c.syncWorkUsed}
	}
	if readyAtomic {
		c.readyAtomicUnitDispatched = true
	}
	c.syncWorkUsed = saturatingWorkAdd(c.syncWorkUsed, work)
	c.a3Stats.totalWork = saturatingWorkAdd(c.a3Stats.totalWork, work)
	return nil
}

func (c *Client) checkSyncWorkspace(unit string, entries, bytes, extraEntries, extraBytes int64) error {
	for _, bound := range []struct {
		name               string
		limit, used, extra int64
	}{
		{"entries", c.config.MaxSyncWorkspaceEntries, entries, extraEntries},
		{"bytes", c.config.MaxSyncWorkspaceBytes, bytes, extraBytes},
	} {
		if bound.used < 0 || bound.extra < 0 || bound.used > bound.limit || bound.extra > bound.limit-bound.used {
			return &SyncWorkspaceLimitError{Unit: unit, Dimension: bound.name, Limit: bound.limit, Used: bound.used, Reserved: bound.extra}
		}
	}
	return nil
}

// Attach progress under a final reserved local read. The existing invocation
// owns the unique confirmed prefix across nested public push calls.
func (c *Client) withSyncErrorProgress(ctx context.Context, result error) error {
	if result == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var group *PushDependencyGroupExceedsAdmittedLimitError
	var fk *FKReconciliationRequiredError
	var base *ResolutionBaseUnavailableError
	var work *SyncWorkLimitError
	var space *SyncWorkspaceLimitError
	if !errors.As(result, &group) && !errors.As(result, &fk) && !errors.As(result, &base) && !errors.As(result, &work) && !errors.As(result, &space) {
		return result
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return syncProgressReadFailure(ctx, err, result)
	}
	defer tx.Rollback()
	var progress SyncProgress
	err = scanTransactionRow(ctx, tx, `SELECT (SELECT COUNT(*) FROM _sync_dirty_rows),(SELECT COUNT(*) FROM _sync_outbox_rows)`, nil, &progress.DirtyRows, &progress.OutboundRows)
	if err != nil {
		return syncProgressReadFailure(ctx, err, result)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if invocation := invocationFromContext(ctx); invocation != nil {
		progress.CommittedBundles = int64(len(invocation.confirmed))
	}
	switch {
	case group != nil:
		group.Progress = progress
	case fk != nil:
		fk.Progress = progress
	case base != nil:
		base.Progress = progress
	case work != nil:
		work.Progress = progress
	case space != nil:
		space.Progress = progress
	}
	return result
}

// A failed final read must retain cancellation identity and precedence even
// when cancellation arrived after admission or while reserving its connection.
func syncProgressReadFailure(ctx context.Context, readErr, result error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
		return readErr
	}
	return fmt.Errorf("final progress read: %v: %w", readErr, result)
}

// workspaceCharge counts one independently retained representation. All callers
// use the same database-owned aggregate, including stages and live reservations.
type workspaceCharge struct{ entries, bytes int64 }

func (c *Client) addWorkspaceCharge(unit string, total, extra workspaceCharge) (workspaceCharge, error) {
	for _, value := range []struct {
		dimension              string
		used, requested, limit int64
	}{
		{"entries", total.entries, extra.entries, c.config.MaxSyncWorkspaceEntries},
		{"bytes", total.bytes, extra.bytes, c.config.MaxSyncWorkspaceBytes},
	} {
		if value.used < 0 || value.requested < 0 {
			return workspaceCharge{}, fmt.Errorf("invalid negative workspace %s metadata", value.dimension)
		}
		if value.requested > int64(^uint64(0)>>1)-value.used {
			return workspaceCharge{}, &SyncWorkspaceLimitError{Unit: unit, Dimension: value.dimension, Limit: value.limit, Used: value.used, Reserved: value.requested}
		}
	}
	return workspaceCharge{total.entries + extra.entries, total.bytes + extra.bytes}, nil
}

func (c *Client) workspaceScale(unit string, bytes, copies int64) (int64, error) {
	if bytes < 0 || copies < 0 {
		return 0, fmt.Errorf("invalid negative workspace byte measurement")
	}
	if copies != 0 && bytes > int64(^uint64(0)>>1)/copies {
		return 0, &SyncWorkspaceLimitError{Unit: unit, Dimension: "bytes", Limit: c.config.MaxSyncWorkspaceBytes, Used: bytes, Reserved: int64(^uint64(0) >> 1)}
	}
	return bytes * copies, nil
}

// Read only singleton owner metadata; never scan payload rows for admission.
// Overrides contain the writer's current unsaved plan state, counted once.
func (c *Client) workspaceUsage(ctx context.Context, q queryRower, unit string, apply, push *workspaceCharge) (workspaceCharge, error) {
	scope, _ := ctx.Value(pushPlanningWorkspaceKey{}).(*pushPlanningWorkspace)
	if scope == nil || scope.writer != q {
		return c.workspaceUsageUncached(ctx, q, unit, apply, push)
	}
	cacheCharge := pushPlanningWorkspaceCharge
	if scope.guard != nil {
		var err error
		cacheCharge, err = c.sumWorkspaceCharges(unit, cacheCharge, pushPlanningGuardCharge)
		if err != nil {
			return workspaceCharge{}, err
		}
	}
	if apply == nil {
		if scope.base == nil {
			base, err := c.workspaceUsageUncached(ctx, q, unit, nil, &workspaceCharge{})
			if err != nil {
				return workspaceCharge{}, err
			}
			scope.base = &base
		}
		if push == nil {
			current, err := c.retainedPushWorkspaceCharge(ctx, q, unit)
			if err != nil {
				return workspaceCharge{}, err
			}
			push = &current
		}
		return c.sumWorkspaceCharges(unit, *scope.base, *push, cacheCharge)
	}
	usage, err := c.workspaceUsageUncached(ctx, q, unit, apply, push)
	if err != nil {
		return usage, err
	}
	return c.sumWorkspaceCharges(unit, usage, cacheCharge)
}

func (c *Client) workspaceUsageUncached(ctx context.Context, q queryRower, unit string, apply, push *workspaceCharge) (workspaceCharge, error) {
	c.a3Stats.workspaceMetadataReads++
	operation, err := c.operationWorkspaceCharge(ctx, q, unit)
	if err != nil {
		return workspaceCharge{}, err
	}
	if apply == nil {
		charge, e := c.retainedApplyWorkspaceCharge(ctx, q, unit)
		if e != nil {
			return operation, e
		}
		apply = &charge
	}
	if push == nil {
		charge, e := c.retainedPushWorkspaceCharge(ctx, q, unit)
		if e != nil {
			return operation, e
		}
		push = &charge
	}
	return c.sumWorkspaceCharges(unit, operation, *apply, *push)
}

// Each owner contributes its exact maintained totals and descriptor once.
func (c *Client) sumWorkspaceCharges(unit string, charges ...workspaceCharge) (workspaceCharge, error) {
	var total workspaceCharge
	for _, charge := range charges {
		next, err := c.addWorkspaceCharge(unit, total, charge)
		if err != nil {
			return total, err
		}
		total = next
	}
	return total, nil
}

func (c *Client) operationWorkspaceCharge(ctx context.Context, q queryRower, unit string) (workspaceCharge, error) {
	var reserved, snapshot, stagedPush workspaceCharge
	var textBytes int64
	err := scanTransactionRow(ctx, q, `SELECT reserved_entries,reserved_bytes,snapshot_stage_entries,snapshot_stage_bytes,
        push_stage_entries,push_stage_bytes,length(CAST(workspace_context AS BLOB))+
        length(CAST(kind AS BLOB))+length(CAST(target_user_id AS BLOB))+length(CAST(reason AS BLOB))+
        length(CAST(replacement_source_id AS BLOB))+length(CAST(staged_snapshot_id AS BLOB))+length(CAST(snapshot_transfer AS BLOB))+length(CAST(recovery_state AS BLOB))
        FROM _sync_operation_state WHERE singleton_key=1`, nil, &reserved.entries, &reserved.bytes, &snapshot.entries, &snapshot.bytes, &stagedPush.entries, &stagedPush.bytes, &textBytes)
	if err != nil {
		return workspaceCharge{}, err
	}
	// Only fresh lifetime admission can discount terminated host allocations;
	// exact ledger validation and reclamation still precede writable admission.
	if fresh, _ := ctx.Value(freshLifetimeAdmissionKey{}).(bool); fresh {
		var ledgerBytes int64
		if err = scanTransactionRow(ctx, q, `SELECT length(CAST(workspace_context AS BLOB)) FROM _sync_operation_state WHERE singleton_key=1`, nil, &ledgerBytes); err != nil {
			return workspaceCharge{}, err
		}
		textBytes -= ledgerBytes
		reserved = workspaceCharge{}
	}
	descriptor, err := c.workspaceScale(unit, textBytes, 6)
	if err != nil {
		return workspaceCharge{}, err
	}
	var journalEntries, journalBytes int64
	if err := scanTransactionRow(ctx, q, `SELECT count(*),COALESCE(sum(512+6*length(CAST(binding AS BLOB))+length(request_body)+length(outcome_body)),0) FROM _sync_upload_journal`, nil, &journalEntries, &journalBytes); err != nil {
		return workspaceCharge{}, err
	}
	return c.sumWorkspaceCharges(unit, workspaceCharge{1, 512}, workspaceCharge{0, descriptor}, reserved, snapshot, stagedPush, workspaceCharge{journalEntries, journalBytes})
}

func (c *Client) retainedApplyWorkspaceCharge(ctx context.Context, q queryRower, unit string) (workspaceCharge, error) {
	var row, edges workspaceCharge
	var textBytes int64
	err := scanTransactionRow(ctx, q, `SELECT entries,bytes,edge_entries,edge_bytes,length(CAST(unit AS BLOB))+
        length(CAST(guard AS BLOB))+length(CAST(COALESCE(row_cursor,'') AS BLOB))+
        length(CAST(preserve_schema AS BLOB))+length(CAST(preserve_table AS BLOB))+length(CAST(preserve_key AS BLOB))
        FROM _sync_apply_plan WHERE singleton_key=1`, nil, &row.entries, &row.bytes, &edges.entries, &edges.bytes, &textBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return workspaceCharge{}, nil
	}
	if err != nil {
		return workspaceCharge{}, err
	}
	descriptor, err := c.workspaceScale(unit, textBytes, 6)
	if err != nil {
		return workspaceCharge{}, err
	}
	return c.sumWorkspaceCharges(unit, row, edges, workspaceCharge{1, 512}, workspaceCharge{0, descriptor})
}

func (c *Client) retainedPushWorkspaceCharge(ctx context.Context, q queryRower, unit string) (workspaceCharge, error) {
	var row workspaceCharge
	var textBytes int64
	err := scanTransactionRow(ctx, q, `SELECT entries,bytes,length(CAST(unit AS BLOB))+length(CAST(guard AS BLOB))+length(CAST(state AS BLOB)) FROM _sync_push_plan WHERE singleton_key=1`, nil, &row.entries, &row.bytes, &textBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return workspaceCharge{}, nil
	}
	if err != nil {
		return workspaceCharge{}, err
	}
	descriptor, err := c.workspaceScale(unit, textBytes, 6)
	if err != nil {
		return workspaceCharge{}, err
	}
	return c.sumWorkspaceCharges(unit, row, workspaceCharge{1, 2048}, workspaceCharge{0, descriptor})
}

func (c *Client) checkAggregateWorkspace(ctx context.Context, q queryRower, unit string, apply, push *workspaceCharge, extra workspaceCharge) error {
	total, err := c.workspaceUsage(ctx, q, unit, apply, push)
	if err == nil {
		err = c.checkSyncWorkspace(unit, total.entries, total.bytes, extra.entries, extra.bytes)
	}
	if _, capacity := err.(*SyncWorkspaceLimitError); capacity {
		if scope, _ := ctx.Value(pushPlanningWorkspaceKey{}).(*pushPlanningWorkspace); scope != nil && scope.writer == q && scope.guard != nil {
			scope.guard = nil
			total, err = c.workspaceUsage(ctx, q, unit, apply, push)
			if err == nil {
				err = c.checkSyncWorkspace(unit, total.entries, total.bytes, extra.entries, extra.bytes)
			}
		}
	}
	return err
}

// Stage charge is encoded payload bytes plus six-byte worst-case JSON escaping
// for every textual identity field and 64 fixed bytes. SQLite evaluates only one
// row here; overflow of maintained totals is rejected by their INTEGER checks.
func stageAccountingDDL() map[string]string {
	result := map[string]string{}
	for _, stage := range []struct{ name, identity, fields, predicate string }{
		{"snapshot", "snapshot_id,schema_name,table_name,key_json", "snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload", "snapshot_id=NEW.snapshot_id AND row_ordinal=NEW.row_ordinal"},
		{"push", "schema_name,table_name,key_json,op", "bundle_seq,row_ordinal,schema_name,table_name,key_json,op,row_version,payload", "bundle_seq=NEW.bundle_seq AND row_ordinal=NEW.row_ordinal"},
	} {
		var terms []string
		for _, field := range strings.Split(stage.identity, ",") {
			terms = append(terms, "length(CAST(NEW."+field+" AS BLOB))")
		}
		charge := "64+6*(" + strings.Join(terms, "+") + ")+length(CAST(COALESCE(NEW.payload,'') AS BLOB))"
		table := "_sync_" + stage.name + "_stage"
		entries, bytes := stage.name+"_stage_entries", stage.name+"_stage_bytes"
		insert := table + "_charge_insert"
		result[insert] = "CREATE TRIGGER IF NOT EXISTS " + insert + " AFTER INSERT ON " + table + " BEGIN UPDATE " + table + " SET charge=" + charge + " WHERE " + stage.predicate + "; UPDATE _sync_operation_state SET " + entries + "=" + entries + "+1," + bytes + "=" + bytes + "+(" + charge + ") WHERE singleton_key=1; END"
		update := table + "_charge_update"
		result[update] = "CREATE TRIGGER IF NOT EXISTS " + update + " AFTER UPDATE OF " + stage.fields + " ON " + table + " BEGIN UPDATE " + table + " SET charge=" + charge + " WHERE " + stage.predicate + "; UPDATE _sync_operation_state SET " + bytes + "=" + bytes + "-OLD.charge+(" + charge + ") WHERE singleton_key=1; END"
		del := table + "_charge_delete"
		result[del] = "CREATE TRIGGER IF NOT EXISTS " + del + " AFTER DELETE ON " + table + " BEGIN UPDATE _sync_operation_state SET " + entries + "=" + entries + "-1," + bytes + "=" + bytes + "-OLD.charge WHERE singleton_key=1; END"
	}
	return result
}

func (c *Client) stageWorkspaceCharge(unit string, payloadBytes int64, identity ...string) (workspaceCharge, error) {
	charge := workspaceCharge{1, 64}
	var err error
	for _, value := range identity {
		bytes, e := c.workspaceScale(unit, int64(len(value)), 6)
		if e != nil {
			return charge, e
		}
		charge, err = c.addWorkspaceCharge(unit, charge, workspaceCharge{0, bytes})
		if err != nil {
			return charge, err
		}
	}
	return c.addWorkspaceCharge(unit, charge, workspaceCharge{0, payloadBytes})
}

// One numeric suballocation of the invocation's existing operation token.
// Four 64KiB encoded envelopes per slot cover old/native/new/scratch data;
// compiler objects are separately observed engine overhead under Option1.
type statementReservation struct {
	client                  *Client
	tx                      *immediateTx
	scope                   *workspaceBufferScope
	capacity                workspaceCharge
	handles, sqlBytes       int64
	reuse, released, atomic bool
	credits, cleanupWork    int64
}

func (c *Client) payStatementWork(ctx context.Context, scope *workspaceBufferScope, n int64) error {
	if err := c.reserveSyncWork(ctx, scope.unit, n); err != nil {
		return err
	}
	scope.statementWork += n
	return nil
}
func (p *statementReservation) work(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.atomic {
		return p.client.payStatementWork(ctx, p.scope, n)
	}
	if n < 0 || n > p.credits {
		return fmt.Errorf("prepaid statement work exhausted")
	}
	p.credits -= n
	return nil
}

func (c *Client) statementPoolDimensions(ctx context.Context, scope *workspaceBufferScope) (int64, int64, workspaceCharge, workspaceCharge, error) {
	unit := scope.unit
	if c.config.localContract == nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, fmt.Errorf("statement pool requires an admitted contract")
	}
	var columns, definitions, fks int64
	for _, t := range c.config.localContract.Generation.Tables {
		if err := c.payStatementWork(ctx, scope, int64(1+len(t.Columns))); err != nil {
			return 0, 0, workspaceCharge{}, workspaceCharge{}, err
		}
		columns += int64(len(t.Columns))
		definitions += int64(len(t.Schema) + len(t.Table))
		for _, col := range t.Columns {
			definitions += int64(len(col.Name) + len(col.WireType))
		}
		for _, fk := range t.ForeignKeys {
			if err := c.payStatementWork(ctx, scope, int64(1+len(fk.Columns)+len(fk.ReferencedColumns))); err != nil {
				return 0, 0, workspaceCharge{}, workspaceCharge{}, err
			}
			fks++
			definitions += int64(len(fk.ReferencedSchema) + len(fk.ReferencedTable))
			for _, name := range fk.Columns {
				definitions += int64(len(name))
			}
			for _, name := range fk.ReferencedColumns {
				definitions += int64(len(name))
			}
		}
	}
	h := int64(64+12*len(c.config.Tables)) + 4*fks
	q, err := c.workspaceScale(unit, h, 8192)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	d, err := c.workspaceScale(unit, definitions, 512)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	col, err := c.workspaceScale(unit, columns, 8192)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	text, err := c.sumWorkspaceCharges(unit, workspaceCharge{0, q}, workspaceCharge{0, d}, workspaceCharge{0, col})
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	// The temporary wrapper/argument arrays coexist with caller-owned SQL and
	// the separately admitted row/input copies; include their fixed descriptors.
	tmpBytes, err := c.workspaceScale(unit, definitions, 12)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	colBytes, err := c.workspaceScale(unit, columns, 512)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	minimum, err := c.sumWorkspaceCharges(unit, workspaceCharge{4 + 2*columns, 8192}, workspaceCharge{0, tmpBytes}, workspaceCharge{0, colBytes})
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	copies, err := c.workspaceScale(unit, h, 4096+4*(1<<16))
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	sqlCopies, err := c.workspaceScale(unit, text.bytes, 6)
	if err != nil {
		return 0, 0, workspaceCharge{}, workspaceCharge{}, err
	}
	pool, err := c.sumWorkspaceCharges(unit, minimum, workspaceCharge{33 * h, copies}, workspaceCharge{0, sqlCopies})
	return h, text.bytes, pool, minimum, err
}

func statementAccountingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, statementAccountingKey{}, true)
}

func (c *Client) configureStatementCache(ctx context.Context, tx *immediateTx, cache *txStmtCache) (context.Context, sqliteTransaction, error) {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return ctx, tx, nil
	}
	if err := c.payStatementWork(ctx, scope, 64); err != nil {
		return ctx, tx, err
	}
	h, q, charge, minimum, err := c.statementPoolDimensions(ctx, scope)
	if err != nil {
		return ctx, tx, err
	}
	rawCtx := statementAccountingContext(ctx)
	reuse := true
	if err = c.checkAggregateWorkspace(rawCtx, tx, scope.unit, nil, nil, charge); err != nil {
		var capacity *SyncWorkspaceLimitError
		if !errors.As(err, &capacity) {
			return ctx, tx, err
		}
		charge = minimum
		reuse = false
		if err = c.checkAggregateWorkspace(rawCtx, tx, scope.unit, nil, nil, charge); err != nil {
			return ctx, tx, err
		}
	}
	var bytes, records int64
	if err = scanTransactionRow(rawCtx, tx, `SELECT length(CAST(workspace_context AS BLOB)) FROM _sync_operation_state WHERE singleton_key=1`, nil, &bytes); err != nil {
		return ctx, tx, err
	}
	traversal, err := c.workspaceScale(scope.unit, bytes, 2)
	if err != nil {
		return ctx, tx, err
	}
	if err = c.payStatementWork(ctx, scope, traversal+8); err != nil {
		return ctx, tx, err
	}
	if err = scanTransactionRow(rawCtx, tx, `SELECT json_array_length(workspace_context) FROM _sync_operation_state WHERE singleton_key=1`, nil, &records); err != nil {
		return ctx, tx, err
	}
	ledger, err := c.statementLedgerWork(scope.unit, bytes+38, records)
	if err != nil {
		return ctx, tx, err
	}
	closeWork := ledger + h + 16
	if ledger > math.MaxInt64-closeWork {
		return ctx, tx, fmt.Errorf("statement work overflow")
	}
	if err = c.payStatementWork(ctx, scope, ledger+closeWork); err != nil {
		return ctx, tx, err
	}
	pool := &statementReservation{client: c, tx: tx, scope: scope, capacity: charge, handles: h, sqlBytes: q, reuse: reuse, cleanupWork: closeWork}
	if err = pool.adjust(rawCtx, true); err != nil {
		return ctx, tx, err
	}
	cache.pool = pool
	cache.active = true
	ctx = context.WithValue(ctx, statementCacheKey{}, cache)
	return ctx, &statementTransaction{sqliteTransaction: tx, cache: cache}, nil
}
func (c *Client) statementLedgerWork(unit string, bytes, n int64) (int64, error) {
	if bytes < 0 || n < 1 {
		return 0, fmt.Errorf("invalid statement reservation ledger dimensions")
	}
	quadratic, err := c.workspaceScale(unit, n, n)
	if err != nil {
		return 0, err
	}
	sort, err := c.workspaceScale(unit, quadratic, 16)
	if err != nil {
		return 0, err
	}
	text, err := c.workspaceScale(unit, bytes, 8)
	if err != nil {
		return 0, err
	}
	visits, err := c.workspaceScale(unit, n, 64)
	if err != nil {
		return 0, err
	}
	total, err := c.sumWorkspaceCharges(unit, workspaceCharge{0, 128}, workspaceCharge{0, sort}, workspaceCharge{0, text}, workspaceCharge{0, visits})
	return total.bytes, err
}
func decimalWidth(n int64) int64 {
	if n < 0 {
		return 20
	}
	width := int64(1)
	for n >= 10 {
		width++
		n /= 10
	}
	return width
}
func (p *statementReservation) adjust(ctx context.Context, grow bool) error {
	records, err := p.client.loadWorkspaceReservations(ctx, p.tx)
	if err != nil {
		return err
	}
	for i, r := range records {
		if r.Token != p.scope.token {
			continue
		}
		next := workspaceCharge{r.Entries, r.Bytes}
		if grow {
			next, err = p.client.addWorkspaceCharge(r.Unit, next, p.capacity)
			if err != nil {
				return err
			}
			growth := max(int64(0), decimalWidth(next.entries)+decimalWidth(next.bytes)-decimalWidth(r.Entries)-decimalWidth(r.Bytes))
			extra, err := p.client.addWorkspaceCharge(r.Unit, p.capacity, workspaceCharge{0, 6 * growth})
			if err != nil {
				return err
			}
			if err = p.client.checkAggregateWorkspace(ctx, p.tx, r.Unit, nil, nil, extra); err != nil {
				return err
			}
		} else {
			if next.entries < p.capacity.entries || next.bytes < p.capacity.bytes {
				return fmt.Errorf("statement reservation is missing its capacity")
			}
			next.entries -= p.capacity.entries
			next.bytes -= p.capacity.bytes
		}
		records[i].Entries = next.entries
		records[i].Bytes = next.bytes
		if err = storeWorkspaceReservations(ctx, p.tx, records); err != nil {
			return err
		}
		return p.client.checkAggregateWorkspace(ctx, p.tx, r.Unit, nil, nil, workspaceCharge{})
	}
	return fmt.Errorf("live statement reservation token is missing")
}
func (p *statementReservation) release(ctx context.Context) error {
	if p.released {
		return nil
	}
	if err := p.adjust(statementAccountingContext(ctx), false); err != nil {
		return err
	}
	p.released = true
	p.capacity = workspaceCharge{}
	return nil
}
func (p *statementReservation) reserveAtomic(ctx context.Context, original int64) error {
	if original < 0 {
		return fmt.Errorf("invalid atomic work estimate")
	}
	extra := int64(math.MaxInt64)
	if original <= math.MaxInt64/8 {
		extra = original * 8
	}
	credits := saturatingWorkAdd(extra, 4096)
	if err := p.client.reserveReadySyncWork(ctx, p.scope.unit, saturatingWorkAdd(original, credits)); err != nil {
		return err
	}
	p.scope.statementWork = saturatingWorkAdd(p.scope.statementWork, credits)
	p.atomic = true
	p.credits = credits
	return nil
}
