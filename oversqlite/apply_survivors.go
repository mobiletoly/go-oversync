package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

const applyPlanDDL = `CREATE TABLE IF NOT EXISTS _sync_apply_plan (
	singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), unit TEXT NOT NULL, guard TEXT NOT NULL,
	phase TEXT NOT NULL, root_cursor INTEGER NOT NULL, visit_cursor INTEGER NOT NULL,
	table_cursor INTEGER NOT NULL, fk_cursor INTEGER NOT NULL, row_cursor TEXT,
	entries INTEGER NOT NULL, bytes INTEGER NOT NULL,
    edge_entries INTEGER NOT NULL DEFAULT 0, edge_bytes INTEGER NOT NULL DEFAULT 0,
    operation_kind TEXT NOT NULL DEFAULT 'pull' CHECK(operation_kind IN ('pull','settlement','snapshot','conflict')),
    preserve_schema TEXT NOT NULL DEFAULT '', preserve_table TEXT NOT NULL DEFAULT '', preserve_key TEXT NOT NULL DEFAULT '',
    atomic_work INTEGER NOT NULL DEFAULT 0 CHECK(typeof(atomic_work)='integer' AND atomic_work>=0),
    preservation_complete INTEGER NOT NULL DEFAULT 0 CHECK(preservation_complete IN (0,1)))`

const applyRowsDDL = `CREATE TABLE IF NOT EXISTS _sync_apply_rows (
	ordinal INTEGER PRIMARY KEY, table_name TEXT NOT NULL, local_pk TEXT NOT NULL,
	payload TEXT, wire INTEGER NOT NULL, pending INTEGER NOT NULL, bytes INTEGER NOT NULL,
    role TEXT NOT NULL DEFAULT 'survivor' CHECK(role IN ('intended','survivor','exception')),
    owner_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(owner_revision)='integer' AND owner_revision>=0),
	UNIQUE(table_name,local_pk))`

const applyEdgesDDL = `CREATE TABLE IF NOT EXISTS _sync_apply_edges (
    ordinal INTEGER PRIMARY KEY, parent_table TEXT NOT NULL, parent_pk TEXT NOT NULL,
    child_table TEXT NOT NULL, child_pk TEXT NOT NULL, bytes INTEGER NOT NULL,
    UNIQUE(parent_table,parent_pk,child_table,child_pk))`

// The existing apply transaction owns this derived state. The persisted cursor
// is also the traversal frontier; no separate graph, coordinator or write guard.
type applyPlan struct {
	unit, guard, phase     string
	root, visit, table, fk int64
	cursor                 sql.NullString
	entries, bytes         int64
	edgeEntries, edgeBytes int64
	kind                   string
	preserve               [3]string
	atomicWork             int64
	preservationComplete   bool
	workspaceContext       context.Context
	workspaceQuery         queryRower
	diagnosticCharge       workspaceCharge // invocation-only, charged through the existing apply owner
}

type applySurvivor struct {
	table, pk string
	payload   sql.NullString
	wire      bool
	role      string
}

func applyUnit(kind string, seq int64, hash string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", kind, seq, hash))))
}

func (c *Client) applyGuard(ctx context.Context, tx sqliteTransaction, unit string) (string, error) {
	charge, err := c.applyGuardWorkspaceCharge(ctx, tx, unit)
	if err != nil {
		return "", err
	}
	for _, table := range c.config.Tables {
		info, e := c.getTableInfoTx(tx, strings.ToLower(table.TableName))
		if e != nil {
			return "", e
		}
		charge, e = c.addWorkspaceCharge(unit, charge, workspaceCharge{4, 512 + 12*int64(len(table.TableName)+len(table.SyncKeyColumnName))})
		if e != nil {
			return "", e
		}
		for _, fk := range info.ForeignKeys {
			charge, e = c.addWorkspaceCharge(unit, charge, workspaceCharge{8, 512 + 12*int64(len(fk.RefTable)+len(fk.FromCol)+len(fk.ToCol))})
			if e != nil {
				return "", e
			}
		}
	}
	if err = c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, charge); err != nil {
		return "", err
	}
	var values [19]any
	targets := make([]any, len(values))
	for i := range values {
		targets[i] = &values[i]
	}
	err = scanTransactionRow(ctx, tx, `SELECT i.database_id,i.service_id,i.generation_id,i.local_version,i.physical_shape,i.fingerprint,
        a.attachment_epoch,a.current_source_id,a.binding_state,a.attached_user_id,a.pending_initialization_id,a.last_bundle_seq_seen,
        f.revision,f.baseline_revision,s.next_source_bundle_id,o.state,o.source_id,o.source_bundle_id,o.canonical_request_hash
        FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_conflict_state f
        LEFT JOIN _sync_source_state s ON s.source_id=a.current_source_id CROSS JOIN _sync_outbox_bundle o`, nil, targets...)
	if err != nil {
		return "", err
	}
	if values[14] == nil {
		if err := c.requirePendingUpgradeSourceAbsence(ctx, tx, unit, charge); err != nil {
			return "", err
		}
	}
	var remoteLimits any
	if c.discovery != nil {
		remoteLimits = c.discovery.Limits
	}
	data, err := json.Marshal([]any{"a3-context", unit, values, remoteLimits, c.config.MaxSyncWorkspaceEntries, c.config.MaxSyncWorkspaceBytes, c.config.MaxSyncWorkUnitsPerInvocation})
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write(data)
	encoder := json.NewEncoder(h)
	// Admission allows reordering registration; positional cursors do not.
	for _, table := range c.config.Tables {
		if err := encoder.Encode(table); err != nil {
			return "", err
		}
		info, err := c.getTableInfoTx(tx, strings.ToLower(table.TableName))
		if err != nil {
			return "", err
		}
		if err := encoder.Encode(info.ForeignKeys); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Measure every column of the guard vector on the same writer snapshot before
// its first host read. INTEGER affinity alone does not exclude oversized TEXT.
// The 32-byte envelope per input byte covers driver strings plus simultaneous
// six-byte JSON escaping, encoder buffer growth and the returned marshal copy;
// fixed metadata also covers scalar representations, vector/targets and hash.
func (c *Client) applyGuardWorkspaceCharge(ctx context.Context, tx sqliteTransaction, unit string) (workspaceCharge, error) {
	// Replace the existing fixed metadata-size read within the caller's work
	// allowance; do not charge a second admission step for the same guard.
	if err := ctx.Err(); err != nil {
		return workspaceCharge{}, err
	}
	var inputBytes int64
	if err := scanTransactionRow(ctx, tx, `SELECT COALESCE(length(CAST(i.database_id AS BLOB)),0)
        +COALESCE(length(CAST(i.service_id AS BLOB)),0)
        +COALESCE(length(CAST(i.generation_id AS BLOB)),0)
        +COALESCE(length(CAST(i.local_version AS BLOB)),0)
        +COALESCE(length(CAST(i.physical_shape AS BLOB)),0)
        +COALESCE(length(CAST(i.fingerprint AS BLOB)),0)
        +COALESCE(length(CAST(a.attachment_epoch AS BLOB)),0)
        +COALESCE(length(CAST(a.current_source_id AS BLOB)),0)
        +COALESCE(length(CAST(a.binding_state AS BLOB)),0)
        +COALESCE(length(CAST(a.attached_user_id AS BLOB)),0)
        +COALESCE(length(CAST(a.pending_initialization_id AS BLOB)),0)
        +COALESCE(length(CAST(a.last_bundle_seq_seen AS BLOB)),0)
        +COALESCE(length(CAST(f.revision AS BLOB)),0)
        +COALESCE(length(CAST(f.baseline_revision AS BLOB)),0)
        +COALESCE(length(CAST(s.next_source_bundle_id AS BLOB)),0)
        +COALESCE(length(CAST(o.state AS BLOB)),0)
        +COALESCE(length(CAST(o.source_id AS BLOB)),0)
        +COALESCE(length(CAST(o.source_bundle_id AS BLOB)),0)
        +COALESCE(length(CAST(o.canonical_request_hash AS BLOB)),0)
        FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_conflict_state f
        LEFT JOIN _sync_source_state s ON s.source_id=a.current_source_id CROSS JOIN _sync_outbox_bundle o`, nil, &inputBytes); err != nil {
		return workspaceCharge{}, err
	}
	copies, err := c.workspaceScale(unit, inputBytes, 32)
	if err != nil {
		return workspaceCharge{}, err
	}
	return c.addWorkspaceCharge(unit, workspaceCharge{32, 8192}, workspaceCharge{0, copies})
}

// Generation replacement has deliberately removed the old source counters.
// Pin that absence as null in the existing guard vector only while the existing
// integrity-checked journal proves this client's confirmed target snapshot.
func (c *Client) requirePendingUpgradeSourceAbsence(ctx context.Context, tx sqliteTransaction, unit string, contextCharge workspaceCharge) error {
	// Four proof reads, three scalar size reads and aggregate owner admission
	// (including its optional fresh-lifetime read) are all singleton work.
	if err := c.reserveSyncWork(ctx, unit, 44); err != nil {
		return err
	}
	proofCharge, err := c.pendingUpgradeProofWorkspaceCharge(ctx, tx, unit)
	if err != nil {
		return err
	}
	extra, err := c.addWorkspaceCharge(unit, contextCharge, proofCharge)
	if err != nil {
		return err
	}
	if err := c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, extra); err != nil {
		return err
	}
	record, err := loadUpgradeJournal(ctx, tx)
	if err != nil {
		return fmt.Errorf("validate source absence against upgrade journal: %w", err)
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	identity, err := loadRetainedUpgradeContractIdentity(ctx, tx)
	if err != nil {
		return err
	}
	var anySource bool
	if err := scanTransactionRow(ctx, tx, `SELECT EXISTS(SELECT 1 FROM _sync_source_state)`, nil, &anySource); err != nil {
		return err
	}
	if record == nil || identity == nil || anySource || record.Mode != UpgradeModeGeneration ||
		record.Phase != UpgradePhaseRebuildRunning || !record.DrainComplete || !record.MigrationComplete ||
		!record.ReplacementConfirmed || record.SnapshotID == "" || record.NewSourceID == "" || record.NewSourceID == record.OldSourceID ||
		c.sourceID != record.NewSourceID || int64(c.config.GenerationID) != record.TargetGenerationID ||
		c.UserID != record.UserID || c.config.DatabaseID != record.DatabaseID || c.config.ServiceID != record.ServiceID ||
		c.config.LocalVersion != record.TargetVersion || c.schemaGenerationProof.Fingerprint != record.TargetGenerationFingerprint ||
		identity.databaseID != record.DatabaseID || identity.serviceID != record.ServiceID ||
		identity.generation != record.TargetGenerationID || identity.localVersion != record.TargetVersion ||
		identity.fingerprint != record.TargetGenerationFingerprint ||
		attachment.CurrentSourceID != record.OldSourceID || attachment.AttachedUserID != record.UserID ||
		attachment.BindingState != attachmentBindingAttached || !attachment.RebuildRequired || attachment.PendingInitializationID != "" {
		return fmt.Errorf("source counters are missing outside a confirmed generation replacement")
	}
	return nil
}

// Measure the existing proof readers before they allocate host strings. Every
// scanned column is included, even numeric columns with a corrupt storage type.
// The journal envelope covers driver/record copies, six-byte JSON escaping,
// marshal buffers, decoder/token copies and canonicalizer input/output growth.
// Attachment and retained identity copies coexist with the apply context too.
func (c *Client) pendingUpgradeProofWorkspaceCharge(ctx context.Context, tx sqliteTransaction, unit string) (workspaceCharge, error) {
	var journalBytes, attachmentBytes, identityBytes int64
	if err := scanTransactionRow(ctx, tx, `SELECT COALESCE((SELECT COALESCE(length(CAST(format_version AS BLOB)),0)
            +COALESCE(length(CAST(mode AS BLOB)),0)
            +COALESCE(length(CAST(phase AS BLOB)),0)
            +COALESCE(length(CAST(revision AS BLOB)),0)
            +COALESCE(length(CAST(plan_id AS BLOB)),0)
            +COALESCE(length(CAST(plan_digest AS BLOB)),0)
            +COALESCE(length(CAST(database_id AS BLOB)),0)
            +COALESCE(length(CAST(service_id AS BLOB)),0)
            +COALESCE(length(CAST(user_id AS BLOB)),0)
            +COALESCE(length(CAST(old_generation_id AS BLOB)),0)
            +COALESCE(length(CAST(old_generation_fingerprint AS BLOB)),0)
            +COALESCE(length(CAST(old_version AS BLOB)),0)
            +COALESCE(length(CAST(target_generation_id AS BLOB)),0)
            +COALESCE(length(CAST(target_generation_fingerprint AS BLOB)),0)
            +COALESCE(length(CAST(target_version AS BLOB)),0)
            +COALESCE(length(CAST(old_source_id AS BLOB)),0)
            +COALESCE(length(CAST(new_source_id AS BLOB)),0)
            +COALESCE(length(CAST(replacement_reason AS BLOB)),0)
            +COALESCE(length(CAST(drain_complete AS BLOB)),0)
            +COALESCE(length(CAST(migration_complete AS BLOB)),0)
            +COALESCE(length(CAST(validated_target_fingerprint AS BLOB)),0)
            +COALESCE(length(CAST(local_only_digest AS BLOB)),0)
            +COALESCE(length(CAST(replacement_confirmed AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_id AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_bundle_seq AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_row_count AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_byte_count AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_rows_staged AS BLOB)),0)
            +COALESCE(length(CAST(snapshot_bytes_staged AS BLOB)),0)
            +COALESCE(length(CAST(outcome_code AS BLOB)),0)
            +COALESCE(length(CAST(failure_code AS BLOB)),0)
            +COALESCE(length(CAST(failure_detail AS BLOB)),0)
            +COALESCE(length(CAST(pending_request_hash AS BLOB)),0)
            +COALESCE(length(CAST(resolution_revision AS BLOB)),0)
            +COALESCE(length(CAST(resolution_action AS BLOB)),0)
            +COALESCE(length(CAST(resolution_request_hash AS BLOB)),0)
            +COALESCE(length(CAST(resolution_payload AS BLOB)),0)
            +COALESCE(length(CAST(integrity_digest AS BLOB)),0)
          FROM _sync_upgrade_journal WHERE singleton_key=1),0),
        COALESCE((SELECT COALESCE(length(CAST(attachment_epoch AS BLOB)),0)
            +COALESCE(length(CAST(current_source_id AS BLOB)),0)
            +COALESCE(length(CAST(binding_state AS BLOB)),0)
            +COALESCE(length(CAST(attached_user_id AS BLOB)),0)
            +COALESCE(length(CAST(schema_name AS BLOB)),0)
            +COALESCE(length(CAST(last_bundle_seq_seen AS BLOB)),0)
            +COALESCE(length(CAST(rebuild_required AS BLOB)),0)
            +COALESCE(length(CAST(pending_initialization_id AS BLOB)),0)
          FROM _sync_attachment_state WHERE singleton_key=1),0),
        COALESCE((SELECT COALESCE(length(CAST(database_id AS BLOB)),0)
            +COALESCE(length(CAST(service_id AS BLOB)),0)
            +COALESCE(length(CAST(generation_id AS BLOB)),0)
            +COALESCE(length(CAST(local_version AS BLOB)),0)
            +COALESCE(length(CAST(physical_shape AS BLOB)),0)
            +COALESCE(length(CAST(fingerprint AS BLOB)),0)
          FROM _sync_contract_identity WHERE singleton_key=1),0)`, nil, &journalBytes, &attachmentBytes, &identityBytes); err != nil {
		return workspaceCharge{}, err
	}
	journalCopies, err := c.workspaceScale(unit, journalBytes, 128)
	if err != nil {
		return workspaceCharge{}, err
	}
	metadataBytes, err := c.addWorkspaceCharge(unit, workspaceCharge{0, attachmentBytes}, workspaceCharge{0, identityBytes})
	if err != nil {
		return workspaceCharge{}, err
	}
	metadataCopies, err := c.workspaceScale(unit, metadataBytes.bytes, 6)
	if err != nil {
		return workspaceCharge{}, err
	}
	charge, err := c.addWorkspaceCharge(unit, workspaceCharge{256, 32768}, workspaceCharge{0, journalCopies})
	if err != nil {
		return workspaceCharge{}, err
	}
	return c.addWorkspaceCharge(unit, charge, workspaceCharge{0, metadataCopies})
}

func (c *Client) checkApplyWorkspace(p *applyPlan, entries, bytes int64) error {
	charge, err := c.addWorkspaceCharge(p.unit, workspaceCharge{p.entries, p.bytes}, workspaceCharge{p.edgeEntries, p.edgeBytes})
	if err != nil {
		return err
	}
	descriptor, err := c.workspaceScale(p.unit, int64(len(p.unit)+len(p.guard)+len(p.cursor.String)+len(p.preserve[0])+len(p.preserve[1])+len(p.preserve[2])), 6)
	if err != nil {
		return err
	}
	charge, err = c.addWorkspaceCharge(p.unit, charge, workspaceCharge{1, 512})
	if err != nil {
		return err
	}
	charge, err = c.addWorkspaceCharge(p.unit, charge, workspaceCharge{0, descriptor})
	if err != nil {
		return err
	}
	if p.workspaceQuery == nil {
		return fmt.Errorf("workspace query owner is required")
	}
	extra, err := c.addWorkspaceCharge(p.unit, p.diagnosticCharge, workspaceCharge{entries, bytes})
	if err != nil {
		return err
	}
	return c.checkAggregateWorkspace(p.workspaceContext, p.workspaceQuery, p.unit, &charge, nil, extra)
}

const applyPlanStatementWork = 2*6 + 18

func saveApplyPlan(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	return saveApplyPlanWithWork(ctx, tx, p, applyPlanStatementWork)
}

func saveApplyPlanWithWork(ctx context.Context, tx sqliteTransaction, p *applyPlan, work int64) error {
	if p.kind == "" {
		p.kind = "pull"
	}
	// A preparation transition prepays this complete save before staging
	// mutation. Standalone saves admit both writes here before the UPSERT.
	_, err := executeApplyPlanStatement(ctx, tx, work, `INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
		phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,
		table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,
		entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes`, p.unit, p.guard, p.phase, p.root, p.visit, p.table, p.fk, p.cursor, p.entries, p.bytes, p.edgeEntries, p.edgeBytes)
	if err != nil {
		return err
	}
	_, err = executeApplyPlanStatement(ctx, tx, 0, `UPDATE _sync_apply_plan SET operation_kind=?,preserve_schema=?,preserve_table=?,preserve_key=?,atomic_work=?,preservation_complete=? WHERE singleton_key=1`, p.kind, p.preserve[0], p.preserve[1], p.preserve[2], p.atomicWork, p.preservationComplete)
	return err
}

func loadApplyPlan(ctx context.Context, tx queryRower) (*applyPlan, error) {
	p := &applyPlan{workspaceContext: ctx, workspaceQuery: tx}
	err := scanTransactionRow(ctx, tx, `SELECT unit,guard,phase,root_cursor,visit_cursor,table_cursor,
		fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete FROM _sync_apply_plan WHERE singleton_key=1`, nil,
		&p.unit, &p.guard, &p.phase, &p.root, &p.visit, &p.table, &p.fk, &p.cursor, &p.entries, &p.bytes, &p.edgeEntries, &p.edgeBytes, &p.kind, &p.preserve[0], &p.preserve[1], &p.preserve[2], &p.atomicWork, &p.preservationComplete)
	return p, err
}

// prepareApplySurvivors returns only after the intended rows and their current
// incoming closure are complete. On work exhaustion its caller commits staging
// alone; it must not run business writes or metadata/checkpoint transitions.
func (c *Client) prepareApplySurvivors(ctx context.Context, tx sqliteTransaction, unit string, roots int,
	seed func(int) (applySurvivor, error), operationKind ...string) error {
	kind := "pull"
	if len(operationKind) == 1 {
		kind = operationKind[0]
	}
	guard, err := c.applyGuard(ctx, tx, unit)
	if err != nil {
		return err
	}
	p, err := loadApplyPlan(ctx, tx)
	if errors.Is(err, sql.ErrNoRows) {
		p = &applyPlan{unit: unit, guard: guard, phase: "roots", kind: kind, workspaceContext: ctx, workspaceQuery: tx}
	} else if err != nil {
		return err
	}
	if err := c.checkApplyWorkspace(p, 0, 0); err != nil {
		return err
	}
	if p.unit != unit || p.guard != guard || p.kind != kind {
		p.unit, p.guard, p.phase = unit, guard, "clear"
		p.kind = kind
	}
	if kind == "snapshot" && (p.phase == "index" || p.phase == "walk") {
		return fmt.Errorf("obsolete snapshot count phase requires fresh installation")
	}
	if err := saveApplyPlan(ctx, tx, p); err != nil {
		return err
	}
	for p.phase != "ready" {
		if err := c.prepareApplySurvivorStep(ctx, tx, p, roots, seed); err != nil {
			return err
		}
	}
	if kind == "settlement" || kind == "conflict" {
		if err := c.prepareSettlementIntent(ctx, tx, p); err != nil {
			return err
		}
	}
	if err := c.preflightApplySurvivors(ctx, tx, p, roots); err != nil {
		return err
	}
	return c.validateApplyDependencies(ctx, tx, p)
}

// Every mutating transition owns a complete checkpoint before it starts.
// A work refusal can publish only staging whose counters/cursor were saved.
func (c *Client) prepareApplySurvivorStep(ctx context.Context, tx sqliteTransaction, p *applyPlan, roots int, seed func(int) (applySurvivor, error)) error {
	if err := c.reserveSyncWork(ctx, p.unit, 16); err != nil {
		return err
	}
	if err := admitApplyPlanCheckpoint(ctx, tx); err != nil {
		return err
	}
	err := c.advanceApplySurvivors(ctx, tx, p, roots, seed)
	if err == nil {
		err = c.checkApplyWorkspace(p, 0, 0)
	}
	return finishApplyPreparationStep(ctx, tx, p, err)
}

// Successful and work-limited transitions use the preadmitted save. Other
// failures cannot publish progress; the owning transaction must roll back.
func finishApplyPreparationStep(ctx context.Context, tx sqliteTransaction, p *applyPlan, stepErr error) error {
	if stepErr != nil && !pureApplyWork(stepErr) {
		return stepErr
	}
	if err := saveApplyPlanWithWork(ctx, tx, p, 0); err != nil {
		return errors.Join(stepErr, err)
	}
	return stepErr
}

// One durable preparation transition, under the caller's guarded writer.
func (c *Client) advanceApplySurvivors(ctx context.Context, tx sqliteTransaction, p *applyPlan, roots int, seed func(int) (applySurvivor, error)) error {
	switch p.phase {
	case "clear":
		return c.clearApplyStep(ctx, tx, p)
	case "roots":
		if p.root == int64(roots) {
			p.phase, p.fk = "index", -1
			if p.kind == "snapshot" {
				p.phase = "ready"
			}
			return nil
		}
		row, err := seed(int(p.root))
		if err != nil {
			return err
		}
		if err = c.putApplySurvivor(ctx, tx, p, row); err != nil {
			return err
		}
		p.root++
		return nil
	case "index":
		return c.indexApplyStep(ctx, tx, p)
	case "walk":
		return c.walkApplyStep(ctx, tx, p)
	default:
		return fmt.Errorf("invalid survivor plan phase %q", p.phase)
	}
}

// Reserve final apply work and simultaneous copies before business mutation.
func (c *Client) preflightApplySurvivors(ctx context.Context, tx sqliteTransaction, p *applyPlan, roots int) error {
	work, err := c.atomicApplyWork(p, int64(roots))
	if err != nil {
		return err
	}
	work, err = c.lifecycleAtomicWork(ctx, tx, p, work)
	if err != nil {
		return err
	}
	if cache := statementCacheFrom(ctx, tx); cache != nil && cache.pool != nil {
		if err := cache.pool.reserveAtomic(ctx, work); err != nil {
			return err
		}
	} else if err := c.reserveReadySyncWork(ctx, p.unit, work); err != nil {
		return err
	}
	// Preflight simultaneous intended/live copies before business mutation.
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		row, size, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		if _, err := c.captureApplySurvivorWithHeld(ctx, tx, p, row.table, row.pk, 6, 6*size); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) clearApplyStep(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	table := "_sync_apply_edges"
	if p.edgeEntries == 0 {
		table = "_sync_apply_rows"
	}
	var ordinal, size int64
	err := scanTransactionRow(ctx, tx, `SELECT ordinal,bytes FROM `+table+` ORDER BY ordinal LIMIT 1`, nil, &ordinal, &size)
	if errors.Is(err, sql.ErrNoRows) {
		p.phase, p.root, p.visit, p.table, p.fk, p.cursor, p.entries, p.bytes = "roots", 0, 0, 0, 0, sql.NullString{}, 0, 0
		p.edgeEntries, p.edgeBytes = 0, 0
		p.preserve, p.atomicWork, p.preservationComplete = [3]string{}, 0, false
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE ordinal=?`, ordinal); err != nil {
		return err
	}
	if table == "_sync_apply_edges" {
		p.edgeEntries--
		p.edgeBytes -= size
	} else {
		p.entries--
		p.bytes -= size
	}
	return nil
}

// Each business key is visited once during index construction. FK columns for
// that key are handled by bounded steps before advancing to the next key.
func (c *Client) indexApplyStep(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	if p.table >= int64(len(c.config.Tables)) {
		p.phase, p.table, p.fk, p.cursor = "walk", 0, 0, sql.NullString{}
		return nil
	}
	table := strings.ToLower(c.config.Tables[p.table].TableName)
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return err
	}
	if len(info.ForeignKeys) == 0 {
		p.table++
		p.fk = -1
		p.cursor = sql.NullString{}
		return nil
	}
	if p.fk < 0 {
		return c.nextApplyIndexKey(ctx, tx, p, table, info)
	}
	if p.fk >= int64(len(info.ForeignKeys)) {
		p.fk = -1
		return nil
	}
	err = c.indexApplyForeignKey(ctx, tx, p, table, info, info.ForeignKeys[p.fk])
	if err == nil {
		p.fk++
	}
	return err
}

func applyKeyExpression(column string, blob bool) string {
	column = quoteIdent(column)
	if blob {
		return "lower(hex(" + column + "))"
	}
	return "CAST(" + column + " AS TEXT)"
}

func (c *Client) nextApplyIndexKey(ctx context.Context, tx sqliteTransaction, p *applyPlan, table string, info *TableInfo) error {
	key := applyKeyExpression(info.PrimaryKey.Name, info.PrimaryKeyIsBlob)
	tail := ` FROM ` + quoteIdent(table)
	var args []any
	if p.cursor.Valid {
		value, err := c.convertPKForQueryInTx(tx, table, p.cursor.String)
		if err != nil {
			return err
		}
		tail += ` WHERE ` + quoteIdent(info.PrimaryKey.Name) + `>?`
		args = append(args, value)
	}
	tail += ` ORDER BY ` + quoteIdent(info.PrimaryKey.Name) + ` LIMIT 1`
	var size int64
	err := scanTransactionRow(ctx, tx, `SELECT length(CAST(`+key+` AS BLOB))`+tail, args, &size)
	if errors.Is(err, sql.ErrNoRows) {
		p.table++
		p.fk = -1
		p.cursor = sql.NullString{}
		return nil
	}
	if err != nil {
		return err
	}
	if err = c.checkApplyWorkspace(p, 3, 18*size+128); err != nil {
		return err
	}
	if err = scanTransactionRow(ctx, tx, `SELECT `+key+tail, args, &p.cursor); err != nil {
		return err
	}
	p.fk = 0
	return nil
}

func (c *Client) indexApplyForeignKey(ctx context.Context, tx sqliteTransaction, p *applyPlan, table string, info *TableInfo, fk ForeignKeyInfo) error {
	parentTable := strings.ToLower(fk.RefTable)
	parentInfo, err := c.getTableInfoTx(tx, parentTable)
	if err != nil {
		return err
	}
	parentExpr := applyKeyExpression(fk.FromCol, parentInfo.PrimaryKeyIsBlob)
	value, err := c.convertPKForQueryInTx(tx, table, p.cursor.String)
	if err != nil {
		return err
	}
	tail := ` FROM ` + quoteIdent(table) + ` WHERE ` + quoteIdent(info.PrimaryKey.Name) + `=? AND ` + quoteIdent(fk.FromCol) + ` IS NOT NULL`
	var size int64
	err = scanTransactionRow(ctx, tx, `SELECT length(CAST(`+parentExpr+` AS BLOB))`+tail, []any{value}, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	size = 6*(size+int64(len(parentTable)+len(table)+len(p.cursor.String))) + 64
	if err = c.checkApplyWorkspace(p, 7, 6*size); err != nil {
		return err
	}
	var parent string
	if err = scanTransactionRow(ctx, tx, `SELECT `+parentExpr+tail, []any{value}, &parent); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO _sync_apply_edges VALUES(?,?,?,?,?,?)`, p.edgeEntries+1, parentTable, parent, table, p.cursor.String, size)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 0 {
		p.edgeEntries++
		p.edgeBytes += size
	}
	return nil
}

func (c *Client) putApplySurvivor(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor) error {
	var present int
	err := scanTransactionRow(ctx, tx, `SELECT pending FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{row.table, row.pk}, &present)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	size := int64(len(row.payload.String) + 6*len(row.table) + 6*len(row.pk) + 64)
	if err := c.checkApplyWorkspace(p, 7, 6*size); err != nil {
		return err
	}
	role, pending := row.role, 1
	if role == "" {
		role = "survivor"
		if row.wire {
			role = "intended"
		}
	}
	if role == "exception" {
		pending = 0
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO _sync_apply_rows(ordinal,table_name,local_pk,payload,wire,pending,bytes,role,owner_revision) VALUES(?,?,?,?,?,?,?,?,(SELECT revision FROM _sync_conflict_state WHERE singleton_key=1))`, p.entries+1, row.table, row.pk, row.payload, row.wire, pending, size, role)
	if err == nil {
		p.entries++
		p.bytes += size
		c.a3Stats.savedCopies = max(c.a3Stats.savedCopies, p.entries)
		c.a3Stats.savedBytes = max(c.a3Stats.savedBytes, p.bytes)
	}
	return err
}

func (c *Client) walkApplyStep(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	if p.visit == 0 {
		err := scanTransactionRow(ctx, tx, `SELECT ordinal FROM _sync_apply_rows WHERE pending=1 ORDER BY ordinal LIMIT 1`, nil, &p.visit)
		if errors.Is(err, sql.ErrNoRows) {
			p.phase = "ready"
			return nil
		}
		if err != nil {
			return err
		}
	}
	if p.table >= int64(len(c.config.Tables)) {
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_apply_rows SET pending=2 WHERE ordinal=?`, p.visit); err != nil {
			return err
		}
		p.visit = 0
		p.table = 0
		p.fk = 0
		p.cursor = sql.NullString{}
		return nil
	}
	var parentTable, parentPK string
	if err := scanTransactionRow(ctx, tx, `SELECT table_name,local_pk FROM _sync_apply_rows WHERE ordinal=?`, []any{p.visit}, &parentTable, &parentPK); err != nil {
		return err
	}
	table := strings.ToLower(c.config.Tables[p.table].TableName)
	tail := ` FROM _sync_apply_edges WHERE parent_table=? AND parent_pk=? AND child_table=?`
	args := []any{parentTable, parentPK, table}
	if p.cursor.Valid {
		tail += ` AND child_pk>?`
		args = append(args, p.cursor.String)
	}
	tail += ` ORDER BY child_pk LIMIT 1`
	var size int64
	err := scanTransactionRow(ctx, tx, `SELECT length(CAST(child_pk AS BLOB))`+tail, args, &size)
	if errors.Is(err, sql.ErrNoRows) {
		p.table++
		p.cursor = sql.NullString{}
		return nil
	}
	if err != nil {
		return err
	}
	if err = c.checkApplyWorkspace(p, 3, 18*size+128); err != nil {
		return err
	}
	var key string
	if err = scanTransactionRow(ctx, tx, `SELECT child_pk`+tail, args, &key); err != nil {
		return err
	}
	var present int
	err = scanTransactionRow(ctx, tx, `SELECT 1 FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{table, key}, &present)
	if errors.Is(err, sql.ErrNoRows) {
		row, captureErr := c.captureApplySurvivor(ctx, tx, p, table, key)
		if captureErr != nil {
			return captureErr
		}
		if err = c.putApplySurvivor(ctx, tx, p, row); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	p.cursor = sql.NullString{String: key, Valid: true}
	return nil
}

// The received wire bytes already exist. Admit the new staged copy before the
// []byte-to-string conversion; a smaller workspace limit must win first.
func (c *Client) wireApplySurvivor(ctx context.Context, tx sqliteTransaction, table, pk string, payload []byte) (applySurvivor, error) {
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return applySurvivor{}, err
	}
	size := int64(len(payload) + 6*len(table) + 6*len(pk) + 64)
	if err = c.checkApplyWorkspace(p, 7, 6*size); err != nil {
		return applySurvivor{}, err
	}
	return applySurvivor{table: table, pk: pk, payload: sql.NullString{String: string(payload), Valid: payload != nil}, wire: true}, nil
}

func (c *Client) captureApplySurvivor(ctx context.Context, tx sqliteTransaction, p *applyPlan, table, pk string) (applySurvivor, error) {
	return c.captureApplySurvivorWithHeld(ctx, tx, p, table, pk, 0, 0)
}

func (c *Client) captureApplySurvivorWithHeld(ctx context.Context, tx sqliteTransaction, p *applyPlan, table, pk string, heldEntries, heldBytes int64) (applySurvivor, error) {
	row := applySurvivor{table: table, pk: pk}
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return row, err
	}
	value, err := c.convertPKForQueryInTx(tx, table, pk)
	if err != nil {
		return row, err
	}
	var size int64
	err = scanTransactionRow(ctx, tx, `SELECT `+livePayloadByteBound(info, "live_row")+` FROM `+quoteIdent(table)+` live_row WHERE `+quoteIdent(info.PrimaryKey.Name)+`=?`, []any{value}, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return row, nil
	}
	if err != nil {
		return row, err
	}
	if err = c.checkApplyWorkspace(p, heldEntries+6, heldBytes+6*(size+int64(6*len(table)+6*len(pk)+64))); err != nil {
		return row, err
	}
	payload, exists, err := c.serializeExistingRowInTx(ctx, tx, table, pk)
	c.a3Stats.serializedBytes += int64(len(payload))
	c.a3Stats.peakPrimaryCopies = max(c.a3Stats.peakPrimaryCopies, heldEntries+6)
	c.a3Stats.peakPrimaryBytes = max(c.a3Stats.peakPrimaryBytes, heldBytes+6*size)
	row.payload = sql.NullString{String: string(payload), Valid: exists}
	return row, err
}

// Reserve the intended row before loading it; callers retain this reservation
// while capturing/comparing the live copy. Neither copy receives sharing credit.
func (c *Client) loadApplySurvivor(ctx context.Context, tx sqliteTransaction, p *applyPlan, ordinal int64) (applySurvivor, int64, error) {
	var row applySurvivor
	var size int64
	if err := ctx.Err(); err != nil {
		return row, 0, err
	}
	if err := scanTransactionRow(ctx, tx, `SELECT bytes FROM _sync_apply_rows WHERE ordinal=?`, []any{ordinal}, &size); err != nil {
		return row, 0, err
	}
	if err := c.checkApplyWorkspace(p, 6, 6*size); err != nil {
		return row, 0, err
	}
	err := scanTransactionRow(ctx, tx, `SELECT table_name,local_pk,payload,wire FROM _sync_apply_rows WHERE ordinal=?`, []any{ordinal}, &row.table, &row.pk, &row.payload, &row.wire)
	return row, size, err
}

func (c *Client) restoreApplySurvivors(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, verifyOnly bool) (bool, error) {
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return false, err
	}
	changed := false
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		row, size, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return false, err
		}
		matches, err := c.applySurvivorMatches(ctx, tx, p, row, size)
		if err != nil {
			return false, err
		}
		if matches {
			continue
		}
		if verifyOnly {
			return false, fmt.Errorf("survivor final state mismatch in %s", row.table)
		}
		if err = c.restoreApplySurvivor(ctx, tx, cache, row); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

func (c *Client) applySurvivorMatches(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, size int64) (bool, error) {
	live, err := c.captureApplySurvivorWithHeld(ctx, tx, p, row.table, row.pk, 6, 6*size)
	if err != nil {
		return false, err
	}
	if live.payload.Valid != row.payload.Valid {
		return false, nil
	}
	if !row.payload.Valid {
		return true, nil
	}
	if !row.wire {
		return canonicalPayloadEqual(live.payload, row.payload)
	}
	wire, err := c.processPayloadForUpload(row.table, live.payload.String)
	if err != nil {
		return false, err
	}
	want, err := c.normalizeWirePayloadForSQLiteRoundTrip(row.table, row.payload.String)
	if err != nil {
		return false, err
	}
	return canonicalPayloadEqual(sql.NullString{String: string(wire), Valid: true}, sql.NullString{String: string(want), Valid: true})
}

func (c *Client) restoreApplySurvivor(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, row applySurvivor) error {
	if row.payload.Valid {
		payload, err := jcs.DecodeObject([]byte(row.payload.String))
		if err != nil {
			return err
		}
		if row.wire {
			return c.upsertRowInTxUsing(ctx, cache, tx, row.table, payload)
		}
		return c.upsertLocalRowInTxUsing(ctx, cache, tx, row.table, payload)
	}
	value, err := c.convertPKForQueryInTx(tx, row.table, row.pk)
	if err != nil {
		return err
	}
	_, err = cache.ExecContext(ctx, `DELETE FROM `+quoteIdent(row.table)+` WHERE `+quoteIdent(c.pkByTable[row.table])+`=?`, value)
	return err
}

func finishApplySurvivors(ctx context.Context, tx sqliteTransaction) error {
	for _, table := range []string{"_sync_apply_edges", "_sync_apply_rows"} {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE ordinal IN (SELECT ordinal FROM `+table+` ORDER BY ordinal LIMIT 128)`)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM _sync_apply_plan`)
	return err
}

func pureApplyWork(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*SyncWorkLimitError); ok {
		return true
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		children := many.Unwrap()
		return len(children) == 1 && pureApplyWork(children[0])
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		return pureApplyWork(one.Unwrap())
	}
	return false
}
func commitApplyProgress(ctx context.Context, tx *immediateTx, err error) error {
	if !pureApplyWork(err) {
		return err
	}
	if cleanup := setApplyMode(ctx, tx, false); cleanup != nil {
		return errors.Join(err, cleanup)
	}
	if commit := tx.Commit(ctx); commit != nil {
		return errors.Join(err, commit)
	}
	return err
}

// Opening rejects limits that cannot retain a previously committed continuation.
func checkRetainedApplyWorkspace(ctx context.Context, tx queryRower, config *Config) error {
	return (&Client{config: config}).checkAggregateWorkspace(ctx, tx, applyUnit("retained", 0, ""), nil, nil, workspaceCharge{})
}

// Bound JSON encoding from scalar column lengths, without constructing JSON
// before admission. BLOB hex and worst-case string escaping are both covered.
func livePayloadByteBound(info *TableInfo, alias string) string {
	parts := []string{"2"}
	for _, column := range info.Columns {
		value := quoteIdent(alias) + "." + quoteIdent(column.Name)
		overhead := fmt.Sprint(80 + 6*len(column.Name))
		parts = append(parts, overhead+"+CASE typeof("+value+") WHEN 'blob' THEN 2*length("+value+")+2 WHEN 'text' THEN 6*length(CAST("+value+" AS BLOB))+2 ELSE 32 END")
	}
	return strings.Join(parts, "+")
}

// Diagnostics are operation-owned counters, not a second accounting owner or
// public report. Tests exclude setup and read them after the public call ends.
type a3AccountingStats struct {
	primaryReads, primaryBytes, indexedVisits, duplicateChecks, restorationPasses int64
	savedCopies, savedBytes, serializedBytes, peakPrimaryCopies, peakPrimaryBytes int64
}
