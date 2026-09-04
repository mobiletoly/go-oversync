// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

const pushPlanDDL = `CREATE TABLE IF NOT EXISTS _sync_push_plan (
 singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), unit TEXT NOT NULL,
 guard TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('clear','discover','scc','pack','ready')),
 state TEXT NOT NULL, entries INTEGER NOT NULL CHECK(entries>=0), bytes INTEGER NOT NULL CHECK(bytes>=0))`
const pushNodesDDL = `CREATE TABLE IF NOT EXISTS _sync_push_nodes (
 unit TEXT NOT NULL, schema_name TEXT NOT NULL, table_name TEXT NOT NULL, key_json TEXT NOT NULL,
 ordinal INTEGER NOT NULL, intent_revision INTEGER NOT NULL, dirty_ordinal INTEGER NOT NULL,
 status TEXT NOT NULL, adjacency_cursor TEXT NOT NULL DEFAULT '', adjacency_phase INTEGER NOT NULL DEFAULT 0,
 dfs_index INTEGER NOT NULL DEFAULT 0, lowlink INTEGER NOT NULL DEFAULT 0, parent INTEGER NOT NULL DEFAULT 0,
 on_stack INTEGER NOT NULL DEFAULT 0, stack_ordinal INTEGER NOT NULL DEFAULT 0, edge_cursor INTEGER NOT NULL DEFAULT 0,
 component INTEGER NOT NULL DEFAULT 0, component_rows INTEGER NOT NULL DEFAULT 0, component_bytes INTEGER NOT NULL DEFAULT 0,
 ready_ordinal INTEGER NOT NULL DEFAULT 0, ready_schema TEXT NOT NULL DEFAULT '', ready_table TEXT NOT NULL DEFAULT '', ready_key TEXT NOT NULL DEFAULT '',
 prerequisites INTEGER NOT NULL DEFAULT 0, blocked TEXT NOT NULL DEFAULT '', selected INTEGER NOT NULL DEFAULT 0,
 diagnostic_id TEXT NOT NULL DEFAULT '', diagnostic_ordinal INTEGER NOT NULL DEFAULT 0,
 charge INTEGER NOT NULL CHECK(charge>=0),
 PRIMARY KEY(unit,schema_name,table_name,key_json), UNIQUE(unit,ordinal))`
const pushEdgesDDL = `CREATE TABLE IF NOT EXISTS _sync_push_edges (
 unit TEXT NOT NULL, dependent INTEGER NOT NULL, prerequisite INTEGER NOT NULL, ordinal INTEGER NOT NULL,
 charge INTEGER NOT NULL CHECK(charge>=0), PRIMARY KEY(unit,dependent,prerequisite), UNIQUE(unit,ordinal))`

var pushIndexDDL = map[string]string{
	"_sync_push_nodes_frontier_idx":             `CREATE INDEX IF NOT EXISTS _sync_push_nodes_frontier_idx ON _sync_push_nodes(unit,status,ordinal)`,
	"_sync_push_nodes_stack_idx":                `CREATE INDEX IF NOT EXISTS _sync_push_nodes_stack_idx ON _sync_push_nodes(unit,on_stack,stack_ordinal)`,
	"_sync_push_nodes_component_idx":            `CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_idx ON _sync_push_nodes(unit,component,schema_name,table_name,key_json)`,
	"_sync_push_nodes_ready_idx":                `CREATE INDEX IF NOT EXISTS _sync_push_nodes_ready_idx ON _sync_push_nodes(unit,status,ready_ordinal,ready_schema,ready_table,ready_key)`,
	"_sync_push_nodes_selected_idx":             `CREATE INDEX IF NOT EXISTS _sync_push_nodes_selected_idx ON _sync_push_nodes(unit,selected)`,
	"_sync_push_nodes_component_diagnostic_idx": `CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_diagnostic_idx ON _sync_push_nodes(unit,component,diagnostic_ordinal)`,
	"_sync_push_nodes_component_ordinal_idx":    `CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_ordinal_idx ON _sync_push_nodes(unit,component,ordinal)`,
	"_sync_push_nodes_diagnostic_idx":           `CREATE INDEX IF NOT EXISTS _sync_push_nodes_diagnostic_idx ON _sync_push_nodes(unit,diagnostic_id,diagnostic_ordinal)`,
	"_sync_push_edges_outgoing_idx":             `CREATE INDEX IF NOT EXISTS _sync_push_edges_outgoing_idx ON _sync_push_edges(unit,dependent,ordinal)`,
	"_sync_push_edges_incoming_idx":             `CREATE INDEX IF NOT EXISTS _sync_push_edges_incoming_idx ON _sync_push_edges(unit,prerequisite,ordinal)`,
}

// Only scalar cursors are serialized here. Payloads remain with their primary owners.
type pushPlan struct {
	Unit, Guard, Phase                                                                        string `json:"-"`
	Entries, Bytes                                                                            int64  `json:"-"`
	SeedOrdinal                                                                               int64
	SeedSchema, SeedTable, SeedKey                                                            string
	NextNode, Visit, NextEdge, Index, Stack, Frame, Pop, Component                            int64
	Subphase                                                                                  string
	Cursor, Current, Count, BodyBytes, LastTable, CandidateRows, CandidateBytes, SelectedNext int64
	BlockedComponent                                                                          int64
	BlockedReason                                                                             string
	CandidateLastTable, MeasureCandidateBytes, MeasureCandidateLastTable                      int64
	SourceBundleID                                                                            int64
	InitializationID                                                                          string
	CursorSecret                                                                              string
	CleanupDiagnosticID                                                                       string
}
type pushNode struct {
	ordinal                                                                                                int64
	schema, table, key, status, cursor, blocked, diagnostic                                                string
	revision, dirtyOrdinal, adjacencyPhase, index, lowlink, parent, stack, edgeCursor, component, selected int64
}

func (c *Client) readPushNode(ctx context.Context, tx sqliteTransaction, p *pushPlan, ordinal int64) (*pushNode, error) {
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT charge FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, ordinal).Scan(&size); err != nil {
		return nil, err
	}
	if size < 0 || size > (1<<63-1)/2 {
		return nil, &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "bytes", Limit: c.config.MaxSyncWorkspaceBytes, Used: p.Bytes, Reserved: 1<<63 - 1}
	}
	if err := c.pushWorkspace(ctx, tx, p, 2, size*2); err != nil {
		return nil, err
	}
	n := &pushNode{}
	err := tx.QueryRowContext(ctx, `SELECT ordinal,schema_name,table_name,key_json,status,adjacency_cursor,blocked,diagnostic_id,intent_revision,dirty_ordinal,adjacency_phase,dfs_index,lowlink,parent,stack_ordinal,edge_cursor,component,selected FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, ordinal).Scan(&n.ordinal, &n.schema, &n.table, &n.key, &n.status, &n.cursor, &n.blocked, &n.diagnostic, &n.revision, &n.dirtyOrdinal, &n.adjacencyPhase, &n.index, &n.lowlink, &n.parent, &n.stack, &n.edgeCursor, &n.component, &n.selected)
	return n, err
}
func (c *Client) loadPushPlan(ctx context.Context, tx sqliteTransaction, forCleanup bool) (*pushPlan, error) {
	p := &pushPlan{}
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT unit,guard,phase,entries,bytes,length(CAST(state AS BLOB)) FROM _sync_push_plan WHERE singleton_key=1`).Scan(&p.Unit, &p.Guard, &p.Phase, &p.Entries, &p.Bytes, &size)
	if err != nil {
		return nil, err
	}
	if forCleanup || p.Phase == "clear" {
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(json_extract(state,'$.CleanupDiagnosticID'),'') FROM _sync_push_plan WHERE singleton_key=1`).Scan(&p.CleanupDiagnosticID)
		return p, err
	}
	if size < 0 || size > math.MaxInt64/12 {
		return nil, &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "bytes", Limit: c.config.MaxSyncWorkspaceBytes, Reserved: math.MaxInt64}
	}
	if err = c.pushWorkspace(ctx, tx, p, 1, size*12); err != nil {
		return nil, err
	}
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM _sync_push_plan WHERE singleton_key=1`).Scan(&state); err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(state), p)
	return p, err
}
func savePushPlan(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	state, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO _sync_push_plan VALUES(1,?,?,?,?,?,?) ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,phase=excluded.phase,state=excluded.state,entries=excluded.entries,bytes=excluded.bytes`, p.Unit, p.Guard, p.Phase, string(state), p.Entries, p.Bytes)
	return err
}
func (c *Client) pushGuard(ctx context.Context, tx sqliteTransaction, p *pushPlan) (string, int64, string, error) {
	var contextBytes int64
	err := tx.QueryRowContext(ctx, `SELECT 8192+6*(length(CAST(i.database_id AS BLOB))+length(CAST(i.service_id AS BLOB))+
		length(CAST(i.physical_shape AS BLOB))+length(CAST(i.fingerprint AS BLOB))+length(CAST(a.current_source_id AS BLOB))+
		length(CAST(a.attached_user_id AS BLOB))+length(CAST(a.pending_initialization_id AS BLOB))+length(CAST(o.source_id AS BLOB))+
		length(CAST(o.canonical_request_hash AS BLOB))) FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_outbox_bundle o`).Scan(&contextBytes)
	if err != nil {
		return "", 0, "", err
	}
	var contextEntries int64 = 32
	for _, table := range c.config.Tables {
		info, e := c.pushConfiguredTableInfo(table.TableName)
		if e != nil {
			return "", 0, "", e
		}
		contextEntries += int64(len(info.ForeignKeys))*8 + 4
		contextBytes += 512 + 12*int64(len(table.TableName)+len(table.SyncKeyColumnName))
		for _, fk := range info.ForeignKeys {
			contextBytes += 512 + 12*int64(len(fk.RefTable)+len(fk.FromCol)+len(fk.ToCol))
		}
	}
	if err = c.pushWorkspace(ctx, tx, p, contextEntries, contextBytes); err != nil {
		return "", 0, "", err
	}
	var database, service, shape, fingerprint, source, account, binding, init, outboxState, outboxSource, outboxHash string
	var epoch, generation, localVersion, dirty, baseline, checkpoint, next, outboxID int64
	err = tx.QueryRowContext(ctx, `SELECT i.database_id,i.service_id,i.generation_id,i.local_version,i.physical_shape,i.fingerprint,a.attachment_epoch,a.current_source_id,a.binding_state,a.attached_user_id,a.pending_initialization_id,a.last_bundle_seq_seen,f.revision,f.baseline_revision,s.next_source_bundle_id,o.state,o.source_id,o.source_bundle_id,o.canonical_request_hash FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_conflict_state f JOIN _sync_source_state s ON s.source_id=a.current_source_id CROSS JOIN _sync_outbox_bundle o`).Scan(&database, &service, &generation, &localVersion, &shape, &fingerprint, &epoch, &source, &binding, &account, &init, &checkpoint, &dirty, &baseline, &next, &outboxState, &outboxSource, &outboxID, &outboxHash)
	if err != nil {
		return "", 0, "", err
	}
	h := sha256.New()
	enc := json.NewEncoder(h)
	if err = enc.Encode([]any{database, service, generation, localVersion, shape, fingerprint, epoch, source, binding, account, init, checkpoint, dirty, baseline, next, outboxState, outboxSource, outboxID, outboxHash, c.discovery.Limits}); err != nil {
		return "", 0, "", err
	}
	for _, table := range c.config.Tables {
		if err = enc.Encode(table); err != nil {
			return "", 0, "", err
		}
		info, e := c.pushConfiguredTableInfo(table.TableName)
		if e != nil {
			return "", 0, "", e
		}
		if err = enc.Encode(info.ForeignKeys); err != nil {
			return "", 0, "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), next, init, nil
}
func newPushUnit() (string, error) {
	var bytes [32]byte
	_, err := rand.Read(bytes[:])
	return fmt.Sprintf("%x", bytes), err
}
func (c *Client) chargePushWork(ctx context.Context, unit string, units int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	limit := c.config.MaxSyncWorkUnitsPerInvocation
	if units > limit-c.syncWorkUsed {
		return &SyncWorkLimitError{Unit: unit, Limit: limit, Used: c.syncWorkUsed}
	}
	c.syncWorkUsed += units
	return nil
}
func (c *Client) pushWorkspace(ctx context.Context, tx sqliteTransaction, p *pushPlan, entries, bytes int64) error {
	stateBytes, err := c.workspaceScale(p.Unit, int64(len(p.Unit)+len(p.Guard)+len(p.SeedSchema)+len(p.SeedTable)+len(p.SeedKey)+len(p.InitializationID)), 6)
	if err != nil {
		return err
	}
	charge, err := c.addWorkspaceCharge(p.Unit, workspaceCharge{p.Entries, p.Bytes}, workspaceCharge{1, 2048})
	if err != nil {
		return err
	}
	charge, err = c.addWorkspaceCharge(p.Unit, charge, workspaceCharge{0, stateBytes})
	if err != nil {
		return err
	}
	return c.checkAggregateWorkspace(ctx, tx, p.Unit, nil, &charge, workspaceCharge{entries, bytes})
}

func (c *Client) clearPushStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) (bool, error) {
	for _, table := range []string{"_sync_push_edges", "_sync_push_nodes"} {
		var ordinal, bytes int64
		err := tx.QueryRowContext(ctx, `SELECT ordinal,charge FROM `+table+` WHERE unit=? ORDER BY ordinal LIMIT 1`, p.Unit).Scan(&ordinal, &bytes)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE unit=? AND ordinal=?`, p.Unit, ordinal); err != nil {
			return false, err
		}
		p.Entries--
		p.Bytes -= bytes
		return false, savePushPlan(ctx, tx, p)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_plan WHERE singleton_key=1`)
	return true, err
}

func (c *Client) seedPushStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	query := `SELECT d.schema_name,d.table_name,d.key_json,d.dirty_ordinal,d.op,d.base_kind,COALESCE(rs.server_kind,'unknown') FROM _sync_dirty_rows d LEFT JOIN _sync_row_state rs ON rs.schema_name=d.schema_name AND rs.table_name=d.table_name AND rs.key_json=d.key_json WHERE (d.dirty_ordinal,d.schema_name,d.table_name,d.key_json)>(?,?,?,?) ORDER BY d.dirty_ordinal,d.schema_name,d.table_name,d.key_json LIMIT 1`
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT 1024+12*(length(CAST(d.schema_name AS BLOB))+length(CAST(d.table_name AS BLOB))+length(CAST(d.key_json AS BLOB))) FROM _sync_dirty_rows d WHERE (d.dirty_ordinal,d.schema_name,d.table_name,d.key_json)>(?,?,?,?) ORDER BY d.dirty_ordinal,d.schema_name,d.table_name,d.key_json LIMIT 1`, p.SeedOrdinal, p.SeedSchema, p.SeedTable, p.SeedKey).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		p.Subphase = "adjacency"
		p.Visit = 1
		return nil
	}
	if err != nil {
		return err
	}
	if err = c.pushWorkspace(ctx, tx, p, 2, size*2); err != nil {
		return err
	}
	var schema, table, key, op, base, kind string
	var ordinal int64
	if err = tx.QueryRowContext(ctx, query, p.SeedOrdinal, p.SeedSchema, p.SeedTable, p.SeedKey).Scan(&schema, &table, &key, &ordinal, &op, &base, &kind); err != nil {
		return err
	}
	if op == "DELETE" && (kind == "unpublished" || kind == "absent" || kind == "deleted") {
		// The primary owner validates full B and projections before safe pruning.
		row, e := c.loadPushCapture(ctx, tx, p, schema, table, key)
		if e != nil {
			return e
		}
		if e = c.validateHistoricalBase(ctx, tx, table, row.BaseRowVersion, historicalBase{row.BaseKind, row.BasePayload, row.BaseProvenance}); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?`, schema, table, key); e != nil {
			return e
		}
		// Pruning invalidates previous traversal; cleanup is charged on subsequent steps.
		p.Phase = "clear"
		return nil
	}
	blocked := ""
	if kind == "unknown" {
		blocked = "relationship_unknown"
	}
	if base == "unavailable" || base == "" {
		blocked = "base"
	}
	if p.NextNode == math.MaxInt64 {
		return fmt.Errorf("push node ordinal overflow")
	}
	p.NextNode++
	var revision int64
	if err = tx.QueryRowContext(ctx, `SELECT revision FROM _sync_conflict_state WHERE singleton_key=1`).Scan(&revision); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO _sync_push_nodes(unit,schema_name,table_name,key_json,ordinal,intent_revision,dirty_ordinal,status,blocked,charge) VALUES(?,?,?,?,?,?,?,'discover',?,?)`, p.Unit, schema, table, key, p.NextNode, revision, ordinal, blocked, size)
	if err != nil {
		return err
	}
	p.Entries++
	p.Bytes += size
	p.SeedOrdinal, p.SeedSchema, p.SeedTable, p.SeedKey = ordinal, schema, table, key
	return nil
}
func (c *Client) loadPushCapture(ctx context.Context, tx sqliteTransaction, p *pushPlan, schema, table, key string) (*dirtyRowCapture, error) {
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT 2048+64*(length(CAST(COALESCE(payload,'') AS BLOB))+length(CAST(COALESCE(base_payload,'') AS BLOB))+length(CAST(base_provenance AS BLOB))+length(CAST(key_json AS BLOB))) FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?`, schema, table, key).Scan(&size)
	if err != nil {
		return nil, err
	}
	if err = c.pushWorkspace(ctx, tx, p, 4, size); err != nil {
		return nil, err
	}
	row := &dirtyRowCapture{}
	var kind string
	err = tx.QueryRowContext(ctx, `SELECT d.schema_name,d.table_name,d.key_json,d.op,d.base_row_version,d.payload,d.base_kind,d.base_payload,d.base_provenance,d.dirty_ordinal,COALESCE(rs.server_kind,'unknown') FROM _sync_dirty_rows d LEFT JOIN _sync_row_state rs ON rs.schema_name=d.schema_name AND rs.table_name=d.table_name AND rs.key_json=d.key_json WHERE d.schema_name=? AND d.table_name=? AND d.key_json=?`, schema, table, key).Scan(&row.SchemaName, &row.TableName, &row.KeyJSON, &row.Op, &row.BaseRowVersion, &row.LocalPayload, &row.BaseKind, &row.BasePayload, &row.BaseProvenance, &row.DirtyOrdinal, &kind)
	if err != nil {
		return nil, err
	}
	if row.Op != "DELETE" {
		if kind == "live" {
			row.Op = "UPDATE"
		} else {
			row.Op = "INSERT"
		}
	}
	row.LocalPK, row.WireKey, err = c.decodeDirtyKeyForPush(tx, row.TableName, row.KeyJSON)
	return row, err
}
func blockPushNode(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode, reason string) error {
	if n.blocked == "" {
		n.blocked = reason
	}
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET blocked=? WHERE unit=? AND ordinal=?`, n.blocked, p.Unit, n.ordinal)
	return err
}
func (c *Client) putPushEdge(ctx context.Context, tx sqliteTransaction, p *pushPlan, dependent, prerequisite int64) error {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_push_edges WHERE unit=? AND dependent=? AND prerequisite=?`, p.Unit, dependent, prerequisite).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = c.pushWorkspace(ctx, tx, p, 1, 256); err != nil {
		return err
	}
	if p.NextEdge == math.MaxInt64 {
		return fmt.Errorf("push edge ordinal overflow")
	}
	p.NextEdge++
	_, err = tx.ExecContext(ctx, `INSERT INTO _sync_push_edges VALUES(?,?,?,?,256)`, p.Unit, dependent, prerequisite, p.NextEdge)
	if err == nil {
		p.Entries++
		p.Bytes += 256
	}
	return err
}
func (c *Client) unknownIncomingPushCoverage(ctx context.Context, tx sqliteTransaction, n *pushNode) (bool, error) {
	for _, table := range c.config.Tables {
		info, err := c.getTableInfoTx(tx, table.TableName)
		if err != nil {
			return false, err
		}
		for _, fk := range info.ForeignKeys {
			if !strings.EqualFold(fk.RefTable, n.table) {
				continue
			}
			var unknown int
			err = tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_row_state WHERE relationships_known=0 AND table_name=? LIMIT 1`, strings.ToLower(table.TableName)).Scan(&unknown)
			if err == nil {
				return true, nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
		}
	}
	return false, nil
}
func (c *Client) discoverPushStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	if p.Visit > p.NextNode {
		p.Phase = "scc"
		p.Subphase = "walk"
		p.Visit = 0
		return nil
	}
	n, err := c.readPushNode(ctx, tx, p, p.Visit)
	if err != nil {
		return err
	}
	var op string
	if err = tx.QueryRowContext(ctx, `SELECT op FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?`, n.schema, n.table, n.key).Scan(&op); err != nil {
		return err
	}
	if n.adjacencyPhase == 0 {
		capture, e := c.loadPushCapture(ctx, tx, p, n.schema, n.table, n.key)
		if e != nil {
			return e
		}
		if e = c.validateHistoricalBase(ctx, tx, n.table, capture.BaseRowVersion, historicalBase{Kind: capture.BaseKind, Payload: capture.BasePayload, Provenance: capture.BaseProvenance}); e != nil {
			var unavailable *ResolutionBaseUnavailableError
			if !errors.As(e, &unavailable) {
				return e
			}
			if e = blockPushNode(ctx, tx, p, n, "base"); e != nil {
				return e
			}
		}
		if n.blocked != "relationship_unknown" {
			desired := capture.LocalPayload
			if op == "DELETE" {
				desired = sql.NullString{}
			}
			for _, view := range []string{"server", "desired"} {
				if e = c.validateBaselineProjection(ctx, tx, n.schema, n.table, n.key, view, desired); e != nil {
					if e = blockPushNode(ctx, tx, p, n, "historical_repair_required"); e != nil {
						return e
					}
				}
			}
		}
		if op == "DELETE" {
			unknown, e := c.unknownIncomingPushCoverage(ctx, tx, n)
			if e != nil {
				return e
			}
			if unknown {
				if e = blockPushNode(ctx, tx, p, n, "relationship_unknown"); e != nil {
					return e
				}
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET adjacency_phase=1 WHERE unit=? AND ordinal=?`, p.Unit, n.ordinal)
		return err
	}
	if op != "DELETE" {
		return c.discoverDesiredParent(ctx, tx, p, n)
	}
	return c.discoverDeleteChild(ctx, tx, p, n)
}
func finishPushDiscoveryNode(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode) error {
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET status='scc' WHERE unit=? AND ordinal=?`, p.Unit, n.ordinal)
	if err == nil {
		p.Visit++
	}
	return err
}
func (c *Client) discoverDesiredParent(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode) error {
	var column, schema, table, key string
	var size int64
	args := []any{n.schema, n.table, n.key, n.cursor}
	where := ` FROM _sync_relationships WHERE view='desired' AND child_schema=? AND child_table=? AND child_key=? AND fk_column>? ORDER BY fk_column LIMIT 1`
	err := tx.QueryRowContext(ctx, `SELECT 1024+12*(length(CAST(parent_schema AS BLOB))+length(CAST(parent_table AS BLOB))+length(CAST(parent_key AS BLOB))+length(CAST(fk_column AS BLOB)))`+where, args...).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return finishPushDiscoveryNode(ctx, tx, p, n)
	}
	if err != nil {
		return err
	}
	if err = c.pushWorkspace(ctx, tx, p, 3, size); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, `SELECT fk_column,parent_schema,parent_table,parent_key`+where, args...).Scan(&column, &schema, &table, &key); err != nil {
		return err
	}
	var kind, parentOp string
	var parent int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(rs.server_kind,'unknown'),COALESCE(d.op,''),COALESCE(pn.ordinal,0) FROM (SELECT 1) LEFT JOIN _sync_row_state rs ON rs.schema_name=? AND rs.table_name=? AND rs.key_json=? LEFT JOIN _sync_dirty_rows d ON d.schema_name=? AND d.table_name=? AND d.key_json=? LEFT JOIN _sync_push_nodes pn ON pn.unit=? AND pn.schema_name=? AND pn.table_name=? AND pn.key_json=?`, schema, table, key, schema, table, key, p.Unit, schema, table, key).Scan(&kind, &parentOp, &parent)
	if err != nil {
		return err
	}
	switch {
	case kind == "unknown":
		err = blockPushNode(ctx, tx, p, n, "relationship_unknown")
	case parentOp == "DELETE":
		err = blockPushNode(ctx, tx, p, n, "dependency_unavailable")
	case kind == "live":
	case parent != 0 && parentOp != "":
		err = c.putPushEdge(ctx, tx, p, n.ordinal, parent)
	default:
		err = blockPushNode(ctx, tx, p, n, "dependency_unavailable")
	}
	if err != nil {
		return err
	}
	return updatePushAdjacencyCursor(ctx, tx, p, n, column)
}
func updatePushAdjacencyCursor(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode, cursor string) error {
	delta := 6 * int64(len(cursor)-len(n.cursor))
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET adjacency_cursor=?,charge=charge+? WHERE unit=? AND ordinal=?`, cursor, delta, p.Unit, n.ordinal)
	if err == nil {
		p.Bytes += delta
	}
	return err
}
func (c *Client) discoverDeleteChild(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode) error {
	view := "server"
	if n.adjacencyPhase == 2 {
		view = "desired"
	}
	cursor := []string{"", "", "", ""}
	if n.cursor != "" {
		if err := json.Unmarshal([]byte(n.cursor), &cursor); err != nil {
			return err
		}
	}
	if len(cursor) != 4 {
		return fmt.Errorf("invalid push adjacency cursor")
	}
	args := []any{view, n.schema, n.table, n.key, cursor[0], cursor[1], cursor[2], cursor[3]}
	where := ` FROM _sync_relationships WHERE view=? AND parent_schema=? AND parent_table=? AND parent_key=? AND (child_schema,child_table,child_key,fk_column)>(?,?,?,?) ORDER BY child_schema,child_table,child_key,fk_column LIMIT 1`
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT 1024+24*(length(CAST(child_schema AS BLOB))+length(CAST(child_table AS BLOB))+length(CAST(child_key AS BLOB))+length(CAST(fk_column AS BLOB)))`+where, args...).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		if n.adjacencyPhase == 2 {
			return finishPushDiscoveryNode(ctx, tx, p, n)
		}
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET adjacency_phase=2,adjacency_cursor='',charge=charge-? WHERE unit=? AND ordinal=?`, 6*int64(len(n.cursor)), p.Unit, n.ordinal)
		if err == nil {
			p.Bytes -= 6 * int64(len(n.cursor))
		}
		return err
	}
	if err != nil {
		return err
	}
	if err = c.pushWorkspace(ctx, tx, p, 4, size); err != nil {
		return err
	}
	var schema, table, key, column string
	if err = tx.QueryRowContext(ctx, `SELECT child_schema,child_table,child_key,fk_column`+where, args...).Scan(&schema, &table, &key, &column); err != nil {
		return err
	}
	if view == "desired" {
		err = blockPushNode(ctx, tx, p, n, "dependency_unavailable")
	} else {
		var child int64
		var op string
		err = tx.QueryRowContext(ctx, `SELECT pn.ordinal,d.op FROM _sync_push_nodes pn JOIN _sync_dirty_rows d ON d.schema_name=pn.schema_name AND d.table_name=pn.table_name AND d.key_json=pn.key_json WHERE pn.unit=? AND pn.schema_name=? AND pn.table_name=? AND pn.key_json=?`, p.Unit, schema, table, key).Scan(&child, &op)
		if errors.Is(err, sql.ErrNoRows) {
			err = blockPushNode(ctx, tx, p, n, "dependency_unavailable")
		} else if err == nil {
			var stillReferences int
			e := tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_relationships WHERE view='desired' AND child_schema=? AND child_table=? AND child_key=? AND fk_column=? AND parent_schema=? AND parent_table=? AND parent_key=?`, schema, table, key, column, n.schema, n.table, n.key).Scan(&stillReferences)
			if e == nil {
				err = blockPushNode(ctx, tx, p, n, "dependency_unavailable")
			} else if errors.Is(e, sql.ErrNoRows) {
				err = c.putPushEdge(ctx, tx, p, n.ordinal, child)
			} else {
				err = e
			}
		}
	}
	if err != nil {
		return err
	}
	encoded, err := json.Marshal([]string{schema, table, key, column})
	if err != nil {
		return err
	}
	return updatePushAdjacencyCursor(ctx, tx, p, n, string(encoded))
}

func (c *Client) enterPushDFS(ctx context.Context, tx sqliteTransaction, p *pushPlan, ordinal int64) error {
	if p.Index == math.MaxInt64 || p.Stack == math.MaxInt64 {
		return fmt.Errorf("push DFS ordinal overflow")
	}
	p.Index++
	p.Stack++
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET status='walking',dfs_index=?,lowlink=?,parent=?,on_stack=1,stack_ordinal=? WHERE unit=? AND ordinal=?`, p.Index, p.Index, p.Frame, p.Stack, p.Unit, ordinal)
	if err == nil {
		p.Frame = ordinal
	}
	return err
}
func finishPushDFSFrame(ctx context.Context, tx sqliteTransaction, p *pushPlan, n *pushNode) error {
	if n.parent != 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET lowlink=MIN(lowlink,?) WHERE unit=? AND ordinal=?`, n.lowlink, p.Unit, n.parent); err != nil {
			return err
		}
	}
	p.Frame = n.parent
	return nil
}
func (c *Client) sccPushStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	if p.Subphase == "pop" {
		return c.popPushSCC(ctx, tx, p)
	}
	if p.Frame == 0 {
		var ordinal int64
		err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND status='scc' ORDER BY ordinal LIMIT 1`, p.Unit).Scan(&ordinal)
		if errors.Is(err, sql.ErrNoRows) {
			p.Phase = "pack"
			p.Subphase = "counts"
			p.Cursor = 0
			return nil
		}
		if err != nil {
			return err
		}
		return c.enterPushDFS(ctx, tx, p, ordinal)
	}
	n, err := c.readPushNode(ctx, tx, p, p.Frame)
	if err != nil {
		return err
	}
	var edge, next int64
	err = tx.QueryRowContext(ctx, `SELECT ordinal,prerequisite FROM _sync_push_edges WHERE unit=? AND dependent=? AND ordinal>? ORDER BY ordinal LIMIT 1`, p.Unit, n.ordinal, n.edgeCursor).Scan(&edge, &next)
	if errors.Is(err, sql.ErrNoRows) {
		if n.index == n.lowlink {
			p.Subphase = "pop"
			p.Pop = n.ordinal
			return nil
		}
		if _, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET status='visited' WHERE unit=? AND ordinal=?`, p.Unit, n.ordinal); err != nil {
			return err
		}
		return finishPushDFSFrame(ctx, tx, p, n)
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET edge_cursor=? WHERE unit=? AND ordinal=?`, edge, p.Unit, n.ordinal); err != nil {
		return err
	}
	peer, err := c.readPushNode(ctx, tx, p, next)
	if err != nil {
		return err
	}
	if peer.index == 0 {
		return c.enterPushDFS(ctx, tx, p, next)
	}
	var onStack int
	if err = tx.QueryRowContext(ctx, `SELECT on_stack FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, next).Scan(&onStack); err != nil {
		return err
	}
	if onStack == 1 {
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET lowlink=MIN(lowlink,?) WHERE unit=? AND ordinal=?`, peer.index, p.Unit, n.ordinal)
	}
	return err
}
func (c *Client) popPushSCC(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	var ordinal int64
	if err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND on_stack=1 ORDER BY stack_ordinal DESC LIMIT 1`, p.Unit).Scan(&ordinal); err != nil {
		return err
	}
	n, err := c.readPushNode(ctx, tx, p, ordinal)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET on_stack=0,component=?,status='member' WHERE unit=? AND ordinal=?`, p.Pop, p.Unit, ordinal)
	if err != nil {
		return err
	}
	var replace bool
	var delta int64
	err = tx.QueryRowContext(ctx, `SELECT ready_ordinal=0 OR (?,?,?,?)<(ready_ordinal,ready_schema,ready_table,ready_key),
		6*(? - length(CAST(ready_schema AS BLOB))-length(CAST(ready_table AS BLOB))-length(CAST(ready_key AS BLOB)))
		FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, n.dirtyOrdinal, n.schema, n.table, n.key, int64(len(n.schema)+len(n.table)+len(n.key)), p.Unit, p.Pop).Scan(&replace, &delta)
	if err != nil {
		return err
	}
	if replace {
		if delta > 0 {
			if err = c.pushWorkspace(ctx, tx, p, 0, delta); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET ready_ordinal=?,ready_schema=?,ready_table=?,ready_key=?,charge=charge+? WHERE unit=? AND ordinal=?`, n.dirtyOrdinal, n.schema, n.table, n.key, delta, p.Unit, p.Pop)
		if err != nil {
			return err
		}
		p.Bytes += delta
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET component_rows=component_rows+1,blocked=CASE WHEN blocked='' THEN ? ELSE blocked END WHERE unit=? AND ordinal=?`, n.blocked, p.Unit, p.Pop)
	if err != nil {
		return err
	}
	if ordinal == p.Pop {
		p.Subphase = "walk"
		root, e := c.readPushNode(ctx, tx, p, p.Pop)
		if e != nil {
			return e
		}
		return finishPushDFSFrame(ctx, tx, p, root)
	}
	return nil
}
func comparePushRef(as, at, ak, bs, bt, bk string) int {
	if c := strings.Compare(as, bs); c != 0 {
		return c
	}
	if c := strings.Compare(at, bt); c != 0 {
		return c
	}
	return strings.Compare(ak, bk)
}
func countPushDAGStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	var ordinal, dependent, prerequisite int64
	err := tx.QueryRowContext(ctx, `SELECT e.ordinal,d.component,r.component FROM _sync_push_edges e JOIN _sync_push_nodes d ON d.unit=e.unit AND d.ordinal=e.dependent JOIN _sync_push_nodes r ON r.unit=e.unit AND r.ordinal=e.prerequisite WHERE e.unit=? AND e.ordinal>? ORDER BY e.ordinal LIMIT 1`, p.Unit, p.Cursor).Scan(&ordinal, &dependent, &prerequisite)
	if errors.Is(err, sql.ErrNoRows) {
		p.Subphase = "queue_init"
		p.Cursor = 0
		return nil
	}
	if err != nil {
		return err
	}
	if dependent != prerequisite {
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET prerequisites=prerequisites+1 WHERE unit=? AND ordinal=?`, p.Unit, dependent)
	}
	if err == nil {
		p.Cursor = ordinal
	}
	return err
}
func initPushQueueStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	if p.Cursor == p.NextNode {
		p.Subphase = "choose"
		return nil
	}
	p.Cursor++
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET status=CASE WHEN prerequisites=0 THEN 'ready' ELSE 'waiting' END WHERE unit=? AND ordinal=? AND component=ordinal`, p.Unit, p.Cursor)
	return err
}

func (c *Client) emptyPushEnvelopeBytes(p *pushPlan) (int64, error) {
	body, err := json.Marshal(oversync.CompactPushRequest{SourceBundleID: p.SourceBundleID, CanonicalRequestHash: strings.Repeat("0", 64), InitializationID: p.InitializationID, Rows: []oversync.CompactRowGroup{}})
	return int64(len(body)), err
}
func addMeasuredPushTuple(bytes, count, lastTable, table, tupleBytes int64) (int64, error) {
	extra := tupleBytes + 1
	if lastTable != table {
		group, err := json.Marshal([]any{table, count, []any{}})
		if err != nil {
			return 0, err
		}
		extra = tupleBytes + int64(len(group))
		if count != 0 {
			extra++
		}
	}
	if extra > math.MaxInt64-bytes {
		return 0, fmt.Errorf("push envelope byte count overflow")
	}
	return bytes + extra, nil
}
func (c *Client) choosePushComponent(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	var ordinal int64
	err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND status='ready' ORDER BY ready_ordinal,ready_schema,ready_table,ready_key LIMIT 1`, p.Unit).Scan(&ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		p.Phase = "ready"
		return nil
	}
	if err != nil {
		return err
	}
	n, err := c.readPushNode(ctx, tx, p, ordinal)
	if err != nil {
		return err
	}
	p.Current, p.Cursor, p.Count, p.LastTable = ordinal, 0, 0, -1
	if n.blocked != "" {
		return c.blockPushComponent(ctx, tx, p, n.blocked)
	}
	p.BodyBytes, err = c.emptyPushEnvelopeBytes(p)
	if err != nil {
		return err
	}
	if p.CandidateRows == 0 {
		p.CandidateBytes = p.BodyBytes
		p.CandidateLastTable = -1
	}
	p.MeasureCandidateBytes, p.MeasureCandidateLastTable = p.CandidateBytes, p.CandidateLastTable
	p.Subphase = "measure"
	return nil
}
func (c *Client) blockPushComponent(ctx context.Context, tx sqliteTransaction, p *pushPlan, reason string) error {
	if p.BlockedComponent == 0 {
		p.BlockedComponent, p.BlockedReason = p.Current, reason
	} else {
		var earlier bool
		err := tx.QueryRowContext(ctx, `SELECT (a.ready_ordinal,a.ready_schema,a.ready_table,a.ready_key)<(b.ready_ordinal,b.ready_schema,b.ready_table,b.ready_key)
		FROM _sync_push_nodes a JOIN _sync_push_nodes b ON b.unit=a.unit WHERE a.unit=? AND a.ordinal=? AND b.ordinal=?`, p.Unit, p.Current, p.BlockedComponent).Scan(&earlier)
		if err != nil {
			return err
		}
		if earlier {
			p.BlockedComponent, p.BlockedReason = p.Current, reason
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET status='blocked',blocked=?,component_bytes=? WHERE unit=? AND ordinal=?`, reason, p.BodyBytes, p.Unit, p.Current)
	if err != nil {
		return err
	}
	p.Cursor = 0
	p.Count = 0
	p.Subphase = "diagnostic"
	return nil
}
func (c *Client) nextPushComponentMember(ctx context.Context, tx sqliteTransaction, p *pushPlan) (*pushNode, error) {
	var ordinal int64
	err := tx.QueryRowContext(ctx, `SELECT n.ordinal FROM _sync_push_nodes n WHERE n.unit=? AND n.component=? AND (n.schema_name,n.table_name,n.key_json)>(COALESCE((SELECT schema_name FROM _sync_push_nodes WHERE unit=? AND ordinal=?),''),COALESCE((SELECT table_name FROM _sync_push_nodes WHERE unit=? AND ordinal=?),''),COALESCE((SELECT key_json FROM _sync_push_nodes WHERE unit=? AND ordinal=?),'')) ORDER BY n.schema_name,n.table_name,n.key_json LIMIT 1`, p.Unit, p.Current, p.Unit, p.Cursor, p.Unit, p.Cursor, p.Unit, p.Cursor).Scan(&ordinal)
	if err != nil {
		return nil, err
	}
	return c.readPushNode(ctx, tx, p, ordinal)
}
func (c *Client) measurePushComponentStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	n, err := c.nextPushComponentMember(ctx, tx, p)
	if errors.Is(err, sql.ErrNoRows) {
		return c.finishPushComponentMeasurement(ctx, tx, p)
	}
	if err != nil {
		return err
	}
	row, err := c.loadPushCapture(ctx, tx, p, n.schema, n.table, n.key)
	if err != nil {
		return err
	}
	if err = c.validateHistoricalBase(ctx, tx, row.TableName, row.BaseRowVersion, historicalBase{row.BaseKind, row.BasePayload, row.BaseProvenance}); err != nil {
		var missing *ResolutionBaseUnavailableError
		if errors.As(err, &missing) {
			return c.blockPushComponent(ctx, tx, p, "base")
		}
		return err
	}
	for _, view := range []string{"server", "desired"} {
		if err = c.validateBaselineProjection(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, view, row.LocalPayload); err != nil {
			return err
		}
	}
	logical, err := c.buildPushRequestRows([]dirtyRowCapture{*row})
	if err != nil {
		return err
	}
	rowBytes, err := oversync.ExpandedRowDecodedBytes(logical[0])
	if err != nil {
		return err
	}
	if rowBytes > c.discovery.Limits.MaxRowDecodedBytes {
		return c.blockPushComponent(ctx, tx, p, "row")
	}
	groups, err := oversync.CompactGroupsForRows(c.discovery.SchemaGeneration, logical)
	if err != nil {
		return err
	}
	tuple, err := json.Marshal(groups[0].Rows[0])
	if err != nil {
		return err
	}
	table := int64(groups[0].TableIndex)
	p.BodyBytes, err = addMeasuredPushTuple(p.BodyBytes, p.Count, p.LastTable, table, int64(len(tuple)))
	if err != nil {
		return err
	}
	p.MeasureCandidateBytes, err = addMeasuredPushTuple(p.MeasureCandidateBytes, p.CandidateRows+p.Count, p.MeasureCandidateLastTable, table, int64(len(tuple)))
	if err != nil {
		return err
	}
	p.LastTable, p.MeasureCandidateLastTable = table, table
	p.Count++
	p.Cursor = n.ordinal
	_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET diagnostic_ordinal=? WHERE unit=? AND ordinal=?`, p.Count, p.Unit, n.ordinal)
	return err
}
func (c *Client) finishPushComponentMeasurement(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	limits := c.discovery.Limits
	reason := ""
	if p.Count > 1 {
		if p.Count > int64(limits.MaxPushRows) {
			reason = "rows"
		} else if p.BodyBytes > limits.MaxPushDecodedBytes {
			reason = "decoded_bytes"
		}
	} else if p.BodyBytes > limits.MaxPushDecodedBytes {
		reason = "envelope"
	}
	if reason != "" {
		return c.blockPushComponent(ctx, tx, p, reason)
	}
	softRows := int64(limits.MaxPushRows)
	if c.config.UploadLimit > 0 && int64(c.config.UploadLimit) < softRows {
		softRows = int64(c.config.UploadLimit)
	}
	softBytes := limits.MaxPushDecodedBytes
	if c.config.UploadByteLimit > 0 && c.config.UploadByteLimit < softBytes {
		softBytes = c.config.UploadByteLimit
	}
	if p.CandidateRows > 0 && (p.CandidateRows+p.Count > softRows || p.MeasureCandidateBytes > softBytes || p.CandidateRows+p.Count > int64(limits.MaxPushRows) || p.MeasureCandidateBytes > limits.MaxPushDecodedBytes) {
		p.Phase = "ready"
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET component_bytes=?,status='selected' WHERE unit=? AND ordinal=?`, p.BodyBytes, p.Unit, p.Current)
	if err != nil {
		return err
	}
	p.CandidateRows += p.Count
	p.CandidateBytes = p.MeasureCandidateBytes
	p.CandidateLastTable = p.MeasureCandidateLastTable
	p.Cursor = 0
	p.Subphase = "mark"
	return nil
}
func (c *Client) markPushComponentStep(ctx context.Context, tx sqliteTransaction, p *pushPlan, diagnostic bool) error {
	n, err := c.nextPushComponentMember(ctx, tx, p)
	if errors.Is(err, sql.ErrNoRows) {
		p.Cursor = 0
		p.Visit = 0
		p.Subphase = "propagate"
		return nil
	}
	if err != nil {
		return err
	}
	if diagnostic {
		root, e := c.readPushNode(ctx, tx, p, p.Current)
		if e != nil {
			return e
		}
		id := ""
		if root.blocked == "rows" || root.blocked == "decoded_bytes" {
			id = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d", p.Unit, p.Current))))
		}
		p.Count++
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET diagnostic_id=?,diagnostic_ordinal=? WHERE unit=? AND ordinal=?`, id, p.Count, p.Unit, n.ordinal)
	} else {
		p.SelectedNext++
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET selected=? WHERE unit=? AND ordinal=?`, p.SelectedNext, p.Unit, n.ordinal)
	}
	if err == nil {
		p.Cursor = n.ordinal
	}
	return err
}
func (c *Client) propagatePushComponentStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	if p.Visit == 0 {
		var ordinal int64
		err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND component=? AND ordinal>? ORDER BY ordinal LIMIT 1`, p.Unit, p.Current, p.Cursor).Scan(&ordinal)
		if errors.Is(err, sql.ErrNoRows) {
			p.Subphase = "choose"
			return nil
		}
		if err != nil {
			return err
		}
		p.Cursor = ordinal
		p.Visit = ordinal
		p.Count = 0
	}
	var edge, component int64
	err := tx.QueryRowContext(ctx, `SELECT e.ordinal,n.component FROM _sync_push_edges e JOIN _sync_push_nodes n ON n.unit=e.unit AND n.ordinal=e.dependent WHERE e.unit=? AND e.prerequisite=? AND e.ordinal>? ORDER BY e.ordinal LIMIT 1`, p.Unit, p.Visit, p.Count).Scan(&edge, &component)
	if errors.Is(err, sql.ErrNoRows) {
		p.Visit = 0
		return nil
	}
	if err != nil {
		return err
	}
	if component != p.Current {
		root, e := c.readPushNode(ctx, tx, p, p.Current)
		if e != nil {
			return e
		}
		reason := ""
		if root.blocked != "" {
			reason = "dependency_unavailable"
		}
		_, err = tx.ExecContext(ctx, `UPDATE _sync_push_nodes SET prerequisites=prerequisites-1,status=CASE WHEN prerequisites=1 THEN 'ready' ELSE status END,blocked=CASE WHEN blocked='' THEN ? ELSE blocked END WHERE unit=? AND ordinal=?`, reason, p.Unit, component)
	}
	if err == nil {
		p.Count = edge
	}
	return err
}
func (c *Client) packPushStep(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	switch p.Subphase {
	case "counts":
		return countPushDAGStep(ctx, tx, p)
	case "queue_init":
		return initPushQueueStep(ctx, tx, p)
	case "choose":
		return c.choosePushComponent(ctx, tx, p)
	case "measure":
		return c.measurePushComponentStep(ctx, tx, p)
	case "mark":
		return c.markPushComponentStep(ctx, tx, p, false)
	case "diagnostic":
		return c.markPushComponentStep(ctx, tx, p, true)
	case "propagate":
		return c.propagatePushComponentStep(ctx, tx, p)
	default:
		return fmt.Errorf("invalid push packing phase")
	}
}

func (c *Client) advancePushPlanInTx(ctx context.Context, tx sqliteTransaction) (*pushPlan, bool, error) {
	defer c.releasePushTableInfoScope(tx)
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, false, err
	}
	if outbox.State != outboxStateNone {
		return nil, true, nil
	}
	p, err := c.loadPushPlan(ctx, tx, false)
	newPlan := errors.Is(err, sql.ErrNoRows)
	if newPlan {
		unit, e := newPushUnit()
		if e != nil {
			return nil, false, e
		}
		secret, e := newPushUnit()
		if e != nil {
			return nil, false, e
		}
		p = &pushPlan{CursorSecret: secret, Unit: unit, Phase: "discover", Subphase: "seed"}

	} else if err != nil {
		return nil, false, err
	}
	stepWork, e := c.pushPreparationStepWork()
	if e != nil {
		return p, false, e
	}
	if err = c.chargePushWork(ctx, p.Unit, stepWork); err != nil {
		return p, false, err
	}
	if newPlan {
		var present int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_dirty_rows LIMIT 1`).Scan(&present)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, true, nil
		}
		if err != nil {
			return p, false, err
		}
	}
	if p.Phase == "clear" {
		_, e := c.clearPushStep(ctx, tx, p)
		return p, false, e
	}
	if err = c.pushWorkspace(ctx, tx, p, 0, 0); err != nil {
		return p, false, err
	}
	guard, id, init, err := c.pushGuard(ctx, tx, p)
	if err != nil {
		return p, false, err
	}
	if p.Guard == "" {
		p.Guard, p.SourceBundleID, p.InitializationID = guard, id, init
	}
	if p.Guard != guard {
		p.Phase = "clear"
	}
	if p.Phase == "clear" {
		_, e := c.clearPushStep(ctx, tx, p)
		return p, false, e
	}
	switch p.Phase {
	case "discover":
		if p.Subphase == "seed" {
			err = c.seedPushStep(ctx, tx, p)
		} else {
			err = c.discoverPushStep(ctx, tx, p)
		}
	case "scc":
		err = c.sccPushStep(ctx, tx, p)
	case "pack":
		err = c.packPushStep(ctx, tx, p)
	case "ready":
		return p, true, nil
	default:
		return p, false, fmt.Errorf("invalid retained push plan phase")
	}
	if err != nil {
		return p, false, err
	}
	if err = c.pushWorkspace(ctx, tx, p, 0, 0); err != nil {
		return p, false, err
	}
	if err = savePushPlan(ctx, tx, p); err != nil {
		return p, false, err
	}
	return p, p.Phase == "ready", nil
}
func (c *Client) preparePushPlan(ctx context.Context) (*pushPlan, error) {
	for {
		tx, err := beginImmediateTx(ctx, c.db)
		if err != nil {
			return nil, err
		}
		p, ready, err := c.advancePushPlanInTx(ctx, tx)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		rollbackErr := tx.Rollback()
		if err == nil {
			err = rollbackErr
		}
		if err != nil {
			return nil, err
		}
		// Finishing stale cleanup is not finishing the replacement plan.
		if p != nil && p.Phase == "clear" {
			continue
		}
		if ready {
			if p != nil && p.CandidateRows == 0 && p.BlockedComponent != 0 {
				return nil, c.pushPlanBlockedError(ctx, p)
			}
			return p, nil
		}
	}
}

// Admit all simultaneous frozen request representations before loading them.
func (c *Client) reserveSelectedPushCopies(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	rowWork, e := c.pushPreparationStepWork()
	if e != nil {
		return e
	}
	if p.CandidateRows > (math.MaxInt64-32)/rowWork {
		return &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "atomic_apply_work", Limit: c.config.MaxSyncWorkUnitsPerInvocation, Reserved: math.MaxInt64}
	}
	if err := c.reserveSyncWork(ctx, p.Unit, 32+p.CandidateRows*rowWork); err != nil {
		return err
	}
	var copyBytes int64
	for ordinal := int64(1); ordinal <= p.SelectedNext; ordinal++ {
		var size int64
		if err := tx.QueryRowContext(ctx, `SELECT 2048+64*(length(CAST(COALESCE(d.payload,'') AS BLOB))+length(CAST(COALESCE(d.base_payload,'') AS BLOB))+length(CAST(d.base_provenance AS BLOB))+length(CAST(d.key_json AS BLOB))) FROM _sync_push_nodes n JOIN _sync_dirty_rows d ON d.schema_name=n.schema_name AND d.table_name=n.table_name AND d.key_json=n.key_json WHERE n.unit=? AND n.selected=?`, p.Unit, ordinal).Scan(&size); err != nil {
			return err
		}
		if size < 0 || size > math.MaxInt64-copyBytes {
			return &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "bytes", Limit: c.config.MaxSyncWorkspaceBytes, Reserved: math.MaxInt64}
		}
		copyBytes += size
	}

	if p.CandidateRows > math.MaxInt64/32 || p.CandidateBytes > math.MaxInt64/32 || copyBytes > math.MaxInt64-32*p.CandidateBytes {
		return &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "bytes", Limit: c.config.MaxSyncWorkspaceBytes, Reserved: math.MaxInt64}
	}
	if err := c.pushWorkspace(ctx, tx, p, p.CandidateRows*8, copyBytes+8*p.CandidateBytes); err != nil {
		return err
	}
	if err := c.admitPushBufferCopies(ctx, tx, workspaceCharge{p.CandidateRows * 32, copyBytes + 32*p.CandidateBytes}); err != nil {
		return err
	}
	return nil
}

func (c *Client) selectedPushCaptures(ctx context.Context, tx sqliteTransaction, p *pushPlan) ([]dirtyRowCapture, error) {
	if p == nil {
		return nil, nil
	}
	if err := c.reserveSelectedPushCopies(ctx, tx, p); err != nil {
		return nil, err
	}
	captures := make([]dirtyRowCapture, 0, int(p.CandidateRows))
	for ordinal := int64(1); ordinal <= p.SelectedNext; ordinal++ {
		var node int64
		if err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND selected=?`, p.Unit, ordinal).Scan(&node); err != nil {
			return nil, err
		}
		n, err := c.readPushNode(ctx, tx, p, node)
		if err != nil {
			return nil, err
		}
		row, err := c.loadPushCapture(ctx, tx, p, n.schema, n.table, n.key)
		if err != nil {
			return nil, err
		}
		if err = c.validateHistoricalBase(ctx, tx, row.TableName, row.BaseRowVersion, historicalBase{row.BaseKind, row.BasePayload, row.BaseProvenance}); err != nil {
			return nil, err
		}
		for _, view := range []string{"server", "desired"} {
			if err = c.validateBaselineProjection(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, view, row.LocalPayload); err != nil {
				return nil, err
			}
		}
		captures = append(captures, *row)
	}
	body, err := c.compactPushRequestBody(p.SourceBundleID, p.InitializationID, captures)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != p.CandidateBytes || len(captures) != int(p.CandidateRows) {
		return nil, fmt.Errorf("guarded push measurement disagrees with final encoding")
	}
	if len(captures) > c.discovery.Limits.MaxPushRows || int64(len(body)) > c.discovery.Limits.MaxPushDecodedBytes {
		return nil, &InvalidAdmittedContractError{Message: "guarded push exceeds hard request limits"}
	}
	return captures, nil
}

// These refs contain canonical typed keys only; captured payloads are never diagnostic data.
type PushDependencyRecordRef struct {
	Schema string           `json:"schema"`
	Table  string           `json:"table"`
	Key    oversync.SyncKey `json:"key"`
}
type SyncProgress struct{ DirtyRows, OutboundRows, CommittedBundles int64 }
type FKReconciliationRequiredError struct {
	Detail, Unit string
	Sample       []PushDependencyRecordRef
	HasMore      bool
	Progress     SyncProgress
}

func (e *FKReconciliationRequiredError) Error() string {
	return "fk_reconciliation_required: " + e.Detail
}

type PushDependencyDiagnosticPage struct {
	DiagnosticID string
	Identities   []PushDependencyRecordRef
	NextCursor   string
}
type StalePushDependencyDiagnosticError struct{ DiagnosticID string }

func (e *StalePushDependencyDiagnosticError) Error() string {
	return "stale_push_dependency_diagnostic: " + e.DiagnosticID
}
func (c *Client) pushPlanBlockedError(ctx context.Context, p *pushPlan) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	defer c.releasePushTableInfoScope(tx)
	guard, _, _, err := c.pushGuard(ctx, tx, p)
	if err != nil {
		return err
	}
	if guard != p.Guard {
		return &SyncWorkLimitError{Unit: p.Unit, Limit: c.config.MaxSyncWorkUnitsPerInvocation, Used: c.syncWorkUsed}
	}
	return c.pushPlanBlockedErrorInTx(ctx, tx, p)
}
func (c *Client) pushPlanBlockedErrorInTx(ctx context.Context, tx sqliteTransaction, p *pushPlan) error {
	var count, bytes int64
	var reason, id string
	err := tx.QueryRowContext(ctx, `SELECT component_rows,component_bytes,blocked,diagnostic_id FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, p.BlockedComponent).Scan(&count, &bytes, &reason, &id)
	if err != nil {
		return err
	}
	sample, more, err := c.pushDiagnosticSample(ctx, tx, p, p.BlockedComponent, count)
	if err != nil {
		return err
	}
	switch reason {
	case "rows", "decoded_bytes":
		limit, actual := int64(c.discovery.Limits.MaxPushRows), count
		if reason == "decoded_bytes" {
			limit, actual = c.discovery.Limits.MaxPushDecodedBytes, bytes
		}
		return &PushDependencyGroupExceedsAdmittedLimitError{Dimension: reason, Limit: limit, Actual: actual, ComponentRows: count, ComponentDecodedBytes: bytes, DiagnosticID: id, Sample: sample, HasMore: more}
	case "base":
		return &ResolutionBaseUnavailableError{Unit: p.Unit, Sample: sample, HasMore: more}
	case "row":
		return &PushRowExceedsAdmittedLimitError{Limit: c.discovery.Limits.MaxRowDecodedBytes}
	case "envelope":
		return &InvalidAdmittedContractError{Message: "one-row compact request exceeds max_push_decoded_bytes"}
	default:
		return &FKReconciliationRequiredError{Detail: reason, Unit: p.Unit, Sample: sample, HasMore: more}
	}
}
func (c *Client) pushDiagnosticSample(ctx context.Context, tx sqliteTransaction, p *pushPlan, component, count int64) ([]PushDependencyRecordRef, bool, error) {
	var refs []PushDependencyRecordRef
	var cursor, size int64
	for len(refs) < 16 {
		var ordinal int64
		err := tx.QueryRowContext(ctx, `SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND component=? AND diagnostic_ordinal>? ORDER BY diagnostic_ordinal LIMIT 1`, p.Unit, component, cursor).Scan(&ordinal)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		n, err := c.readPushNode(ctx, tx, p, ordinal)
		if err != nil {
			return nil, false, err
		}
		if err = c.pushWorkspace(ctx, tx, p, int64(len(refs)+2), size+1024+12*int64(len(n.key)+len(n.schema)+len(n.table))); err != nil {
			return nil, false, err
		}
		_, key, err := c.decodeDirtyKeyForPush(tx, n.table, n.key)
		if err != nil {
			return nil, false, err
		}
		ref := PushDependencyRecordRef{Schema: n.schema, Table: n.table, Key: key}
		encoded, err := json.Marshal(ref)
		if err != nil {
			return nil, false, err
		}
		if size+int64(len(encoded)) > 4096 {
			break
		}
		size += int64(len(encoded))
		refs = append(refs, ref)
		if err = tx.QueryRowContext(ctx, `SELECT diagnostic_ordinal FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, ordinal).Scan(&cursor); err != nil {
			return nil, false, err
		}
	}
	return refs, int64(len(refs)) < count, nil
}
func boundedPushDiagnosticID(id string) string {
	if len(id) == 64 && strings.Trim(id, "0123456789abcdef") == "" {
		return id
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
}
func diagnosticPushCursor(p *pushPlan, id string, ordinal int64) string {
	text := fmt.Sprintf("%d", ordinal)
	mac := hmac.New(sha256.New, []byte(p.CursorSecret))
	_, _ = mac.Write([]byte(id + ":" + text))
	return text + "." + fmt.Sprintf("%x", mac.Sum(nil))
}
func parsePushDiagnosticCursor(p *pushPlan, id, cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	if len(cursor) > 85 {
		return 0, &StalePushDependencyDiagnosticError{DiagnosticID: boundedPushDiagnosticID(id)}
	}
	ordinalText, signature, ok := strings.Cut(cursor, ".")
	ordinal, err := strconv.ParseInt(ordinalText, 10, 64)
	if !ok || err != nil || ordinal <= 0 || ordinalText != strconv.FormatInt(ordinal, 10) {
		return 0, &StalePushDependencyDiagnosticError{DiagnosticID: boundedPushDiagnosticID(id)}
	}
	expected := diagnosticPushCursor(p, id, ordinal)
	_, expectedSignature, _ := strings.Cut(expected, ".")
	if !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		return 0, &StalePushDependencyDiagnosticError{DiagnosticID: boundedPushDiagnosticID(id)}
	}
	return ordinal, nil
}
func (c *Client) ReadPushDependencyDiagnostic(ctx context.Context, id, cursor string, pageSize int64) (pageResult PushDependencyDiagnosticPage, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	defer c.writeMu.Unlock()
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if pageSize == 0 {
		pageSize = 64
	}
	if pageSize < 1 || pageSize > 128 {
		return PushDependencyDiagnosticPage{}, fmt.Errorf("diagnostic page size must be between 1 and 128")
	}
	stale := &StalePushDependencyDiagnosticError{DiagnosticID: boundedPushDiagnosticID(id)}
	if boundedPushDiagnosticID(id) != id {
		return PushDependencyDiagnosticPage{}, stale
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	defer tx.Rollback()
	defer c.releasePushTableInfoScope(tx)
	p, err := c.loadPushPlan(ctx, tx, false)
	if errors.Is(err, sql.ErrNoRows) {
		return PushDependencyDiagnosticPage{}, stale
	}
	if err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	guard, _, _, err := c.pushGuard(ctx, tx, p)
	if err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	if guard != p.Guard || p.Phase == "clear" {
		return PushDependencyDiagnosticPage{}, stale
	}
	var present int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? LIMIT 1`, p.Unit, id).Scan(&present); errors.Is(err, sql.ErrNoRows) {
		return PushDependencyDiagnosticPage{}, stale
	} else if err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	after, err := parsePushDiagnosticCursor(p, id, cursor)
	if err != nil {
		return PushDependencyDiagnosticPage{}, err
	}
	page := PushDependencyDiagnosticPage{DiagnosticID: id, Identities: []PushDependencyRecordRef{}}
	var bytes int64
	for int64(len(page.Identities)) < pageSize {
		if err = c.chargePushWork(ctx, p.Unit, 8); err != nil {
			return PushDependencyDiagnosticPage{}, err
		}
		var ordinal, size int64
		err = tx.QueryRowContext(ctx, `SELECT ordinal,512+12*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB))) FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? AND diagnostic_ordinal>? ORDER BY diagnostic_ordinal LIMIT 1`, p.Unit, id, after).Scan(&ordinal, &size)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return PushDependencyDiagnosticPage{}, err
		}
		if err = c.pushWorkspace(ctx, tx, p, int64(len(page.Identities))+2, bytes+size); err != nil {
			return PushDependencyDiagnosticPage{}, err
		}
		n, e := c.readPushNode(ctx, tx, p, ordinal)
		if e != nil {
			return PushDependencyDiagnosticPage{}, e
		}
		_, key, e := c.decodeDirtyKeyForPush(tx, n.table, n.key)
		if e != nil {
			return PushDependencyDiagnosticPage{}, e
		}
		ref := PushDependencyRecordRef{Schema: n.schema, Table: n.table, Key: key}
		encoded, e := json.Marshal(ref)
		if e != nil {
			return PushDependencyDiagnosticPage{}, e
		}
		refBytes := int64(len(encoded))
		if bytes+refBytes > 64<<10 && len(page.Identities) > 0 {
			page.NextCursor = diagnosticPushCursor(p, id, after)
			break
		}
		if refBytes > c.discovery.Limits.MaxRowDecodedBytes {
			return PushDependencyDiagnosticPage{}, &SyncWorkspaceLimitError{Unit: p.Unit, Dimension: "bytes", Limit: c.discovery.Limits.MaxRowDecodedBytes, Reserved: refBytes}
		}
		page.Identities = append(page.Identities, ref)
		bytes += refBytes
		if err = tx.QueryRowContext(ctx, `SELECT diagnostic_ordinal FROM _sync_push_nodes WHERE unit=? AND ordinal=?`, p.Unit, ordinal).Scan(&after); err != nil {
			return PushDependencyDiagnosticPage{}, err
		}
	}
	if int64(len(page.Identities)) == pageSize {
		var next int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? AND diagnostic_ordinal>? LIMIT 1`, p.Unit, id, after).Scan(&next)
		if err == nil {
			page.NextCursor = diagnosticPushCursor(p, id, after)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return PushDependencyDiagnosticPage{}, err
		}
	}
	return page, nil
}
func (c *Client) ReleasePushDependencyDiagnostic(ctx context.Context, id string) (resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if boundedPushDiagnosticID(id) != id {
		return &StalePushDependencyDiagnosticError{DiagnosticID: boundedPushDiagnosticID(id)}
	}
	return c.clearPushPlanForLifecycle(ctx, false, id)
}

// Lifecycle and successful settlement share the same one-record cleanup owner.
// Each committed deletion releases its own charge once; no primary owner moves.
func (c *Client) clearPushPlanForLifecycle(ctx context.Context, onlyInvalid bool, diagnosticID ...string) error {
	var clearingUnit string
	for {
		tx, err := beginImmediateTx(ctx, c.db)
		if err != nil {
			return err
		}
		p, err := c.loadPushPlan(ctx, tx, true)
		if errors.Is(err, sql.ErrNoRows) {
			tx.Rollback()
			return nil
		}
		if err != nil {
			tx.Rollback()
			return err
		}
		if len(diagnosticID) > 0 {
			id := diagnosticID[0]
			if p.CleanupDiagnosticID != id {
				var found int
				err = tx.QueryRowContext(ctx, `SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? LIMIT 1`, p.Unit, id).Scan(&found)
				if errors.Is(err, sql.ErrNoRows) {
					tx.Rollback()
					return nil
				}
				if err != nil {
					tx.Rollback()
					return err
				}
			}
			p.CleanupDiagnosticID = id
		}
		if onlyInvalid && p.Phase != "clear" {
			tx.Rollback()
			return nil
		}
		if clearingUnit == "" {
			clearingUnit = p.Unit
		}
		if clearingUnit != p.Unit {
			tx.Rollback()
			return nil
		}
		if err = c.chargePushWork(ctx, p.Unit, 8); err != nil {
			tx.Rollback()
			return err
		}
		p.Phase = "clear"
		done, err := c.clearPushStep(ctx, tx, p)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		tx.Rollback()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// The admitted metadata is already owned by this client. Reading it through
// each fresh transaction would retain a distinct cache copy for every page.
func (c *Client) pushConfiguredTableInfo(table string) (*TableInfo, error) {
	provider := c.tableInfoProvider()
	provider.mutex.RLock()
	info, ok := provider.cache[tableInfoCacheKey{scope: reflect.ValueOf(c.db).Pointer(), table: strings.ToLower(table)}]
	provider.mutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("admitted table metadata unavailable for %s", table)
	}
	return info, nil
}
func (c *Client) pushPreparationStepWork() (int64, error) {
	work := int64(32)
	for _, table := range c.config.Tables {
		info, err := c.pushConfiguredTableInfo(table.TableName)
		if err != nil {
			return 0, err
		}
		work += 4 + 16*int64(len(info.Columns)) + 32*int64(len(info.ForeignKeys))
	}
	return work, nil
}
func (c *Client) releasePushTableInfoScope(tx tableInfoQueryer) {
	provider := c.tableInfoProvider()
	scope := reflect.ValueOf(tx).Pointer()
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	for _, table := range c.config.Tables {
		delete(provider.cache, tableInfoCacheKey{scope: scope, table: strings.ToLower(table.TableName)})
	}
}
