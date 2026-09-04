package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

// historicalBase belongs to one dirty or frozen intent. It is never reconstructed
// from the business row or a newer authoritative observation during requeue.
type historicalBase struct {
	Kind       string
	Payload    sql.NullString
	Provenance string
}

// captureAuthoritativePayload converts validated wire values through the same
// declared codecs and SQLite storage classes as application, without reading a
// business row that application triggers or preserved local intent may change.
func (c *Client) captureAuthoritativePayload(ctx context.Context, tx sqliteTransaction, table string, wire []byte) (string, error) {
	payload, err := jcs.DecodeObject(wire)
	if err != nil {
		return "", err
	}
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return "", err
	}
	if len(payload) != len(info.Columns) {
		return "", fmt.Errorf("authoritative payload for %s must contain every column", table)
	}
	columns := make([]string, 0, len(info.Columns))
	values := make([]any, 0, len(info.Columns))
	for _, col := range info.Columns {
		raw, ok := payload[strings.ToLower(col.Name)]
		if !ok {
			return "", fmt.Errorf("authoritative payload for %s is missing %s", table, col.Name)
		}
		value, err := c.sqliteValueWithDeclaredCodec(table, col, raw)
		if err != nil {
			return "", fmt.Errorf("authoritative payload %s.%s: %w", table, col.Name, err)
		}
		columns = append(columns, "? AS "+quoteIdent(col.Name))
		values = append(values, value)
	}
	var captured string
	query := "SELECT " + buildJsonObjectExprHexAware(info, "observed") + " FROM (SELECT " + strings.Join(columns, ",") + ") AS observed"
	if err := tx.QueryRowContext(ctx, query, values...).Scan(&captured); err != nil {
		return "", err
	}
	return captured, nil
}

// baselineProvenanceSQL is evaluated inside the owner's writer transaction.
// The explicit binding/admission tags distinguish an unbound local origin from
// a real account or generation whose identifier happens to resemble a marker.
func baselineProvenanceSQL(origin, version, sequence string) string {
	return `json_object(
 'database_id',(SELECT database_id FROM _sync_contract_identity WHERE singleton_key=1),
 'service_id',(SELECT service_id FROM _sync_contract_identity WHERE singleton_key=1),
 'account_scope',json_object(
 'binding',CASE WHEN (SELECT kind FROM _sync_operation_state WHERE singleton_key=1)='remote_replace' THEN 'attached' ELSE (SELECT binding_state FROM _sync_attachment_state WHERE singleton_key=1) END,
 'account_id',CASE WHEN (SELECT kind FROM _sync_operation_state WHERE singleton_key=1)='remote_replace' THEN (SELECT target_user_id FROM _sync_operation_state WHERE singleton_key=1) ELSE (SELECT attached_user_id FROM _sync_attachment_state WHERE singleton_key=1) END),
 'generation_id',(SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1),
 'generation_fingerprint',(SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1),
 'admission',CASE WHEN (SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1)='' THEN 'unadmitted' ELSE 'admitted' END,
 'local_version',(SELECT local_version FROM _sync_contract_identity WHERE singleton_key=1),
 'physical_shape',(SELECT physical_shape FROM _sync_contract_identity WHERE singleton_key=1),
 'origin_source_id',(SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1),
 'origin',` + origin + `,'bundle_sequence',` + sequence + `,'authoritative_version',` + version + `)`
}

type baselineObservation struct {
	Origin    string
	BundleSeq *int64
}

func (c *Client) installAuthoritativeBaseline(ctx context.Context, tx sqliteTransaction, schema, table, key, kind string, version int64, wire []byte, observation baselineObservation) error {
	if err := c.validateStoredServerProjection(ctx, tx, schema, table, key); err != nil {
		return err
	}
	var payload sql.NullString
	if kind == "live" {
		captured, err := c.captureAuthoritativePayload(ctx, tx, table, wire)
		if err != nil {
			return err
		}
		payload = sql.NullString{String: captured, Valid: true}
	}
	if version > 0 {
		var oldVersion int64
		var oldKind string
		var oldPayload sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT row_version,server_kind,server_payload FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, schema, table, key).Scan(&oldVersion, &oldKind, &oldPayload)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && oldVersion >= version {
			if oldVersion > version {
				return fmt.Errorf("authoritative observation predates retained baseline")
			}
			same, err := canonicalPayloadEqual(oldPayload, payload)
			if err != nil {
				return err
			}
			if oldKind != kind || !same {
				return fmt.Errorf("inconsistent authoritative payload at equal version")
			}
			return nil
		}
	}
	query := `INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
 VALUES(?,?,?,?,?,?,` + baselineProvenanceSQL("?", "?", "?") + `,1)
 ON CONFLICT(schema_name,table_name,key_json) DO UPDATE SET row_version=excluded.row_version,
 server_kind=excluded.server_kind,server_payload=excluded.server_payload,server_provenance=excluded.server_provenance,
 relationships_known=excluded.relationships_known,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')`
	_, err := tx.ExecContext(ctx, query, schema, table, key, version, kind, nullStringValue(payload), observation.Origin, observation.BundleSeq, version)
	return err
}

func (c *Client) loadHistoricalBase(ctx context.Context, tx sqliteTransaction, schema, table, key string, version int64) (historicalBase, error) {
	var base historicalBase
	var retainedVersion int64
	err := tx.QueryRowContext(ctx, `SELECT row_version,CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END,server_payload,server_provenance FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, schema, table, key).Scan(&retainedVersion, &base.Kind, &base.Payload, &base.Provenance)
	if err != nil {
		return base, fmt.Errorf("historical base for %s.%s: %w", schema, table, err)
	}
	if retainedVersion != version {
		return base, fmt.Errorf("historical base version mismatch")
	}
	if base.Kind == "unavailable" {
		return base, &ResolutionBaseUnavailableError{Schema: schema, Table: table}
	}
	return base, nil
}

func bindLocalBaselineContext(ctx context.Context, e execer) error {
	for _, owner := range []struct{ table, column string }{{"_sync_row_state", "server_provenance"}, {"_sync_dirty_rows", "base_provenance"}} {
		column := owner.column
		query := "UPDATE " + owner.table + " SET " + column + "=json_set(" + column + ",'$.account_scope',json_object('binding','attached','account_id',(SELECT attached_user_id FROM _sync_attachment_state WHERE singleton_key=1))) WHERE json_extract(" + column + ",'$.account_scope.binding')='anonymous' AND (SELECT binding_state FROM _sync_attachment_state WHERE singleton_key=1)='attached'"
		if _, err := e.ExecContext(ctx, query); err != nil {
			return err
		}
		query = "UPDATE " + owner.table + " SET " + column + "=json_set(" + column + ",'$.generation_fingerprint',(SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1),'$.admission','admitted') WHERE json_extract(" + column + ",'$.admission')='unadmitted' AND (SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1)!=''"
		if _, err := e.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// ResolutionBaseUnavailableError preserves pending intent when its full base is unavailable.
type ResolutionBaseUnavailableError struct {
	Schema, Table, Unit string
	Sample              []PushDependencyRecordRef
	HasMore             bool
	Progress            SyncProgress
}

func (e *ResolutionBaseUnavailableError) Error() string {
	return "resolution_base_unavailable: " + e.Schema + "." + e.Table
}

func (c *Client) validateHistoricalBase(ctx context.Context, q queryRower, table string, version int64, base historicalBase) error {
	if base.Kind == "unavailable" || base.Kind == "" {
		return &ResolutionBaseUnavailableError{Table: table}
	}
	switch base.Kind {
	case "present":
		if version <= 0 || !base.Payload.Valid {
			return &ResolutionBaseUnavailableError{Table: table}
		}
		if _, err := c.processPayloadForUpload(table, base.Payload.String); err != nil {
			return &ResolutionBaseUnavailableError{Table: table}
		}
	case "deleted":
		if version <= 0 || base.Payload.Valid {
			return &ResolutionBaseUnavailableError{Table: table}
		}
	case "absent":
		if version != 0 || base.Payload.Valid {
			return &ResolutionBaseUnavailableError{Table: table}
		}
	default:
		return &ResolutionBaseUnavailableError{Table: table}
	}
	provenance, err := jcs.DecodeObject([]byte(base.Provenance))
	if err != nil || !validHistoricalObservation(provenance, base.Kind, version) {
		return &ResolutionBaseUnavailableError{Table: table}
	}
	// Read the existing identity owners on the caller's transaction/connection.
	// Source identity is deliberately excluded: B retains its historical origin.
	var currentJSON string
	if err := q.QueryRowContext(ctx, "SELECT "+baselineProvenanceSQL("'never_published'", "0", "NULL")).Scan(&currentJSON); err != nil {
		return err
	}
	current, err := jcs.DecodeObject([]byte(currentJSON))
	if err != nil {
		return err
	}
	for _, field := range []string{"database_id", "service_id", "account_scope", "generation_id", "generation_fingerprint", "admission", "local_version", "physical_shape"} {
		if !reflect.DeepEqual(provenance[field], current[field]) {
			return &ResolutionBaseUnavailableError{Table: table}
		}
	}

	return nil
}

// Validate historical observation values independently of current source/sequence.
func validHistoricalObservation(p map[string]any, kind string, version int64) bool {
	for _, field := range []string{"database_id", "service_id", "generation_fingerprint", "admission", "physical_shape", "origin_source_id", "origin"} {
		if _, ok := p[field].(string); !ok {
			return false
		}
	}
	for _, field := range []string{"database_id", "service_id", "physical_shape"} {
		if p[field] == "" {
			return false
		}
	}
	for _, field := range []string{"generation_id", "local_version", "authoritative_version"} {
		n, ok := p[field].(json.Number)
		if !ok {
			return false
		}
		value, err := n.Int64()
		if err != nil || value < 0 || (field == "generation_id" && value == 0) {
			return false
		}
	}
	if p["authoritative_version"] != json.Number(fmt.Sprint(version)) {
		return false
	}
	sequence, exists := p["bundle_sequence"]
	if !exists {
		return false
	}
	if sequence != nil {
		n, ok := sequence.(json.Number)
		if !ok {
			return false
		}
		value, err := n.Int64()
		if err != nil || value < 0 {
			return false
		}
	}
	origin := p["origin"].(string)
	switch origin {
	case "never_published":
		return kind == "absent" && sequence == nil
	case "explicit_conflict":
		return sequence == nil && p["origin_source_id"] != ""
	case "snapshot":
		return sequence != nil && p["origin_source_id"] != ""
	case "pull", "settlement":
		return p["origin_source_id"] != "" && kind != "absent"
	default:
		return false
	}
}

// Missing A is valid only for the first observation. Retained A is checked by
// the same full typed owner/projection validator used at freeze.
func (c *Client) validateStoredServerProjection(ctx context.Context, tx sqliteTransaction, schema, table, key string) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?)`, schema, table, key).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return c.validateBaselineProjection(ctx, tx, schema, table, key, "server", sql.NullString{})
}

// Check the bounded projection for one affected owner before freezing its intent.
// This compares retained owner values, never reconstructs them from business rows.
func (c *Client) validateBaselineProjection(ctx context.Context, tx sqliteTransaction, schema, table, key, view string, desired sql.NullString) error {
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return err
	}
	payload := desired
	var revision int64
	if view == "desired" {
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM _sync_conflict_state WHERE singleton_key=1`).Scan(&revision); err != nil {
			return err
		}
	}
	if view == "server" {
		var kind string
		var known int
		err = tx.QueryRowContext(ctx, `SELECT server_kind,server_payload,relationships_known,server_revision FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, schema, table, key).Scan(&kind, &payload, &known, &revision)
		if err != nil {
			return fmt.Errorf("authoritative baseline missing: %w", err)
		}
		if known != 1 || kind == "unknown" {
			return fmt.Errorf("authoritative relationship evidence unavailable")
		}
	}
	expected, err := c.expectedBaselineParents(table, info, payload)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT fk_column,parent_schema,parent_table,parent_key,owner_revision FROM _sync_relationships WHERE view=? AND child_schema=? AND child_table=? AND child_key=? LIMIT ?`, view, schema, table, key, len(info.ForeignKeys)+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var column, parentSchema, parentTable, parentKey string
		var ownerRevision int64
		if err := rows.Scan(&column, &parentSchema, &parentTable, &parentKey, &ownerRevision); err != nil {
			return err
		}
		identity, err := jcs.Marshal([]string{parentSchema, parentTable, parentKey})
		if err != nil {
			return err
		}
		if expected[column] != string(identity) || ownerRevision < 1 || (view == "server" && ownerRevision != revision || view == "desired" && ownerRevision > revision) {
			return fmt.Errorf("relationship projection differs from %s owner", view)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(expected) {
		return fmt.Errorf("relationship projection is incomplete for %s owner", view)
	}
	return nil
}

// Decode the owner's admitted typed FK columns; the business table is never read.
func (c *Client) expectedBaselineParents(table string, info *TableInfo, payload sql.NullString) (map[string]string, error) {
	expected := map[string]string{}
	if payload.Valid {
		if _, err := c.processPayloadForUpload(table, payload.String); err != nil {
			return nil, fmt.Errorf("invalid relationship owner payload: %w", err)
		}
		var fields map[string]struct {
			Storage string `json:"storage"`
			Value   string `json:"value"`
		}
		if err := json.Unmarshal([]byte(payload.String), &fields); err != nil {
			return nil, err
		}
		for _, fk := range info.ForeignKeys {
			value, ok := fields[strings.ToLower(fk.FromCol)]
			if !ok {
				return nil, fmt.Errorf("relationship owner missing FK column")
			}
			if value.Storage == "null" {
				continue
			}
			encoded, err := jcs.Marshal(map[string]string{strings.ToLower(fk.ToCol): value.Value})
			if err != nil {
				return nil, err
			}
			identity, err := jcs.Marshal([]string{c.schemaForTable(fk.RefTable), strings.ToLower(fk.RefTable), string(encoded)})
			if err != nil {
				return nil, err
			}
			expected[strings.ToLower(fk.FromCol)] = string(identity)
		}
	}
	return expected, nil
}
