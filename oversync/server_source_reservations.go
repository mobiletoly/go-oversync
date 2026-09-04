// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

func cloneDependencyOverrides(input map[string][]string) map[string][]string {
	if input == nil {
		return nil
	}
	result := make(map[string][]string, len(input))
	for key, values := range input {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func validateReservedServerSourceIDs(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if err := sourceid.Validate(id); err != nil {
			return fmt.Errorf("reserved server source_id is invalid: %w", err)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("reserved server source_id %q is duplicated", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (s *SyncService) reservedServerSourceIDs() []string {
	if s == nil || s.config == nil {
		return nil
	}
	return s.config.ReservedServerSourceIDs
}

func (s *SyncService) isReservedServerSourceID(sourceID string) bool {
	for _, reserved := range s.reservedServerSourceIDs() {
		if sourceID == reserved {
			return true
		}
	}
	return false
}

func (s *SyncService) validateClientActor(actor Actor, requireSource bool) error {
	if err := actor.validate(requireSource); err != nil {
		return err
	}
	if actor.SourceID != "" && s.isReservedServerSourceID(actor.SourceID) {
		return fmt.Errorf("actor source_id is invalid: %w", sourceid.ErrInvalid)
	}
	return nil
}

func (s *SyncService) installServerSourceReservations(ctx context.Context, tx pgx.Tx) error {
	for _, sourceID := range s.reservedServerSourceIDs() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.server_source_reservations (source_id, ever_used)
			VALUES ($1, FALSE)
		`, sourceID); err != nil {
			return fmt.Errorf("install server source reservation: %w", err)
		}
	}
	return nil
}

func (s *SyncService) validateServerSourceReservations(ctx context.Context, q syncCatalogQuerier) error {
	expected := append([]string(nil), s.reservedServerSourceIDs()...)
	sort.Strings(expected)
	rows, err := q.Query(ctx, `
		SELECT source_id
		FROM sync.server_source_reservations
		ORDER BY source_id COLLATE "C"
		LIMIT $1
	`, len(expected)+1)
	if err != nil {
		return fmt.Errorf("load server source reservations: %w", err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var sourceID string
		if err := rows.Scan(&sourceID); err != nil {
			return fmt.Errorf("scan server source reservation: %w", err)
		}
		actual = append(actual, sourceID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate server source reservations: %w", err)
	}
	if len(actual) != len(expected) {
		return unsupportedSchemaf("sync.server_source_reservations row count = %d, expected %d", len(actual), len(expected))
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return unsupportedSchemaf("sync.server_source_reservations source_id at position %d differs from configured exact set", index)
		}
	}
	return nil
}

func (s *SyncService) validateIdentityCollations(ctx context.Context, q syncCatalogQuerier) error {
	rows, err := q.Query(ctx, `
		SELECT namespace.nspname, relation.relname, attribute.attname,
		       collation_namespace.nspname, identity_collation.collname, identity_collation.collisdeterministic
		FROM pg_attribute AS attribute
		JOIN pg_class AS relation ON relation.oid = attribute.attrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		JOIN pg_collation AS identity_collation ON identity_collation.oid = attribute.attcollation
		JOIN pg_namespace AS collation_namespace ON collation_namespace.oid = identity_collation.collnamespace
		WHERE namespace.nspname = 'sync'
		  AND (relation.relname, attribute.attname) IN (
		    ('user_state', 'user_id'),
		    ('source_state', 'source_id'),
		    ('source_state', 'replaced_by_source_id'),
		    ('scope_state', 'initializer_source_id'),
		    ('scope_state', 'initialized_by_source_id'),
		    ('bundle_log', 'source_id'),
		    ('bundle_log', 'canonical_request_hash'),
		    ('scope_write_receipts', 'writer_id'),
		    ('scope_write_receipts', 'receipt_state'),
		    ('scope_write_receipts', 'committed_canonical_request_hash'),
		    ('server_source_reservations', 'source_id')
		  )
	`)
	if err != nil {
		return fmt.Errorf("inspect sync identity collations: %w", err)
	}
	for rows.Next() {
		var schemaName, tableName, columnName, collationSchema, collationName string
		var deterministic bool
		if err := rows.Scan(&schemaName, &tableName, &columnName, &collationSchema, &collationName, &deterministic); err != nil {
			rows.Close()
			return fmt.Errorf("scan sync identity collation: %w", err)
		}
		if !deterministic {
			rows.Close()
			return unsupportedSchemaf("identity column %s.%s.%s uses nondeterministic collation %s.%s", schemaName, tableName, columnName, collationSchema, collationName)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate sync identity collations: %w", err)
	}
	rows.Close()

	if s == nil || s.config == nil || len(s.config.RegisteredTables) == 0 {
		return nil
	}
	schemas := make([]string, 0, len(s.config.RegisteredTables))
	tables := make([]string, 0, len(s.config.RegisteredTables))
	for _, table := range s.config.RegisteredTables {
		schemas = append(schemas, table.normalizedSchema())
		tables = append(tables, table.normalizedTable())
	}
	rows, err = q.Query(ctx, `
		WITH RECURSIVE roots AS (
			SELECT relation.oid
			FROM unnest($1::text[], $2::text[]) AS requested(schema_name, table_name)
			JOIN pg_namespace AS namespace ON namespace.nspname = requested.schema_name
			JOIN pg_class AS relation ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
		), relation_tree AS (
			SELECT oid FROM roots
			UNION ALL
			SELECT inheritance.inhrelid
			FROM pg_inherits AS inheritance
			JOIN relation_tree AS parent ON parent.oid = inheritance.inhparent
		)
		SELECT namespace.nspname, relation.relname, collation_namespace.nspname,
		       identity_collation.collname, identity_collation.collisdeterministic
		FROM relation_tree AS tree
		JOIN pg_class AS relation ON relation.oid = tree.oid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		JOIN pg_attribute AS attribute ON attribute.attrelid = relation.oid AND attribute.attname = '_sync_scope_id' AND NOT attribute.attisdropped
		JOIN pg_collation AS identity_collation ON identity_collation.oid = attribute.attcollation
		JOIN pg_namespace AS collation_namespace ON collation_namespace.oid = identity_collation.collnamespace
		ORDER BY namespace.nspname, relation.relname
	`, schemas, tables)
	if err != nil {
		return fmt.Errorf("inspect registered identity collations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schemaName, tableName, collationSchema, collationName string
		var deterministic bool
		if err := rows.Scan(&schemaName, &tableName, &collationSchema, &collationName, &deterministic); err != nil {
			return fmt.Errorf("scan registered identity collation: %w", err)
		}
		if !deterministic {
			return unsupportedSchemaf("registered identity column %s.%s._sync_scope_id uses nondeterministic collation %s.%s", schemaName, tableName, collationSchema, collationName)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate registered identity collations: %w", err)
	}
	return nil
}
