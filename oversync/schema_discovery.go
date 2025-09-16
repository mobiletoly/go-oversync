// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FK represents a foreign key constraint
type FK struct {
	Col       string // Column name (e.g., "user_id")
	RefSchema string // Referenced schema (e.g., "public")
	RefTable  string // Referenced table (e.g., "users")
	RefCol    string // Referenced column (e.g., "id")
}

// SchemaDiscovery handles automatic discovery of table relationships and ordering
type SchemaDiscovery struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// ForeignKeyConstraint represents a foreign key relationship
type ForeignKeyConstraint struct {
	ConstraintName string // Name of the FK constraint
	TableSchema    string // Schema of the child table
	TableName      string // Name of the child table
	ColumnName     string // Column in child table
	RefSchema      string // Schema of the referenced table
	RefTable       string // Referenced parent table
	RefColumn      string // Referenced column in parent table
}

// DiscoveredSchema contains the discovered table relationships
type DiscoveredSchema struct {
	TableOrder   []string            // Ordered list of schema.table (parents first)
	OrderIdx     map[string]int      // schema.table -> order index for O(1) lookups
	FKMap        map[string][]FK     // schema.table -> FK constraints
	Dependencies map[string][]string // schema.table -> list of parent schema.table
}

// Key creates a normalized schema.table key (public helper)
func Key(schema, table string) string {
	return strings.ToLower(schema + "." + table)
}

// key creates a normalized schema.table key (internal helper)
func key(schema, table string) string {
	return Key(schema, table)
}

// NewSchemaDiscovery creates a new schema discovery instance
func NewSchemaDiscovery(pool *pgxpool.Pool, logger *slog.Logger) *SchemaDiscovery {
	return &SchemaDiscovery{
		pool:   pool,
		logger: logger,
	}
}

// DiscoverSchema analyzes registered tables and builds dependency graph
func (sd *SchemaDiscovery) DiscoverSchema(ctx context.Context, registeredTables map[string]bool) (*DiscoveredSchema, error) {
	// Step 1: Get all foreign key constraints for registered tables
	fkConstraints, err := sd.getForeignKeyConstraints(ctx, registeredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign key constraints: %w", err)
	}

	// Step 2: Build dependency graph
	dependencies := sd.buildDependencyGraph(fkConstraints, registeredTables)

	// Step 3: Perform topological sort to get table ordering
	tableOrder, err := sd.topologicalSort(dependencies, registeredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to sort tables topologically: %w", err)
	}

	// Step 4: Build FK map for runtime use
	fkMap := sd.buildFKMap(fkConstraints, registeredTables)

	// Step 5: Validate FK constraints are deferrable (important for batch processing)
	sd.validateDeferrableConstraints(ctx, fkConstraints, registeredTables)

	// Step 6: Build order index for O(1) lookups
	orderIdx := make(map[string]int, len(tableOrder))
	for i, table := range tableOrder {
		orderIdx[table] = i
	}

	// Log detailed discovery results
	fkCounts := make(map[string]int)
	for table, fks := range fkMap {
		fkCounts[table] = len(fks)
	}

	sd.logger.Info("Schema discovery completed",
		"table_count", len(tableOrder),
		"fk_constraints_found", len(fkConstraints),
		"fk_map_entries", len(fkMap),
		"fk_counts_per_table", fkCounts,
		"table_order", tableOrder)
	return &DiscoveredSchema{
		TableOrder:   tableOrder,
		OrderIdx:     orderIdx,
		FKMap:        fkMap,
		Dependencies: dependencies,
	}, nil
}

// getForeignKeyConstraints queries the database for FK constraints
func (sd *SchemaDiscovery) getForeignKeyConstraints(ctx context.Context, registeredTables map[string]bool) ([]ForeignKeyConstraint, error) {
	// Build list of registered schema.table combinations for parameterized query
	var registeredTablesList []string
	for schemaTable := range registeredTables {
		registeredTablesList = append(registeredTablesList, schemaTable)
	}
	sort.Strings(registeredTablesList) // Deterministic query plans

	if len(registeredTablesList) == 0 {
		return nil, nil // No registered tables
	}

	query := `
		SELECT
			kcu.constraint_name,
			kcu.table_schema,
			kcu.table_name,
			kcu.column_name,
			rc.unique_constraint_schema AS referenced_table_schema,
			kcu2.table_name AS referenced_table_name,
			kcu2.column_name AS referenced_column_name
		FROM information_schema.key_column_usage AS kcu
		JOIN information_schema.referential_constraints AS rc
			ON kcu.constraint_name = rc.constraint_name
			AND kcu.constraint_schema = rc.constraint_schema
		JOIN information_schema.key_column_usage AS kcu2
			ON rc.unique_constraint_name = kcu2.constraint_name
			AND rc.unique_constraint_schema = kcu2.constraint_schema
			AND kcu.ordinal_position = kcu2.ordinal_position
		WHERE (kcu.table_schema || '.' || kcu.table_name) = ANY(@registered_tables::text[])
		ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position`

	rows, err := sd.pool.Query(ctx, query, pgx.NamedArgs{
		"registered_tables": registeredTablesList,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign key constraints: %w", err)
	}
	defer rows.Close()

	var constraints []ForeignKeyConstraint
	for rows.Next() {
		var fk ForeignKeyConstraint
		err := rows.Scan(
			&fk.ConstraintName,
			&fk.TableSchema,
			&fk.TableName,
			&fk.ColumnName,
			&fk.RefSchema,
			&fk.RefTable,
			&fk.RefColumn,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan foreign key constraint: %w", err)
		}

		constraints = append(constraints, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating foreign key constraints: %w", err)
	}

	return constraints, nil
}

// buildDependencyGraph creates a dependency map from FK constraints
func (sd *SchemaDiscovery) buildDependencyGraph(fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) map[string][]string {
	dependencies := make(map[string][]string)

	// Initialize ALL registered tables in the dependency map (even those without FK relationships)
	for table := range registeredTables {
		dependencies[table] = []string{}
	}

	// Use sets to avoid O(n^2) duplicate checks
	depSets := make(map[string]map[string]struct{})
	for table := range registeredTables {
		depSets[table] = make(map[string]struct{})
	}

	// Add dependencies based on FK constraints
	for _, fk := range fkConstraints {
		childTable := key(fk.TableSchema, fk.TableName)
		parentTable := key(fk.RefSchema, fk.RefTable)

		// Only add dependency if BOTH child and parent tables are registered
		if registeredTables[childTable] && registeredTables[parentTable] {
			// Avoid self-references
			if childTable != parentTable {
				// Use set to avoid duplicates
				if _, exists := depSets[childTable][parentTable]; !exists {
					depSets[childTable][parentTable] = struct{}{}
					dependencies[childTable] = append(dependencies[childTable], parentTable)
				}
			}
		}
	}

	return dependencies
}

// topologicalSort performs topological sorting to determine table order
func (sd *SchemaDiscovery) topologicalSort(dependencies map[string][]string, registeredTables map[string]bool) ([]string, error) {
	// Kahn's algorithm for topological sorting
	inDegree := make(map[string]int)
	for table := range registeredTables {
		inDegree[table] = 0
	}

	// Calculate in-degrees (number of dependencies each table has)
	// Only count dependencies on other registered tables
	for child, deps := range dependencies {
		if !registeredTables[child] {
			continue // Skip unregistered tables
		}
		for _, parent := range deps {
			if registeredTables[parent] {
				inDegree[child]++
			}
		}
	}

	// Find zero-indegree nodes deterministically
	queue := make([]string, 0, len(inDegree))
	for table, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, table)
		}
	}
	sort.Strings(queue) // Ensure deterministic order

	// Helper functions for deterministic queue management
	pop := func() string {
		x := queue[0]
		queue = queue[1:]
		return x
	}
	insertSorted := func(t string) {
		i := sort.SearchStrings(queue, t)
		queue = append(queue, "")
		copy(queue[i+1:], queue[i:])
		queue[i] = t
	}

	var result []string
	for len(queue) > 0 {
		// Remove a table with no incoming edges (deterministic order)
		current := pop()
		result = append(result, current)

		// For each table that depends on current table
		for child, deps := range dependencies {
			if !registeredTables[child] {
				continue
			}
			for _, parent := range deps {
				if parent == current && registeredTables[parent] {
					inDegree[child]--
					if inDegree[child] == 0 {
						insertSorted(child) // Maintain sorted order
					}
				}
			}
		}
	}

	// Check for circular dependencies
	if len(result) != len(registeredTables) {
		sd.logger.Warn("Circular dependency detected, falling back to alphabetical order",
			"processed", len(result), "registered", len(registeredTables))

		// Fallback to stable alphabetical ordering
		var fallbackResult []string
		for table := range registeredTables {
			fallbackResult = append(fallbackResult, table)
		}
		sort.Strings(fallbackResult)
		return fallbackResult, nil
	}

	// Keep schema.table format (do NOT strip schema)
	return result, nil
}

// buildFKMap creates the runtime FK map from constraints with composite FK handling
func (sd *SchemaDiscovery) buildFKMap(fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) map[string][]FK {
	// Group constraints by (child_schema, child_table, constraint_name) to handle composites
	type gkey struct{ childSchema, childTable, constraintName string }
	groups := make(map[gkey][]ForeignKeyConstraint)

	for _, fk := range fkConstraints {
		k := gkey{fk.TableSchema, fk.TableName, fk.ConstraintName}
		groups[k] = append(groups[k], fk)
	}

	fkMap := make(map[string][]FK)
	for k, cols := range groups {
		childTableKey := key(k.childSchema, k.childTable)

		// Handle composite FKs (multiple columns in same constraint)
		if len(cols) > 1 {
			var columnNames []string
			for _, col := range cols {
				columnNames = append(columnNames, col.ColumnName)
			}
			sd.logger.Warn("Composite FK skipped for precheck",
				"table", childTableKey,
				"constraint", k.constraintName,
				"column_count", len(cols),
				"columns", columnNames)
			continue // Skip composite FKs - let PostgreSQL enforce at COMMIT
		}

		// Single-column FK
		c := cols[0]

		// Always include FK in fkMap (for DB existence precheck)
		// The dependency graph only creates edges for registered parents (handled in buildDependencyGraph)
		fk := FK{
			Col:       c.ColumnName,
			RefSchema: c.RefSchema,
			RefTable:  c.RefTable,
			RefCol:    c.RefColumn,
		}
		fkMap[childTableKey] = append(fkMap[childTableKey], fk)
	}

	return fkMap
}

// validateDeferrableConstraints checks if FK constraints are deferrable and warns if not
func (sd *SchemaDiscovery) validateDeferrableConstraints(ctx context.Context, fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) {
	if len(fkConstraints) == 0 {
		return
	}

	// Build constraint names for batch query
	var constraintNames []string
	constraintSet := make(map[string]bool)

	for _, fk := range fkConstraints {
		childTableKey := key(fk.TableSchema, fk.TableName)
		if registeredTables[childTableKey] {
			constraintKey := fk.TableSchema + "." + fk.ConstraintName
			if !constraintSet[constraintKey] {
				constraintSet[constraintKey] = true
				constraintNames = append(constraintNames, constraintKey)
			}
		}
	}
	if len(constraintNames) == 0 {
		return
	}

	// Query for deferrable status using pg_catalog for better reliability
	query := `
		SELECT
			n.nspname AS schema_name,
			con.conname AS constraint_name,
			con.condeferrable AS is_deferrable,
			con.condeferred AS is_deferred
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
		WHERE con.contype = 'f'
		  AND n.nspname || '.' || con.conname = ANY(@constraint_names)`

	rows, err := sd.pool.Query(ctx, query, pgx.NamedArgs{
		"constraint_names": constraintNames,
	})
	if err != nil {
		sd.logger.Warn("Failed to check FK deferrable status", "error", err)
		return
	}
	defer rows.Close()

	nonDeferrableCount := 0
	for rows.Next() {
		var schemaName, constraintName string
		var isDeferrable, isDeferred bool

		if err := rows.Scan(&schemaName, &constraintName, &isDeferrable, &isDeferred); err != nil {
			sd.logger.Warn("Failed to scan FK deferrable status", "error", err)
			continue
		}

		if !isDeferrable {
			nonDeferrableCount++
			sd.logger.Warn("FK constraint is NOT DEFERRABLE - may cause batch processing issues",
				"schema", schemaName,
				"constraint", constraintName,
				"recommendation", "ALTER TABLE ... ALTER CONSTRAINT ... DEFERRABLE INITIALLY DEFERRED")
		} else if !isDeferred {
			sd.logger.Debug("FK constraint is deferrable but not initially deferred",
				"schema", schemaName,
				"constraint", constraintName,
				"note", "Will work correctly due to SET CONSTRAINTS ALL DEFERRED, but INITIALLY DEFERRED is preferred")
		}
	}

	if nonDeferrableCount > 0 {
		sd.logger.Warn("Found non-deferrable FK constraints",
			"count", nonDeferrableCount,
			"impact", "May cause batch processing failures when parent/child are in same request")
	}
}

// Compare returns -1/0/1 comparing two schema.table keys using parent-first order
func (d *DiscoveredSchema) Compare(aSchema, aTable, bSchema, bTable string) int {
	aKey := Key(aSchema, aTable)
	bKey := Key(bSchema, bTable)
	return d.CompareKeys(aKey, bKey)
}

// CompareKeys returns -1/0/1 comparing two schema.table keys using parent-first order
func (d *DiscoveredSchema) CompareKeys(aKey, bKey string) int {
	aOrder, aExists := d.OrderIdx[aKey]
	bOrder, bExists := d.OrderIdx[bKey]

	// If neither table is in order map, use alphabetical
	if !aExists && !bExists {
		if aKey < bKey {
			return -1
		} else if aKey > bKey {
			return 1
		}
		return 0
	}

	// If only one table is in order map, prioritize the one that is
	if !aExists {
		return 1 // b comes first
	}
	if !bExists {
		return -1 // a comes first
	}

	// Both tables are in order map, compare by order index
	if aOrder < bOrder {
		return -1
	} else if aOrder > bOrder {
		return 1
	}
	return 0
}

// SortUpserts sorts changes parent-first (in-place), preserving order within same table
func (d *DiscoveredSchema) SortUpserts(changes []ChangeUpload) {
	sort.SliceStable(changes, func(i, j int) bool {
		// Default schema to "public" if not provided
		schemaI, schemaJ := changes[i].Schema, changes[j].Schema
		if schemaI == "" {
			schemaI = "public"
		}
		if schemaJ == "" {
			schemaJ = "public"
		}

		cmp := d.Compare(schemaI, changes[i].Table, schemaJ, changes[j].Table)
		return cmp < 0
	})
}

// SortDeletes sorts changes child-first (reverse of parent-first), preserving order within same table
func (d *DiscoveredSchema) SortDeletes(changes []ChangeUpload) {
	sort.SliceStable(changes, func(i, j int) bool {
		// Default schema to "public" if not provided
		schemaI, schemaJ := changes[i].Schema, changes[j].Schema
		if schemaI == "" {
			schemaI = "public"
		}
		if schemaJ == "" {
			schemaJ = "public"
		}

		cmp := d.Compare(schemaI, changes[i].Table, schemaJ, changes[j].Table)
		return cmp > 0
	})
}

// HasDependencies returns true if there are any FK dependencies among the given tables that require ordering
func (d *DiscoveredSchema) HasDependencies(changes []ChangeUpload) bool {
	if len(d.FKMap) == 0 {
		return false
	}

	// Check if any of the tables in the changes have FK dependencies
	tablesInBatch := make(map[string]bool)
	for _, ch := range changes {
		// Default schema to "public" if not provided (same as validation)
		schema := ch.Schema
		if schema == "" {
			schema = "public"
		}
		tablesInBatch[Key(schema, ch.Table)] = true
	}

	// Check if any table in the batch has FKs to other tables in the batch
	for tableKey := range tablesInBatch {
		if fks, exists := d.FKMap[tableKey]; exists {
			for _, fk := range fks {
				refKey := Key(fk.RefSchema, fk.RefTable)
				if tablesInBatch[refKey] {
					return true // Found a dependency within the batch
				}
			}
		}
	}

	return false
}

// ParentsMissing returns a list of "schema.table:pk" that are not present
// in the DB and not scheduled to be created earlier in this request
// tableHandler is optional and can be used to convert payload keys for database comparison
func (d *DiscoveredSchema) ParentsMissing(
	ctx context.Context,
	tx pgx.Tx,
	childSchema, childTable string,
	payload map[string]any,
	willExist map[string]map[string]struct{}, // batchIndex keyed by "schema.table"
	tableHandler TableHandler,
) ([]string, error) {
	childKey := Key(childSchema, childTable)

	// Get FK constraints for this table
	fks := d.FKMap[childKey]
	if len(fks) == 0 {
		return nil, nil // No FK constraints to check
	}

	var missing []string

	for _, fk := range fks {
		// 1) Extract the foreign key value from payload
		refVal, found := payload[fk.Col]
		if !found {
			continue // FK column not present - skip validation
		}

		// Apply key conversion if available
		dbRefVal := refVal
		if tableHandler != nil {
			if converted, err := tableHandler.ConvertReferenceKey(fk.Col, refVal.(string)); err != nil {
				// Conversion error - this is a parsing error, return it as bad payload
				return []string{ReasonBadPayload}, nil
			} else {
				dbRefVal = converted
			}
		}

		// Convert to string for comparison with proper type handling
		refValStr, hasValue := formatFKValue(refVal)
		if !hasValue {
			continue // Null or empty FK value - skip validation
		}

		// Convert database value to string for DB query
		dbRefValStr, dbHasValue := formatFKValue(dbRefVal)
		if !dbHasValue {
			continue // Converted value is null or empty - skip validation
		}

		// 2) Check if parent will be created in this request (and parent table sorts before this table)
		parentKey := Key(fk.RefSchema, fk.RefTable)
		if willExistSet, ok := willExist[parentKey]; ok {
			if _, exists := willExistSet[refValStr]; exists {
				// Parent will be created - check if it comes before child in ordering
				parentOrder, parentExists := d.OrderIdx[parentKey]
				childOrder, childExists := d.OrderIdx[childKey]

				// For self-references (parentKey == childKey), allow if parent PK will be created
				// For different tables, require parent to sort before child
				if parentExists && childExists && (parentKey == childKey || parentOrder < childOrder) {
					continue // Parent will be created before child (or is self-ref) - OK
				}
			}
		}

		// 3) Check if parent exists in database
		var exists bool
		query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.%s WHERE %s = @ref_val)`,
			pgx.Identifier{fk.RefSchema}.Sanitize(),
			pgx.Identifier{fk.RefTable}.Sanitize(),
			pgx.Identifier{fk.RefCol}.Sanitize())

		if err := tx.QueryRow(ctx, query, pgx.NamedArgs{"ref_val": dbRefValStr}).Scan(&exists); err != nil {
			return nil, fmt.Errorf("parent check %s.%s(%s): %w", fk.RefSchema, fk.RefTable, fk.RefCol, err)
		}
		if exists {
			continue // Parent exists in DB - OK
		}

		// Parent is missing
		missing = append(missing, fmt.Sprintf("%s.%s:%s", fk.RefSchema, fk.RefTable, refValStr))
	}

	return missing, nil
}

// formatFKValue converts various FK value types to string for comparison
func formatFKValue(value any) (string, bool) {
	if value == nil {
		return "", false
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return "", false
		}
		return v, true
	case []byte:
		if len(v) == 0 {
			return "", false
		}
		return string(v), true
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), true
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), true
	case float32, float64:
		return fmt.Sprintf("%g", v), true
	default:
		// Fallback to string representation
		str := fmt.Sprintf("%v", v)
		if str == "" || str == "<nil>" {
			return "", false
		}
		return str, true
	}
}

// RefreshTopology re-runs schema discovery and atomically updates the DiscoveredSchema
// This is useful when FK relationships change without restarting the service
func (sd *SchemaDiscovery) RefreshTopology(ctx context.Context, registeredTables map[string]bool) (*DiscoveredSchema, error) {
	sd.logger.Info("Refreshing schema topology", "registered_tables", len(registeredTables))

	// Run discovery with current registered tables
	newSchema, err := sd.DiscoverSchema(ctx, registeredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh schema topology: %w", err)
	}

	sd.logger.Info("Schema topology refreshed successfully",
		"table_count", len(newSchema.TableOrder),
		"fk_map_entries", len(newSchema.FKMap))

	return newSchema, nil
}
