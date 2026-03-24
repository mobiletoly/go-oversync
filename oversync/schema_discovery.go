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
	Ordinal        int    // Column ordinal within the FK constraint
	RefSchema      string // Schema of the referenced table
	RefTable       string // Referenced parent table
	RefColumn      string // Referenced column in parent table
	MatchOption    string // SIMPLE / FULL / PARTIAL
	OnDeleteAction string // CASCADE / RESTRICT / SET NULL / SET DEFAULT / NO ACTION
	OnUpdateAction string // CASCADE / RESTRICT / SET NULL / SET DEFAULT / NO ACTION
}

// DiscoveredSchema contains the discovered table relationships
type DiscoveredSchema struct {
	TableOrder   []string       // Ordered list of schema.table (parents first, cycle members grouped deterministically)
	OrderIdx     map[string]int // schema.table -> order index for O(1) lookups
	Dependencies map[string][]string
	CycleGroup   map[string]int   // schema.table -> SCC id (>0 only when part of a multi-table cycle)
	Cycles       map[int][]string // SCC id -> sorted cycle members
}

// Key creates a normalized schema.table key (public helper).
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

// DiscoverSchemaWithDependencyOverrides runs schema discovery and merges explicit dependencies
// (ordering constraints) into the discovered dependency graph.
// Overrides only affect ordering (TableOrder/OrderIdx/Dependencies) and do not add FK validation rules.
func (sd *SchemaDiscovery) DiscoverSchemaWithDependencyOverrides(
	ctx context.Context,
	registeredTables map[string]bool,
	dependencyOverrides map[string][]string,
) (*DiscoveredSchema, error) {
	// Step 1: Get all foreign key constraints for registered tables
	fkConstraints, err := sd.getForeignKeyConstraints(ctx, registeredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign key constraints: %w", err)
	}
	if err := validateRegisteredForeignKeyClosure(fkConstraints, registeredTables); err != nil {
		return nil, err
	}
	if err := validateSupportedForeignKeyConstraints(fkConstraints, registeredTables); err != nil {
		return nil, err
	}

	// Step 2: Build dependency graph
	dependencies := sd.buildDependencyGraph(fkConstraints, registeredTables)
	applyDependencyOverrides(dependencies, registeredTables, dependencyOverrides)

	// Step 3: Perform SCC-aware topological sort to get table ordering
	tableOrder, cycleGroup, cycles, err := sd.topologicalSort(dependencies, registeredTables)
	if err != nil {
		return nil, fmt.Errorf("failed to sort tables topologically: %w", err)
	}

	// Step 4: Validate FK constraints are deferrable (important for bundle-era transactional apply)
	if err := sd.validateDeferrableConstraints(ctx, fkConstraints, registeredTables); err != nil {
		return nil, err
	}

	// Step 5: Build order index for O(1) lookups
	orderIdx := make(map[string]int, len(tableOrder))
	for i, table := range tableOrder {
		orderIdx[table] = i
	}

	sd.logger.Info("Schema discovery completed",
		"table_count", len(tableOrder),
		"fk_constraints_found", len(fkConstraints),
		"dependency_entries", len(dependencies),
		"table_order", tableOrder)
	return &DiscoveredSchema{
		TableOrder:   tableOrder,
		OrderIdx:     orderIdx,
		Dependencies: dependencies,
		CycleGroup:   cycleGroup,
		Cycles:       cycles,
	}, nil
}

func validateRegisteredForeignKeyClosure(fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) error {
	if len(fkConstraints) == 0 || len(registeredTables) == 0 {
		return nil
	}

	missingByChild := make(map[string][]string)
	for _, fk := range fkConstraints {
		childKey := key(fk.TableSchema, fk.TableName)
		parentKey := key(fk.RefSchema, fk.RefTable)
		if !registeredTables[childKey] {
			continue
		}
		if registeredTables[parentKey] {
			continue
		}

		detail := fmt.Sprintf("%s -> %s (%s)", childKey, parentKey, fk.ConstraintName)
		missingByChild[childKey] = append(missingByChild[childKey], detail)
	}

	if len(missingByChild) == 0 {
		return nil
	}

	children := make([]string, 0, len(missingByChild))
	for child := range missingByChild {
		children = append(children, child)
	}
	sort.Strings(children)

	var details []string
	for _, child := range children {
		sort.Strings(missingByChild[child])
		details = append(details, missingByChild[child]...)
	}

	return unsupportedSchemaf("registered tables are not FK-closed: %s", strings.Join(details, "; "))
}

func validateSupportedForeignKeyConstraints(fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) error {
	if len(fkConstraints) == 0 || len(registeredTables) == 0 {
		return nil
	}

	type fkGroupKey struct {
		childSchema string
		childTable  string
		constraint  string
	}

	allowedActions := map[string]struct{}{
		"":            {},
		"NO ACTION":   {},
		"RESTRICT":    {},
		"CASCADE":     {},
		"SET NULL":    {},
		"SET DEFAULT": {},
	}

	groups := make(map[fkGroupKey][]ForeignKeyConstraint)
	for _, fk := range fkConstraints {
		childKey := key(fk.TableSchema, fk.TableName)
		if !registeredTables[childKey] {
			continue
		}

		deleteRule := strings.ToUpper(strings.TrimSpace(fk.OnDeleteAction))
		if _, ok := allowedActions[deleteRule]; !ok {
			return unsupportedSchemaf("registered schema contains unsupported FK ON DELETE action %q on %s.%s (%s)", fk.OnDeleteAction, fk.TableSchema, fk.TableName, fk.ConstraintName)
		}

		updateRule := strings.ToUpper(strings.TrimSpace(fk.OnUpdateAction))
		if _, ok := allowedActions[updateRule]; !ok {
			return unsupportedSchemaf("registered schema contains unsupported FK ON UPDATE action %q on %s.%s (%s)", fk.OnUpdateAction, fk.TableSchema, fk.TableName, fk.ConstraintName)
		}

		matchOption := strings.ToUpper(strings.TrimSpace(fk.MatchOption))
		if matchOption != "" && matchOption != "NONE" && matchOption != "SIMPLE" {
			return unsupportedSchemaf("registered schema contains unsupported FK MATCH option %q on %s.%s (%s)", fk.MatchOption, fk.TableSchema, fk.TableName, fk.ConstraintName)
		}

		groupKey := fkGroupKey{
			childSchema: fk.TableSchema,
			childTable:  fk.TableName,
			constraint:  fk.ConstraintName,
		}
		groups[groupKey] = append(groups[groupKey], fk)
	}

	var composite []string
	for groupKey, group := range groups {
		if len(group) <= 1 {
			continue
		}

		parentKey := key(group[0].RefSchema, group[0].RefTable)
		composite = append(composite, fmt.Sprintf("%s.%s -> %s (%s)", groupKey.childSchema, groupKey.childTable, parentKey, groupKey.constraint))
	}
	if len(composite) > 0 {
		sort.Strings(composite)
		return unsupportedSchemaf("registered schema contains unsupported composite FK constraints: %s", strings.Join(composite, "; "))
	}

	return nil
}

func applyDependencyOverrides(
	dependencies map[string][]string,
	registeredTables map[string]bool,
	overrides map[string][]string,
) {
	if len(overrides) == 0 {
		return
	}

	depSets := make(map[string]map[string]struct{}, len(dependencies))
	for child, deps := range dependencies {
		m := make(map[string]struct{}, len(deps))
		for _, parent := range deps {
			m[parent] = struct{}{}
		}
		depSets[child] = m
	}

	for childRaw, parentsRaw := range overrides {
		childKey, ok := normalizeSchemaTableKey(childRaw)
		if !ok || !registeredTables[childKey] {
			continue
		}

		if depSets[childKey] == nil {
			depSets[childKey] = make(map[string]struct{})
		}

		for _, parentRaw := range parentsRaw {
			parentKey, ok := normalizeSchemaTableKey(parentRaw)
			if !ok || !registeredTables[parentKey] {
				continue
			}
			if childKey == parentKey {
				continue
			}
			if _, exists := depSets[childKey][parentKey]; exists {
				continue
			}
			depSets[childKey][parentKey] = struct{}{}
			dependencies[childKey] = append(dependencies[childKey], parentKey)
		}
	}
}

func normalizeSchemaTableKey(raw string) (string, bool) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "", false
	}

	var schema, table string
	parts := strings.Split(raw, ".")
	switch len(parts) {
	case 1:
		schema = "public"
		table = parts[0]
	case 2:
		schema = parts[0]
		table = parts[1]
	default:
		return "", false
	}

	if !isValidSchemaName(schema) || !isValidTableName(table) {
		return "", false
	}
	return Key(schema, table), true
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
			kcu.ordinal_position,
			rc.unique_constraint_schema AS referenced_table_schema,
			kcu2.table_name AS referenced_table_name,
			kcu2.column_name AS referenced_column_name,
			rc.match_option,
			rc.delete_rule,
			rc.update_rule
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
			&fk.Ordinal,
			&fk.RefSchema,
			&fk.RefTable,
			&fk.RefColumn,
			&fk.MatchOption,
			&fk.OnDeleteAction,
			&fk.OnUpdateAction,
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

// topologicalSort performs SCC-aware topological sorting to determine table order.
func (sd *SchemaDiscovery) topologicalSort(
	dependencies map[string][]string,
	registeredTables map[string]bool,
) ([]string, map[string]int, map[int][]string, error) {
	componentByTable, componentMembers := stronglyConnectedComponents(dependencies, registeredTables)

	componentInDegree := make(map[int]int, len(componentMembers))
	componentChildren := make(map[int]map[int]struct{}, len(componentMembers))
	componentLabel := make(map[int]string, len(componentMembers))
	cycleGroup := make(map[string]int)
	cycles := make(map[int][]string)

	for compID, members := range componentMembers {
		componentInDegree[compID] = 0
		sortedMembers := append([]string(nil), members...)
		sort.Strings(sortedMembers)
		componentMembers[compID] = sortedMembers
		componentLabel[compID] = sortedMembers[0]
		if len(sortedMembers) > 1 {
			cycles[compID] = sortedMembers
			for _, member := range sortedMembers {
				cycleGroup[member] = compID
			}
		}
	}

	for child, deps := range dependencies {
		if !registeredTables[child] {
			continue
		}
		childComp := componentByTable[child]
		for _, parent := range deps {
			if !registeredTables[parent] {
				continue
			}
			parentComp := componentByTable[parent]
			if childComp == parentComp {
				continue
			}
			children := componentChildren[parentComp]
			if children == nil {
				children = make(map[int]struct{})
				componentChildren[parentComp] = children
			}
			if _, exists := children[childComp]; exists {
				continue
			}
			children[childComp] = struct{}{}
			componentInDegree[childComp]++
		}
	}

	queue := make([]int, 0, len(componentMembers))
	for compID, degree := range componentInDegree {
		if degree == 0 {
			queue = append(queue, compID)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		return componentLabel[queue[i]] < componentLabel[queue[j]]
	})

	pop := func() int {
		x := queue[0]
		queue = queue[1:]
		return x
	}
	insertSorted := func(compID int) {
		label := componentLabel[compID]
		i := sort.Search(len(queue), func(i int) bool {
			return componentLabel[queue[i]] >= label
		})
		queue = append(queue, 0)
		copy(queue[i+1:], queue[i:])
		queue[i] = compID
	}

	var result []string
	processedComponents := 0
	for len(queue) > 0 {
		currentComp := pop()
		processedComponents++
		result = append(result, componentMembers[currentComp]...)

		for childComp := range componentChildren[currentComp] {
			componentInDegree[childComp]--
			if componentInDegree[childComp] == 0 {
				insertSorted(childComp)
			}
		}
	}

	if processedComponents != len(componentMembers) {
		return nil, nil, nil, fmt.Errorf("scc condensation graph still contains a cycle")
	}

	if len(cycles) > 0 {
		sd.logger.Info("Circular dependency groups detected; preserving deterministic order within strongly connected components",
			"cycle_count", len(cycles))
		sd.logger.Debug("Circular dependency group details",
			"cycles", cycles)
	}

	return result, cycleGroup, cycles, nil
}

func stronglyConnectedComponents(dependencies map[string][]string, registeredTables map[string]bool) (map[string]int, map[int][]string) {
	index := 0
	stack := make([]string, 0, len(registeredTables))
	onStack := make(map[string]bool, len(registeredTables))
	indexByNode := make(map[string]int, len(registeredTables))
	lowLink := make(map[string]int, len(registeredTables))
	componentByTable := make(map[string]int, len(registeredTables))
	componentMembers := make(map[int][]string)
	componentID := 0

	var visit func(string)
	visit = func(node string) {
		indexByNode[node] = index
		lowLink[node] = index
		index++
		stack = append(stack, node)
		onStack[node] = true

		for _, parent := range dependencies[node] {
			if !registeredTables[parent] {
				continue
			}
			if _, seen := indexByNode[parent]; !seen {
				visit(parent)
				if lowLink[parent] < lowLink[node] {
					lowLink[node] = lowLink[parent]
				}
			} else if onStack[parent] && indexByNode[parent] < lowLink[node] {
				lowLink[node] = indexByNode[parent]
			}
		}

		if lowLink[node] != indexByNode[node] {
			return
		}

		componentID++
		for {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[last] = false
			componentByTable[last] = componentID
			componentMembers[componentID] = append(componentMembers[componentID], last)
			if last == node {
				break
			}
		}
	}

	nodes := make([]string, 0, len(registeredTables))
	for table := range registeredTables {
		nodes = append(nodes, table)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if _, seen := indexByNode[node]; !seen {
			visit(node)
		}
	}

	return componentByTable, componentMembers
}

// validateDeferrableConstraints checks if registered-table FK constraints are deferrable.
// DEFERRABLE is required by the current runtime; INITIALLY DEFERRED remains preferred but optional
// because the hot path still issues SET CONSTRAINTS ALL DEFERRED.
func (sd *SchemaDiscovery) validateDeferrableConstraints(ctx context.Context, fkConstraints []ForeignKeyConstraint, registeredTables map[string]bool) error {
	if len(fkConstraints) == 0 {
		return nil
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
		return nil
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
		return fmt.Errorf("check FK deferrable status: %w", err)
	}
	defer rows.Close()

	var nonDeferrable []string
	for rows.Next() {
		var schemaName, constraintName string
		var isDeferrable, isDeferred bool

		if err := rows.Scan(&schemaName, &constraintName, &isDeferrable, &isDeferred); err != nil {
			return fmt.Errorf("scan FK deferrable status: %w", err)
		}

		if !isDeferrable {
			nonDeferrable = append(nonDeferrable, schemaName+"."+constraintName)
			sd.logger.Error("FK constraint is NOT DEFERRABLE and blocks bootstrap",
				"schema", schemaName,
				"constraint", constraintName,
				"recommendation", "ALTER TABLE ... ALTER CONSTRAINT ... DEFERRABLE")
		} else if !isDeferred {
			sd.logger.Debug("FK constraint is deferrable but not initially deferred",
				"schema", schemaName,
				"constraint", constraintName,
				"note", "Will work correctly due to SET CONSTRAINTS ALL DEFERRED, but INITIALLY DEFERRED is preferred")
		}
	}
	if rows.Err() != nil {
		return fmt.Errorf("iterate FK deferrable status: %w", rows.Err())
	}

	if len(nonDeferrable) > 0 {
		sort.Strings(nonDeferrable)
		return unsupportedSchemaf(
			"registered schema contains non-deferrable FK constraints: %s; make these constraints DEFERRABLE before bootstrap (INITIALLY DEFERRED recommended)",
			strings.Join(nonDeferrable, ", "),
		)
	}
	return nil
}
