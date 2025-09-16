// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ForeignKeyInfo represents a foreign key constraint
type ForeignKeyInfo struct {
	SchemaName       string `db:"table_schema"`
	TableName        string `db:"table_name"`
	ConstraintName   string `db:"constraint_name"`
	ReferencedSchema string `db:"referenced_schema"`
	ReferencedTable  string `db:"referenced_table"`
	ChildColumns     string `db:"column_name"`       // Single column for now
	ParentColumns    string `db:"referenced_column"` // Single column for now
	OnDeleteAction   string `db:"delete_rule"`
	OnUpdateAction   string `db:"update_rule"`
	IsDeferrable     string `db:"is_deferrable"`      // YES/NO from information_schema
	IsDeferred       string `db:"initially_deferred"` // YES/NO from information_schema
}

// IsDeferrableFK returns true if the FK constraint is deferrable
func (fk *ForeignKeyInfo) IsDeferrableFK() bool {
	return fk.IsDeferrable == "YES"
}

// IsDeferredFK returns true if the FK constraint is initially deferred
func (fk *ForeignKeyInfo) IsDeferredFK() bool {
	return fk.IsDeferred == "YES"
}

// DeferrableFKManager handles migration of foreign keys to deferrable constraints
type DeferrableFKManager struct {
	pool             *pgxpool.Pool
	logger           *slog.Logger
	registeredTables []RegisteredTable // Specific schema.table combinations to migrate
}

// NewDeferrableFKManager creates a new manager for deferrable foreign key migrations
func NewDeferrableFKManager(pool *pgxpool.Pool, logger *slog.Logger, registeredTables []RegisteredTable) *DeferrableFKManager {
	return &DeferrableFKManager{
		pool:             pool,
		logger:           logger,
		registeredTables: registeredTables,
	}
}

// MigrateToDeferredInTx migrates FKs to deferrable within an existing transaction
func (m *DeferrableFKManager) MigrateToDeferredInTx(ctx context.Context, tx pgx.Tx) error {
	if len(m.registeredTables) == 0 {
		m.logger.Info("No registered tables, skipping FK migration")
		return nil
	}

	// Collect all FKs that need migration first
	var allFKsToMigrate []ForeignKeyInfo
	for _, regTable := range m.registeredTables {
		m.logger.Debug("Processing FK migration for table",
			"schema", regTable.Schema,
			"table", regTable.Table)

		// Find non-deferrable FKs for this specific table
		fks, err := m.getNonDeferrableFKsForTableInTx(ctx, tx, regTable.Schema, regTable.Table)
		if err != nil {
			return fmt.Errorf("failed to get FKs for %s.%s: %w", regTable.Schema, regTable.Table, err)
		}

		allFKsToMigrate = append(allFKsToMigrate, fks...)
	}

	if len(allFKsToMigrate) == 0 {
		m.logger.Info("All FK constraints are already deferrable")
		return nil
	}

	m.logger.Info("Migrating FK constraints to deferrable in single transaction",
		"count", len(allFKsToMigrate))

	// Migrate all FKs within the provided transaction
	var upgraded, recreated int
	for _, fk := range allFKsToMigrate {
		if err := m.migrateToDeferrableInTx(ctx, tx, fk); err != nil {
			// Log warning but continue with other FKs
			m.logger.Error("Failed to migrate FK to deferrable",
				"constraint", fk.ConstraintName,
				"table", fk.SchemaName+"."+fk.TableName,
				"error", err)
			return err
		} else {
			if fk.IsDeferrableFK() && !fk.IsDeferredFK() {
				upgraded++
				m.logger.Info("Upgraded FK to initially deferred",
					"constraint", fk.ConstraintName,
					"table", fk.SchemaName+"."+fk.TableName)
			} else {
				recreated++
				m.logger.Info("Recreated FK as deferrable",
					"constraint", fk.ConstraintName,
					"table", fk.SchemaName+"."+fk.TableName)
			}
		}
	}

	m.logger.Info("FK migration summary", "upgraded", upgraded, "recreated", recreated)
	return nil
}

// getNonDeferrableFKsForTable gets non-deferrable FKs for a specific table
func (m *DeferrableFKManager) getNonDeferrableFKsForTable(ctx context.Context, schemaName, tableName string) ([]ForeignKeyInfo, error) {
	return m.getNonDeferrableFKsForTableInTx(ctx, nil, schemaName, tableName)
}

// getNonDeferrableFKsForTableInTx gets non-deferrable FKs for a specific table within a transaction
func (m *DeferrableFKManager) getNonDeferrableFKsForTableInTx(
	ctx context.Context, tx pgx.Tx, schemaName, tableName string,
) ([]ForeignKeyInfo, error) {
	query := `
		SELECT
			tc.table_schema,
			tc.table_name,
			tc.constraint_name,
			ccu.table_schema AS referenced_schema,
			ccu.table_name AS referenced_table,
			kcu.column_name,
			ccu.column_name AS referenced_column,
			rc.delete_rule,
			rc.update_rule,
			tc.is_deferrable,
			tc.initially_deferred
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.constraint_schema = tc.constraint_schema
		JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
			AND tc.constraint_schema = rc.constraint_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = @schema_name
			AND tc.table_name = @table_name
			AND (tc.is_deferrable = 'NO' OR (tc.is_deferrable = 'YES' AND tc.initially_deferred = 'NO'))
		ORDER BY tc.constraint_name`

	var rows pgx.Rows
	var err error

	if tx != nil {
		rows, err = tx.Query(ctx, query, pgx.NamedArgs{
			"schema_name": schemaName,
			"table_name":  tableName,
		})
	} else {
		rows, err = m.pool.Query(ctx, query, pgx.NamedArgs{
			"schema_name": schemaName,
			"table_name":  tableName,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	allFKs, err := pgx.CollectRows(rows, pgx.RowToStructByName[ForeignKeyInfo])
	if err != nil {
		return nil, fmt.Errorf("failed to collect FK rows: %w", err)
	}

	// Group by constraint name to detect composite FKs
	constraintGroups := make(map[string][]ForeignKeyInfo)
	for _, fk := range allFKs {
		constraintGroups[fk.ConstraintName] = append(constraintGroups[fk.ConstraintName], fk)
	}

	// Filter out composite FKs for now
	var singleColumnFKs []ForeignKeyInfo
	for constraintName, fkGroup := range constraintGroups {
		if len(fkGroup) > 1 {
			m.logger.Warn("Skipping composite FK constraint (not supported yet)",
				"constraint", constraintName,
				"table", schemaName+"."+tableName,
				"columns", len(fkGroup))
			continue
		}
		singleColumnFKs = append(singleColumnFKs, fkGroup[0])
	}
	return singleColumnFKs, nil
}

// migrateToDeferrableInTx converts a foreign key constraint to deferrable within an existing transaction
func (m *DeferrableFKManager) migrateToDeferrableInTx(
	ctx context.Context, tx pgx.Tx, fk ForeignKeyInfo,
) error {
	// Upgrade-in-place if already DEFERRABLE but not initially deferred
	if fk.IsDeferrableFK() && !fk.IsDeferredFK() {
		alterSQL := fmt.Sprintf(
			"ALTER TABLE %s.%s ALTER CONSTRAINT %s DEFERRABLE INITIALLY DEFERRED",
			pgx.Identifier{fk.SchemaName}.Sanitize(),
			pgx.Identifier{fk.TableName}.Sanitize(),
			pgx.Identifier{fk.ConstraintName}.Sanitize(),
		)
		m.logger.Debug("Upgrading FK to initially deferred", "sql", alterSQL)
		_, err := tx.Exec(ctx, alterSQL)
		if err != nil {
			m.logger.Error("Failed to upgrade FK to initially deferred", "sql", alterSQL, "error", err)
			return fmt.Errorf("failed to upgrade FK to initially deferred: %w", err)
		}
		return err
	}

	if fk.IsDeferrableFK() {
		m.logger.Debug("FK already deferrable and initially deferred; skipping", "constraint", fk.ConstraintName)
		return nil
	}

	// Build ON DELETE clause
	onDeleteClause := ""
	switch strings.ToUpper(fk.OnDeleteAction) {
	case "CASCADE":
		onDeleteClause = "ON DELETE CASCADE"
	case "SET NULL":
		onDeleteClause = "ON DELETE SET NULL"
	case "RESTRICT":
		onDeleteClause = "ON DELETE RESTRICT"
	case "SET DEFAULT":
		onDeleteClause = "ON DELETE SET DEFAULT"
	default:
		onDeleteClause = "" // NO ACTION
	}

	// Build ON UPDATE clause
	onUpdateClause := ""
	switch strings.ToUpper(fk.OnUpdateAction) {
	case "CASCADE":
		onUpdateClause = "ON UPDATE CASCADE"
	case "SET NULL":
		onUpdateClause = "ON UPDATE SET NULL"
	case "RESTRICT":
		onUpdateClause = "ON UPDATE RESTRICT"
	case "SET DEFAULT":
		onDeleteClause = "ON UPDATE SET DEFAULT"
	default:
		onUpdateClause = "" // NO ACTION
	}

	// Combine clauses
	actionClause := strings.TrimSpace(onDeleteClause + " " + onUpdateClause)
	tempName := fk.ConstraintName + "_deferrable"

	// Step 1: Add new deferrable constraint (NOT VALID)
	addSQL := fmt.Sprintf(`
		ALTER TABLE %s.%s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s.%s(%s)
		%s
		DEFERRABLE INITIALLY DEFERRED
		NOT VALID`,
		pgx.Identifier{fk.SchemaName}.Sanitize(),
		pgx.Identifier{fk.TableName}.Sanitize(),
		pgx.Identifier{tempName}.Sanitize(),
		pgx.Identifier{fk.ChildColumns}.Sanitize(),
		pgx.Identifier{fk.ReferencedSchema}.Sanitize(),
		pgx.Identifier{fk.ReferencedTable}.Sanitize(),
		pgx.Identifier{fk.ParentColumns}.Sanitize(),
		actionClause,
	)

	m.logger.Debug("Adding deferrable constraint", "sql", addSQL)
	if _, err := tx.Exec(ctx, addSQL); err != nil {
		m.logger.Error("Failed to add deferrable constraint", "sql", addSQL, "error", err)
		return fmt.Errorf("failed to add deferrable constraint: %w", err)
	}

	// Step 2: Validate the new constraint
	validateSQL := fmt.Sprintf(
		"ALTER TABLE %s.%s VALIDATE CONSTRAINT %s",
		pgx.Identifier{fk.SchemaName}.Sanitize(),
		pgx.Identifier{fk.TableName}.Sanitize(),
		pgx.Identifier{tempName}.Sanitize(),
	)

	m.logger.Debug("Validating deferrable constraint", "sql", validateSQL)
	if _, err := tx.Exec(ctx, validateSQL); err != nil {
		m.logger.Error("Failed to validate deferrable constraint", "sql", validateSQL, "error", err)
		return fmt.Errorf("failed to validate deferrable constraint: %w", err)
	}

	// Step 3: Drop the old constraint
	dropSQL := fmt.Sprintf(
		"ALTER TABLE %s.%s DROP CONSTRAINT %s",
		pgx.Identifier{fk.SchemaName}.Sanitize(),
		pgx.Identifier{fk.TableName}.Sanitize(),
		pgx.Identifier{fk.ConstraintName}.Sanitize(),
	)

	m.logger.Debug("Dropping old constraint", "sql", dropSQL)
	if _, err := tx.Exec(ctx, dropSQL); err != nil {
		m.logger.Error("Failed to drop old constraint", "sql", dropSQL, "error", err)
		return fmt.Errorf("failed to drop old constraint: %w", err)
	}

	// Step 4: Rename new constraint to canonical name
	renameSQL := fmt.Sprintf(
		"ALTER TABLE %s.%s RENAME CONSTRAINT %s TO %s",
		pgx.Identifier{fk.SchemaName}.Sanitize(),
		pgx.Identifier{fk.TableName}.Sanitize(),
		pgx.Identifier{tempName}.Sanitize(),
		pgx.Identifier{fk.ConstraintName}.Sanitize(),
	)

	m.logger.Debug("Renaming constraint", "sql", renameSQL)
	if _, err := tx.Exec(ctx, renameSQL); err != nil {
		m.logger.Error("Failed to rename constraint", "sql", renameSQL, "error", err)
		return fmt.Errorf("failed to rename constraint: %w", err)
	}
	return nil
}
