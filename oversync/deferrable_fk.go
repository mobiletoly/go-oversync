// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	for i, regTable := range m.registeredTables {
		m.logger.Debug("Processing FK migration for table",
			"schema", regTable.Schema,
			"table", regTable.Table)

		spName := fmt.Sprintf("sp_fk_list_%d", i)
		if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize())); err != nil {
			return fmt.Errorf("create fk discovery savepoint: %w", err)
		}

		// Find non-deferrable FKs for this specific table
		fks, err := m.getNonDeferrableFKsForTableInTx(ctx, tx, regTable.Schema, regTable.Table)
		if err != nil {
			m.logger.Error("Failed to list foreign keys for table; skipping table",
				"schema", regTable.Schema,
				"table", regTable.Table,
				"error", err)
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			continue
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))

		allFKsToMigrate = append(allFKsToMigrate, fks...)
	}

	if len(allFKsToMigrate) == 0 {
		m.logger.Info("All FK constraints are already deferrable")
		return nil
	}

	m.logger.Info("Migrating FK constraints to deferrable in single transaction",
		"count", len(allFKsToMigrate))

	// Migrate all FKs within the provided transaction
	var upgraded, recreated, failed int
	var firstFailure error
	for i, fk := range allFKsToMigrate {
		spName := fmt.Sprintf("sp_fk_mig_%d", i)
		if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize())); err != nil {
			return fmt.Errorf("create fk migration savepoint: %w", err)
		}

		if err := m.migrateToDeferrableInTx(ctx, tx, fk); err != nil {
			failed++
			if firstFailure == nil {
				firstFailure = err
			}

			// Log error but continue with other FKs.
			m.logger.Error("Failed to migrate FK to deferrable",
				"constraint", fk.ConstraintName,
				"table", fk.SchemaName+"."+fk.TableName,
				"error", err)

			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			continue
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize())); err != nil {
			return fmt.Errorf("release fk migration savepoint: %w", err)
		}

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

	m.logger.Info("FK migration summary", "upgraded", upgraded, "recreated", recreated, "failed", failed)
	if failed > 0 {
		return fmt.Errorf("fk migration failed for %d constraints (first error: %w)", failed, firstFailure)
	}
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
		onUpdateClause = "ON UPDATE SET DEFAULT"
	default:
		onUpdateClause = "" // NO ACTION
	}

	// Combine clauses
	actionClause := strings.TrimSpace(onDeleteClause + " " + onUpdateClause)
	tempName := makeTempConstraintName(fk.ConstraintName)

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

func makeTempConstraintName(canonical string) string {
	const (
		maxIdentBytes = 63
		suffix        = "_deferrable"
	)

	if len(canonical)+len(suffix) <= maxIdentBytes {
		return canonical + suffix
	}

	sum := sha256.Sum256([]byte(canonical))
	hash := hex.EncodeToString(sum[:4]) // 8 hex chars

	keep := maxIdentBytes - len(suffix) - 1 - len(hash)
	if keep < 1 {
		name := hash + suffix
		if len(name) > maxIdentBytes {
			return name[:maxIdentBytes]
		}
		return name
	}

	return canonical[:keep] + "_" + hash + suffix
}
