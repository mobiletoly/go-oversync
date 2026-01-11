package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestMakeTempConstraintName_LimitsIdentifierLength(t *testing.T) {
	canonical := "b_" + strings.Repeat("x", 61) // 63 bytes
	tmp := makeTempConstraintName(canonical)
	if len(tmp) > 63 {
		t.Fatalf("expected temp name to be <= 63 bytes, got %d", len(tmp))
	}
	if tmp == canonical+"_deferrable" {
		t.Fatalf("expected truncation+hash for long canonical name")
	}
}

func TestAutoMigrateForeignKeys_UsesSavepointsAndHandlesNameLength(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "business_fk_mig_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.parent_ok (id UUID PRIMARY KEY)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.parent_bad (id UUID PRIMARY KEY)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.child (id UUID PRIMARY KEY, ok_id UUID, bad_id UUID)`, schemaIdent))
	require.NoError(t, err)

	okParent := uuid.New()
	badParentMissing := uuid.New()
	childID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.parent_ok (id) VALUES ($1)`, schemaIdent), okParent)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.child (id, ok_id, bad_id) VALUES ($1, $2, $3)`, schemaIdent), childID, okParent, badParentMissing)
	require.NoError(t, err)

	longOKName := "b_" + strings.Repeat("x", 61) // 63 bytes -> temp name must be shortened
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.child ADD CONSTRAINT a_fk_bad FOREIGN KEY (bad_id) REFERENCES %s.parent_bad(id) NOT VALID`,
		schemaIdent, schemaIdent,
	))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.child ADD CONSTRAINT %s FOREIGN KEY (ok_id) REFERENCES %s.parent_ok(id)`,
		schemaIdent, pgx.Identifier{longOKName}.Sanitize(), schemaIdent,
	))
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	svc, err := NewSyncService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "deferrable-fk-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "child"},
			{Schema: schemaName, Table: "parent_ok"},
			{Schema: schemaName, Table: "parent_bad"},
		},
	}, logger)
	require.NoError(t, err)
	defer svc.Close()

	// The failing FK should not abort the init transaction, and the next FK should still be migrated.
	var okDef, okDeferred string
	err = pool.QueryRow(ctx, `
		SELECT tc.is_deferrable, tc.initially_deferred
		FROM information_schema.table_constraints tc
		WHERE tc.constraint_type='FOREIGN KEY'
		  AND tc.table_schema=$1 AND tc.table_name='child' AND tc.constraint_name=$2
	`, schemaName, longOKName).Scan(&okDef, &okDeferred)
	require.NoError(t, err)
	require.Equal(t, "YES", okDef)
	require.Equal(t, "YES", okDeferred)

	var badDef, badDeferred string
	err = pool.QueryRow(ctx, `
		SELECT tc.is_deferrable, tc.initially_deferred
		FROM information_schema.table_constraints tc
		WHERE tc.constraint_type='FOREIGN KEY'
		  AND tc.table_schema=$1 AND tc.table_name='child' AND tc.constraint_name='a_fk_bad'
	`, schemaName).Scan(&badDef, &badDeferred)
	require.NoError(t, err)
	require.Equal(t, "NO", badDef)
	require.Equal(t, "NO", badDeferred)
}
