package oversync

import (
	"context"
	"encoding/json"
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

func TestFKPrecheck_TenantScopeColumn_CompositeFK_ScopesParentExistence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = "postgresql://postgres:password@localhost:5432/clisync_example"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "tenant_composite_fk_" + suffix
	userB := "user-b-" + suffix
	userAScoped := "user-a-scoped-" + suffix
	userAUnscoped := "user-a-unscoped-" + suffix

	require.NoError(t, createTenantCompositeFKPrecheckSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })
	t.Cleanup(func() { _ = cleanupSyncUser(ctx, pool, userAScoped) })
	t.Cleanup(func() { _ = cleanupSyncUser(ctx, pool, userAUnscoped) })
	t.Cleanup(func() { _ = cleanupSyncUser(ctx, pool, userB) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()

	// Seed parent row for user B only.
	docID := "apple"
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.parent_docs (id, owner_user_id, doc_id) VALUES ($1, $2, $3)`,
		schemaIdent,
	), uuid.New(), userB, docID)
	require.NoError(t, err)

	makeChildReq := func(scid int64, owner string) *UploadRequest {
		childID := uuid.New()
		payload, err := json.Marshal(map[string]any{
			"id":            childID.String(),
			"owner_user_id": owner,
			"parent_doc_id": docID,
		})
		require.NoError(t, err)

		return &UploadRequest{
			LastServerSeqSeen: 0,
			Changes: []ChangeUpload{
				{
					SourceChangeID: scid,
					Schema:         schemaName,
					Table:          "child_docs",
					Op:             OpInsert,
					PK:             childID.String(),
					ServerVersion:  0,
					Payload:        payload,
				},
			},
		}
	}

	t.Run("TenantScopingEnabledReducesCompositeFKForPrecheck", func(t *testing.T) {
		svc, err := NewSyncService(pool, &ServiceConfig{
			MaxSupportedSchemaVersion: 1,
			AppName:                   "tenant-scope-composite-fk-precheck-test",
			RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "parent_docs"},
				{Schema: schemaName, Table: "child_docs"},
			},
			DisableAutoMigrateFKs: true,
			FKPrecheckMode:        FKPrecheckRefColumnAware,
			TenantScopeColumn:     "owner_user_id",
		}, logger)
		require.NoError(t, err)

		childKey := Key(schemaName, "child_docs")
		require.NotNil(t, svc.discoveredSchema)
		require.Len(t, svc.discoveredSchema.FKMap[childKey], 1)
		require.Equal(t, "parent_doc_id", svc.discoveredSchema.FKMap[childKey][0].Col)
		require.Equal(t, schemaName, svc.discoveredSchema.FKMap[childKey][0].RefSchema)
		require.Equal(t, "parent_docs", svc.discoveredSchema.FKMap[childKey][0].RefTable)
		require.Equal(t, "doc_id", svc.discoveredSchema.FKMap[childKey][0].RefCol)

		req := makeChildReq(1, userAScoped)
		resp, err := svc.ProcessUpload(ctx, userAScoped, "device-a", req)
		require.NoError(t, err)
		require.Len(t, resp.Statuses, 1)

		st := resp.Statuses[0]
		require.Equal(t, StInvalid, st.Status)
		require.Equal(t, ReasonFKMissing, st.Invalid["reason"])

		details, ok := st.Invalid["details"].(map[string]any)
		require.True(t, ok)
		missing, ok := details["missing"].([]string)
		require.True(t, ok)
		require.Contains(t, missing, fmt.Sprintf("%s.%s:%s", schemaName, "parent_docs", docID))
	})

	t.Run("TenantScopingDisabledSkipsCompositeFKPrecheck", func(t *testing.T) {
		svc, err := NewSyncService(pool, &ServiceConfig{
			MaxSupportedSchemaVersion: 1,
			AppName:                   "tenant-scope-composite-fk-precheck-test",
			RegisteredTables: []RegisteredTable{
				{Schema: schemaName, Table: "parent_docs"},
				{Schema: schemaName, Table: "child_docs"},
			},
			DisableAutoMigrateFKs: true,
			FKPrecheckMode:        FKPrecheckRefColumnAware,
			TenantScopeColumn:     "",
		}, logger)
		require.NoError(t, err)

		require.NotNil(t, svc.discoveredSchema)
		require.Len(t, svc.discoveredSchema.FKMap[Key(schemaName, "child_docs")], 0)

		req := makeChildReq(1, userAUnscoped)
		resp, err := svc.ProcessUpload(ctx, userAUnscoped, "device-b", req)
		require.NoError(t, err)
		require.Len(t, resp.Statuses, 1)

		st := resp.Statuses[0]
		require.Equal(t, StApplied, st.Status)
	})
}

func createTenantCompositeFKPrecheckSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()

	stmts := []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaIdent),
		fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent),
		fmt.Sprintf(`
			CREATE TABLE %s.parent_docs (
				id UUID PRIMARY KEY,
				owner_user_id TEXT NOT NULL,
				doc_id TEXT NOT NULL,
				UNIQUE (owner_user_id, doc_id)
			)`, schemaIdent),
		fmt.Sprintf(`
			CREATE TABLE %s.child_docs (
				id UUID PRIMARY KEY,
				owner_user_id TEXT NOT NULL,
				parent_doc_id TEXT NOT NULL,
				CONSTRAINT child_docs_parent_doc_id_fkey
					FOREIGN KEY (owner_user_id, parent_doc_id)
					REFERENCES %s.parent_docs(owner_user_id, doc_id)
					ON DELETE CASCADE
			)`, schemaIdent, schemaIdent),
	}

	for _, sql := range stmts {
		if _, err := pool.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
