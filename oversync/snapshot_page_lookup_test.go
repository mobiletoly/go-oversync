package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Snapshot materialization joins each page of row_state keys to the business
// rows. The join must be a primary-key lookup on (_sync_scope_id, key); if the
// key is compared through an expression on the business column, every page
// scans all of the user's rows (or the whole table).
func TestSnapshotMaterializationPage_LooksUpBusinessRowsByPrimaryKey(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_page_lookup_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.notes (
		_sync_scope_id TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY (_sync_scope_id, id))`,
		pgx.Identifier{schemaName}.Sanitize()))
	require.NoError(t, err)

	// Realistic populations, so the planner costs the lookup against real
	// statistics: two owners with 2,000 rows each in both tables. The users
	// rows exist before registration and are adopted at bootstrap.
	const scopeUser = "scope-user"
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	for _, owner := range []string{scopeUser, "other-user"} {
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users SELECT $1, gen_random_uuid(), 'name', 'mail' FROM generate_series(1, 2000)`, schemaIdent), owner)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.notes SELECT $1, 'note-' || g, 'body' FROM generate_series(1, 2000) AS g`, schemaIdent), owner)
		require.NoError(t, err)
	}

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered), AppName: "snapshot-page-lookup-test",
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	for _, relation := range []string{schemaIdent + ".users", schemaIdent + ".notes", "sync.row_state"} {
		_, err = pool.Exec(ctx, "ANALYZE "+relation)
		require.NoError(t, err)
	}
	var userPK int64
	var usersTableID int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, scopeUser).Scan(&userPK))
	require.NoError(t, pool.QueryRow(ctx, `SELECT table_id FROM sync.table_catalog WHERE schema_name = $1 AND table_name = 'users'`, schemaName).Scan(&usersTableID))

	for _, table := range []registeredTableRuntimeInfo{
		{schemaName: schemaName, tableName: "users", tableID: usersTableID, syncKeyColumn: "id", syncKeyType: syncKeyTypeUUID},
		{schemaName: schemaName, tableName: "notes", tableID: usersTableID + 1000, syncKeyColumn: "id", syncKeyType: syncKeyTypeText},
	} {
		for _, afterCursor := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/afterCursor=%v", table.syncKeyType, afterCursor), func(t *testing.T) {
				args := []any{scopeUser, userPK, table.tableID, int64(1 << 20), int64(1 << 20), 512}
				if afterCursor {
					args = []any{scopeUser, userPK, table.tableID, int64(1 << 20), int64(1 << 20), []byte{0x01}, 512}
				}
				tx, err := pool.Begin(ctx)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback(ctx) }()
				// Rule out the plans that read all of the owner's rows at once, so the
				// remaining choice is a per-key lookup or a rescanned scope-only scan.
				for _, setting := range []string{"enable_seqscan", "enable_hashjoin", "enable_mergejoin"} {
					_, err = tx.Exec(ctx, "SET LOCAL "+setting+" = off")
					require.NoError(t, err)
				}
				var raw []byte
				require.NoError(t, tx.QueryRow(ctx, `EXPLAIN (FORMAT JSON, COSTS OFF) `+
					snapshotMaterializationPageSQL(table, "to_jsonb(src)", afterCursor), args...).Scan(&raw))

				var plans []map[string]any
				require.NoError(t, json.Unmarshal(raw, &plans))
				var conditions, filters []string
				found := false
				var visit func(node map[string]any, insideBusinessScan bool)
				visit = func(node map[string]any, insideBusinessScan bool) {
					if node["Relation Name"] == table.tableName {
						found, insideBusinessScan = true, true
					}
					if insideBusinessScan {
						if condition, ok := node["Index Cond"].(string); ok {
							conditions = append(conditions, condition)
						}
						if filter, ok := node["Filter"].(string); ok {
							filters = append(filters, filter)
						}
					}
					children, _ := node["Plans"].([]any)
					for _, child := range children {
						visit(child.(map[string]any), insideBusinessScan)
					}
				}
				for _, plan := range plans {
					visit(plan["Plan"].(map[string]any), false)
				}
				require.True(t, found, "plan must read %s: %s", table.tableName, raw)
				condition := strings.Join(conditions, " ")
				require.Contains(t, condition, "_sync_scope_id", "scope must be an index condition: %s", raw)
				require.Contains(t, condition, "(id = ", "sync key must be an index condition, not a join filter: %s", raw)
				if table.syncKeyType == syncKeyTypeUUID {
					require.Empty(t, filters, "a UUID key needs no recheck: %s", raw)
				}
			})
		}
	}
}
