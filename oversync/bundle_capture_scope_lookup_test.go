package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The final-state projection reads each touched business row by its full
// (_sync_scope_id, key) identity. sync.user_state.user_id is declared COLLATE "C";
// if that collation reaches the comparison, the planner cannot match the business
// primary key on the scope column and walks the index (or the table) per touched row.
func TestCapturedFinalState_ScopeLookupUsesBusinessPrimaryKey(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "capture_scope_plan_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered), AppName: "capture-scope-plan-test",
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "capture-scope-plan-user-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	var userPK int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, userID).Scan(&userPK))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	// With sequential scans priced out, the only question left is whether the
	// scope predicate is usable as an index condition on the business key.
	_, err = tx.Exec(ctx, `SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)
	var raw []byte
	require.NoError(t, tx.QueryRow(ctx, `EXPLAIN (FORMAT JSON, COSTS OFF) `+svc.capturedFinalStateSQL()+
		`SELECT first_ordinal, table_id, key_bytes, op_code, payload_db FROM final_state WHERE has_effect`, int64(0), userPK).Scan(&raw))

	var plans []map[string]any
	require.NoError(t, json.Unmarshal(raw, &plans))
	var businessScans []map[string]any
	var find func(node map[string]any)
	find = func(node map[string]any) {
		if node["Relation Name"] == "users" {
			businessScans = append(businessScans, node)
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			find(child.(map[string]any))
		}
	}
	for _, plan := range plans {
		find(plan["Plan"].(map[string]any))
	}
	require.Len(t, businessScans, 1, "one registered table yields one business-row lookup: %s", raw)

	// A bitmap heap scan carries its index condition on the child index scan.
	var indexConditions, filters []string
	var collect func(node map[string]any)
	collect = func(node map[string]any) {
		if condition, ok := node["Index Cond"].(string); ok {
			indexConditions = append(indexConditions, condition)
		}
		if filter, ok := node["Filter"].(string); ok {
			filters = append(filters, filter)
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			collect(child.(map[string]any))
		}
	}
	collect(businessScans[0])
	indexCondition := strings.Join(indexConditions, " ")
	require.Contains(t, indexCondition, "_sync_scope_id", "scope must be an index condition, not a filter: %s", raw)
	require.Contains(t, indexCondition, "id = ", "sync key must be an index condition: %s", raw)
	require.NotContains(t, strings.Join(filters, " "), "_sync_scope_id", "scope predicate fell back to a per-row filter: %s", raw)
}
