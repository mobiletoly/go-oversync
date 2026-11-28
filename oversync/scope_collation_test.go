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

// Registered scope columns may use any deterministic collation. Queries that
// combine them with sync.user_state.user_id (COLLATE "C") must state the
// collation explicitly; two different implicit collations make PostgreSQL
// refuse the comparison or ordering outright.
func TestScopeColumnWithItsOwnCollation_BootstrapsAndCapturesPerOwner(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_collation_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })
	// "POSIX" exists in every PostgreSQL build, is deterministic, and differs
	// from both the database default and "C".
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			_sync_scope_id TEXT COLLATE "POSIX" NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	// Rows written before registration exercise populated-table adoption.
	preexistingID := uuid.New()
	preexistingOwner := "scope-collation-preexisting-" + suffix
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users VALUES ($1, $2, 'carol', 'carol@example.com')`, schemaIdent),
		preexistingOwner, preexistingID)
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered), AppName: "scope-collation-test",
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	var adopted int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.user_state WHERE user_id = $1`, preexistingOwner).Scan(&adopted))
	require.Equal(t, 1, adopted, "populated-table adoption must discover the pre-existing owner")

	userID := "scope-collation-user-" + suffix
	otherUserID := "scope-collation-other-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	mustInitializeEmptyScope(t, ctx, svc, otherUserID, "bootstrap-client")
	rowID := uuid.New()
	insert := func(owner, name string) error {
		return svc.WithinSyncBundle(ctx, Actor{UserID: owner}, BundleSource{SourceID: "server-app", SourceBundleID: 1},
			retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users (id, name, email) VALUES ($1, $2, $3)`, schemaIdent),
					rowID, name, name+"@example.com")
				return err
			})
	}
	// The same visible key under two owners: each bundle must project its own row.
	require.NoError(t, insert(userID, "alice"))
	require.NoError(t, insert(otherUserID, "bob"))

	for owner, name := range map[string]string{userID: "alice", otherUserID: "bob"} {
		bundle := loadCommittedBundleForUser(t, ctx, svc, owner, 1)
		require.Len(t, bundle.Rows, 1)
		require.Equal(t, OpInsert, bundle.Rows[0].Op)
		require.Equal(t, SyncKey{"id": rowID.String()}, bundle.Rows[0].Key)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(bundle.Rows[0].Payload, &payload))
		require.Equal(t, name, payload["name"], "bundle for %s must carry that owner's row", owner)
	}
}
