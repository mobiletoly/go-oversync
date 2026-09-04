package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// FK-01: canonical effect order follows the first captured ordinal, not a safe
// SQLite application order. The client must preserve D even though D has no effect.
func TestA1ServerNormalizesDeleteBeforeSurvivingReparent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "a1_survivors_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ident := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, ident))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dropTestSchema(context.Background(), pool, schema)) })
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.nodes (
		_sync_scope_id TEXT NOT NULL, id UUID NOT NULL, name TEXT NOT NULL, parent_id UUID,
		PRIMARY KEY (_sync_scope_id, id),
		FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.nodes(_sync_scope_id, id)
		ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
	)`, ident, ident))
	require.NoError(t, err)
	tables := []RegisteredTable{{Schema: schema, Table: "nodes", SyncKeyColumns: []string{"id"}}}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		AppName: "a1-survivors", RegisteredTables: tables, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, tables, nil),
	}, integrationTestLogger(slog.LevelWarn))
	owner := "a1-owner"
	mustInitializeEmptyScope(t, ctx, svc, owner, "server-app")
	p, q, c, d := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: owner},
		BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(),
		func(ctx context.Context, tx DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.nodes(id,name,parent_id) VALUES
				($1,'parent',NULL),($2,'other',NULL),($3,'child',$1),($4,'grandchild',$3)`, ident), p, q, c, d)
			return err
		}))
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: owner},
		BundleSource{SourceID: "server-app", SourceBundleID: 2}, retryableBundleWriteOptionsForTest(),
		func(ctx context.Context, tx DatabaseWriteTx) error {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.nodes SET name='changed' WHERE id=$1`, ident), p); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.nodes SET parent_id=$2 WHERE id=$1`, ident), c, q); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.nodes WHERE id=$1`, ident), p)
			return err
		}))
	bundle := loadCommittedBundleForUser(t, ctx, svc, owner, 2)
	require.Len(t, bundle.Rows, 2, "clean D must not acquire a synthetic server effect")
	require.Equal(t, OpDelete, bundle.Rows[0].Op)
	require.Equal(t, p, bundle.Rows[0].Key["id"])
	require.Equal(t, OpUpdate, bundle.Rows[1].Op)
	require.Equal(t, c, bundle.Rows[1].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":%q,"name":"child","parent_id":%q}`, c, q), string(bundle.Rows[1].Payload))
	var name, parent string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name,parent_id::text FROM %s.nodes WHERE id=$1`, ident), d).Scan(&name, &parent))
	require.Equal(t, "grandchild", name)
	require.Equal(t, c, parent)
	var remaining int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.nodes`, ident)).Scan(&remaining))
	require.Equal(t, 3, remaining)
}
