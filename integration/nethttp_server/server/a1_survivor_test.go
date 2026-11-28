package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/stretchr/testify/require"
)

func TestA1LiveSettlementAndPullPreserveGrandchild(t *testing.T) {
	authority, err := NewA1SurvivorTestServer()
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	ctx := context.Background()
	user := "a1-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	clients := make([]*oversqlite.Client, 2)
	for i := range clients {
		clients[i] = newReadyOnlyTestClient(t, filepath.Join(t.TempDir(), "client.sqlite"), authority.URL(),
			func(context.Context) (string, error) { return token, nil },
			defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}}),
			`CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
		defer clients[i].Close()
		_, err := clients[i].Attach(ctx, user)
		require.NoError(t, err)
	}
	p, q, c, d := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err = clients[0].Database().Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'a1-other',NULL),(?,'child',?),(?,'grandchild',?)`, p, q, c, p, d, c)
	require.NoError(t, err)
	_, err = clients[0].PushPending(ctx)
	require.NoError(t, err)
	_, err = clients[1].PullToStable(ctx)
	require.NoError(t, err)
	_, err = clients[0].Database().Exec(`UPDATE categories SET name='a1-delete' WHERE id=?`, p)
	require.NoError(t, err)
	_, err = clients[0].PushPending(ctx)
	require.NoError(t, err)
	_, err = clients[1].PullToStable(ctx)
	require.NoError(t, err)
	for i, client := range clients {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			db := client.Database()
			var parent, name string
			require.NoError(t, db.QueryRow(`SELECT parent_id,name FROM categories WHERE id=?`, d).Scan(&parent, &name))
			require.Equal(t, c, parent)
			require.Equal(t, "grandchild", name)
			require.NoError(t, db.QueryRow(`SELECT parent_id FROM categories WHERE id=?`, c).Scan(&parent))
			require.Equal(t, q, parent)
			for _, query := range []string{`SELECT count(*) FROM categories WHERE id='` + p + `'`, `SELECT count(*) FROM pragma_foreign_key_check`,
				`SELECT count(*) FROM _sync_dirty_rows`, `SELECT count(*) FROM _sync_outbox_rows`, `SELECT count(*) FROM _sync_apply_rows`, `SELECT count(*) FROM _sync_apply_plan`} {
				var count int
				require.NoError(t, db.QueryRow(query).Scan(&count))
				require.Zero(t, count)
			}
			var checkpoint int
			require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
			require.Equal(t, 2, checkpoint)
		})
	}
}
