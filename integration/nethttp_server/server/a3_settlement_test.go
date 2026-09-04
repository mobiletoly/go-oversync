package server

import (
	"context"
	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestA3Live_SettlementCancellationRetainsHistoricalIntent(t *testing.T) {
	authority, err := NewA1SurvivorTestServer()
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	user := "a3-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	client := newReadyOnlyTestClient(t, filepath.Join(t.TempDir(), "client.sqlite"), authority.URL(), func(context.Context) (string, error) { return token, nil }, defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}}), `CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
	defer client.Close()
	ctx := context.Background()
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	p, q, c, d, u := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	db := client.Database()
	_, err = db.Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'a1-other',NULL),(?,'child',?),(?,'grandchild',?),(?,'historical',NULL)`, p, q, c, p, d, c, u)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	var historical string
	require.NoError(t, db.QueryRow(`SELECT server_payload FROM _sync_row_state WHERE key_json=?`, `{"id":"`+u+`"}`).Scan(&historical))
	_, err = db.Exec(`UPDATE categories SET name='a1-delete' WHERE id=?`, p)
	require.NoError(t, err)
	client.SetBeforePushReplayHook(func(context.Context) error {
		_, err := db.Exec(`UPDATE categories SET name='new independent L' WHERE id=?`, u)
		require.NoError(t, err)
		return context.Canceled
	})
	_, err = client.PushPending(ctx)
	require.ErrorIs(t, err, context.Canceled)
	var hash string
	var sourceID, checkpoint, reserved int64
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash,source_bundle_id FROM _sync_outbox_bundle`).Scan(&hash, &sourceID))
	require.EqualValues(t, 2, sourceID)
	require.Len(t, hash, 64)
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	require.EqualValues(t, 1, checkpoint)
	require.NoError(t, db.QueryRow(`SELECT reserved_entries FROM _sync_operation_state`).Scan(&reserved))
	require.Zero(t, reserved)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	var name, parent, basis, desired string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM categories WHERE id=?`, d).Scan(&name, &parent))
	require.Equal(t, "grandchild", name)
	require.Equal(t, c, parent)
	require.NoError(t, db.QueryRow(`SELECT parent_id FROM categories WHERE id=?`, c).Scan(&parent))
	require.Equal(t, q, parent)
	require.NoError(t, db.QueryRow(`SELECT base_payload,payload FROM _sync_dirty_rows WHERE key_json=?`, `{"id":"`+u+`"}`).Scan(&basis, &desired))
	require.Equal(t, historical, basis)
	require.Contains(t, desired, "new independent L")
	for _, sql := range []string{`SELECT count(*) FROM pragma_foreign_key_check`, `SELECT count(*) FROM _sync_outbox_rows`, `SELECT reserved_entries+push_stage_entries FROM _sync_operation_state`} {
		var count int
		require.NoError(t, db.QueryRow(sql).Scan(&count))
		require.Zero(t, count)
	}
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state`).Scan(&sourceID))
	require.EqualValues(t, 3, sourceID)
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	require.EqualValues(t, 2, checkpoint)
}
