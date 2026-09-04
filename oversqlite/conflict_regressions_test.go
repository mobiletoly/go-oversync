package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPendingConflict_NewerDeleteAfterInsert(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "tombstoned"}[tombstone], func(t *testing.T) {
			ctx := context.Background()
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			id := uuid.NewString()
			if tombstone {
				seedSyncedUserRow(t, c, db, id, "Before", 1)
				_, err := db.Exec(`DELETE FROM users WHERE id=?`, id)
				require.NoError(t, err)
				_, err = db.Exec(`DELETE FROM _sync_dirty_rows; UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL`)
				require.NoError(t, err)
			}
			_, err := db.Exec(`INSERT INTO users VALUES(?,?,?)`, id, "Offline", "ada@example.com")
			require.NoError(t, err)
			server := newMockPushServer(t)
			server.pushHook = func(s *mockDirectPush) *http.Response {
				return structuredConflictResponse(s.Rows[0], 2, false, map[string]any{"id": id, "name": "Server", "email": "ada@example.com"})
			}
			c.HTTP = &http.Client{Transport: server}
			_, err = c.PushPending(ctx)
			var needs *ConflictNeedsResolutionError
			require.ErrorAs(t, err, &needs)
			require.Equal(t, oversync.OpInsert, needs.Pending.Conflict.LocalOp)
			_, err = db.Exec(`DELETE FROM users WHERE id=?`, id)
			require.NoError(t, err)
			p, err := c.PendingConflict(ctx)
			require.NoError(t, err)
			require.NoError(t, c.ResolveConflict(ctx, *p, AcceptServer{}))
			path := filepath.Join(t.TempDir(), "reopen.sqlite")
			_, err = db.Exec(`VACUUM INTO ?`, path)
			require.NoError(t, err)
			reopenedDB, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			defer reopenedDB.Close()
			reopenedDB.SetMaxOpenConns(1)
			reopened, err := newTestClient(t, reopenedDB, "http://example.invalid", tokenProviderForTests, c.config)
			require.NoError(t, err)
			defer reopened.Close()
			// Observe the next actual upload, not just a dirty marker or row-state value.
			sawDelete := false
			server = newMockPushServer(t)
			server.nextBundleSeq = 3
			server.pushHook = func(s *mockDirectPush) *http.Response {
				require.Len(t, s.Rows, 1)
				require.Equal(t, oversync.OpDelete, s.Rows[0].Op)
				require.EqualValues(t, 2, s.Rows[0].BaseRowVersion)
				sawDelete = true
				return server.defaultPushResponse(s)
			}
			attachTestClient(t, reopened, "bundle-user", "bundle-device")
			reopened.HTTP = &http.Client{Transport: server}
			_, err = reopened.PushPending(ctx)
			require.NoError(t, err)
			require.True(t, sawDelete)
			// Historical live evidence followed by our committed delete must not
			// resurrect the row when the ordinary download loop catches up.
			setPullResponses(t, reopened, oversync.PullResponse{StableBundleSeq: 3, Bundles: []oversync.Bundle{
				pullBundle(1, "peer", 1, validUserInsertRow(t, id, 1, "Before", "ada@example.com")),
				pullBundle(2, "peer", 2, validUserInsertRow(t, id, 2, "Server", "ada@example.com")),
				pullBundle(3, reopened.sourceID, 2, oversync.BundleRow{
					Schema: "main", Table: "users", Key: mustBundleKey(id), Op: oversync.OpDelete, RowVersion: 3,
				}),
			}})
			mustPullToStable(t, reopened, ctx)
			requireLastBundleSeqSeen(t, reopened, ctx, 3)
			var count int
			require.NoError(t, reopenedDB.QueryRow(`SELECT count(*) FROM users WHERE id=?`, id).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestUninstall_RejectsRetryAndDirtyDelete(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NoError(t, c.RetryConflict(ctx, *p))
	before := requireOutboxBundle(t, c.db)
	require.ErrorContains(t, c.UninstallSync(ctx), "unresolved work")
	require.Equal(t, before, requireOutboxBundle(t, c.db))
	_, err = c.PushPending(ctx)
	require.Error(t, err)
	p, err = c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NoError(t, c.ResolveConflict(ctx, *p, AcceptServer{}))
	_, err = c.db.Exec(`DELETE FROM users WHERE id=?`, id)
	require.NoError(t, err)
	require.ErrorContains(t, c.UninstallSync(ctx), "unresolved work")
	var count int
	require.NoError(t, c.db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows WHERE op='DELETE'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestUninstall_SettledMetadataAllowed(t *testing.T) {
	c, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	require.NoError(t, c.UninstallSync(context.Background()))
}

func TestUninstall_StagingAndLifecycleAreAtomicGuards(t *testing.T) {
	for name, stmt := range map[string]string{
		"snapshot":       `INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('staged',0,'main','users','{}',1,'{}')`,
		"settlement":     `INSERT INTO _sync_push_stage(bundle_seq,row_ordinal,schema_name,table_name,key_json,op,row_version,payload) VALUES(1,0,'main','users','{}','DELETE',1,NULL)`,
		"initialization": `UPDATE _sync_attachment_state SET pending_initialization_id='pending'`,
		"recovery":       `UPDATE _sync_operation_state SET kind='source_recovery',reason='pending'`,
		"replacement":    `UPDATE _sync_operation_state SET kind='remote_replace',replacement_source_id='replacement'`,
	} {
		t.Run(name, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			_, err := db.Exec(stmt)
			require.NoError(t, err)
			require.ErrorContains(t, c.UninstallSync(context.Background()), "unresolved work")
			var tables int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_sync_source_state'`).Scan(&tables))
			require.Equal(t, 1, tables)
		})
	}
}
