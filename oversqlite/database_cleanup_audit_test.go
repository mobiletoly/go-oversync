package oversqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const auditCategoriesDDL = `CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`

type cleanupAuditMigrator struct{}

func (cleanupAuditMigrator) InstalledVersion(ctx context.Context, r LocalMigrationReader) (int64, error) {
	var v int64
	err := r.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v)
	return v, err
}
func (cleanupAuditMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
	_, err := tx.ExecContext(ctx, auditCategoriesDDL)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `PRAGMA user_version=1`)
	return err
}

type cleanupAuditContext struct {
	context.Context
	client *Client
	armed  bool
	slices int64
	cancel context.CancelFunc
}

func (c *cleanupAuditContext) Err() error {
	if c.armed && c.client.a3Stats.workSlices > c.slices {
		c.cancel()
	}
	return c.Context.Err()
}

// Opt-in retained-file audit. The harness must provide an isolated categories authority.
func TestDatabaseCleanupAudit(t *testing.T) {
	root := os.Getenv("OVERSQLITE_CLEANUP_AUDIT_ROOT")
	if root == "" {
		t.Skip("retained-file live audit requires its isolated runner")
	}
	base := os.Getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
	require.NotEmpty(t, base)
	target, err := strconv.ParseInt(os.Getenv("OVERSQLITE_AUDIT_TARGET"), 10, 64)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(root, 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 570*time.Second)
	defer cancel()
	user := "cleanup-go-" + uuid.NewString()
	ids := make([]string, 125)
	for i := range ids {
		ids[i] = fmt.Sprintf("00000000-0000-4000-8000-%012x", i)
	}
	type owner struct {
		c           *Client
		path, token string
	}
	owners := make([]owner, 2)
	open := func(i int) {
		o := &owners[i]
		config := DefaultConfig(1, []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
		config.DatabaseID = "cleanup-audit"
		config.ServiceID = "business"
		config.LocalVersion = 1
		config.MaxSyncWorkUnitsPerInvocation = target
		config.UploadLimit = 40
		opening, e := NewOpening(&OpeningConfig{DatabasePath: o.path, BaseURL: base, Token: func(context.Context) (string, error) { return o.token, nil }, TargetClientConfig: config, Plan: &UpgradePlan{PlanID: "cleanup-fresh-v1", Mode: UpgradeModeFresh, DatabaseID: config.DatabaseID, ServiceID: config.ServiceID, TargetGenerationID: 1, TargetVersion: 1, Migrator: cleanupAuditMigrator{}}})
		require.NoError(t, e)
		ready, e := opening.Advance(ctx)
		require.NoError(t, e)
		o.c = ready.Ready
		require.NotNil(t, o.c)
		body, _ := json.Marshal(map[string]string{"user": user, "password": "anything", "device": o.c.sourceID})
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, base+"/dummy-signin", bytes.NewReader(body))
		require.NoError(t, e)
		req.Header.Set("Content-Type", "application/json")
		res, e := http.DefaultClient.Do(req)
		require.NoError(t, e)
		defer res.Body.Close()
		require.Equal(t, 200, res.StatusCode)
		var token struct{ Token string }
		require.NoError(t, json.NewDecoder(res.Body).Decode(&token))
		o.token = token.Token
		_, e = o.c.Attach(ctx, user)
		require.NoError(t, e)
	}
	for i := range owners {
		owners[i].path = filepath.Join(root, fmt.Sprintf("%d.sqlite", i))
		open(i)
	}
	defer func() {
		for i := range owners {
			if owners[i].c != nil {
				require.NoError(t, owners[i].c.Close())
			}
		}
	}()
	census := func(label string) {
		for i, o := range owners {
			db := o.c.Database()
			report := map[string]any{"label": label, "owner": i, "target": target, "tables": map[string]int64{}}
			rows, e := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*' ORDER BY name`)
			require.NoError(t, e)
			var names []string
			for rows.Next() {
				var n string
				require.NoError(t, rows.Scan(&n))
				names = append(names, n)
			}
			require.NoError(t, rows.Close())
			for _, n := range names {
				report["tables"].(map[string]int64)[n] = countA3(t, db, `SELECT count(*) FROM `+quoteIdent(n))
			}
			for _, p := range []string{"page_count", "freelist_count", "page_size"} {
				report[p] = countA3(t, db, "PRAGMA "+p)
			}
			report["business_rows"] = countA3(t, db, `SELECT count(*) FROM categories`)
			report["foreign_key_errors"] = countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`)
			report["reservations"] = countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`)
			require.Zero(t, report["foreign_key_errors"], "foreign keys at %s", label)
			require.Zero(t, report["reservations"], "outer release at %s", label)
			require.Zero(t, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
			if label != "cancelled" {
				for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges", "_sync_push_stage", "_sync_snapshot_stage"} {
					require.Zero(t, report["tables"].(map[string]int64)[table], "%s at %s", table, label)
				}
			}
			for _, suffix := range []string{"", "-wal", "-shm"} {
				size := int64(0)
				if stat, e := os.Stat(o.path + suffix); e == nil {
					size = stat.Size()
				} else {
					require.True(t, errors.Is(e, os.ErrNotExist), "%v", e)
				}
				report["file_bytes"+suffix] = size
			}
			raw, e := json.MarshalIndent(report, "", "  ")
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("%s-%d-census.json", label, i)), raw, 0600))
		}
	}
	save := func(label string) {
		census(label)
		for i := range owners {
			o := &owners[i]
			require.NoError(t, o.c.Close())
			o.c = nil
			for _, suffix := range []string{"", "-wal", "-shm"} {
				raw, e := os.ReadFile(o.path + suffix)
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				require.NoError(t, e)
				require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("%s-%d.sqlite%s", label, i, suffix)), raw, 0600))
			}
		}
	}
	resume := func() {
		for i := range owners {
			open(i)
		}
	}
	syncPair := func() {
		_, e := owners[0].c.Sync(ctx)
		require.NoError(t, e)
		_, e = owners[1].c.PullToStable(ctx)
		require.NoError(t, e)
	}
	for i, id := range ids {
		var parent any
		if i > 0 {
			parent = ids[0]
		}
		_, e := owners[0].c.db.ExecContext(ctx, `INSERT INTO categories VALUES(?,?,?)`, id, fmt.Sprintf("initial-%03d", i), parent)
		require.NoError(t, e)
	}
	syncPair()
	save("initial")
	resume()
	for cycle := 1; cycle <= 10; cycle++ {
		for i := 1; i <= 10; i++ {
			_, e := owners[0].c.db.ExecContext(ctx, `UPDATE categories SET name=? WHERE id=?`, fmt.Sprintf("cycle-%02d-update-%03d", cycle, i), ids[i])
			require.NoError(t, e)
		}
		for i := 115; i < 125; i++ {
			_, e := owners[0].c.db.ExecContext(ctx, `DELETE FROM categories WHERE id=?`, ids[i])
			require.NoError(t, e)
		}
		syncPair()
		for i := 115; i < 125; i++ {
			_, e := owners[0].c.db.ExecContext(ctx, `INSERT INTO categories VALUES(?,?,?)`, ids[i], fmt.Sprintf("cycle-%02d-reinsert-%03d", cycle, i), ids[0])
			require.NoError(t, e)
		}
		syncPair()
		if cycle == 5 {
			_, e := owners[1].c.Rebuild(ctx)
			require.NoError(t, e)
		}
		for _, o := range owners {
			require.EqualValues(t, 125, countA3(t, o.c.db, `SELECT count(*) FROM categories`))
			require.Zero(t, countA3(t, o.c.db, `SELECT count(*) FROM _sync_dirty_rows`))
			for i, id := range ids {
				expected := fmt.Sprintf("initial-%03d", i)
				if i >= 1 && i <= 10 {
					expected = fmt.Sprintf("cycle-%02d-update-%03d", cycle, i)
				}
				if i >= 115 {
					expected = fmt.Sprintf("cycle-%02d-reinsert-%03d", cycle, i)
				}
				var actual string
				require.NoError(t, o.c.db.QueryRowContext(ctx, `SELECT name FROM categories WHERE id=?`, id).Scan(&actual))
				require.Equal(t, expected, actual)
			}
		}
		save(fmt.Sprintf("cycle-%02d", cycle))
		resume()
	}
	_, err = owners[0].c.db.ExecContext(ctx, `UPDATE categories SET name='recovered' WHERE id<>?`, ids[0])
	require.NoError(t, err)
	active, cancelActive := context.WithCancel(ctx)
	defer cancelActive()
	interrupt := &cleanupAuditContext{Context: active, client: owners[0].c, cancel: cancelActive}
	owners[0].c.SetBeforePushReplayHook(func(context.Context) error {
		interrupt.armed = true
		interrupt.slices = owners[0].c.a3Stats.workSlices
		if target == 1000000 {
			cancelActive()
		}
		return nil
	})
	_, err = owners[0].c.PushPending(interrupt)
	require.ErrorIs(t, err, context.Canceled)
	var frozen string
	require.NoError(t, owners[0].c.db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&frozen))
	require.NotEmpty(t, frozen)
	save("cancelled")
	resume()
	var retained string
	require.NoError(t, owners[0].c.db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&retained))
	require.Equal(t, frozen, retained)
	syncPair()
	for _, o := range owners {
		require.EqualValues(t, 124, countA3(t, o.c.db, `SELECT count(*) FROM categories WHERE name='recovered'`))
	}
	save("recovered")
	manifest, _ := json.Marshal(map[string]any{"user": user, "target": target, "keys": ids, "cancelled_hash": frozen})
	require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.json"), manifest, 0600))
}
