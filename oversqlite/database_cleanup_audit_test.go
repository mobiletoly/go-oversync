package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	phase := func(name string) {
		raw, e := json.Marshal(map[string]string{"phase": name})
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(root, "audit-phase.json"), raw, 0600))
	}
	phase("initial")
	ctx, cancel := context.WithTimeout(context.Background(), 570*time.Second)
	defer cancel()
	user := "cleanup-go-" + uuid.NewString()
	ids := make([]string, 125)
	for i := range ids {
		ids[i] = fmt.Sprintf("00000000-0000-4000-8000-%012x", i)
	}
	type owner struct {
		c           *Client
		db          *sql.DB
		path, token string
	}
	owners := make([]owner, 2)
	loseAck := false
	var loseAckCancel context.CancelFunc
	ackLosses := 0
	open := func(i int) {
		o := &owners[i]
		config := DefaultConfig(1, []SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
		config.DatabaseID = "cleanup-audit"
		config.ServiceID = "business"
		config.LocalVersion = 1
		config.MaxSyncWorkUnitsPerInvocation = target
		config.UploadLimit = 40
		// The fixture supplies FULL in the DSN so every physical pooled connection,
		// including a replacement after cancellation, has the required durability.
		dsn := url.URL{Scheme: "file", Path: o.path}
		query := url.Values{"_foreign_keys": {"on"}, "_journal_mode": {"WAL"}, "_synchronous": {"FULL"}, "_busy_timeout": {"5000"}, "_txlock": {"immediate"}}
		dsn.RawQuery = query.Encode()
		var e error
		o.db, e = sql.Open("sqlite3", dsn.String())
		require.NoError(t, e)
		o.db.SetMaxOpenConns(4)
		opening, e := newOpeningFromDB(o.db, &openingRuntimeConfig{BaseURL: base, Token: func(context.Context) (string, error) { return o.token, nil }, TargetClientConfig: config, PlanProvided: true, Plan: UpgradePlan{PlanID: "cleanup-fresh-v1", Mode: UpgradeModeFresh, DatabaseID: config.DatabaseID, ServiceID: config.ServiceID, TargetGenerationID: 1, TargetVersion: 1, Migrator: cleanupAuditMigrator{}}})
		require.NoError(t, e)
		ready, e := opening.Advance(ctx)
		require.NoError(t, e)
		o.c = ready.Ready
		require.NotNil(t, o.c)
		validated, e := validateSyncTables(o.db, o.c.tableInfo, config.Tables)
		require.NoError(t, e)
		require.NoError(t, installClientRuntime(ctx, o.db, o.c, validated))
		require.NoError(t, o.c.open(ctx))
		o.c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			response, e := http.DefaultTransport.RoundTrip(req)
			if e == nil && req.URL.Path == "/sync/push-outcomes/ack" && loseAck {
				require.Equal(t, http.StatusNoContent, response.StatusCode)
				_, e = io.Copy(io.Discard, response.Body)
				require.NoError(t, e)
				require.NoError(t, response.Body.Close())
				loseAck = false
				ackLosses++
				loseAckCancel()
				return nil, context.Canceled
			}
			return response, e
		})}
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
	defer func() {
		for i := range owners {
			if owners[i].c != nil {
				require.NoError(t, owners[i].c.Close())
			}
			if owners[i].db != nil {
				require.NoError(t, owners[i].db.Close())
			}
		}
	}()
	for i := range owners {
		owners[i].path = filepath.Join(root, fmt.Sprintf("%d.sqlite", i))
		open(i)
	}
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
			report["synchronous"] = countA3(t, db, "PRAGMA synchronous")
			report["foreign_keys"] = countA3(t, db, "PRAGMA foreign_keys")
			require.EqualValues(t, 2, report["synchronous"], "normal SQLite FULL durability")
			require.EqualValues(t, 1, report["foreign_keys"])
			for _, p := range []string{"page_count", "freelist_count", "page_size"} {
				report["temp_"+p] = countA3(t, db, "PRAGMA temp."+p)
			}
			report["business_rows"] = countA3(t, db, `SELECT count(*) FROM categories`)
			report["foreign_key_errors"] = countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`)
			report["reservations"] = countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`)
			require.Zero(t, report["foreign_key_errors"], "foreign keys at %s", label)
			require.Zero(t, report["reservations"], "outer release at %s", label)
			require.Zero(t, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
			if label != "cancelled" && label != "ack-pending" {
				for _, table := range []string{"_sync_upload_journal", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges", "_sync_push_stage", "_sync_snapshot_stage"} {
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
			require.NoError(t, o.db.Close())
			o.db = nil
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
	phase("steady")
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
			phase("explicit-rebuild")
			_, e := owners[1].c.Rebuild(ctx)
			phase("steady")
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
	vector := func() qualificationVector {
		v := qualificationState(t, owners[0].c.db, "categories")
		for _, table := range []string{"_sync_upload_journal", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges", "_sync_snapshot_stage"} {
			v[table] = qualificationState(t, owners[0].c.db, table)[table]
		}
		return v
	}
	cancelledVector := vector()
	save("cancelled")
	resume()
	var retained string
	require.NoError(t, owners[0].c.db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&retained))
	require.Equal(t, frozen, retained)
	require.Equal(t, cancelledVector, vector(), "full F/C, typed bases/captures and pending intent across physical reopen")
	loseAck = true
	ackAttempt, cancelAck := context.WithCancel(ctx)
	defer cancelAck()
	loseAckCancel = cancelAck
	_, err = owners[0].c.PushPending(ackAttempt)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, ackLosses, "actual authority committed the deliberately lost ack")
	var journalPhase string
	require.NoError(t, owners[0].c.db.QueryRow(`SELECT phase FROM _sync_upload_journal`).Scan(&journalPhase))
	require.Equal(t, "ack_pending", journalPhase)
	require.Zero(t, countA3(t, owners[0].c.db, `SELECT count(*) FROM _sync_outbox_rows`))
	ackVector := vector()
	save("ack-pending")
	resume()
	require.Equal(t, ackVector, vector(), "full ack_pending certificate/completion/later intent survives physical reopen")
	syncPair()
	for _, o := range owners {
		require.EqualValues(t, 124, countA3(t, o.c.db, `SELECT count(*) FROM categories WHERE name='recovered'`))
	}
	save("recovered")
	manifest, _ := json.Marshal(map[string]any{"user": user, "target": target, "keys": ids, "cancelled_hash": frozen, "ack_losses": ackLosses, "ack_reopened": true, "cycles": 10, "keys_count": 125})
	require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.json"), manifest, 0600))
	t.Logf("RECOVERY_RETAINED_AUDIT_COMPLETE go target=%d cycles=10 keys=125 cancelled_reopen=true ack_reopen=true", target)
}
