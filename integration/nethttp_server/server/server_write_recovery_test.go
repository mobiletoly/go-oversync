package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversqlite"
	activityfixture "github.com/mobiletoly/go-oversync/oversqlite/testdata"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type recoveryHTTP func(*http.Request) (*http.Response, error)

func (f recoveryHTTP) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func recoveryLocalVector(t *testing.T, db *sql.DB) string {
	t.Helper()
	names, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	var tables []string
	for names.Next() {
		var name string
		require.NoError(t, names.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, names.Close())
	vector := map[string][]map[string]any{}
	for _, name := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(name, `"`, `""`) + `"`)
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(columns))
			refs := make([]any, len(columns))
			for i := range values {
				refs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(refs...))
			row := map[string]any{}
			for i, column := range columns {
				if raw, ok := values[i].([]byte); ok {
					row[column] = hex.EncodeToString(raw)
				} else {
					row[column] = values[i]
				}
			}
			vector[name] = append(vector[name], row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	raw, err := json.Marshal(vector)
	require.NoError(t, err)
	return string(raw)
}
func recoveryCount(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow(query).Scan(&n))
	return n
}
func insertRecoveryUser(t *testing.T, db *sql.DB, id, name, email string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO users(id,name,email,created_at,updated_at) VALUES(?,?,?,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')`, id, name, email)
	require.NoError(t, err)
}

func TestServerWriteRecovery_LostResponsePrunedHistoryAndLostAck(t *testing.T) {
	authority, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-c03", SchemaGenerations: scopeClientGeneration("users")})
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	scope := uuid.NewString()
	db, client := newUsersClientForTest(t, authority, scope)
	id := uuid.NewString()
	insertRecoveryUser(t, db, id, "frozen", "first@example.invalid")
	var pushes, resolves, acks, snapshots int
	losePush, loseAck := true, true
	client.HTTP = &http.Client{Transport: recoveryHTTP(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/push":
			pushes++
		case "/sync/push-outcomes/resolve":
			resolves++
		case "/sync/push-outcomes/ack":
			acks++
		case "/sync/snapshot-sessions":
			snapshots++
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if (r.URL.Path == "/sync/push" && losePush) || (r.URL.Path == "/sync/push-outcomes/ack" && loseAck) {
			require.True(t, response.StatusCode == 200 || response.StatusCode == 204)
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if r.URL.Path == "/sync/push" {
				losePush = false
			} else {
				loseAck = false
			}
			return nil, context.Canceled
		}
		return response, nil
	})}
	_, err = client.PushPending(context.Background())
	require.Error(t, err)
	var frozen []byte
	require.NoError(t, db.QueryRow(`SELECT request_body FROM _sync_upload_journal`).Scan(&frozen))
	require.EqualValues(t, 1, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	// Synthetic pruning removes the historical copy, leaving the C02 retained outcome owner intact.
	_, err = authority.Pool.Exec(context.Background(), `DELETE FROM sync.bundle_rows; DELETE FROM sync.bundle_log; UPDATE sync.user_state SET retained_bundle_floor=next_bundle_seq-1`)
	require.NoError(t, err)
	_, err = client.PushPending(context.Background())
	require.Error(t, err)
	require.Equal(t, "ack_pending", func() string {
		var s string
		require.NoError(t, db.QueryRow(`SELECT phase FROM _sync_upload_journal`).Scan(&s))
		return s
	}())
	var retained int
	require.NoError(t, authority.Pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.push_outcomes`).Scan(&retained))
	require.Zero(t, retained, "server ack committed before its lost response")
	local := recoveryLocalVector(t, db)
	var durable []byte
	require.NoError(t, db.QueryRow(`SELECT request_body FROM _sync_upload_journal`).Scan(&durable))
	require.Equal(t, frozen, durable)
	var databaseOrdinal int
	var databaseName, databasePath string
	require.NoError(t, db.QueryRow(`PRAGMA database_list`).Scan(&databaseOrdinal, &databaseName, &databasePath))
	require.NotEmpty(t, databasePath)
	transport := client.HTTP
	require.NoError(t, client.Close())
	token, err := authority.GenerateToken(scope, time.Hour)
	require.NoError(t, err)
	client = newReadyOnlyTestClient(t, databasePath, authority.URL(), func(context.Context) (string, error) { return token, nil },
		defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}), "")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	client.HTTP = transport
	db = client.Database()
	require.Equal(t, local, recoveryLocalVector(t, db), "complete ack_pending F/C, bases, captures, intent and source vector survives physical reopen")
	_, err = client.Attach(context.Background(), scope)
	require.NoError(t, err)
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, pushes)
	require.Equal(t, 1, resolves)
	require.Equal(t, 2, acks)
	require.Zero(t, snapshots)
	require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.EqualValues(t, 2, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Contains(t, local, "ack_pending")
	var count int
	require.NoError(t, authority.Pool.QueryRow(context.Background(), `SELECT count(*) FROM business.users WHERE _sync_scope_id=$1 AND id=$2`, scope, id).Scan(&count))
	require.Equal(t, 1, count)
	var beforeAck, afterAck map[string]any
	require.NoError(t, json.Unmarshal([]byte(local), &beforeAck))
	require.NoError(t, json.Unmarshal([]byte(recoveryLocalVector(t, db)), &afterAck))
	delete(beforeAck, "_sync_upload_journal")
	delete(afterAck, "_sync_upload_journal")
	require.Equal(t, beforeAck, afterAck, "idempotent ack retry changes only the upload evidence owner")
	t.Logf("RECOVERY_HTTP_ACK_REOPEN complete frozen_bytes=%d full_vector_unchanged=true pushes=%d resolves=%d acks=%d snapshots=%d", len(frozen), pushes, resolves, acks, snapshots)
}

func TestServerWriteRecovery_CancelBeforeDelayedPush(t *testing.T) {
	authority, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-c03", SchemaGenerations: scopeClientGeneration("users")})
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	scope := uuid.NewString()
	db, client := newUsersClientForTest(t, authority, scope)
	id := uuid.NewString()
	insertRecoveryUser(t, db, id, "preserved", "cancel@example.invalid")
	client.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err = client.PushPending(context.Background())
	require.Error(t, err)
	var body []byte
	var bindingJSON string
	require.NoError(t, db.QueryRow(`SELECT request_body,binding FROM _sync_upload_journal`).Scan(&body, &bindingJSON))
	var binding struct {
		Operation oversync.PushOperationBinding `json:"operation"`
	}
	require.NoError(t, json.Unmarshal([]byte(bindingJSON), &binding))
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, recoveryCount(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.EqualValues(t, 2, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	token, err := authority.GenerateToken(scope, time.Hour)
	require.NoError(t, err)
	request, err := http.NewRequest("POST", authority.URL()+"/sync/push", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Oversync-Protocol-Version", "v1")
	request.Header.Set(oversync.SourceIDHeader, binding.Operation.SourceID)
	request.Header.Set(oversync.SchemaGenerationIDHeader, binding.Operation.GenerationID)
	request.Header.Set(oversync.SchemaGenerationFingerprintHeader, binding.Operation.GenerationFingerprint)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 410, response.StatusCode, string(raw))
	var n int
	require.NoError(t, authority.Pool.QueryRow(context.Background(), `SELECT count(*) FROM business.users WHERE _sync_scope_id=$1`, scope).Scan(&n))
	require.Zero(t, n)
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
}

func TestServerWriteRecovery_RejectedConflictRetainsNewerIntent(t *testing.T) {
	authority, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-c03", SchemaGenerations: scopeClientGeneration("users")})
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	scope := uuid.NewString()
	db, client := newUsersClientForTest(t, authority, scope)
	id := uuid.New()
	insertRecoveryUser(t, db, id.String(), "local", "local@example.invalid")
	reviewSeed(t, authority, scope, id, "server", "server@example.invalid")
	_, err = client.PushPending(context.Background())
	var conflict *oversqlite.ConflictNeedsResolutionError
	require.ErrorAs(t, err, &conflict)
	require.EqualValues(t, 2, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.EqualValues(t, 1, recoveryCount(t, db, `SELECT count(*) FROM _sync_outbox_bundle WHERE state='rejected'`))
	_, err = db.Exec(`UPDATE users SET name='newer' WHERE id=?`, id.String())
	require.NoError(t, err)
	pending, err := client.PendingConflict(context.Background())
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.NoError(t, client.RetryConflict(context.Background(), *pending))
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, id.String()).Scan(&name))
	require.Equal(t, "newer", name)
	var outcomes int
	require.NoError(t, authority.Pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.push_outcomes`).Scan(&outcomes))
	require.Zero(t, outcomes)
}

// Reused for the DT07 upload obligation; this generic shape is not Activity schema qualification.
// Upload-specific half: a legal authoritative normalization conflicts with
// a later local unique value. F/C and every local owner remain intact; C04/C05
// snapshot/automatic-recovery halves are deliberately not implemented here.
func TestServerWriteRecovery_UnsafeCommittedUnique(t *testing.T) {
	t.Run("pending_unique", func(t *testing.T) {
		authority, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-c03", SchemaGenerations: scopeClientGeneration("users"), BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			_, err := pool.Exec(ctx, `CREATE UNIQUE INDEX c03_email ON business.users(_sync_scope_id,email); CREATE FUNCTION business.c03_normalize() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='normalize' THEN NEW.email='occupied@example.invalid'; END IF;RETURN NEW;END $$; CREATE TRIGGER c03_normalize BEFORE UPDATE ON business.users FOR EACH ROW EXECUTE FUNCTION business.c03_normalize()`)
			return err
		}})
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.CloseWithError()) }()
		scope := uuid.NewString()
		token, err := authority.GenerateToken(scope, time.Hour)
		require.NoError(t, err)
		client := newReadyOnlyTestClient(t, filepath.Join(t.TempDir(), "dt07.sqlite"), authority.URL(), func(context.Context) (string, error) { return token, nil }, defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}), `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE UNIQUE INDEX c03_email ON users(email)`)
		defer client.Close()
		_, err = client.Attach(context.Background(), scope)
		require.NoError(t, err)
		db := client.Database()
		id := uuid.NewString()
		insertRecoveryUser(t, db, id, "seed", "seed@example.invalid")
		_, err = client.PushPending(context.Background())
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE users SET name='normalize' WHERE id=?`, id)
		require.NoError(t, err)
		client.SetBeforePushReplayHook(func(context.Context) error {
			insertRecoveryUser(t, db, uuid.NewString(), "later", "occupied@example.invalid")
			return nil
		})
		var acks, snapshots int
		client.HTTP = &http.Client{Transport: recoveryHTTP(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/sync/push-outcomes/ack" {
				acks++
			}
			if r.URL.Path == "/sync/snapshot-sessions" {
				snapshots++
			}
			return http.DefaultTransport.RoundTrip(r)
		})}
		_, err = client.PushPending(context.Background())
		require.Error(t, err)
		require.EqualValues(t, 1, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)>0`))
		before := recoveryLocalVector(t, db)
		_, err = client.PushPending(context.Background())
		require.Error(t, err)
		require.Equal(t, before, recoveryLocalVector(t, db))
		_, err = client.Rebuild(context.Background())
		require.Error(t, err)
		require.Equal(t, before, recoveryLocalVector(t, db))
		require.Zero(t, acks)
		require.Zero(t, snapshots)
		require.EqualValues(t, 2, recoveryCount(t, db, `SELECT count(*) FROM users`))
		t.Log(fmt.Sprintf("DT07 upload preservation: F/C retained, local unique conflict blocked, ack=%d snapshot=%d; later snapshot qualification pending", acks, snapshots))
	})
}

func newActivityClientForTest(t *testing.T, authority *TestServer, scope string) *oversqlite.Client {
	t.Helper()
	generation, registered, err := activityfixture.Definition("business")
	require.NoError(t, err)
	require.Len(t, generation.Tables, 14)
	tables := make([]oversqlite.SyncTable, len(registered))
	for i, table := range registered {
		tables[i] = oversqlite.SyncTable{TableName: table.Table, SyncKeyColumns: table.SyncKeyColumns}
	}
	config := oversqlite.DefaultConfig(1, tables)
	config.DatabaseID = "activity-synthetic"
	config.ServiceID = "business"
	config.LocalVersion = 1
	config.LocalOnlyTables = []string{"app_settings"}
	token, err := authority.GenerateToken(scope, time.Hour)
	require.NoError(t, err)
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: filepath.Join(t.TempDir(), "activity.sqlite"), BaseURL: authority.URL(), Token: func(context.Context) (string, error) { return token, nil }, TargetClientConfig: config, Plan: &oversqlite.UpgradePlan{PlanID: "activity-synthetic-fresh", Mode: oversqlite.UpgradeModeFresh, DatabaseID: config.DatabaseID, ServiceID: config.ServiceID, TargetGenerationID: 1, TargetVersion: 1, LocalOnlyTables: []string{"app_settings"}, Migrator: scopeFreshMigrator{ddl: activityfixture.SQLiteSchema}}})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	client := result.Ready
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	_, err = client.Attach(context.Background(), scope)
	require.NoError(t, err)
	return client
}

func readSnapshotFixtureResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	response.Body = io.NopCloser(bytes.NewReader(raw))
	if response.Header.Get("Content-Encoding") != "gzip" {
		return raw
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	decoded, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	return decoded
}

func TestServerWriteRecovery_ActivitySchema(t *testing.T) {
	authority, err := NewActivitySnapshotTestServer()
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	for _, scenario := range []string{"DT01", "DT02", "DT03", "DT04", "DT05", "DT06"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			scope := uuid.NewString()
			client := newActivityClientForTest(t, authority, scope)
			db := client.Database()
			_, err := db.Exec(activityfixture.SeedSQLite)
			require.NoError(t, err)
			_, err = client.PushPending(ctx)
			require.NoError(t, err)
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			prior, err := client.LastBundleSeqSeen(ctx)
			require.NoError(t, err)
			var settings string
			require.NoError(t, db.QueryRow(`SELECT json_array(rowid,id,bootstrapped_date,most_recent_sync_date,bootstrap_version,wizard_completed) FROM app_settings`).Scan(&settings))
			statements, ok := activitySnapshotChange(scenario)
			require.True(t, ok)
			if scenario == "DT06" {
				before := recoveryLocalVector(t, db)
				_, invalid := oversync.NewScopeManager(authority.SyncService, oversync.ScopeManagerConfig{}).ExecWrite(ctx, scope, oversync.ScopeWriteOptions{WriterID: "activity-invalid-writer", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
					_, err := tx.Exec(ctx, `UPDATE business.program_item_input_goal_revision SET effective_day='2026-1-01' WHERE _sync_scope_id=$1 AND id='R'`, scope)
					return err
				})
				require.Error(t, invalid)
				require.Equal(t, before, recoveryLocalVector(t, db))
			}
			result := reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
				for _, statement := range strings.Split(statements, ";") {
					if strings.TrimSpace(statement) == "" {
						continue
					}
					if _, err := tx.Exec(ctx, statement, scope); err != nil {
						return err
					}
				}
				return nil
			})
			require.NotNil(t, result.Bundle)
			var metadata oversync.CompactSnapshotSessionResponse
			var creates, pushes, acks int
			client.HTTP = &http.Client{Transport: recoveryHTTP(func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/sync/push" {
					pushes++
				}
				if request.URL.Path == "/sync/push-outcomes/ack" {
					acks++
				}
				response, err := http.DefaultTransport.RoundTrip(request)
				if err != nil {
					return nil, err
				}
				if request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions" {
					creates++
					require.NoError(t, json.Unmarshal(readSnapshotFixtureResponse(t, response), &metadata))
				}
				return response, nil
			})}
			report, err := client.Rebuild(ctx)
			require.NoError(t, err)
			require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
			require.Equal(t, 1, creates)
			require.Zero(t, pushes)
			require.Zero(t, acks)
			require.Len(t, metadata.SnapshotHash, 64)
			require.NotNil(t, metadata.Recovery)
			require.GreaterOrEqual(t, metadata.Recovery.MinimumBundleSeq, prior)
			require.Equal(t, result.Bundle.BundleSeq, metadata.SnapshotBundleSeq)
			checkpoint, err := client.LastBundleSeqSeen(ctx)
			require.NoError(t, err)
			require.Equal(t, metadata.SnapshotBundleSeq, checkpoint)
			var actual string
			require.NoError(t, db.QueryRow(`SELECT json_array(rowid,id,bootstrapped_date,most_recent_sync_date,bootstrap_version,wizard_completed) FROM app_settings`).Scan(&actual))
			require.Equal(t, settings, actual)
			switch scenario {
			case "DT01":
				var real float64
				var essential int
				require.NoError(t, db.QueryRow(`SELECT numeric_value FROM value_registry WHERE key='body-weight'`).Scan(&real))
				require.Equal(t, 74.5, real)
				require.NoError(t, db.QueryRow(`SELECT essential_plan FROM profile WHERE name='profile-name'`).Scan(&essential))
				require.Zero(t, essential)
			case "DT02":
				require.EqualValues(t, 74, recoveryCount(t, db, `SELECT body_weight FROM profile WHERE name='profile-name'`))
			case "DT03":
				var value string
				require.NoError(t, db.QueryRow(`SELECT sched_write_ref_start_at FROM activity WHERE id='B'`).Scan(&value))
				require.Equal(t, "date-a", value)
				require.EqualValues(t, 3, recoveryCount(t, db, `SELECT count(*) FROM activity WHERE sched_write_ref_start_at IS NULL`))
				require.EqualValues(t, 3, recoveryCount(t, db, `SELECT count(*) FROM program_item_input_goal_revision`))
			case "DT04":
				require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM activity WHERE id='B'`))
				require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM program_item WHERE id='Q'`))
				require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM program_item_state WHERE program_item_id='Q'`))
				require.EqualValues(t, 3, recoveryCount(t, db, `SELECT count(*) FROM program_item_input_goal_revision`))
			case "DT05":
				var parent string
				require.NoError(t, db.QueryRow(`SELECT parent_daily_log_id FROM daily_log WHERE id='L1'`).Scan(&parent))
				require.Equal(t, "L2", parent)
			case "DT06":
				var date string
				require.NoError(t, db.QueryRow(`SELECT effective_day FROM program_item_input_goal_revision WHERE id='R'`).Scan(&date))
				require.Equal(t, "2026-12-31", date)
			}
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
			t.Logf("Activity %s real HTTP14tables T=%d hash=%s checkpoint=%d creates=%d pushes=%d acks=%d", scenario, metadata.SnapshotBundleSeq, metadata.SnapshotHash, checkpoint, creates, pushes, acks)
		})
	}
}

func TestServerWriteRecovery_AutomaticActivity(t *testing.T) {
	authority, err := NewActivitySnapshotTestServer()
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()

	for _, pending := range []string{"clean", "dirty", "prepared", "unsafe_committed", "ack_pending"} {
		t.Run(pending, func(t *testing.T) {
			ctx := context.Background()
			scope := uuid.NewString()
			client := newActivityClientForTest(t, authority, scope)
			db := client.Database()
			_, err := db.Exec(activityfixture.SeedSQLite)
			require.NoError(t, err)
			_, err = client.PushPending(ctx)
			require.NoError(t, err)
			source := recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`)
			var creates, pushes, acks int
			retain := false
			client.HTTP = &http.Client{Transport: recoveryHTTP(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/sync/push" {
					pushes++
				}
				if r.URL.Path == "/sync/push-outcomes/ack" {
					acks++
					if pending == "ack_pending" {
						return nil, context.Canceled
					}
				}
				if retain && r.URL.Path == "/sync/push-outcomes/resolve" {
					return nil, context.Canceled
				}
				response, err := http.DefaultTransport.RoundTrip(r)
				if err == nil && r.Method == "POST" && r.URL.Path == "/sync/snapshot-sessions" {
					creates++
					reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
						_, err := tx.Exec(ctx, `UPDATE business.value_registry SET numeric_value=74.5 WHERE _sync_scope_id=$1 AND key='body-weight'`, scope)
						return err
					})
				}
				return response, err
			})}
			switch pending {
			case "dirty":
				_, err = db.Exec(`UPDATE daily_log SET notes='offline' WHERE id='L1'`)
				require.NoError(t, err)
				client.PauseUploads()
			case "prepared":
				_, err = db.Exec(`UPDATE daily_log SET notes='offline' WHERE id='L1'`)
				require.NoError(t, err)
				client.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
				_, err = client.PushPending(ctx)
				require.Error(t, err)
				client.SetBeforePushCommitHook(nil)
				retain = true
			case "unsafe_committed":
				_, err = db.Exec(`CREATE UNIQUE INDEX local_notes ON daily_log(notes);UPDATE daily_log SET notes='normalize' WHERE id='L1'`)
				require.NoError(t, err)
				client.SetBeforePushReplayHook(func(context.Context) error {
					_, err := db.Exec(`UPDATE daily_log SET notes='occupied' WHERE id='L2'`)
					return err
				})
				_, err = client.PushPending(ctx)
				require.Error(t, err)
				client.SetBeforePushReplayHook(nil)
				require.EqualValues(t, 1, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal WHERE length(outcome_body)>0`))
			case "ack_pending":
				_, err = db.Exec(`UPDATE daily_log SET notes='ack pending' WHERE id='L1'`)
				require.NoError(t, err)
				_, err = client.PushPending(ctx)
				require.Error(t, err)
				require.EqualValues(t, 1, recoveryCount(t, db, `SELECT count(*) FROM _sync_upload_journal WHERE phase='ack_pending'`))
			}
			if pending == "clean" {
				result := reviewWrite(t, authority, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
					_, err := tx.Exec(ctx, `UPDATE business.profile SET body_weight=74 WHERE _sync_scope_id=$1 AND name='profile-name'`, scope)
					return err
				})
				floor, err := pruneActivitySnapshotHistory(ctx, authority, scope)
				require.NoError(t, err)
				require.Equal(t, result.Bundle.BundleSeq, floor)
				report, err := client.Sync(ctx)
				require.NoError(t, err)
				require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
				status, err := client.RecoveryStatus(ctx)
				require.NoError(t, err)
				require.Equal(t, "completed", status.Phase)
				require.Equal(t, floor, status.ObservedBoundary)
				checkpoint, err := client.LastBundleSeqSeen(ctx)
				require.NoError(t, err)
				require.Equal(t, floor+1, checkpoint)
				require.Equal(t, 1, creates)
				require.Zero(t, pushes)
				require.Zero(t, acks)
				require.Equal(t, source, recoveryCount(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
				require.EqualValues(t, 74, recoveryCount(t, db, `SELECT body_weight FROM profile WHERE name='profile-name'`))
				var value float64
				require.NoError(t, db.QueryRow(`SELECT numeric_value FROM value_registry WHERE key='body-weight'`).Scan(&value))
				require.Equal(t, 74.5, value)
			} else {
				before := recoveryLocalVector(t, db)
				require.NoError(t, client.RetryRecovery(ctx))
				if pending == "dirty" {
					_, err = client.PullToStable(ctx)
				} else {
					_, err = client.Sync(ctx)
				}
				require.Error(t, err)
				require.Equal(t, before, recoveryLocalVector(t, db))
				require.Zero(t, creates)
			}
			require.Zero(t, recoveryCount(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
			t.Logf("automatic Activity %s 14 tables: creates=%d pushes=%d ack attempts=%d", pending, creates, pushes, acks)
		})
	}
}
