package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func snapshotProcessVector(t *testing.T, db *sql.DB) string {
	t.Helper()
	return journalProcessVector(t, db)
}

// The HTTP authority and SQLite file outlive each killed owner. A fresh PID
// compares the entire durable vector before opening a new SDK lifetime.
func TestFinalStateRecovery_ProcessDeath(t *testing.T) {
	if path := os.Getenv("C04_SNAPSHOT_DB"); path != "" {
		snapshotProcessChild(t, path)
		return
	}
	for _, boundary := range []string{"create_receipt", "paging", "full_hash", "verified_expired", "preparation", "ready", "during_dml", "post_commit", "auto_captured", "auto_stage_baseline", "auto_prepare_transition", "auto_ready", "auto_completed", "auto_blocked", "auto_retry_armed"} {
		t.Run(boundary, func(t *testing.T) {
			root := t.TempDir()
			path, marker, expected := filepath.Join(root, "snapshot.sqlite"), filepath.Join(root, "ready"), filepath.Join(root, "expected")
			db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			_, err = db.Exec(usersTestDDL)
			require.NoError(t, err)
			config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
			c, err := newOwnedAdmittedFixtureClient(db, "http://seed.invalid", tokenProviderForTests, config)
			require.NoError(t, err)
			attachTestClient(t, c, "bundle-user", "bundle-device")
			seedSyncedUserRow(t, c, db, "A", "same", 1)
			seedSyncedUserRow(t, c, db, "B", "same", 1)
			_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX unique_email ON users(email)`)
			require.NoError(t, err)
			rows := recoveryUsers(t, c, []string{"remote", "same"}, []string{"b", "a"})
			var mu sync.Mutex
			var session *oversync.SnapshotSession
			creates, pages := 0, 0
			expired := false
			authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				var result *http.Response
				if r.URL.Path == "/sync/pull" {
					if r.URL.Query().Get("after_bundle_seq") == "1" {
						result = errorJSONResponse(409, oversync.ErrorResponse{Error: "history_pruned"})
					} else {
						result = jsonResponse(oversync.PullResponse{StableBundleSeq: 2})
					}
					defer result.Body.Close()
					w.WriteHeader(result.StatusCode)
					_, _ = io.Copy(w, result.Body)
					return
				}
				switch r.Method {
				case http.MethodPost:
					require.Equal(t, "/sync/snapshot-sessions", r.URL.Path)
					var req oversync.SnapshotSessionCreateRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
					creates++
					expired = false
					session = sealedTestSnapshot(t, c, fmt.Sprintf("physical-%d", creates), 2, rows, req.Recovery)
					result = jsonResponse(session)
				case http.MethodGet:
					pages++
					if expired {
						result = errorJSONResponse(http.StatusGone, oversync.ErrorResponse{Error: "snapshot_session_expired"})
						break
					}
					after, err := strconv.ParseInt(r.URL.Query().Get("after_row_ordinal"), 10, 64)
					require.NoError(t, err)
					end := min(int64(len(rows)), after+1)
					groups, bytes, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, rows[after:end], after+1)
					require.NoError(t, err)
					result = jsonResponse(oversync.CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: end, HasMore: end < int64(len(rows)), LogicalByteCount: bytes})
				case http.MethodDelete:
					result = &http.Response{StatusCode: 204, Header: http.Header{}, Body: http.NoBody}
				default:
					t.Errorf("unexpected recovery request: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", 500)
					return
				}
				defer result.Body.Close()
				for key, values := range result.Header {
					w.Header()[key] = values
				}
				w.WriteHeader(result.StatusCode)
				_, _ = io.Copy(w, result.Body)
			}))
			defer authority.Close()
			require.NoError(t, c.Close())
			require.NoError(t, db.Close())
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			makeChild := func(cut string) *exec.Cmd {
				command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinalStateRecovery_ProcessDeath$", "-test.timeout=45s")
				command.Env = append(os.Environ(), "C04_SNAPSHOT_DB="+path, "C04_SNAPSHOT_URL="+authority.URL, "C04_SNAPSHOT_CUT="+cut, "C04_SNAPSHOT_MARKER="+marker, "C04_SNAPSHOT_EXPECTED="+expected)
				return command
			}
			child := makeChild(boundary)
			log, err := os.Create(filepath.Join(root, "child.log"))
			require.NoError(t, err)
			defer log.Close()
			child.Stdout, child.Stderr = log, log
			require.NoError(t, child.Start())
			waited := false
			defer func() {
				if !waited {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()
			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			output, err := os.ReadFile(log.Name())
			require.NoError(t, err)
			_, err = os.Stat(marker)
			require.NoError(t, err, string(output))
			observer, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL")
			require.NoError(t, err)
			observer.SetMaxOpenConns(1)
			before := snapshotProcessVector(t, observer)
			require.NoError(t, child.Process.Kill())
			err = child.Wait()
			waited = true
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited)
			status := exited.Sys().(syscall.WaitStatus)
			require.True(t, status.Signaled())
			require.Equal(t, syscall.SIGKILL, status.Signal())
			require.NoError(t, ctx.Err())
			require.Equal(t, before, snapshotProcessVector(t, observer), "uncommitted delta and auxiliary work must roll back")
			require.NoError(t, os.WriteFile(expected, []byte(before), 0600))
			require.NoError(t, observer.Close())
			mu.Lock()
			oldCreates, oldPages := creates, pages
			if boundary == "verified_expired" {
				expired = true
			}
			mu.Unlock()
			resume := makeChild("resume")
			output, err = resume.CombinedOutput()
			require.NoError(t, err, string(output))
			mu.Lock()
			if boundary == "verified_expired" {
				require.Equal(t, oldCreates, creates)
				require.Equal(t, oldPages, pages)
			}
			mu.Unlock()
			t.Logf("physical SIGKILL pid=%d boundary=%s; fresh owner pid=%d verified durable vector, checkpoint=2 and exact full rows", child.Process.Pid, boundary, resume.Process.Pid)
		})
	}
}

func snapshotProcessChild(t *testing.T, path string) {
	cut := os.Getenv("C04_SNAPSHOT_CUT")
	gate := func() {
		require.NoError(t, os.WriteFile(os.Getenv("C04_SNAPSHOT_MARKER"), []byte(cut), 0600))
		select {}
	}
	sql.Register("snapshot-physical-death", &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		return conn.RegisterFunc("snapshot_crash_gate", func() int { gate(); return 0 }, false)
	}})
	db, err := sql.Open("snapshot-physical-death", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	if cut == "resume" {
		expected, err := os.ReadFile(os.Getenv("C04_SNAPSHOT_EXPECTED"))
		require.NoError(t, err)
		require.Equal(t, string(expected), snapshotProcessVector(t, db))
	}
	config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	config.MaxSyncWorkUnitsPerInvocation = 1
	config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c, err := newOwnedAdmittedFixtureClient(db, os.Getenv("C04_SNAPSHOT_URL"), tokenProviderForTests, config)
	require.NoError(t, err)
	attachTestClient(t, c, "bundle-user", "bundle-device")
	c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if cut == "auto_stage_baseline" && r.Method == http.MethodPost {
			gate()
		}
		if cut == "auto_completed" && r.Method == http.MethodDelete {
			gate()
		}
		if cut == "paging" && r.Method == http.MethodGet && r.URL.Query().Get("after_row_ordinal") == "1" {
			gate()
		}
		if cut == "post_commit" && r.Method == http.MethodDelete {
			gate()
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && cut == "create_receipt" && r.Method == http.MethodPost {
			gate()
		}
		return response, err
	})}
	if strings.HasPrefix(cut, "auto_") {
		phase := map[string]string{"auto_captured": "stage_baseline", "auto_prepare_transition": "ready", "auto_ready": "completed"}[cut]
		if phase != "" {
			_, err = db.Exec(`CREATE TEMP TRIGGER automatic_crash AFTER UPDATE OF recovery_state ON _sync_operation_state WHEN json_extract(new.recovery_state,'$.phase')='` + phase + `' BEGIN SELECT snapshot_crash_gate();END`)
		}
		if cut == "auto_blocked" || cut == "auto_retry_armed" {
			_, err = db.Exec(`CREATE TEMP TRIGGER automatic_block BEFORE UPDATE ON users BEGIN SELECT 1;END`)
		}
	}
	switch cut {
	case "full_hash":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_hash AFTER UPDATE OF snapshot_transfer ON _sync_operation_state WHEN CASE WHEN json_valid(new.snapshot_transfer) THEN json_extract(new.snapshot_transfer,'$.complete') ELSE 0 END=1 BEGIN SELECT snapshot_crash_gate();END`)
	case "preparation":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_prepare AFTER INSERT ON _sync_apply_rows WHEN new.ordinal=2 BEGIN SELECT snapshot_crash_gate();END`)
	case "ready":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_ready AFTER UPDATE OF phase ON _sync_apply_plan WHEN new.phase='ready' BEGIN SELECT snapshot_crash_gate();END`)
	case "verified_expired":
		c.beforeSnapshotApplyHook = func(context.Context) error { gate(); return nil }
	case "during_dml":
		c.afterSnapshotRowsHook = gate
	}
	require.NoError(t, err)
	if strings.HasPrefix(cut, "auto_") {
		_, err = c.Sync(context.Background())
		if cut == "auto_blocked" || cut == "auto_retry_armed" {
			require.Error(t, err)
			if cut == "auto_retry_armed" {
				require.NoError(t, c.RetryRecovery(context.Background()))
			}
			gate()
		}
	} else if cut == "resume" {
		status, statusErr := c.RecoveryStatus(context.Background())
		require.NoError(t, statusErr)
		if status.Phase != "idle" {
			_, err = c.Sync(context.Background())
		} else {
			_, err = c.Rebuild(context.Background())
		}
	} else {
		_, err = c.Rebuild(context.Background())
	}
	require.NoError(t, err)
	require.Equal(t, "resume", cut, "child must reach its physical barrier")
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	var result string
	require.NoError(t, db.QueryRow(`SELECT group_concat(id||':'||name||':'||email,'|') FROM (SELECT * FROM users ORDER BY id)`).Scan(&result))
	require.Equal(t, "A:remote:b|B:same:a", result)
	for _, table := range []string{"_sync_dirty_rows", "_sync_upload_journal", "_sync_snapshot_stage", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan"} {
		require.Zero(t, countA3(t, db, "SELECT count(*) FROM "+table))
	}
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}
