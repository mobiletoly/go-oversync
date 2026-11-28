package oversqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Each boundary kills the actual owner PID while the file and HTTP authority
// remain alive. A second child verifies the complete raw vector before opening
// a new client lifetime, then finishes through the ordinary upload path.
func TestRecoveryJournal_ProcessDeath(t *testing.T) {
	if path := os.Getenv("C03_JOURNAL_DB"); path != "" {
		recoveryJournalChild(t, path)
		return
	}
	for _, boundary := range []string{"frozen_before_dispatch", "committed_before_response", "during_C_publication", "partial_stage", "after_C", "during_settlement", "before_ack", "after_ack"} {
		t.Run(boundary, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "journal.sqlite")
			marker := filepath.Join(root, "ready")
			expected := filepath.Join(root, "expected.json")
			s := newMockPushServer(t)
			var mu sync.Mutex
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				response, err := s.RoundTrip(r)
				if err != nil {
					t.Errorf("HTTP fixture: %v", err)
					http.Error(w, "fixture error", 500)
					return
				}
				defer response.Body.Close()
				for k, v := range response.Header {
					w.Header()[k] = v
				}
				w.WriteHeader(response.StatusCode)
				_, _ = io.Copy(w, response.Body)
			}))
			defer httpServer.Close()
			db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			_, err = db.Exec(usersTestDDL)
			require.NoError(t, err)
			cfg := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
			c, err := newOwnedAdmittedFixtureClient(db, httpServer.URL, tokenProviderForTests, cfg)
			require.NoError(t, err)
			attachTestClient(t, c, "bundle-user", "bundle-device")
			_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES('item','frozen','process@example.invalid')`)
			require.NoError(t, err)
			require.NoError(t, c.Close())
			require.NoError(t, db.Close())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryJournal_ProcessDeath$", "-test.timeout=25s")
			child.Env = append(os.Environ(), "C03_JOURNAL_DB="+path, "C03_JOURNAL_URL="+httpServer.URL, "C03_JOURNAL_BOUNDARY="+boundary, "C03_JOURNAL_MARKER="+marker)
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
			deadline := time.Now().Add(10 * time.Second)
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
			defer observer.Close()
			observer.SetMaxOpenConns(1)
			before := journalProcessVector(t, observer)
			require.NoError(t, child.Process.Kill())
			err = child.Wait()
			waited = true
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			status := exit.Sys().(syscall.WaitStatus)
			require.True(t, status.Signaled())
			require.Equal(t, syscall.SIGKILL, status.Signal())
			require.NoError(t, ctx.Err())
			require.Equal(t, before, journalProcessVector(t, observer), "uncommitted work must roll back after physical death")
			require.NoError(t, os.WriteFile(expected, []byte(before), 0600))
			require.NoError(t, observer.Close())
			resume := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryJournal_ProcessDeath$", "-test.timeout=15s")
			resume.Env = append(os.Environ(), "C03_JOURNAL_DB="+path, "C03_JOURNAL_URL="+httpServer.URL, "C03_JOURNAL_BOUNDARY=resume", "C03_JOURNAL_EXPECTED="+expected)
			output, err = resume.CombinedOutput()
			require.NoError(t, err, string(output))
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, s.pushRequests, 1)
			if boundary == "frozen_before_dispatch" {
				require.EqualValues(t, 2, s.pushRequests[0].SourceBundleID)
			} else {
				require.EqualValues(t, 1, s.pushRequests[0].SourceBundleID)
			}
			if boundary == "frozen_before_dispatch" || boundary == "committed_before_response" || boundary == "during_C_publication" {
				require.Len(t, s.resolveRequests, 1)
			} else {
				require.Empty(t, s.resolveRequests)
			}
			expectedAcks := 1
			if boundary == "frozen_before_dispatch" || boundary == "after_ack" {
				expectedAcks = 2
			}
			require.Len(t, s.ackRequests, expectedAcks)
			t.Logf("physical SIGKILL pid=%d boundary=%s; fresh child pid=%d verified full vector; push=%d resolve=%d ack=%d", child.Process.Pid, boundary, resume.Process.Pid, len(s.pushRequests), len(s.resolveRequests), len(s.ackRequests))
		})
	}
}

func journalProcessVector(t *testing.T, db *sql.DB) string {
	return recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_attachment_state", "_sync_upload_journal", "_sync_relationships", "_sync_conflict_state", "_sync_push_rejection_state", "_sync_operation_state", "_sync_push_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
}

func recoveryJournalChild(t *testing.T, path string) {
	boundary := os.Getenv("C03_JOURNAL_BOUNDARY")
	gate := func() {
		require.NoError(t, os.WriteFile(os.Getenv("C03_JOURNAL_MARKER"), []byte(boundary), 0600))
		select {}
	}
	sql.Register("c03-journal-death", &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		return conn.RegisterFunc("c03_crash_gate", func() int { gate(); return 0 }, false)
	}})
	db, err := sql.Open("c03-journal-death", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	if boundary == "resume" {
		expected, err := os.ReadFile(os.Getenv("C03_JOURNAL_EXPECTED"))
		require.NoError(t, err)
		require.Equal(t, string(expected), journalProcessVector(t, db))
	}
	cfg := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	cfg.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c, err := newOwnedAdmittedFixtureClient(db, os.Getenv("C03_JOURNAL_URL"), tokenProviderForTests, cfg)
	require.NoError(t, err)
	attachTestClient(t, c, "bundle-user", "bundle-device")
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		ack := strings.HasSuffix(r.URL.Path, "/ack")
		if boundary == "before_ack" && ack {
			gate()
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && ((boundary == "after_ack" && ack) || (boundary == "committed_before_response" && r.URL.Path == "/sync/push")) {
			gate()
		}
		return response, err
	})}
	switch boundary {
	case "frozen_before_dispatch":
		c.SetBeforePushCommitHook(func(context.Context) error { gate(); return nil })
	case "after_C":
		c.SetBeforePushReplayHook(func(context.Context) error { gate(); return nil })
	case "during_C_publication":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_C AFTER UPDATE OF outcome_body ON _sync_upload_journal BEGIN SELECT c03_crash_gate();END`)
	case "partial_stage":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_stage AFTER INSERT ON _sync_push_stage BEGIN SELECT c03_crash_gate();END`)
	case "during_settlement":
		_, err = db.Exec(`CREATE TEMP TRIGGER crash_apply AFTER UPDATE ON users BEGIN SELECT c03_crash_gate();END`)
	}
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	if boundary != "resume" {
		t.Fatal("crash boundary not reached")
	}
	if countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`) > 0 {
		_, err = c.PushPending(context.Background())
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='item' AND name='frozen'`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

func TestRecoveryJournal_IntegrityAndBudgets(t *testing.T) {
	for _, status := range []string{"cancelled", "rejected"} {
		t.Run("initialization_ack_after_retirement_"+status, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			initialization := "44444444-4444-4444-8444-444444444444"
			_, err := db.Exec(`UPDATE _sync_attachment_state SET pending_initialization_id=?`, initialization)
			require.NoError(t, err)
			c.pendingInitializationID = initialization
			if status == "cancelled" {
				c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
				_, err = c.PushPending(context.Background())
				require.Error(t, err)
			} else {
				s.pushHook = func(p *mockDirectPush) *http.Response {
					return s.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, InitializationID: &initialization, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "application_rejected"}}, p.Generation)
				}
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/ack") {
					return nil, context.Canceled
				}
				return s.RoundTrip(r)
			})}
			_, err = c.PushPending(context.Background())
			require.Error(t, err)
			_, certificate, phase := frozenJournalBodies(t, db)
			require.NotEmpty(t, certificate)
			require.Equal(t, "ack_pending", phase)
			original := c.schemaGenerationProof
			c.schemaGenerationProof = oversync.SchemaGenerationProof{ID: 2, Fingerprint: strings.Repeat("f", 64)}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "/sync/push-outcomes/ack", r.URL.Path)
				require.Equal(t, fmt.Sprint(original.ID), r.Header.Get(oversync.SchemaGenerationIDHeader))
				require.Equal(t, original.Fingerprint, r.Header.Get(oversync.SchemaGenerationFingerprintHeader))
				return s.RoundTrip(r)
			})}
			_, err = c.PushPending(context.Background())
			var reconnect *AttachRequiredError
			require.ErrorAs(t, err, &reconnect)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.EqualValues(t, 1, countA3(t, db, `SELECT initialization_reconnect_required FROM _sync_attachment_state`))
			require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
			require.Equal(t, initialization, requirePendingInitializationID(t, db))
			require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE name='frozen'`))
			require.Len(t, s.ackRequests, 1)
		})
	}
	t.Run("resolve_capacity_dispatches", func(t *testing.T) {
		c, db, _ := recoveryJournalClient(t)
		c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
		_, err := c.PushPending(context.Background())
		require.Error(t, err)
		before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_upload_journal", "_sync_source_state", "_sync_attachment_state")
		requests := 0
		c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "/sync/push-outcomes/resolve", r.URL.Path)
			requests++
			return errorJSONResponse(503, oversync.ErrorResponse{Error: "history_projection_capacity_exceeded"}), nil
		})}
		c.config.PushCapacityRetryPolicy = &PushCapacityRetryPolicy{Enabled: true, MaxWait: time.Minute, FallbackDelay: time.Millisecond}
		_, err = c.PushPending(context.Background())
		var incomplete *SyncIncompleteError
		require.ErrorAs(t, err, &incomplete)
		require.Equal(t, SyncIncompleteRecoveryExchangeLimit, incomplete.Reason)
		require.Equal(t, 64, requests)
		require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_upload_journal", "_sync_source_state", "_sync_attachment_state"))
	})
	for _, defect := range []string{"key", "op", "base", "payload"} {
		t.Run("conflict_"+defect, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			s.pushHook = func(p *mockDirectPush) *http.Response {
				conflict := &oversync.PushConflictDetails{Schema: "main", Table: "users", Key: p.Rows[0].Key, Op: "i", BaseRowVersion: 0, ServerRowVersion: 1, ServerRow: json.RawMessage(`{"id":"item","name":"remote","email":"remote@example.invalid"}`)}
				switch defect {
				case "key":
					conflict.Key = oversync.SyncKey{"id": "other"}
				case "op":
					conflict.Op = "u"
				case "base":
					conflict.BaseRowVersion = 1
				case "payload":
					conflict.ServerRow = json.RawMessage(`{"id":"item","name":"remote"}`)
				}
				return s.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "push_conflict", Conflict: conflict}}, p.Generation)
			}
			_, err := c.PushPending(context.Background())
			require.Error(t, err)
			_, certificate, phase := frozenJournalBodies(t, db)
			require.Empty(t, certificate)
			require.Equal(t, "frozen", phase)
			require.Empty(t, s.ackRequests)
			require.EqualValues(t, 1, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
		})
	}
	for _, defect := range []string{"missing_journal", "missing_C", "hash_mismatch"} {
		t.Run(defect, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
			_, err := c.PushPending(context.Background())
			require.Error(t, err)
			switch defect {
			case "missing_journal":
				_, err = db.Exec(`DELETE FROM _sync_upload_journal`)
			case "missing_C":
				_, err = db.Exec(`UPDATE _sync_upload_journal SET outcome_body=X'',outcome_hash=''`)
			case "hash_mismatch":
				_, err = db.Exec(`UPDATE _sync_upload_journal SET outcome_hash=?`, strings.Repeat("0", 64))
			}
			require.NoError(t, err)
			before := recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_source_state", "_sync_attachment_state")
			if defect == "missing_C" {
				tx, err := beginImmediateTx(context.Background(), db)
				require.NoError(t, err)
				err = markUploadDispositionInTx(context.Background(), tx)
				require.Error(t, err)
				require.NoError(t, tx.Rollback())
			} else {
				_, err = c.PushPending(context.Background())
				require.Error(t, err)
			}
			require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_source_state", "_sync_attachment_state"))
			require.Empty(t, s.ackRequests)
		})
	}
}

func recoveryJournalClient(t *testing.T) (*Client, *sql.DB, *mockPushServer) {
	t.Helper()
	c, db := newUsersBundleClient(t)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	s := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: s}
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('item','frozen','item@example.invalid')`)
	require.NoError(t, err)
	return c, db, s
}

func recoveryVector(t *testing.T, db *sql.DB, tables ...string) string {
	t.Helper()
	v := map[string][]map[string]any{}
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM ` + quoteIdent(table))
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		var values []map[string]any
		for rows.Next() {
			cells := make([]any, len(columns))
			refs := make([]any, len(columns))
			for i := range cells {
				refs[i] = &cells[i]
			}
			require.NoError(t, rows.Scan(refs...))
			record := map[string]any{}
			for i, name := range columns {
				value := cells[i]
				if raw, ok := value.([]byte); ok {
					value = hex.EncodeToString(raw)
				}
				record[name] = value
			}
			values = append(values, record)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		v[table] = values
	}
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}

func frozenJournalBodies(t *testing.T, db *sql.DB) (string, string, string) {
	t.Helper()
	var f, c, phase string
	require.NoError(t, db.QueryRow(`SELECT hex(request_body),hex(outcome_body),phase FROM _sync_upload_journal`).Scan(&f, &c, &phase))
	return f, c, phase
}

func TestRecoveryJournal_OutcomeCaptureCrashMatrix(t *testing.T) {
	for _, failure := range []string{"lost_response", "partial_decode", "outcome_store", "partial_stage", "after_outcome"} {
		t.Run(failure, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			if failure == "outcome_store" {
				_, err := db.Exec(`CREATE TRIGGER fail_capture BEFORE UPDATE OF outcome_body ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'synthetic capture failure');END`)
				require.NoError(t, err)
			}
			if failure == "partial_stage" {
				_, err := db.Exec(`CREATE TRIGGER fail_stage BEFORE INSERT ON _sync_push_stage BEGIN SELECT RAISE(ABORT,'synthetic partial stage');END`)
				require.NoError(t, err)
			}
			if failure == "after_outcome" {
				c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := s.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				if r.URL.Path == "/sync/push" {
					switch failure {
					case "lost_response":
						response.Body.Close()
						return nil, context.Canceled
					case "partial_decode":
						response.Body.Close()
						return rawMockOutcome([]byte(`{"source_id":`)), nil
					}
				}
				return response, nil
			})}
			before := recoveryVector(t, db, "users", "_sync_row_state", "_sync_source_state", "_sync_attachment_state")
			_, err := c.PushPending(context.Background())
			require.Error(t, err)
			f, certificate, phase := frozenJournalBodies(t, db)
			require.NotEmpty(t, f)
			require.Equal(t, "frozen", phase)
			if failure == "partial_stage" || failure == "after_outcome" {
				require.NotEmpty(t, certificate)
			} else {
				require.Empty(t, certificate)
			}
			require.Equal(t, before, recoveryVector(t, db, "users", "_sync_row_state", "_sync_source_state", "_sync_attachment_state"))
			require.Empty(t, s.ackRequests)
			if failure == "outcome_store" {
				_, err = db.Exec(`DROP TRIGGER fail_capture`)
				require.NoError(t, err)
			}
			if failure == "partial_stage" {
				_, err = db.Exec(`DROP TRIGGER fail_stage`)
				require.NoError(t, err)
			}
			// A retained C must be enough on its own; prunable history is irrelevant.
			s.committedBySource = map[int64]*mockCommittedBundle{}
			s.committedBySeq = map[int64]*mockCommittedBundle{}
			c.HTTP = &http.Client{Transport: s}
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			require.Len(t, s.pushRequests, 1)
			require.Len(t, s.ackRequests, 1)
			if certificate != "" {
				require.Empty(t, s.resolveRequests)
			} else {
				require.Len(t, s.resolveRequests, 1)
			}
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
		})
	}
}

func TestRecoveryJournal_AckCrashMatrix(t *testing.T) {
	for _, failure := range []string{"before_ack", "lost_ack_response", "after_ack_response", "original_binding"} {
		t.Run(failure, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			fail := true
			if failure == "after_ack_response" {
				_, err := db.Exec(`CREATE TRIGGER fail_ack_cleanup BEFORE DELETE ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'synthetic cleanup failure');END`)
				require.NoError(t, err)
			}
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/sync/push-outcomes/ack" && fail {
					if failure == "before_ack" {
						return nil, context.Canceled
					}
					response, err := s.RoundTrip(r)
					if err != nil {
						return nil, err
					}
					if failure != "after_ack_response" {
						response.Body.Close()
						return nil, context.Canceled
					}
					return response, nil
				}
				return s.RoundTrip(r)
			})}
			_, err := c.PushPending(context.Background())
			require.Error(t, err)
			f, certificate, phase := frozenJournalBodies(t, db)
			require.NotEmpty(t, f)
			require.NotEmpty(t, certificate)
			require.Equal(t, "ack_pending", phase)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
			local := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state")
			if failure == "after_ack_response" {
				_, err = db.Exec(`DROP TRIGGER fail_ack_cleanup`)
				require.NoError(t, err)
			}
			if failure == "original_binding" {
				original := c.schemaGenerationProof
				c.schemaGenerationProof = oversync.SchemaGenerationProof{ID: original.ID + 1, Fingerprint: strings.Repeat("f", 64)}
				c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					require.Equal(t, fmt.Sprint(original.ID), r.Header.Get(oversync.SchemaGenerationIDHeader))
					require.Equal(t, original.Fingerprint, r.Header.Get(oversync.SchemaGenerationFingerprintHeader))
					return s.RoundTrip(r)
				})}
			}
			fail = false
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			require.Equal(t, local, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state"))
			require.Len(t, s.pushRequests, 1)
			require.Empty(t, s.resolveRequests)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
		})
	}
}

func TestRecoveryJournal_CancelledRequestPreservesIntent(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err := c.PushPending(context.Background())
	require.Error(t, err)
	_, err = db.Exec(`UPDATE users SET name='newer' WHERE id='item'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Empty(t, s.pushRequests)
	require.Len(t, s.resolveRequests, 1)
	require.Len(t, s.ackRequests, 1)
	require.Equal(t, "cancelled", s.terminalOutcomes[1].Status)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id='item'`).Scan(&name))
	require.Equal(t, "newer", name)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, s.pushRequests, 1)
	require.EqualValues(t, 2, s.pushRequests[0].SourceBundleID)
	require.Equal(t, "newer", name)
	require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
}

func TestRecoveryJournal_RejectedRequestPreservesIntent(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	s.pushHook = func(p *mockDirectPush) *http.Response {
		return s.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "application_rejected"}}, p.Generation)
	}
	_, err := c.PushPending(context.Background())
	var rejected *PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &rejected)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	outbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, outboxStateRejected, outbox.State)
	require.Len(t, s.ackRequests, 1)
	require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	_, err = db.Exec(`UPDATE users SET name='newer' WHERE id='item'`)
	require.NoError(t, err)
	selected, err := c.PendingPushRejection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, selected)
	require.NoError(t, c.RetryPushRejection(context.Background(), *selected))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Len(t, s.ackRequests, 1)
	s.pushHook = nil
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, s.ackRequests, 2)
	require.Len(t, s.pushRequests, 2)
	require.EqualValues(t, 1, s.pushRequests[0].SourceBundleID)
	require.EqualValues(t, 2, s.pushRequests[1].SourceBundleID)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id='item'`).Scan(&name))
	require.Equal(t, "newer", name)
}

func TestRecoveryJournal_SourceSequenceGaps(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err := c.PushPending(context.Background())
	require.Error(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, "cancelled", s.terminalOutcomes[1].Status)
	require.Equal(t, "committed", s.terminalOutcomes[2].Status)
	require.EqualValues(t, 1, s.terminalOutcomes[2].Committed.BundleSeq)
	require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
}

func TestRecoveryJournal_SourceRecoveryCannotBypassFrozen(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	c.sourceIDGenerator = func() string { return "reserved-successor" }
	s.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(409, oversync.ErrorResponse{Error: "history_pruned", Message: "history was pruned"})
	}
	_, err := c.PushPending(context.Background())
	var originalRecovery *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &originalRecovery)
	f, certificate, _ := frozenJournalBodies(t, db)
	require.NotEmpty(t, f)
	require.Empty(t, certificate)
	s.pushHook = nil
	// Sync resolves/cancels the original F before honoring the mutable marker.
	_, err = c.Sync(context.Background())
	var recovery *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recovery)
	require.Len(t, s.resolveRequests, 1)
	require.Len(t, s.ackRequests, 1)
	require.Len(t, s.pushRequests, 1)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state")
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Errorf("pending intent must not request recovery: %s", r.URL.Path)
		return nil, context.Canceled
	})}
	require.Error(t, requireNoUnresolvedUpload(context.Background(), db))
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state"))
}

func TestRecoveryJournal_FrozenBytesUnchanged(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err := c.PushPending(context.Background())
	require.Error(t, err)
	f, cert, _ := frozenJournalBodies(t, db)
	require.Empty(t, cert)
	_, err = db.Exec(`UPDATE users SET name='later' WHERE id='item'`)
	require.NoError(t, err)
	c.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.Canceled })}
	_, err = c.PushPending(context.Background())
	require.Error(t, err)
	f2, _, _ := frozenJournalBodies(t, db)
	require.Equal(t, f, f2)
	require.Empty(t, s.pushRequests)
	_, err = db.Exec(`UPDATE _sync_upload_journal SET request_body=CAST('changed' AS BLOB)`)
	require.NoError(t, err)
	j, err := loadUploadJournal(context.Background(), db)
	require.NoError(t, err)
	_, err = c.journalRequestBody(context.Background(), j)
	var integrity *UploadJournalIntegrityError
	require.ErrorAs(t, err, &integrity)
}

func TestRecoveryJournal_BlockedSettlementRetainsOutcome(t *testing.T) {
	c, db, s := recoveryJournalClient(t)
	c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
	_, err := c.PushPending(context.Background())
	require.Error(t, err)
	_, err = db.Exec(`CREATE TRIGGER block_settlement BEFORE UPDATE ON users BEGIN SELECT RAISE(ABORT,'unsafe local transition');END`)
	require.NoError(t, err)
	f, cert, phase := frozenJournalBodies(t, db)
	require.NotEmpty(t, cert)
	require.Equal(t, "frozen", phase)
	before := recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state", "_sync_upload_journal")
	_, err = c.PushPending(context.Background())
	require.Error(t, err)
	f2, cert2, phase2 := frozenJournalBodies(t, db)
	require.Equal(t, f, f2)
	require.Equal(t, cert, cert2)
	require.Equal(t, phase, phase2)
	require.Equal(t, before, recoveryVector(t, db, "users", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_row_state", "_sync_source_state", "_sync_attachment_state", "_sync_upload_journal"))
	require.Empty(t, s.ackRequests)
	require.Empty(t, s.resolveRequests)
	require.Len(t, s.pushRequests, 1)
}

func TestRecoveryJournal_AckObligationBlocksDestructiveLifecycle(t *testing.T) {
	c, db, server := recoveryJournalClient(t)
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push-outcomes/ack" {
			return nil, context.Canceled
		}
		return server.RoundTrip(r)
	})}
	_, err := c.PushPending(context.Background())
	require.Error(t, err)
	var tables []string
	rows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Close())
	before := recoveryVector(t, db, tables...)
	status, err := c.PendingSyncStatus(context.Background())
	require.NoError(t, err)
	require.True(t, status.BlocksDetach)
	detached, err := c.Detach(context.Background())
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detached.Outcome)
	require.Equal(t, before, recoveryVector(t, db, tables...))
	require.Error(t, c.UninstallSync(context.Background()))
	require.Equal(t, before, recoveryVector(t, db, tables...))
	require.Empty(t, server.ackRequests)
	c.HTTP = &http.Client{Transport: server}
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.ackRequests, 1)
	require.Len(t, server.pushRequests, 1)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
}

func TestRecoveryJournal_OldFormatRejectedWithoutMutation(t *testing.T) {
	c, db, _ := recoveryJournalClient(t)
	require.NoError(t, c.Close())
	_, err := db.Exec(`DROP TABLE _sync_upload_journal`)
	require.NoError(t, err)
	rows, err := db.Query(`SELECT name,COALESCE(sql,'') FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	var tables []string
	schema := map[string]string{}
	for rows.Next() {
		var name, ddl string
		require.NoError(t, rows.Scan(&name, &ddl))
		tables = append(tables, name)
		schema[name] = ddl
	}
	require.NoError(t, rows.Close())
	before := recoveryVector(t, db, tables...)
	cfg := DefaultConfig(c.schemaGenerationProof.ID, []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.DatabaseID, cfg.ServiceID, cfg.LocalVersion = c.config.DatabaseID, c.config.ServiceID, c.config.LocalVersion
	_, err = newClientFromDB(db, "http://example.invalid", tokenProviderForTests, cfg)
	require.Error(t, err)
	require.Equal(t, before, recoveryVector(t, db, tables...))
	var journalCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='_sync_upload_journal'`).Scan(&journalCount))
	require.Zero(t, journalCount)
	for name, ddl := range schema {
		var after string
		require.NoError(t, db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=?`, name).Scan(&after))
		require.Equal(t, ddl, after)
	}
}
