package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type automaticFixture struct {
	t                                          *testing.T
	c                                          *Client
	db                                         *sql.DB
	rows                                       []oversync.SnapshotRow
	session                                    *oversync.SnapshotSession
	creates, pages, deletes, pulls, dispatches int
	boundary                                   int64
	createError                                string
	corruptHash                                bool
	pruned, invalid                            bool
	post                                       bool
	before                                     func(*http.Request) error
}

func newAutomaticFixture(t *testing.T) *automaticFixture {
	c, db := seedRecoveryUsers(t)
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	f := &automaticFixture{t: t, c: c, db: db, boundary: 2, rows: recoveryUsers(t, c, []string{"remote", "same"}, []string{"b", "a"})}
	c.HTTP = &http.Client{Transport: roundTripFunc(f.roundTrip)}
	return f
}
func (f *automaticFixture) roundTrip(r *http.Request) (*http.Response, error) {
	t := f.t
	f.dispatches++
	if f.before != nil {
		if err := f.before(r); err != nil {
			return nil, err
		}
	}
	switch {
	case r.URL.Path == "/sync/pull":
		f.pulls++
		after, _ := strconv.ParseInt(r.URL.Query().Get("after_bundle_seq"), 10, 64)
		if after < f.boundary {
			if f.pruned {
				return errorJSONResponse(409, oversync.ErrorResponse{Error: "history_pruned"}), nil
			}
			rows := []oversync.BundleRow{}
			for _, row := range f.rows {
				rows = append(rows, oversync.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: oversync.OpUpdate, RowVersion: f.boundary, Payload: mustJSONPayload(t, map[string]any{"id": row.Key["id"], "name": "historical", "email": "duplicate"})})
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: f.boundary, Bundles: []oversync.Bundle{pullBundle(f.boundary, "peer", 1, rows...)}}), nil
		}
		if f.post && after == 2 {
			row := f.rows[0]
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 3, Bundles: []oversync.Bundle{pullBundle(3, "peer", 2, oversync.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: oversync.OpUpdate, RowVersion: 3, Payload: mustJSONPayload(t, map[string]any{"id": row.Key["id"], "name": "post-T", "email": "b"})})}}), nil
		}
		return jsonResponse(oversync.PullResponse{StableBundleSeq: after}), nil
	case r.URL.Path == "/sync/snapshot-sessions":
		f.creates++
		if f.createError != "" {
			return errorJSONResponse(409, oversync.ErrorResponse{Error: f.createError}), nil
		}
		var request oversync.SnapshotSessionCreateRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NotNil(t, request.Recovery)
		require.GreaterOrEqual(t, request.Recovery.MinimumBundleSeq, int64(0))
		f.session = sealedTestSnapshot(t, f.c, fmt.Sprintf("auto-%d", f.creates), f.boundary, f.rows, request.Recovery)
		if f.corruptHash {
			f.session.SnapshotHash = strings.Repeat("0", 64)
		}
		return jsonResponse(f.session), nil
	case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodDelete:
		f.deletes++
		return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	case strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/"):
		f.pages++
		after, _ := strconv.Atoi(r.URL.Query().Get("after_row_ordinal"))
		maxRows, _ := strconv.Atoi(r.URL.Query().Get("max_rows"))
		if maxRows < 1 {
			maxRows = 1
		}
		end := min(len(f.rows), after+maxRows)
		rows := f.rows[after:end]
		_, size, err := oversync.CompactGroupsForSnapshotRowsAt(f.c.discovery.SchemaGeneration, rows, int64(after+1))
		require.NoError(t, err)
		return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: f.session.SnapshotID, SnapshotBundleSeq: f.boundary, Rows: rows, NextRowOrdinal: int64(end), HasMore: end < len(f.rows), ByteCount: size}), nil
	}
	return nil, fmt.Errorf("unexpected automatic request %s", r.URL.Path)
}

func TestFinalStateRecovery_PublicSyncHistoricalRecovery(t *testing.T) {
	for _, target := range []int64{1, 8, 17, defaultSyncWorkUnits} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			f := newAutomaticFixture(t)
			f.c.config.MaxSyncWorkUnitsPerInvocation = target
			f.post = true
			source := recoveryVector(t, f.db, "_sync_source_state", "_sync_upload_journal", "_sync_outbox_bundle", "_sync_outbox_rows")
			report, err := f.c.Sync(context.Background())
			require.NoError(t, err)
			require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.RemoteOutcome)
			require.Equal(t, 1, f.creates)
			requireLastBundleSeqSeen(t, f.c, context.Background(), 3)
			require.Equal(t, "post-T", baselineText(t, f.db, `SELECT name FROM users WHERE id='A'`))
			require.Equal(t, source, recoveryVector(t, f.db, "_sync_source_state", "_sync_upload_journal", "_sync_outbox_bundle", "_sync_outbox_rows"))
			status, err := f.c.RecoveryStatus(context.Background())
			require.NoError(t, err)
			require.Equal(t, "completed", status.Phase)
			require.EqualValues(t, 2, status.ObservedBoundary)
		})
	}
}

func TestFinalStateRecovery_DisabledMode(t *testing.T) {
	f := newAutomaticFixture(t)
	require.NoError(t, f.c.SetRecoveryMode(context.Background(), RecoveryDisabled))
	before := automaticBusinessVector(t, f.db)
	_, err := f.c.Sync(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Zero(t, f.creates)
	require.Equal(t, before, automaticBusinessVector(t, f.db))
	require.NoError(t, f.c.RetryRecovery(context.Background()))
	_, err = f.c.Sync(context.Background())
	require.ErrorAs(t, err, &blocked)
	require.Zero(t, f.creates)
	require.NoError(t, f.c.SetRecoveryMode(context.Background(), RecoveryAutomaticClean))
	_, err = f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, f.creates)
}

func TestFinalStateRecovery_DirtyStateNeverDownloads(t *testing.T) {
	for _, state := range []string{"dirty", "prepared", "outcome", "rejected", "ack_pending"} {
		t.Run(state, func(t *testing.T) {
			c, db, s := recoveryJournalClient(t)
			switch state {
			case "dirty":
				c.PauseUploads()
			case "prepared":
				c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
			case "outcome":
				c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
			case "rejected":
				s.pushHook = func(p *mockDirectPush) *http.Response {
					return s.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "application_rejected"}}, p.Generation)
				}
			case "ack_pending":
				c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/sync/push-outcomes/ack" {
						return nil, context.Canceled
					}
					return s.RoundTrip(r)
				})}
			}
			if state != "dirty" {
				_, err := c.PushPending(context.Background())
				require.Error(t, err)
			}
			if state == "outcome" {
				_, err := db.Exec(`CREATE TRIGGER unsafe_settlement BEFORE UPDATE ON users BEGIN SELECT RAISE(ABORT,'unsafe'); END`)
				require.NoError(t, err)
			}
			snapshots := 0
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions") {
					snapshots++
				}
				return nil, context.Canceled
			})}
			acks := len(s.ackRequests)
			before := journalProcessVector(t, db)
			require.NoError(t, c.RetryRecovery(context.Background()))
			_, err := c.Sync(context.Background())
			require.Error(t, err)
			require.Zero(t, snapshots)
			require.Equal(t, before, journalProcessVector(t, db))
			require.Len(t, s.ackRequests, acks)
		})
	}
}

func TestRecoveryJournal_GuardedPreparationRaces(t *testing.T) {
	for _, change := range []string{"dirty", "main", "temp", "comparison", "mode", "child"} {
		t.Run(change, func(t *testing.T) {
			f := newAutomaticFixture(t)
			before := automaticBusinessVector(t, f.db)
			changed := ""
			if change == "child" {
				_, err := f.db.Exec(`CREATE TABLE notes(id INTEGER PRIMARY KEY,user_id TEXT REFERENCES users(id) ON DELETE CASCADE,body BLOB)`)
				require.NoError(t, err)
			}
			f.c.beforeSnapshotApplyHook = func(ctx context.Context) error {
				var err error
				switch change {
				case "dirty":
					_, err = f.db.Exec(`UPDATE users SET name='offline' WHERE id='A'`)
				case "main":
					_, err = f.db.Exec(`CREATE INDEX changed_name ON users(name)`)
				case "temp":
					_, err = f.db.Exec(`CREATE TEMP TRIGGER changed_temp AFTER UPDATE ON users BEGIN SELECT 1; END`)
				case "comparison":
					_, err = f.db.Exec(`DROP INDEX users_email_unique; CREATE UNIQUE INDEX users_email_unique ON users(email COLLATE NOCASE)`)
				case "mode":
					err = f.c.SetRecoveryMode(ctx, RecoveryDisabled)
				case "child":
					_, err = f.db.Exec(`INSERT INTO notes VALUES(7,'A',x'0001ff')`)
				}
				require.NoError(t, err)
				changed = automaticBusinessVector(t, f.db)
				return nil
			}
			_, err := f.c.Sync(context.Background())
			require.Error(t, err)
			if change == "dirty" {
				require.Equal(t, "offline", baselineText(t, f.db, `SELECT name FROM users WHERE id='A'`))
			} else {
				require.Equal(t, before, automaticBusinessVector(t, f.db))
			}
			require.Equal(t, changed, automaticBusinessVector(t, f.db))
			if change == "child" {
				require.Equal(t, "0001FF", baselineText(t, f.db, `SELECT hex(body) FROM notes WHERE id=7`))
			}
		})
	}
}

func TestRecoveryJournal_CancelAndReopen(t *testing.T) {
	for _, cut := range []string{"page", "prepare", "after_commit"} {
		t.Run(cut, func(t *testing.T) {
			f := newAutomaticFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			before := automaticBusinessVector(t, f.db)
			if cut == "page" {
				f.before = func(r *http.Request) error {
					if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "snapshot-sessions/") {
						cancel()
						return ctx.Err()
					}
					return nil
				}
			}
			if cut == "prepare" {
				f.c.beforeSnapshotApplyHook = func(context.Context) error { cancel(); return ctx.Err() }
			}
			if cut == "after_commit" {
				f.before = func(r *http.Request) error {
					if r.Method == http.MethodDelete {
						cancel()
						return ctx.Err()
					}
					return nil
				}
			}
			_, err := f.c.Sync(ctx)
			require.Error(t, err)
			status, e := f.c.RecoveryStatus(context.Background())
			require.NoError(t, e)
			if cut == "after_commit" {
				require.Equal(t, "completed", status.Phase)
				requireLastBundleSeqSeen(t, f.c, context.Background(), 2)
				return
			}
			require.Equal(t, before, automaticBusinessVector(t, f.db))
			f.before = nil
			f.c.beforeSnapshotApplyHook = nil
			_, err = f.c.Sync(context.Background())
			require.NoError(t, err)
			require.Equal(t, 1, f.creates)
		})
	}
}

func TestRecoveryJournal_UnchangedFailureParks(t *testing.T) {
	f := newAutomaticFixture(t)
	f.rows = recoveryUsers(t, f.c, []string{"bad", "bad"}, []string{"duplicate", "duplicate"})
	before := automaticBusinessVector(t, f.db)
	_, err := f.c.Sync(context.Background())
	require.Error(t, err)
	require.Equal(t, before, automaticBusinessVector(t, f.db))
	status, e := f.c.RecoveryStatus(context.Background())
	require.NoError(t, e)
	require.Equal(t, "blocked", status.Phase)
	count := f.dispatches
	_, err = f.c.Sync(context.Background())
	require.Error(t, err)
	require.Equal(t, count, f.dispatches)
	require.NoError(t, f.c.RetryRecovery(context.Background()))
	require.NoError(t, f.c.RetryRecovery(context.Background()))
	_, err = f.c.Sync(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, f.creates)
	_, err = f.db.Exec(`DROP INDEX users_email_unique`)
	require.NoError(t, err)
	_, err = f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, f.creates)
}

func TestRecoveryJournal_ExchangeBudget(t *testing.T) {
	f := newAutomaticFixture(t)
	f.c.config.SnapshotChunkRows = 1
	for i := 2; i < 70; i++ {
		id := fmt.Sprintf("row-%03d", i)
		f.rows = append(f.rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": id, "email": id})})
	}
	_, err := f.c.Sync(context.Background())
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompleteRecoveryExchangeLimit, incomplete.Reason)
	require.Equal(t, 1, f.creates)
	require.Equal(t, 63, f.pages)
	requireLastBundleSeqSeen(t, f.c, context.Background(), 1)
	_, err = f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, f.creates)
	require.Equal(t, 70, f.pages)
}

func TestRecoveryJournal_AggregateBoundsAndCleanup(t *testing.T) {
	for _, dimension := range []string{"entries", "bytes"} {
		t.Run(dimension, func(t *testing.T) {
			f := newAutomaticFixture(t)
			before := automaticBusinessVector(t, f.db)
			if dimension == "entries" {
				f.c.config.MaxSyncWorkspaceEntries = 1
			} else {
				f.c.config.MaxSyncWorkspaceBytes = 1
			}
			_, err := f.c.Sync(context.Background())
			var capacity *SyncWorkspaceLimitError
			require.ErrorAs(t, err, &capacity)
			require.Zero(t, f.creates)
			require.Equal(t, before, automaticBusinessVector(t, f.db))
		})
	}
	f := newAutomaticFixture(t)
	_, err := f.c.Sync(context.Background())
	require.NoError(t, err)
	for _, table := range []string{"_sync_snapshot_stage", "_sync_apply_rows", "_sync_apply_edges", "_sync_upload_journal"} {
		var count int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM `+quoteIdent(table)).Scan(&count))
		require.Zero(t, count, table)
	}
}

// The capture may publish rebuild_required and auxiliary phase/session metadata.
// All preexisting business, intent, source progress, checkpoint and authority
// fields are compared independently of those explicitly expected publications.
func automaticBusinessVector(t *testing.T, db *sql.DB) string {
	return recoveryVector(t, db, "users", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_bundle", "_sync_outbox_rows", "_sync_upload_journal", "_sync_source_state") + baselineText(t, db, `SELECT json_array(attachment_epoch,current_source_id,binding_state,attached_user_id,schema_name,last_bundle_seq_seen,pending_initialization_id,initialization_reconnect_required) FROM _sync_attachment_state`)
}

func TestFinalStateRecovery_StaticFailuresNeverDownload(t *testing.T) {
	for _, failure := range []string{"index", "corrupt", "intrinsic"} {
		t.Run(failure, func(t *testing.T) {
			f := newAutomaticFixture(t)
			before := automaticBusinessVector(t, f.db)
			switch failure {
			case "index":
				_, err := f.db.Exec(`CREATE UNIQUE INDEX unsupported_index ON users(lower(email))`)
				require.NoError(t, err)
			case "corrupt":
				_, err := f.db.Exec(`UPDATE _sync_operation_state SET recovery_state='{"version":1,"version":1}'`)
				require.NoError(t, err)
			case "intrinsic":
				f.c.config.MaxSyncWorkspaceEntries = 1
			}
			_, err := f.c.Sync(context.Background())
			require.Error(t, err)
			require.Zero(t, f.creates)
			require.Equal(t, before, automaticBusinessVector(t, f.db))
		})
	}
}

func TestRecoveryJournal_StatusAndRetryBounds(t *testing.T) {
	f := newAutomaticFixture(t)
	ctx := context.Background()
	require.NoError(t, f.c.captureAutomaticRecovery(ctx, "history_pruned", 2, "historical-unit"))
	r, err := f.c.readRecovery(ctx, f.db)
	require.NoError(t, err)
	ctx = context.WithValue(ctx, automaticRecoveryKey{}, automaticRecoveryPin{r.RecoveryID, r.ModeRevision})
	keys := []string{"", " spaced ", "quote\"猫"}
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	require.NoError(t, f.c.parkAutomaticRecovery(ctx, &FinalStateApplyBlockedError{Reason: "conflicting_desired_state", Operation: "snapshot", Table: "users", Index: "users_email_unique", Keys: keys}))
	status, err := f.c.RecoveryStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status.Keys, 64)
	require.True(t, status.Truncated)
	require.Equal(t, keys[:64], status.Keys)
	require.NoError(t, f.c.RetryRecovery(ctx))
	require.NoError(t, f.c.RetryRecovery(ctx))
	r, err = f.c.readRecovery(ctx, f.db)
	require.NoError(t, err)
	require.True(t, r.Armed)
	f.c.recoveryControlMu.Lock()
	require.Error(t, f.c.RetryRecovery(ctx))
	f.c.recoveryControlMu.Unlock()
	invocation := &syncInvocation{}
	ctx = context.WithValue(context.Background(), syncInvocationKey{}, invocation)
	for i := 0; i < 4; i++ {
		require.NoError(t, f.c.captureAutomaticRecovery(ctx, "history_pruned", 2, fmt.Sprintf("relevant-%d", i)))
	}
	require.Equal(t, 3, invocation.recoveryReplans)
	err = f.c.captureAutomaticRecovery(ctx, "history_pruned", 2, "fifth-relevant-unit")
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompleteRecoveryReplanLimit, incomplete.Reason)
	require.Zero(t, f.creates)
}

func TestRecoveryJournal_RelevantLocalDependencyRearms(t *testing.T) {
	f := newAutomaticFixture(t)
	_, err := f.db.Exec(`CREATE TABLE notes(id INTEGER PRIMARY KEY,parent TEXT REFERENCES users(id) ON DELETE CASCADE,body BLOB); INSERT INTO notes VALUES(7,'A',x'0001ff')`)
	require.NoError(t, err)
	_, err = f.c.Sync(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, f.creates)
	requests := f.dispatches
	_, err = f.c.Sync(context.Background())
	var blocked *RecoveryBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, requests, f.dispatches)
	_, err = f.db.Exec(`DELETE FROM notes WHERE id=7`)
	require.NoError(t, err)
	_, err = f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, f.creates)
}

func TestRecoveryJournal_ClosedFileCleanup(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "automatic.sqlite")
	open := func() *sql.DB {
		db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		return db
	}
	db := open()
	_, err := db.Exec(usersTestDDL)
	require.NoError(t, err)
	config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	c, err := newOwnedAdmittedFixtureClient(db, "http://automatic.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	attachTestClient(t, c, "bundle-user", "bundle-device")
	for _, id := range []string{"A", "B"} {
		seedSyncedUserRow(t, c, db, id, "same", 1)
	}
	_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX users_email_unique ON users(email)`)
	require.NoError(t, err)
	f := &automaticFixture{t: t, c: c, db: db, boundary: 2, pruned: true}
	c.HTTP = &http.Client{Transport: roundTripFunc(f.roundTrip)}
	defer func() { require.NoError(t, f.c.Close()); require.NoError(t, f.db.Close()) }()
	var warmedPages, peakWal int64
	for cycle := 0; cycle < 10; cycle++ {
		f.boundary = int64(cycle + 2)
		f.rows = recoveryUsers(t, f.c, []string{fmt.Sprintf("cycle-%d", cycle), "same"}, []string{"b", "a"})
		if cycle%3 == 1 {
			f.rows = f.rows[:1]
		}
		for i := range f.rows {
			f.rows[i].RowVersion = f.boundary
		}
		if cycle == 4 {
			f.c.beforeSnapshotApplyHook = func(context.Context) error { return context.Canceled }
			_, err = f.c.Sync(context.Background())
			require.Error(t, err)
			previousCreates := f.creates
			require.NoError(t, f.c.Close())
			require.NoError(t, f.db.Close())
			f.db = open()
			f.c, err = newOwnedAdmittedFixtureClient(f.db, "http://automatic.invalid", tokenProviderForTests, config)
			require.NoError(t, err)
			attachTestClient(t, f.c, "bundle-user", "bundle-device")
			f.c.HTTP = &http.Client{Transport: roundTripFunc(f.roundTrip)}
			_, err = f.c.Sync(context.Background())
			require.NoError(t, err)
			require.Equal(t, previousCreates, f.creates)
		} else {
			_, err = f.c.Sync(context.Background())
			require.NoError(t, err)
		}
		for _, table := range []string{"_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_upload_journal", "_sync_dirty_rows", "_sync_outbox_rows"} {
			require.Zero(t, countA3(t, f.db, "SELECT count(*) FROM "+quoteIdent(table)), table)
		}
		pages := countA3(t, f.db, "PRAGMA page_count")
		if cycle == 2 {
			warmedPages = int64(pages)
		}
		if cycle > 2 {
			require.LessOrEqual(t, int64(pages), warmedPages+8)
		}
		if info, e := os.Stat(path + "-wal"); e == nil {
			peakWal = max(peakWal, info.Size())
		}
	}
	require.LessOrEqual(t, peakWal, int64(8<<20))
	require.Equal(t, "wal", baselineText(t, f.db, "PRAGMA journal_mode"))
	require.EqualValues(t, 2, countA3(t, f.db, "PRAGMA synchronous"))
	census := map[string]int64{}
	tables, err := f.db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*' ORDER BY name`)
	require.NoError(t, err)
	var names []string
	for tables.Next() {
		var name string
		require.NoError(t, tables.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, tables.Close())
	for _, name := range names {
		census[name] = countA3(t, f.db, "SELECT count(*) FROM "+quoteIdent(name))
	}
	require.NoError(t, f.c.Close())
	require.NoError(t, f.db.Close())
	closed, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_foreign_keys=on")
	require.NoError(t, err)
	require.Equal(t, "ok", baselineText(t, closed, "PRAGMA integrity_check"))
	require.Zero(t, countA3(t, closed, "SELECT count(*) FROM pragma_foreign_key_check"))
	require.NoError(t, closed.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	t.Logf("closed SQLite bytes=%d peak WAL bytes=%d warm pages=%d census=%v; ten update/delete/reinsert snapshots and cancellation/reopen; no VACUUM/manual cleanup", info.Size(), peakWal, warmedPages, census)
}

func TestRecoveryJournal_UploadAndSnapshotShareDispatchBudget(t *testing.T) {
	c, db := newUsersBundleClient(t)
	c.config.UploadLimit = 1
	c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
	server := newMockPushServer(t)
	var rows []oversync.SnapshotRow
	for i := 0; i < 64; i++ {
		id := fmt.Sprintf("budget-%03d", i)
		_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, id, id, id)
		require.NoError(t, err)
		rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 65, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": id, "email": id})})
	}
	creates := 0
	var session *oversync.SnapshotSession
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" || strings.HasPrefix(r.URL.Path, "/sync/push-outcomes/") {
			return server.RoundTrip(r)
		}
		if r.URL.Path == "/sync/pull" {
			if r.URL.Query().Get("after_bundle_seq") == "64" {
				return errorJSONResponse(409, oversync.ErrorResponse{Error: "history_pruned"}), nil
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 65}), nil
		}
		if r.URL.Path == "/sync/snapshot-sessions" {
			creates++
			var request oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.EqualValues(t, 64, request.Recovery.MinimumBundleSeq)
			session = sealedTestSnapshot(t, c, "shared-budget", 65, rows, request.Recovery)
			return jsonResponse(session), nil
		}
		if r.Method == http.MethodDelete {
			return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: session.SnapshotID, SnapshotBundleSeq: 65, Rows: rows, NextRowOrdinal: 64, ByteCount: session.ByteCount}), nil
	})}
	_, err := c.Sync(context.Background())
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompleteRecoveryExchangeLimit, incomplete.Reason)
	require.Len(t, server.pushRequests, 64)
	require.Len(t, server.ackRequests, 64)
	require.Zero(t, creates)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	source := recoveryVector(t, db, "_sync_source_state")
	_, err = c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, creates)
	require.Len(t, server.ackRequests, 64)
	require.Equal(t, source, recoveryVector(t, db, "_sync_source_state"))
	requireLastBundleSeqSeen(t, c, context.Background(), 65)
}

func TestRecoveryJournal_ControlMetadataExactBoundaries(t *testing.T) {
	f := newAutomaticFixture(t)
	ctx := context.Background()
	require.NoError(t, f.c.captureAutomaticRecovery(ctx, "history_pruned", 2, "unit"))
	usage, err := f.c.workspaceUsageUncached(ctx, f.db, "boundary", nil, nil)
	require.NoError(t, err)
	extra := workspaceCharge{32, 4096}
	for _, dimension := range []string{"entries", "bytes"} {
		f.c.config.MaxSyncWorkspaceEntries = usage.entries + extra.entries
		f.c.config.MaxSyncWorkspaceBytes = usage.bytes + extra.bytes
		require.NoError(t, f.c.admitRecoveryControl(ctx, f.db, extra))
		if dimension == "entries" {
			f.c.config.MaxSyncWorkspaceEntries--
		} else {
			f.c.config.MaxSyncWorkspaceBytes--
		}
		err = f.c.admitRecoveryControl(ctx, f.db, extra)
		var limit *SyncWorkspaceLimitError
		require.ErrorAs(t, err, &limit)
		require.Equal(t, dimension, limit.Dimension)
	}
}

func TestFinalStateRecovery_DisabledOrdinaryCorrection(t *testing.T) {
	f := newAutomaticFixture(t)
	require.NoError(t, f.c.SetRecoveryMode(context.Background(), RecoveryDisabled))
	_, err := f.c.Sync(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Zero(t, f.creates)
	require.Zero(t, countA3(t, f.db, `SELECT rebuild_required FROM _sync_attachment_state`))
	_, err = f.db.Exec(`DROP INDEX users_email_unique`)
	require.NoError(t, err)
	report, err := f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedIncremental, report.RemoteOutcome)
	require.Zero(t, f.creates)
	requireLastBundleSeqSeen(t, f.c, context.Background(), 2)
}

func TestRecoveryJournal_FailedSnapshotAdmissionParks(t *testing.T) {
	for _, failure := range []string{"recovery_floor_ahead", "push_outcome_pending", "snapshot_integrity"} {
		t.Run(failure, func(t *testing.T) {
			f := newAutomaticFixture(t)
			if failure == "snapshot_integrity" {
				f.corruptHash = true
			} else {
				f.createError = failure
			}
			before := automaticBusinessVector(t, f.db)
			_, err := f.c.Sync(context.Background())
			require.Error(t, err)
			require.Equal(t, before, automaticBusinessVector(t, f.db))
			status, err := f.c.RecoveryStatus(context.Background())
			require.NoError(t, err)
			require.Equal(t, "blocked", status.Phase)
			require.Equal(t, failure, status.Reason)
			requests := f.dispatches
			_, err = f.c.Sync(context.Background())
			var blocked *RecoveryBlockedError
			require.ErrorAs(t, err, &blocked)
			require.Equal(t, requests, f.dispatches)
			require.NoError(t, f.c.RetryRecovery(context.Background()))
			_, err = f.c.Sync(context.Background())
			require.Error(t, err)
			require.Greater(t, f.dispatches, requests)
			require.Equal(t, before, automaticBusinessVector(t, f.db))
		})
	}
}

func TestRecoveryJournal_CorruptCoordinatorNeverDownloads(t *testing.T) {
	for _, field := range []string{"empty_object", "authority_hash", "recovery_id", "local_guard", "version", "mode", "phase", "captured_mode_revision"} {
		t.Run(field, func(t *testing.T) {
			f := newAutomaticFixture(t)
			ctx := context.Background()
			require.NoError(t, f.c.captureAutomaticRecovery(ctx, "history_pruned", 2, ""))
			var raw string
			require.NoError(t, f.db.QueryRow(`SELECT recovery_state FROM _sync_operation_state`).Scan(&raw))
			values := map[string]any{}
			require.NoError(t, json.Unmarshal([]byte(raw), &values))
			delete(values, field)
			if field == "empty_object" {
				values = map[string]any{}
			}
			broken, err := json.Marshal(values)
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE _sync_operation_state SET recovery_state=?`, string(broken))
			require.NoError(t, err)
			before := automaticBusinessVector(t, f.db)
			_, err = f.c.Sync(ctx)
			require.Error(t, err)
			require.Zero(t, f.creates)
			require.Equal(t, before, automaticBusinessVector(t, f.db))
		})
	}
}
func TestRecoveryJournal_ResumeHonorsDownloadPause(t *testing.T) {
	f := newAutomaticFixture(t)
	f.before = func(r *http.Request) error {
		if r.Method == "GET" && strings.Contains(r.URL.Path, "snapshot-sessions/") {
			return context.Canceled
		}
		return nil
	}
	_, err := f.c.Sync(context.Background())
	require.Error(t, err)
	before := f.dispatches
	f.c.PauseDownloads()
	f.before = nil
	report, err := f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeSkippedPaused, report.RemoteOutcome)
	require.Equal(t, before, f.dispatches)
	f.c.ResumeDownloads()
	_, err = f.c.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, f.creates)
}
func TestRecoveryJournal_CloseJoinsControl(t *testing.T) {
	f := newAutomaticFixture(t)
	ctx := context.Background()
	require.NoError(t, f.c.beginRecoveryControl())
	tx, err := beginImmediateTx(ctx, f.db)
	require.NoError(t, err)
	r, err := f.c.readRecovery(ctx, tx)
	require.NoError(t, err)
	r.Mode = RecoveryDisabled
	require.NoError(t, f.c.writeRecovery(ctx, tx, tx, r))
	done := make(chan error, 1)
	go func() { done <- f.c.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close passed active control: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	f.c.recoveryControlMu.Unlock()
	require.NoError(t, <-done)
	_, err = f.c.RecoveryStatus(ctx)
	var closed *ClientClosedError
	require.ErrorAs(t, err, &closed)
}
func TestRecoveryJournal_MalformedSnapshotParks(t *testing.T) {
	for _, failure := range []string{"metadata", "chunk_fields", "chunk_bytes", "sealed_totals", "body_limit", "encoding"} {
		t.Run(failure, func(t *testing.T) {
			f := newAutomaticFixture(t)
			delegate := f.c.HTTP.Transport
			f.c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := delegate.RoundTrip(r)
				if err != nil {
					return response, err
				}
				if r.Method == "GET" && strings.Contains(r.URL.Path, "snapshot-sessions/") {
					if failure == "body_limit" {
						response.Body.Close()
						n, _ := strconv.ParseInt(r.URL.Query().Get("max_bytes"), 10, 64)
						rows, _ := strconv.Atoi(r.URL.Query().Get("max_rows"))
						limit, err := checkedSnapshotChunkBodyLimit(n, rows)
						require.NoError(t, err)
						response.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", int(limit)+1)))
					}
					if failure == "encoding" {
						response.Header.Set("Content-Encoding", "unsupported")
					}
				}
				if (failure == "metadata" || failure == "sealed_totals") && r.Method == "POST" && r.URL.Path == "/sync/snapshot-sessions" || (failure == "chunk_fields" || failure == "chunk_bytes") && r.Method == "GET" && strings.Contains(r.URL.Path, "snapshot-sessions/") {
					var fields map[string]any
					require.NoError(t, json.NewDecoder(response.Body).Decode(&fields))
					response.Body.Close()
					switch failure {
					case "metadata":
						fields["row_count"] = -1
					case "sealed_totals":
						fields["row_count"] = 99
					case "chunk_fields":
						delete(fields, "has_more")
					case "chunk_bytes":
						fields["logical_byte_count"] = 99999
					}
					raw, err := json.Marshal(fields)
					require.NoError(t, err)
					response.Body = io.NopCloser(strings.NewReader(string(raw)))
				}
				return response, nil
			})
			_, err := f.c.Sync(context.Background())
			require.Error(t, err)
			status, err := f.c.RecoveryStatus(context.Background())
			require.NoError(t, err)
			require.Equal(t, "blocked", status.Phase)
			requests := f.dispatches
			_, err = f.c.Sync(context.Background())
			var blocked *RecoveryBlockedError
			require.ErrorAs(t, err, &blocked)
			require.Equal(t, requests, f.dispatches)
		})
	}
}

func TestRecoveryJournal_PublicRelevantReplanLimit(t *testing.T) {
	f := newAutomaticFixture(t)
	delegate := f.c.HTTP.Transport
	f.c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := delegate.RoundTrip(r)
		if err == nil && r.Method == "GET" && strings.Contains(r.URL.Path, "snapshot-sessions/") {
			f.boundary++
		}
		return response, err
	})
	_, err := f.c.Sync(context.Background())
	var incomplete *SyncIncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, SyncIncompleteRecoveryReplanLimit, incomplete.Reason)
	require.Equal(t, 4, f.creates)
	requireLastBundleSeqSeen(t, f.c, context.Background(), 5)
}

func TestRecoveryJournal_RetainedCorruptStageParks(t *testing.T) {
	for _, change := range []string{"key_json='{}'", "payload='{\"id\":\"A\",\"name\":\"a different valid payload\",\"email\":\"b\"}'", "table_name='unknown'", "row_version=999"} {
		t.Run(change, func(t *testing.T) {
			f := newAutomaticFixture(t)
			f.c.config.SnapshotChunkRows = 1
			f.before = func(r *http.Request) error {
				if r.Method == "GET" && strings.Contains(r.URL.Path, "snapshot-sessions/") && f.pages == 1 {
					return context.Canceled
				}
				return nil
			}
			_, err := f.c.Sync(context.Background())
			require.Error(t, err)
			f.before = nil
			_, err = f.db.Exec(`UPDATE _sync_snapshot_stage SET ` + change + ` WHERE row_ordinal=1`)
			require.NoError(t, err)
			_, err = f.c.Sync(context.Background())
			require.Error(t, err)
			status, err := f.c.RecoveryStatus(context.Background())
			require.NoError(t, err)
			require.Equal(t, "blocked", status.Phase)
			requests := f.dispatches
			_, err = f.c.Sync(context.Background())
			var blocked *RecoveryBlockedError
			require.ErrorAs(t, err, &blocked)
			require.Equal(t, requests, f.dispatches)
			requireLastBundleSeqSeen(t, f.c, context.Background(), 1)
		})
	}
}
func TestRecoveryJournal_EmptyDependencyMetadataWork(t *testing.T) {
	for _, target := range []int64{1, 8, 17, defaultSyncWorkUnits} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			f := newAutomaticFixture(t)
			f.c.config.MaxSyncWorkUnitsPerInvocation = target
			var columns []string
			for i := 0; i < 64; i++ {
				columns = append(columns, fmt.Sprintf("c%d TEXT", i))
			}
			_, err := f.db.Exec(`CREATE TABLE empty_child(id INTEGER PRIMARY KEY,parent TEXT REFERENCES users(id),` + strings.Join(columns, ",") + `)`)
			require.NoError(t, err)
			for i := 0; i < 32; i++ {
				_, err = f.db.Exec(fmt.Sprintf("CREATE TABLE local_%d(value TEXT)", i))
				require.NoError(t, err)
			}
			_, err = f.c.Sync(context.Background())
			require.NoError(t, err)
			require.GreaterOrEqual(t, f.c.a3Stats.totalWork, int64(64*128))
			require.Equal(t, 1, f.creates)
		})
	}
}
