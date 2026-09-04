package oversqlite_e2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sqlite3 "github.com/mattn/go-sqlite3"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Helper is reachable only by an explicitly guarded invocation of this test binary.
func TestMixedSchemaProcessHelper(t *testing.T) {
	if os.Getenv("MIXED_GENERATION_HELPER") != "current-test-subprocess-v1" {
		t.Skip("guarded subprocess only")
	}
	timeout := 120 * time.Second
	if os.Getenv("MIXED_GENERATION_ROLE") == "server" {
		if raw := os.Getenv("MIXED_GENERATION_SERVER_TIMEOUT_SECONDS"); raw != "" {
			seconds, err := strconv.Atoi(raw)
			require.NoError(t, err)
			require.Greater(t, seconds, 0)
			require.LessOrEqual(t, seconds, 600)
			timeout = time.Duration(seconds) * time.Second
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	barrier := func(phase string) {
		fmt.Println("MIXED_GENERATION:" + phase)
		select {
		case <-time.After(30 * time.Second):
			t.Fatal("barrier deadline: " + phase)
		case <-ctx.Done():
			t.Fatal("scenario deadline: " + phase)
		}
	}
	if os.Getenv("MIXED_GENERATION_ROLE") == "server" {
		s, err := exampleserver.NewTestServer(mixedServerConfig(os.Getenv("MIXED_GENERATION_DSN"), false))
		require.NoError(t, err)
		defer s.Close()
		fmt.Println("MIXED_GENERATION:" + s.URL())
		<-ctx.Done()
		return
	}
	if action := os.Getenv("MIXED_GENERATION_ACTION"); action != "" {
		runMixedExternalPeerAction(t, ctx, action)
		return
	}
	g, err := strconv.Atoi(os.Getenv("MIXED_GENERATION_GENERATION"))
	require.NoError(t, err)
	c := mixedOpen(t, ctx, os.Getenv("MIXED_GENERATION_FILE"), os.Getenv("MIXED_GENERATION_URL"), os.Getenv("MIXED_GENERATION_TOKEN"), g, false)
	defer c.c.Close()
	defer c.db.Close()
	phase := os.Getenv("MIXED_GENERATION_BOUNDARY")
	if phase == "staged" {
		c.c.SetBeforePushCommitHook(func(context.Context) error { barrier("staged"); return ctx.Err() })
	}
	if phase == "committed" || phase == "newer" {
		c.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := mixedVisibilityTransport(g, http.DefaultTransport).RoundTrip(r)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(r.URL.Path, "/commit") && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				if phase == "newer" || phase == "committed" {
					_, err = c.db.ExecContext(ctx, `UPDATE people SET name='A2' WHERE id=?`, mixedP)
					require.NoError(t, err)
				}
				barrier(phase)
				return nil, io.ErrUnexpectedEOF
			}
			return resp, nil
		})}
	}
	if phase == "nonfinal" {
		c.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/snapshot-sessions/") && r.Method == "GET" && r.URL.Query().Get("after_row_ordinal") == "1" {
				barrier("nonfinal")
			}
			return http.DefaultTransport.RoundTrip(r)
		})}
	}
	if phase == "final" {
		conn, err := c.db.Conn(ctx)
		require.NoError(t, err)
		require.NoError(t, conn.Raw(func(raw any) error {
			return raw.(*sqlite3.SQLiteConn).RegisterFunc("mixed_pause", func() int { barrier("final"); return 0 }, false)
		}))
		require.NoError(t, conn.Close())
		// This TEMP trigger pauses before the first business-row deletion in promotion.
		// The snapshot stage was committed by previous transactions, and no business
		// deletion from this transaction is visible to the parent observer.
		_, err = c.db.ExecContext(ctx, `CREATE TEMP TRIGGER mixed_pause_promotion BEFORE DELETE ON people BEGIN SELECT mixed_pause(); END`)
		require.NoError(t, err)
	}
	if phase == "final" || phase == "nonfinal" {
		_, err = c.c.Rebuild(ctx)
	} else {
		_, err = c.c.PushPending(ctx)
	}
	require.NoError(t, err)
	t.Fatal("required fault boundary was not reached")
}

// runMixedExternalPeerAction is a deliberately guarded test-binary control seam for the
// cross-runtime mixed-generation JVM driver. It reuses the real mixed-generation client, SQLite file, HTTP server,
// and PostgreSQL-backed server path; it does not expose an application endpoint or production API.
func runMixedExternalPeerAction(t *testing.T, ctx context.Context, action string) {
	t.Helper()
	g, err := strconv.Atoi(os.Getenv("MIXED_GENERATION_GENERATION"))
	require.NoError(t, err)
	file, url, token := os.Getenv("MIXED_GENERATION_FILE"), os.Getenv("MIXED_GENERATION_URL"), os.Getenv("MIXED_GENERATION_TOKEN")
	require.NotEmpty(t, file)
	require.NotEmpty(t, url)
	require.NotEmpty(t, token)
	c := mixedOpen(t, ctx, file, url, token, g, action == "init")
	defer c.c.Close()
	defer c.db.Close()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	switch action {
	case "init":
		// Opening and attaching is the action; local schema creation happened in mixedOpen.
	case "exec":
		query := os.Getenv("MIXED_GENERATION_SQL")
		require.NotEmpty(t, query)
		_, err = c.db.ExecContext(opCtx, query)
		require.NoError(t, err)
	case "push":
		mustPushPendingE2E(t, c.c, opCtx)
	case "pull":
		mustPullToStableE2E(t, c.c, opCtx)
	case "sync":
		_, err = c.c.Sync(opCtx)
		require.NoError(t, err)
	case "pending":
		_, err = c.c.PushPending(opCtx)
		var conflict *oversqlite.ConflictNeedsResolutionError
		require.ErrorAs(t, err, &conflict)
		pending, pendingErr := c.c.PendingConflict(opCtx)
		require.NoError(t, pendingErr)
		require.NotNil(t, pending)
		encoded, marshalErr := json.Marshal(pending)
		require.NoError(t, marshalErr)
		fmt.Println("MIXED_GENERATION:pending=" + string(encoded))
	case "resolve_accept", "resolve_local", "resolve_merged":
		pending, pendingErr := c.c.PendingConflict(opCtx)
		require.NoError(t, pendingErr)
		require.NotNil(t, pending)
		var decision oversqlite.MergeResult = oversqlite.AcceptServer{}
		if action == "resolve_local" {
			decision = oversqlite.KeepLocal{}
		} else if action == "resolve_merged" {
			payload := os.Getenv("MIXED_GENERATION_MERGED_PAYLOAD")
			require.NotEmpty(t, payload)
			decision = oversqlite.KeepMerged{MergedPayload: json.RawMessage(payload)}
		}
		require.NoError(t, c.c.ResolveConflict(opCtx, *pending, decision))
	case "rebuild":
		_, err = c.c.Rebuild(opCtx)
		require.NoError(t, err)
	case "query":
		query := os.Getenv("MIXED_GENERATION_SQL")
		require.NotEmpty(t, query)
		rows, queryErr := c.db.QueryContext(opCtx, query)
		require.NoError(t, queryErr)
		defer rows.Close()
		columns, queryErr := rows.Columns()
		require.NoError(t, queryErr)
		result := make([]map[string]any, 0)
		for rows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for index := range values {
				destinations[index] = &values[index]
			}
			require.NoError(t, rows.Scan(destinations...))
			row := make(map[string]any, len(columns))
			for index, value := range values {
				if bytes, ok := value.([]byte); ok {
					row[columns[index]] = string(bytes)
				} else {
					row[columns[index]] = value
				}
			}
			result = append(result, row)
		}
		require.NoError(t, rows.Err())
		encoded, marshalErr := json.Marshal(result)
		require.NoError(t, marshalErr)
		fmt.Println("MIXED_GENERATION:result=" + string(encoded))
	default:
		t.Fatalf("unknown MIXED_GENERATION_ACTION %q", action)
	}
	fmt.Println("MIXED_GENERATION:action=" + action)
}

type mixedProcess struct {
	cmd     *exec.Cmd
	lines   chan string
	done    chan error
	stopped bool
}

func startMixedProcess(t *testing.T, ctx context.Context, env map[string]string) *mixedProcess {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMixedSchemaProcessHelper$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), "MIXED_GENERATION_HELPER=current-test-subprocess-v1")
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	pipe, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = cmd.Stdout
	p := &mixedProcess{cmd: cmd, lines: make(chan string, 8), done: make(chan error, 1)}
	require.NoError(t, cmd.Start())
	go func() {
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "MIXED_GENERATION:") {
				p.lines <- strings.TrimPrefix(line, "MIXED_GENERATION:")
			}
		}
		close(p.lines)
		p.done <- cmd.Wait()
	}()
	t.Cleanup(func() { p.kill(t) })
	return p
}
func (p *mixedProcess) observed(t *testing.T) string {
	t.Helper()
	select {
	case line, ok := <-p.lines:
		require.True(t, ok, "child exited before required boundary")
		t.Logf("observed subprocess pid=%d boundary=%s", p.cmd.Process.Pid, line)
		return line
	case <-time.After(30 * time.Second):
		t.Fatal("30s child boundary deadline")
		return ""
	}
}
func (p *mixedProcess) kill(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	_ = p.cmd.Process.Kill()
	select {
	case err := <-p.done:
		require.Error(t, err, "expected actual process termination")
	case <-time.After(30 * time.Second):
		t.Error("child cleanup exceeded 30s")
	}
}

func mixedFrozen(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	var source, hash string
	var bundle int64
	require.NoError(t, db.QueryRowContext(opCtx, `SELECT source_id,source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&source, &bundle, &hash))
	require.NotEmpty(t, source)
	require.Positive(t, bundle)
	require.Len(t, hash, 64)
	rows, err := db.QueryContext(opCtx, `SELECT row_ordinal,schema_name,table_name,key_json,wire_key_json,op,base_row_version,local_payload,wire_payload FROM _sync_outbox_rows ORDER BY row_ordinal`)
	require.NoError(t, err)
	defer rows.Close()
	result := []any{source, bundle, hash}
	count := 0
	for rows.Next() {
		var ordinal, version int64
		var schema, table, key, wireKey, op string
		var local, wire sql.NullString
		require.NoError(t, rows.Scan(&ordinal, &schema, &table, &key, &wireKey, &op, &version, &local, &wire))
		result = append(result, []any{ordinal, schema, table, key, wireKey, op, version, local, wire})
		count++
	}
	require.NoError(t, rows.Err())
	require.Positive(t, count)
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	return string(raw)
}

type mixedRequestIdentity struct {
	SourceID       string
	SourceBundleID int64
	RequestHash    string
	PlannedRows    int64
}

func mixedOutboxIdentity(t *testing.T, ctx context.Context, db *sql.DB) mixedRequestIdentity {
	t.Helper()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	var identity mixedRequestIdentity
	require.NoError(t, db.QueryRowContext(opCtx, `SELECT source_id,source_bundle_id,canonical_request_hash,row_count FROM _sync_outbox_bundle`).Scan(&identity.SourceID, &identity.SourceBundleID, &identity.RequestHash, &identity.PlannedRows))
	require.NotEmpty(t, identity.SourceID)
	require.Positive(t, identity.SourceBundleID)
	require.Len(t, identity.RequestHash, 64)
	require.Positive(t, identity.PlannedRows)
	return identity
}

func (f *mixedFixture) effect() int64 {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	var n int64
	require.NoError(f.t, f.server.Pool.QueryRow(opCtx, `SELECT n FROM mixed.effects WHERE id=$1`, mixedP).Scan(&n))
	return n
}
func (f *mixedFixture) reopen(c *mixedClient, url string) {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	token, err := f.server.GenerateToken(mixedScope, time.Hour)
	require.NoError(f.t, err)
	opened := mixedOpen(f.t, opCtx, c.path, url, token, c.g, false)
	*c = *opened
}
func (f *mixedFixture) child(c *mixedClient, url, phase string) *mixedProcess {
	f.t.Helper()
	require.NoError(f.t, c.c.Close())
	require.NoError(f.t, c.db.Close())
	c.c = nil
	token, err := f.server.GenerateToken(mixedScope, time.Hour)
	require.NoError(f.t, err)
	return startMixedProcess(f.t, f.ctx, map[string]string{"MIXED_GENERATION_ROLE": "client", "MIXED_GENERATION_FILE": c.path, "MIXED_GENERATION_URL": url, "MIXED_GENERATION_TOKEN": token, "MIXED_GENERATION_GENERATION": fmt.Sprint(c.g), "MIXED_GENERATION_BOUNDARY": phase})
}
func mixedObserveDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMixedSchema_C08_LostReplyNewerIntent(t *testing.T) {
	f := newMixedFixture(t, true)
	f.exec(f.b, `UPDATE people SET note='secret-2'`)
	f.push(f.b)
	f.pull(f.a)
	f.exec(f.a, `UPDATE people SET name='A1'`)
	seq, effects := f.seq(), f.effect()
	source := requireCurrentSourceIDE2E(t, f.a.c, f.ctx)
	checkpoint := mixedCheckpoint(t, f.ctx, f.a.db)
	child := f.child(f.a, f.server.URL(), "newer")
	require.Equal(t, "newer", child.observed(t))
	db := mixedObserveDB(t, f.a.path)
	frozen := mixedFrozen(t, f.ctx, db)
	request := mixedOutboxIdentity(t, f.ctx, db)
	require.Equal(t, checkpoint, mixedCheckpoint(t, f.ctx, db), "ambiguous response cannot acknowledge a checkpoint")
	var name string
	require.NoError(t, db.QueryRowContext(f.ctx, `SELECT name FROM people WHERE id=?`, mixedP).Scan(&name))
	require.Equal(t, "A2", name)
	require.Equal(t, seq+1, f.seq())
	require.Equal(t, effects+1, f.effect())
	child.kill(t)
	f.reopen(f.a, f.server.URL())
	require.Equal(t, source, requireCurrentSourceIDE2E(t, f.a.c, f.ctx))
	require.Equal(t, frozen, mixedFrozen(t, f.ctx, f.a.db))
	f.push(f.a)
	mixedCommittedIdentity(t, f, request)
	require.Equal(t, seq+1, f.seq())
	require.Equal(t, effects+1, f.effect())
	require.NoError(t, f.a.db.QueryRowContext(f.ctx, `SELECT name FROM people WHERE id=?`, mixedP).Scan(&name))
	require.Equal(t, "A2", name)
	var captureErr error
	request = mixedRequestIdentity{SourceID: source}
	f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedCapturePushIdentity(&request, &captureErr, mixedVisibilityTransport(1, http.DefaultTransport))}
	f.push(f.a)
	require.NoError(t, captureErr)
	mixedCommittedIdentity(t, f, request)
	require.Equal(t, seq+2, f.seq())
	require.Equal(t, effects+2, f.effect())
	f.settle()
	f.person(mixedP, "A2", "secret-2", mixedQ, true)
}
func TestMixedSchema_C10_ProcessRecovery(t *testing.T) {
	for _, phase := range []string{"staged", "committed"} {
		t.Run(phase, func(t *testing.T) {
			f := newMixedFixture(t, true)
			f.exec(f.b, `UPDATE people SET note='secret-2'`)
			f.push(f.b)
			f.pull(f.a)
			f.exec(f.a, `UPDATE people SET name='from-A'`)
			seq, effects := f.seq(), f.effect()
			source := requireCurrentSourceIDE2E(t, f.a.c, f.ctx)
			checkpoint := mixedCheckpoint(t, f.ctx, f.a.db)
			// Retire the initial fixture server completely. Both subsequent serving
			// lifetimes are real child processes sharing only retained PostgreSQL state.
			f.server.Close()
			poolCtx, poolCancel := mixedOperationContext(f.ctx)
			pool, err := pgxpool.New(poolCtx, mixedBoundedDatabaseURL(f.dsn))
			poolCancel()
			require.NoError(t, err)
			f.server.Pool = pool
			server := startMixedProcess(t, f.ctx, map[string]string{"MIXED_GENERATION_ROLE": "server", "MIXED_GENERATION_DSN": f.dsn})
			url := server.observed(t)
			require.True(t, strings.HasPrefix(url, "http://127.0.0.1:"))
			child := f.child(f.a, url, phase)
			require.Equal(t, phase, child.observed(t))
			db := mixedObserveDB(t, f.a.path)
			frozen := mixedFrozen(t, f.ctx, db)
			request := mixedOutboxIdentity(t, f.ctx, db)
			require.Equal(t, checkpoint, mixedCheckpoint(t, f.ctx, db), "ambiguous response cannot acknowledge a checkpoint")
			delta := int64(0)
			if phase == "committed" {
				delta = 1
			}
			require.Equal(t, seq+delta, f.seq())
			require.Equal(t, effects+delta, f.effect())
			if phase == "staged" {
				var relation *string
				require.NoError(t, pool.QueryRow(f.ctx, `SELECT to_regclass('sync.push_session_rows')::text`).Scan(&relation))
				require.Nil(t, relation, "direct push interruption must not leave server-side staging rows")
			}
			child.kill(t)
			server.kill(t)
			restarted := startMixedProcess(t, f.ctx, map[string]string{"MIXED_GENERATION_ROLE": "server", "MIXED_GENERATION_DSN": f.dsn})
			url = restarted.observed(t)
			f.reopen(f.a, url)
			f.b.c.BaseURL = url
			require.Equal(t, source, requireCurrentSourceIDE2E(t, f.a.c, f.ctx))
			require.Equal(t, frozen, mixedFrozen(t, f.ctx, f.a.db))
			f.push(f.a)
			mixedCommittedIdentity(t, f, request)
			require.Equal(t, seq+1, f.seq())
			require.Equal(t, effects+1, f.effect())
			name := "from-A"
			if phase == "committed" {
				// Repeat C08's newer intent across the real server restart.
				var local string
				require.NoError(t, f.a.db.QueryRowContext(f.ctx, `SELECT name FROM people WHERE id=?`, mixedP).Scan(&local))
				require.Equal(t, "A2", local)
				var captureErr error
				request = mixedRequestIdentity{SourceID: source}
				f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedCapturePushIdentity(&request, &captureErr, mixedVisibilityTransport(1, http.DefaultTransport))}
				f.push(f.a)
				require.NoError(t, captureErr)
				mixedCommittedIdentity(t, f, request)
				require.Equal(t, seq+2, f.seq())
				require.Equal(t, effects+2, f.effect())
				name = "A2"
			}
			f.settle()
			f.person(mixedP, name, "secret-2", mixedQ, true)
			f.exec(f.b, `UPDATE people SET name='peer-after-restart'`)
			f.push(f.b)
			f.settle()
			f.person(mixedP, "peer-after-restart", "secret-2", mixedQ, true)
		})
	}
}
func TestMixedSchema_C11_InterruptedRebuild(t *testing.T) {
	for _, g := range []int{1, 2} {
		for _, phase := range []string{"nonfinal", "final"} {
			t.Run(fmt.Sprintf("G%d_%s", g, phase), func(t *testing.T) {
				f := newMixedFixture(t, true)
				subject := f.a
				if g == 2 {
					subject = f.b
				}
				// Multiple rows, including a hidden table, exercise compact snapshot staging.
				f.exec(f.b, `INSERT INTO people VALUES(?,'extra','extra-note',NULL)`, mixedOther)
				f.push(f.b)
				f.settle()
				peer := f.b
				if subject == f.b {
					peer = f.a
				}
				f.exec(peer, `UPDATE people SET name='snapshot-new' WHERE id=?`, mixedP)
				f.push(peer)
				source := requireCurrentSourceIDE2E(t, subject.c, f.ctx)
				checkpoint, err := subject.c.LastBundleSeqSeen(f.ctx)
				require.NoError(t, err)
				child := f.child(subject, f.server.URL(), phase)
				require.Equal(t, phase, child.observed(t))
				db := mixedObserveDB(t, subject.path)
				var stage int
				require.NoError(t, db.QueryRowContext(f.ctx, `SELECT count(*) FROM _sync_snapshot_stage`).Scan(&stage))
				if phase == "nonfinal" {
					require.Equal(t, 1, stage)
				} else {
					expected := 2
					if g == 2 {
						expected = 3
					}
					require.Equal(t, expected, stage)
				}
				var n int
				require.NoError(t, db.QueryRowContext(f.ctx, `SELECT count(*) FROM people`).Scan(&n))
				require.Equal(t, 2, n)
				var oldName string
				require.NoError(t, db.QueryRowContext(f.ctx, `SELECT name FROM people WHERE id=?`, mixedP).Scan(&oldName))
				require.Equal(t, "base", oldName)
				var cp int64
				require.NoError(t, db.QueryRowContext(f.ctx, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&cp))
				require.Equal(t, checkpoint, cp)
				child.kill(t)
				f.reopen(subject, f.server.URL())
				require.Equal(t, source, requireCurrentSourceIDE2E(t, subject.c, f.ctx))
				info, e := subject.c.SourceInfo(f.ctx)
				require.NoError(t, e)
				require.True(t, info.RebuildRequired, "interrupted rebuild remains publicly observable before recovery")
				readCtx, readCancel := mixedOperationContext(f.ctx)
				_, readErr := subject.c.PushPending(readCtx)
				readCancel()
				var rebuildErr *oversqlite.RebuildRequiredError
				require.ErrorAs(t, readErr, &rebuildErr, "public sync operations must remain gated until rebuild")
				_, err = subject.c.Rebuild(f.ctx)
				require.NoError(t, err)
				f.settle()
				f.person(mixedP, "snapshot-new", "secret-1", mixedQ, true)
				f.person(mixedOther, "extra", "extra-note", nil, true)
			})
		}
	}
}
func TestMixedSchema_C12_RetryRecovery(t *testing.T) {
	for _, direction := range []string{"pull", "upload"} {
		t.Run(direction, func(t *testing.T) {
			f := newMixedFixture(t, true)
			// Reopen with a finite retry policy to exercise exhaustion, not one disabled retry.
			require.NoError(t, f.a.c.Close())
			cfg := mixedConfig(t, 1)
			cfg.RetryPolicy = &oversqlite.RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
			token, e := f.server.GenerateToken(mixedScope, time.Hour)
			require.NoError(t, e)
			c := openReadySQLiteClient(t, f.a.path, f.server.URL(), func(context.Context) (string, error) { return token, nil }, cfg)
			f.a.c = c
			f.a.db = c.Database()
			_, e = c.Attach(f.ctx, mixedScope)
			require.NoError(t, e)
			cp, err := f.a.c.LastBundleSeqSeen(f.ctx)
			require.NoError(t, err)
			seq := f.seq()
			if direction == "pull" {
				f.exec(f.b, `UPDATE people SET note='secret-2'`)
				f.push(f.b)
				pendingCreated := false
				f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					resp, err := http.DefaultTransport.RoundTrip(r)
					if err == nil && strings.HasSuffix(r.URL.Path, "/pull") {
						if !pendingCreated {
							pendingCtx, pendingCancel := mixedOperationContext(f.ctx)
							_, pendingErr := f.a.db.ExecContext(pendingCtx, `INSERT INTO people(id,name) VALUES(?,'pull-pending')`, mixedOther)
							pendingCancel()
							require.NoError(t, pendingErr)
							f.expected["people"][mixedOther] = []any{mixedOther, "pull-pending", "server-default", nil}
							pendingCreated = true
						}
						resp.Body = &mixedBrokenBody{body: resp.Body}
					}
					return resp, err
				})}
				_, err = f.a.c.PullToStable(f.ctx)
				var exhausted *oversqlite.RetryExhaustedError
				require.ErrorAs(t, err, &exhausted)
				after, e := f.a.c.LastBundleSeqSeen(f.ctx)
				require.NoError(t, e)
				require.Equal(t, cp, after)
				require.True(t, pendingCreated)
				attempts := 0
				f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/sync/push" {
						attempts++
						return nil, io.ErrUnexpectedEOF
					}
					return http.DefaultTransport.RoundTrip(r)
				})}
				_, err = f.a.c.PushPending(f.ctx)
				var pushExhausted *oversqlite.RetryExhaustedError
				require.ErrorAs(t, err, &pushExhausted)
				require.Equal(t, 2, attempts)
				frozen := mixedFrozen(t, f.ctx, f.a.db)
				f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedVisibilityTransport(1, http.DefaultTransport)}
				require.Equal(t, frozen, mixedFrozen(t, f.ctx, f.a.db), "interrupted pull changed the pending edit")
				f.push(f.a)
				require.Equal(t, seq+2, f.seq())
			} else {
				f.exec(f.a, `UPDATE people SET name='retry'`)
				attempts := 0
				c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path != "/sync/push" {
						return http.DefaultTransport.RoundTrip(r)
					}
					attempts++
					return nil, io.ErrUnexpectedEOF
				})}
				_, err = c.PushPending(f.ctx)
				var exhausted *oversqlite.RetryExhaustedError
				require.ErrorAs(t, err, &exhausted)
				require.Equal(t, 2, attempts)
				require.Equal(t, seq, f.seq())
				frozen := mixedFrozen(t, f.ctx, f.a.db)
				c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedVisibilityTransport(1, http.DefaultTransport)}
				require.Equal(t, frozen, mixedFrozen(t, f.ctx, f.a.db))
				f.push(f.a)
				require.Equal(t, seq+1, f.seq())
			}
			f.a.c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedVisibilityTransport(1, http.DefaultTransport)}
			f.settle()
			name, note := "base", "secret-2"
			if direction == "upload" {
				name, note = "retry", "secret-1"
			}
			f.person(mixedP, name, note, mixedQ, true)
			if direction == "pull" {
				f.person(mixedOther, "pull-pending", "server-default", nil, true)
			}
		})
	}
}

type mixedBrokenBody struct {
	body io.ReadCloser
	sent bool
}

func (b *mixedBrokenBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	if len(p) > 1 {
		p = p[:1]
	}
	return b.body.Read(p)
}
func (b *mixedBrokenBody) Close() error { return b.body.Close() }

func mixedCapturePushIdentity(target *mixedRequestIdentity, captureErr *error, next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push" && target.SourceBundleID == 0 {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				*captureErr = err
				return nil, err
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var request oversync.CompactPushRequest
			if err := json.Unmarshal(body, &request); err != nil {
				*captureErr = err
				return nil, err
			}
			target.SourceBundleID = request.SourceBundleID
			for _, group := range request.Rows {
				target.PlannedRows += int64(len(group.Rows))
			}
			target.RequestHash = request.CanonicalRequestHash
		}
		return next.RoundTrip(r)
	})
}

func mixedCommittedIdentity(t *testing.T, f *mixedFixture, expected mixedRequestIdentity) {
	t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	var actual mixedRequestIdentity
	var rowCount, bundleRowCount int64
	var planned sql.NullInt64
	require.NoError(t, f.server.Pool.QueryRow(opCtx, `
		SELECT bl.source_id, bl.source_bundle_id, bl.canonical_request_hash,
		       bl.planned_row_count, bl.row_count,
		       (SELECT count(*) FROM sync.bundle_rows br WHERE br.user_pk=bl.user_pk AND br.bundle_seq=bl.bundle_seq)
		FROM sync.bundle_log bl
		WHERE bl.source_id=$1 AND bl.source_bundle_id=$2`, expected.SourceID, expected.SourceBundleID).
		Scan(&actual.SourceID, &actual.SourceBundleID, &actual.RequestHash, &planned, &rowCount, &bundleRowCount))
	require.True(t, planned.Valid)
	actual.PlannedRows = planned.Int64
	require.Equal(t, expected, actual)
	require.Equal(t, rowCount, bundleRowCount, "durable bundle row count differs from bundle metadata")
}

func mixedCheckpoint(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	var seq int64
	require.NoError(t, db.QueryRowContext(opCtx, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&seq))
	return seq
}
