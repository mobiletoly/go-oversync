package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This branch never returns through Client.Close, reservation release or test
// cleanup. The parent kills this exact PID after observing its durable marker.
func TestA3ReservationDeath_Process(t *testing.T) {
	if file := os.Getenv("A3_RESERVATION_DEATH_FILE"); file != "" {
		db, err := sql.Open("sqlite3", file+"?_foreign_keys=on")
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
		cfg.GenerationID = 1
		cfg.DatabaseID, cfg.ServiceID = "fixture", "fixture"
		owned, err := newClientFromDBWithOwnership(db, "http://example.invalid", tokenProviderForTests, cfg, true)
		require.NoError(t, err)
		_, err = owned.reserveWorkspace(context.Background(), applyUnit("death-buffer", 0, ""), workspaceCharge{3, 100})
		require.NoError(t, err)
		buffer := make([]byte, 100)
		buffer[0] = 42
		require.NoError(t, os.WriteFile(os.Getenv("A3_RESERVATION_DEATH_MARKER"), []byte("live reservation"), 0600))
		for {
			time.Sleep(50 * time.Millisecond)
			// Keep both the charged allocation and lifetime owner live until SIGKILL.
			buffer[0]++
			require.NotNil(t, owned)
		}
	}
	c, db, file := qualificationDurableClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	c.HTTP = &http.Client{Transport: server}
	c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	_, err = db.Exec(`UPDATE nodes SET name='unrelated local intent' WHERE id='Q'`)
	require.NoError(t, err)
	before := a3ReservationDeathVector(t, db)
	cfg := cloneCoordinatorClientConfig(c.config)
	cfg.GenerationID = c.schemaGenerationProof.ID
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	marker := filepath.Join(t.TempDir(), "child.ready")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestA3ReservationDeath_Process$", "-test.timeout=25s")
	child.Env = append(os.Environ(), "A3_RESERVATION_DEATH_FILE="+file, "A3_RESERVATION_DEATH_MARKER="+marker)
	log, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
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
	ready := assert.Eventually(t, func() bool { _, err := os.Stat(marker); return err == nil }, 10*time.Second, 10*time.Millisecond)
	childOutput, readErr := os.ReadFile(log.Name())
	require.NoError(t, readErr)
	require.True(t, ready, string(childOutput))
	reopened, err := sql.Open("sqlite3", file+"?_foreign_keys=on")
	require.NoError(t, err)
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	// Observe the live ledger and prove that a competing lifetime cannot take it.
	held := countA3(t, reopened, `SELECT reserved_entries FROM _sync_operation_state`)
	require.EqualValues(t, 4, held)
	_, err = newClientFromDBWithOwnership(reopened, c.BaseURL, c.Token, cfg, true)
	var busy *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &busy)
	require.EqualValues(t, held, countA3(t, reopened, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, child.Process.Kill())
	err = child.Wait()
	waited = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status := exit.Sys().(syscall.WaitStatus)
	require.True(t, status.Signaled())
	require.Equal(t, syscall.SIGKILL, status.Signal())
	require.NoError(t, ctx.Err(), "death must be parent-directed, not an outer timeout")
	ordinary, err := newClientFromDB(reopened, c.BaseURL, c.Token, cfg)
	require.NoError(t, err)
	require.EqualValues(t, held, countA3(t, reopened, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, ordinary.Close())
	owned, err := newClientFromDBWithOwnership(reopened, c.BaseURL, c.Token, cfg, true)
	require.NoError(t, err)
	defer owned.Close()
	require.Zero(t, countA3(t, reopened, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
	require.Equal(t, before, a3ReservationDeathVector(t, reopened))
	owned.HTTP = &http.Client{Transport: server}
	attachTestClient(t, owned, c.UserID, "bundle-device")
	owned.HTTP = &http.Client{Transport: server}
	_, err = owned.PushPending(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, reopened)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, server.pushRequests[0], server.pushRequests[1])
	require.EqualValues(t, 1, countA3(t, reopened, `SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{"id":"Q"}'`))
	require.EqualValues(t, 3, countA3(t, reopened, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.EqualValues(t, 2, countA3(t, reopened, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	require.Zero(t, countA3(t, reopened, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Zero(t, countA3(t, reopened, `SELECT push_stage_entries+push_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state`))
	evidence, err := json.Marshal(map[string]any{"child_pid": child.Process.Pid, "signal": "SIGKILL", "held_entries": held, "reacquisition": "PASS", "complete_vector_preserved": true, "exact_frozen_replay": true})
	require.NoError(t, err)
	t.Log(string(evidence))
}

func a3ReservationDeathVector(t *testing.T, db *sql.DB) qualificationVector {
	t.Helper()
	vector := qualificationState(t, db, "nodes")
	for _, owner := range []string{"_sync_push_stage", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"} {
		vector[owner] = qualificationState(t, db, owner)[owner]
	}
	return vector
}
