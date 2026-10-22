package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestA3Replay_PhysicalRestart(t *testing.T) {
	c, db, dbPath := qualificationDurableClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	c.HTTP = &http.Client{Transport: server}
	c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.EqualValues(t, 2, countA3(t, db, `SELECT push_stage_entries FROM _sync_operation_state`))
	before := qualificationState(t, db, "nodes")
	config := cloneCoordinatorClientConfig(c.config)
	config.GenerationID = c.schemaGenerationProof.ID
	// Simulate the durable residue of a terminated buffer owner. There is no host
	// buffer. The ordinary reopen below must retain it; a fresh lease may reclaim.
	_, err = c.reserveWorkspace(context.Background(), applyUnit("crash-buffer", 0, ""), workspaceCharge{3, 100})
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	reopened, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	reopened.SetMaxOpenConns(1)
	ordinary, err := newClientFromDB(reopened, c.BaseURL, c.Token, config)
	require.NoError(t, err)
	require.Positive(t, countA3(t, reopened, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, ordinary.Close())
	owned, err := newClientFromDBWithOwnership(reopened, c.BaseURL, c.Token, config, true)
	require.NoError(t, err)
	defer owned.Close()
	require.Zero(t, countA3(t, reopened, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
	require.Equal(t, before, qualificationState(t, reopened, "nodes"))
	owned.HTTP = &http.Client{Transport: server}
	attachTestClient(t, owned, c.UserID, "bundle-device")
	owned.HTTP = &http.Client{Transport: server}
	_, err = owned.PushPending(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, reopened)
	requireA1ControlState(t, reopened, 3)
	require.Len(t, server.pushRequests, 1)
	require.Empty(t, server.resolveRequests)
	require.Len(t, server.ackRequests, 1)
	require.Zero(t, countA3(t, reopened, `SELECT push_stage_entries+push_stage_bytes FROM _sync_operation_state`))
}
