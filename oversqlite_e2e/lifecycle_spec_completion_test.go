package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_ConcurrentFirstInitializerRaceFollowThroughConvergesLoserViaRemoteAuthoritative(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_race_follow_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-initializer-race-followthrough-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowAID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowAID, "From A", "from-a@example.com")
	require.NoError(t, err)
	rowBID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowBID, "From B", "from-b@example.com")
	require.NoError(t, err)

	type clientState struct {
		client   *oversqlite.Client
		db       *sql.DB
		sourceID string
		localID  string
		remoteID string
	}

	states := map[string]*clientState{
		"device-a": {client: clientA, db: dbA, sourceID: "device-a", localID: rowAID, remoteID: rowBID},
		"device-b": {client: clientB, db: dbB, sourceID: "device-b", localID: rowBID, remoteID: rowAID},
	}

	type connectResult struct {
		sourceID string
		result   oversqlite.ConnectResult
		err      error
	}

	start := make(chan struct{})
	results := make(chan connectResult, 2)
	var wg sync.WaitGroup
	for sourceID, state := range states {
		wg.Add(1)
		go func(sourceID string, state *clientState) {
			defer wg.Done()
			<-start
			result, err := state.client.Connect(ctx, userID)
			results <- connectResult{sourceID: sourceID, result: result, err: err}
		}(sourceID, state)
	}

	close(start)
	wg.Wait()
	close(results)

	var (
		winner *clientState
		loser  *clientState
	)
	for result := range results {
		require.NoError(t, result.err)
		switch {
		case result.result.Status == oversqlite.ConnectStatusConnected && result.result.Outcome == oversqlite.ConnectOutcomeSeededLocal:
			require.Nil(t, winner)
			winner = states[result.sourceID]
		case result.result.Status == oversqlite.ConnectStatusRetryLater:
			require.Nil(t, loser)
			loser = states[result.sourceID]
		default:
			t.Fatalf("unexpected race result for %s: %+v", result.sourceID, result.result)
		}
	}
	require.NotNil(t, winner)
	require.NotNil(t, loser)

	require.NoError(t, winner.client.PushPending(ctx))

	loserReconnect, err := loser.client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, loserReconnect.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, loserReconnect.Outcome)

	var (
		winnerCount int
		loserCount  int
		name        string
	)
	require.NoError(t, loser.db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, winner.localID).Scan(&winnerCount))
	require.NoError(t, loser.db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, loser.localID).Scan(&loserCount))
	require.Equal(t, 1, winnerCount)
	require.Equal(t, 0, loserCount)
	require.NoError(t, loser.db.QueryRow(`SELECT name FROM users WHERE id = ?`, winner.localID).Scan(&name))
	require.Contains(t, name, "From")

	var remoteCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users`, schema)).Scan(&remoteCount))
	require.Equal(t, 1, remoteCount)
}

func TestEndToEnd_RawConnectSameSourceReconnectDuringInitializationReusesInitializationIDAndRefreshesLease(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_raw_refresh_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(cfg *exampleserver.ServerConfig) {
		cfg.InitializationLeaseTTL = 2 * time.Second
	})

	userID := "e2e-raw-connect-refresh-user-" + uuid.NewString()
	first := rawConnect(t, server, userID, "device-a", true)
	require.Equal(t, "initialize_local", first.Resolution)
	require.NotEmpty(t, first.InitializationID)

	firstLease, err := time.Parse(time.RFC3339Nano, first.LeaseExpiresAt)
	require.NoError(t, err)

	time.Sleep(25 * time.Millisecond)

	second := rawConnect(t, server, userID, "device-a", true)
	require.Equal(t, "initialize_local", second.Resolution)
	require.Equal(t, first.InitializationID, second.InitializationID)

	secondLease, err := time.Parse(time.RFC3339Nano, second.LeaseExpiresAt)
	require.NoError(t, err)
	require.True(t, secondLease.After(firstLease), "expected refreshed lease %s to be after %s", secondLease, firstLease)

	var (
		initializationID string
		leaseExpiresAt   time.Time
	)
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT initialization_id, lease_expires_at
		FROM sync.scope_state
		WHERE user_id = $1
	`, userID).Scan(&initializationID, &leaseExpiresAt))
	require.Equal(t, first.InitializationID, initializationID)
	require.True(t, leaseExpiresAt.After(firstLease))
}

func TestEndToEnd_RawConnectSameSourceWithoutPendingRowsTransitionsToInitializeEmpty(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_raw_abandon_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-raw-connect-abandon-user-" + uuid.NewString()
	first := rawConnect(t, server, userID, "device-a", true)
	require.Equal(t, "initialize_local", first.Resolution)

	second := rawConnect(t, server, userID, "device-a", false)
	require.Equal(t, "initialize_empty", second.Resolution)
	require.Empty(t, second.InitializationID)

	var (
		state               string
		initializedBySource string
	)
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT state, initialized_by_source_id
		FROM sync.scope_state
		WHERE user_id = $1
	`, userID).Scan(&state, &initializedBySource))
	require.Equal(t, "INITIALIZED", state)
	require.Equal(t, "device-a", initializedBySource)

	snapshot := rawCreateSnapshotSession(t, server, userID, "device-a", http.StatusOK)
	require.Equal(t, int64(0), snapshot.RowCount)
}

func TestEndToEnd_RawInitializerCrashBeforeCommitDoesNotLeakAuthoritativeRowsAndLaterCallerRecovers(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_raw_crash_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(cfg *exampleserver.ServerConfig) {
		cfg.InitializationLeaseTTL = 200 * time.Millisecond
	})

	userID := "e2e-raw-initializer-crash-user-" + uuid.NewString()
	firstConnect := rawConnect(t, server, userID, "device-a", true)
	require.Equal(t, "initialize_local", firstConnect.Resolution)

	crashedRowID := uuid.NewString()
	createResp := rawCreatePushSession(t, server, userID, "device-a", http.StatusOK, oversync.PushSessionCreateRequest{
		SourceID:         "device-a",
		SourceBundleID:   1,
		PlannedRowCount:  1,
		InitializationID: firstConnect.InitializationID,
	})
	require.Equal(t, "staging", createResp.Status)
	rawUploadPushChunk(t, server, userID, "device-a", createResp.PushID, http.StatusOK, oversync.PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows:            []oversync.PushRequestRow{rawUserInsertRow(schema, crashedRowID, "Crashed Seed")},
	})

	var (
		scopeState       string
		stagedCount      int
		authoritativeCnt int
	)
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT state FROM sync.scope_state WHERE user_id = $1`, userID).Scan(&scopeState))
	require.Equal(t, "INITIALIZING", scopeState)
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.push_session_rows WHERE push_id = $1::uuid`, createResp.PushID).Scan(&stagedCount))
	require.Equal(t, 1, stagedCount)
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, schema), crashedRowID).Scan(&authoritativeCnt))
	require.Equal(t, 0, authoritativeCnt)

	time.Sleep(300 * time.Millisecond)

	recoveryConnect := rawConnect(t, server, userID, "device-b", true)
	require.Equal(t, "initialize_local", recoveryConnect.Resolution)
	require.NotEmpty(t, recoveryConnect.InitializationID)
	require.NotEqual(t, firstConnect.InitializationID, recoveryConnect.InitializationID)

	recoveredRowID := uuid.NewString()
	recoverySession := rawCreatePushSession(t, server, userID, "device-b", http.StatusOK, oversync.PushSessionCreateRequest{
		SourceID:         "device-b",
		SourceBundleID:   1,
		PlannedRowCount:  1,
		InitializationID: recoveryConnect.InitializationID,
	})
	require.Equal(t, "staging", recoverySession.Status)
	rawUploadPushChunk(t, server, userID, "device-b", recoverySession.PushID, http.StatusOK, oversync.PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows:            []oversync.PushRequestRow{rawUserInsertRow(schema, recoveredRowID, "Recovered Seed")},
	})
	rawCommitPushSession(t, server, userID, "device-b", recoverySession.PushID, http.StatusOK)

	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, schema), crashedRowID).Scan(&authoritativeCnt))
	require.Equal(t, 0, authoritativeCnt)
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, schema), recoveredRowID).Scan(&authoritativeCnt))
	require.Equal(t, 1, authoritativeCnt)
}

func TestEndToEnd_RawServerRejectsPushAndSnapshotBeforeInitialization(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_raw_scope_uninit_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-raw-scope-uninitialized-user-" + uuid.NewString()
	pushErr := rawCreatePushSessionError(t, server, userID, "device-a", http.StatusConflict, oversync.PushSessionCreateRequest{
		SourceID:        "device-a",
		SourceBundleID:  1,
		PlannedRowCount: 1,
	})
	require.Equal(t, "scope_uninitialized", pushErr.Error)

	snapshotErr := rawCreateSnapshotSessionError(t, server, userID, "device-a", http.StatusConflict)
	require.Equal(t, "scope_uninitialized", snapshotErr.Error)

	var scopeRows int
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.scope_state WHERE user_id = $1`, userID).Scan(&scopeRows))
	require.LessOrEqual(t, scopeRows, 1)
}

func rawConnect(t *testing.T, server *exampleserver.TestServer, userID, sourceID string, hasLocalPendingRows bool) oversync.ConnectResponse {
	t.Helper()
	var resp oversync.ConnectResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/connect", userID, sourceID, http.StatusOK, oversync.ConnectRequest{
		SourceID:            sourceID,
		HasLocalPendingRows: hasLocalPendingRows,
	}, &resp)
	return resp
}

func rawCreatePushSession(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID string,
	expectedStatus int,
	reqBody oversync.PushSessionCreateRequest,
) oversync.PushSessionCreateResponse {
	t.Helper()
	var resp oversync.PushSessionCreateResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/push-sessions", userID, sourceID, expectedStatus, reqBody, &resp)
	return resp
}

func rawCreatePushSessionError(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID string,
	expectedStatus int,
	reqBody oversync.PushSessionCreateRequest,
) oversync.ErrorResponse {
	t.Helper()
	var resp oversync.ErrorResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/push-sessions", userID, sourceID, expectedStatus, reqBody, &resp)
	return resp
}

func rawUploadPushChunk(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID, pushID string,
	expectedStatus int,
	reqBody oversync.PushSessionChunkRequest,
) oversync.PushSessionChunkResponse {
	t.Helper()
	var resp oversync.PushSessionChunkResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/push-sessions/"+pushID+"/chunks", userID, sourceID, expectedStatus, reqBody, &resp)
	return resp
}

func rawCommitPushSession(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID, pushID string,
	expectedStatus int,
) oversync.PushSessionCommitResponse {
	t.Helper()
	var resp oversync.PushSessionCommitResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/push-sessions/"+pushID+"/commit", userID, sourceID, expectedStatus, nil, &resp)
	return resp
}

func rawCreateSnapshotSession(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID string,
	expectedStatus int,
) oversync.SnapshotSession {
	t.Helper()
	var resp oversync.SnapshotSession
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/snapshot-sessions", userID, sourceID, expectedStatus, nil, &resp)
	return resp
}

func rawCreateSnapshotSessionError(
	t *testing.T,
	server *exampleserver.TestServer,
	userID, sourceID string,
	expectedStatus int,
) oversync.ErrorResponse {
	t.Helper()
	var resp oversync.ErrorResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/snapshot-sessions", userID, sourceID, expectedStatus, nil, &resp)
	return resp
}

func rawDoSyncJSON(
	t *testing.T,
	server *exampleserver.TestServer,
	method, path, userID, sourceID string,
	expectedStatus int,
	reqBody any,
	respBody any,
) {
	t.Helper()

	var body io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		require.NoError(t, err)
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, server.URL()+path, body)
	require.NoError(t, err)
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	token, err := server.GenerateToken(userID, sourceID, time.Hour)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	rawResp, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, expectedStatus, resp.StatusCode, "response body: %s", string(rawResp))

	if respBody == nil || len(rawResp) == 0 {
		return
	}
	require.NoError(t, json.Unmarshal(rawResp, respBody), "response body: %s", string(rawResp))
}

func rawUserInsertRow(schema, rowID, name string) oversync.PushRequestRow {
	payload, err := json.Marshal(map[string]any{
		"id":    rowID,
		"name":  name,
		"email": strings.ToLower(strings.ReplaceAll(name, " ", ".")) + "@example.com",
	})
	if err != nil {
		panic(err)
	}
	return oversync.PushRequestRow{
		Schema:         schema,
		Table:          "users",
		Key:            oversync.SyncKey{"id": rowID},
		Op:             oversync.OpInsert,
		BaseRowVersion: 0,
		Payload:        payload,
	}
}
