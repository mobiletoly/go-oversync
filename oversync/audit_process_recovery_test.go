//go:build oversync_audit

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditProcessHTTPResult struct {
	statusCode int
	body       []byte
	err        error
}
type auditHTTPDirectPush struct {
	userID, sourceID string
	sourceBundleID   int64
	request          CompactPushRequest
	generation       SchemaGenerationCapabilities
}

func newAuditProcessRequest(t *testing.T, method, url, user, source string, body any) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err)
	req.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if user != "" {
		req.Header.Set(auditHelperUserIDHeader, user)
	}
	if source != "" {
		req.Header.Set(SourceIDHeader, source)
	}
	if value, ok := auditSchemaGenerationProofByAddress.Load(req.URL.Host); ok && req.URL.Path != "/sync/generations/1" {
		proof := value.(SchemaGenerationProof)
		req.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		req.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
	}
	return req
}

func runAuditProcessRequest(client *http.Client, request *http.Request) auditProcessHTTPResult {
	resp, err := client.Do(request)
	if err != nil {
		return auditProcessHTTPResult{err: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	return auditProcessHTTPResult{resp.StatusCode, body, readErr}
}
func doAuditProcessRequest(t *testing.T, client *http.Client, method, url, user, source string, body any) auditProcessHTTPResult {
	t.Helper()
	result := runAuditProcessRequest(client, newAuditProcessRequest(t, method, url, user, source, body))
	require.NoError(t, result.err)
	return result
}
func startAuditProcessRequest(t *testing.T, client *http.Client, method, url, user, source string, body any) <-chan auditProcessHTTPResult {
	t.Helper()
	request := newAuditProcessRequest(t, method, url, user, source, body)
	result := make(chan auditProcessHTTPResult, 1)
	go func() {
		result <- runAuditProcessRequest(client, request)
	}()
	return result
}

func newAuditProcessAssertionPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))
	return pool
}

func initializeAuditProcessScope(t *testing.T, client *http.Client, baseURL, user, source string) string {
	t.Helper()
	result := doAuditProcessRequest(t, client, http.MethodPost, baseURL+"/sync/connect", user, source, ConnectRequest{HasLocalPendingRows: false})
	require.Equal(t, http.StatusOK, result.statusCode, string(result.body))
	var response ConnectResponse
	require.NoError(t, json.Unmarshal(result.body, &response))
	require.Contains(t, []string{"initialize_empty", "remote_authoritative"}, response.Resolution)
	return response.InitializationID
}

func prepareAuditHTTPPush(t *testing.T, client *http.Client, baseURL, schema, user, source string, sourceBundleID int64, id uuid.UUID) auditHTTPDirectPush {
	t.Helper()
	init := initializeAuditProcessScope(t, client, baseURL, user, source)
	tables := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	proof := testSchemaGenerationProofForTables(t, tables)
	row := PushRequestRow{Schema: schema, Table: "users", Key: SyncKey{"id": id.String()}, Op: OpInsert, Payload: json.RawMessage(`{"id":"` + id.String() + `","name":"Process Recovery","email":"process-recovery@example.com"}`)}
	hash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{row})
	require.NoError(t, err)
	registry, err := CompileSchemaGenerationRegistry(tables, testSchemaGenerationsForRegisteredTables(tables))
	require.NoError(t, err)
	contract, ok := registry.Generation(proof.ID)
	require.True(t, ok)
	groups, err := CompactGroupsForRows(generationDescriptor(contract), []PushRequestRow{row})
	require.NoError(t, err)
	return auditHTTPDirectPush{
		userID: user, sourceID: source, sourceBundleID: sourceBundleID,
		request:    CompactPushRequest{SourceBundleID: sourceBundleID, CanonicalRequestHash: hash, InitializationID: init, Rows: groups},
		generation: generationDescriptor(contract),
	}
}

func decodeAuditHTTPPushOutcome(t *testing.T, push auditHTTPDirectPush, raw []byte) *RetainedPushOutcome {
	t.Helper()
	var outcome PushOutcome
	require.NoError(t, json.Unmarshal(raw, &outcome))
	var initializationID *string
	if push.request.InitializationID != "" {
		initializationID = &push.request.InitializationID
	}
	require.NoError(t, ValidatePushOutcomeBinding(outcome, PushOperationBinding{
		SourceID: push.sourceID, GenerationID: push.generation.ID,
		GenerationFingerprint: push.generation.Fingerprint, SourceBundleID: push.sourceBundleID,
		CanonicalRequestHash: push.request.CanonicalRequestHash, InitializationID: initializationID,
	}, push.generation))
	require.Equal(t, "committed", outcome.Status)
	require.NotNil(t, outcome.Committed)
	return &RetainedPushOutcome{Outcome: &outcome, encoded: append([]byte(nil), raw...)}
}

func sendAuditHTTPPush(t *testing.T, client *http.Client, baseURL string, push auditHTTPDirectPush) *RetainedPushOutcome {
	t.Helper()
	result := doAuditProcessRequest(t, client, http.MethodPost, baseURL+"/sync/push", push.userID, push.sourceID, push.request)
	require.Equal(t, http.StatusOK, result.statusCode, string(result.body))
	return decodeAuditHTTPPushOutcome(t, push, result.body)
}

func TestAuditProcessRecovery_DirectReplayAfterHelperRestart(t *testing.T) {
	ctx := context.Background()
	databaseURL, schema := newAuditHelperDatabase(t)
	pool := newAuditProcessAssertionPool(t, databaseURL)
	client := newAuditHTTPClient()
	t.Cleanup(client.CloseIdleConnections)
	first := startAuditHTTPHelperProcess(t, databaseURL, schema)
	push := prepareAuditHTTPPush(t, client, first.URL(), schema, "audit-process-user", "audit-process-source", 1, uuid.New())
	committed := sendAuditHTTPPush(t, client, first.URL(), push)
	require.NoError(t, first.Kill())
	second := startAuditHTTPHelperProcess(t, databaseURL, schema)
	replay := sendAuditHTTPPush(t, client, second.URL(), push)
	require.Equal(t, committed.Outcome, replay.Outcome)
	require.Equal(t, committed.encoded, replay.encoded, "helper restart must preserve the exact unacknowledged terminal envelope")
	requireAuditMetadataIntegrity(t, ctx, pool)
}
