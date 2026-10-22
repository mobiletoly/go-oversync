//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditHTTPRecoveryFixture struct {
	assertionPool             *pgxpool.Pool
	process                   *auditHTTPHelperProcess
	proxy                     *auditTCPProxy
	directClient, proxyClient *http.Client
	schemaName                string
}

func newAuditHTTPRecoveryFixture(t *testing.T) *auditHTTPRecoveryFixture {
	t.Helper()
	databaseURL, schemaName := newAuditHelperDatabase(t)
	process := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	proxy, err := newAuditTCPProxy("127.0.0.1:0", process.address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	direct, proxied := newAuditHTTPClient(), newAuditHTTPClient()
	t.Cleanup(direct.CloseIdleConnections)
	t.Cleanup(proxied.CloseIdleConnections)
	return &auditHTTPRecoveryFixture{newAuditProcessAssertionPool(t, databaseURL), process, proxy, direct, proxied, schemaName}
}

func auditResponseGateStatus(client *http.Client, baseURL string) (auditCommitResponseGateStatus, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+auditResponseGateStatusPath, nil)
	if err != nil {
		return auditCommitResponseGateStatus{}, err
	}
	result := runAuditProcessRequest(client, request)
	if result.err != nil {
		return auditCommitResponseGateStatus{}, result.err
	}
	var status auditCommitResponseGateStatus
	return status, json.Unmarshal(result.body, &status)
}

func interruptAuditHTTPResponseAfterEffect(t *testing.T, f *auditHTTPRecoveryFixture, operation, method, url, user, source string, body any, visible func() bool) {
	t.Helper()
	armed := doAuditProcessRequest(t, f.directClient, http.MethodPost, fmt.Sprintf("%s%s?operation=%s", f.process.URL(), auditResponseGateArmPath, operation), "", "", nil)
	require.Equal(t, http.StatusOK, armed.statusCode, string(armed.body))
	result := startAuditProcessRequest(t, f.proxyClient, method, url, user, source, body)
	require.Eventually(t, func() bool {
		status, err := auditResponseGateStatus(f.directClient, f.process.URL())
		return err == nil && status.Waiting && status.Operation == operation
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, f.proxy.Interrupt())
	select {
	case interrupted := <-result:
		require.Error(t, interrupted.err)
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted request did not return")
	}
	released := doAuditProcessRequest(t, f.directClient, http.MethodPost, f.process.URL()+auditResponseGateReleasePath, "", "", nil)
	require.Equal(t, http.StatusOK, released.statusCode, string(released.body))
}

func TestAuditHTTPRecovery_DirectPushReplayAndPullAfterConnectionLoss(t *testing.T) {
	ctx := context.Background()
	f := newAuditHTTPRecoveryFixture(t)
	push := prepareAuditHTTPPush(t, f.directClient, f.process.URL(), f.schemaName, "audit-http-retry-user", "audit-http-retry-source", 1, uuid.New())
	interruptAuditHTTPResponseAfterEffect(t, f, auditResponseGatePushCommit, http.MethodPost, "http://"+f.proxy.Address()+"/sync/push", push.userID, push.sourceID, push.request, func() bool {
		var n int64
		return f.assertionPool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log`).Scan(&n) == nil && n == 1
	})
	replay := doAuditProcessRequest(t, f.directClient, http.MethodPost, f.process.URL()+"/sync/push", push.userID, push.sourceID, push.request)
	require.Equal(t, http.StatusOK, replay.statusCode, string(replay.body))
	committed := decodeAuditHTTPPushOutcome(t, push, replay.body)
	require.EqualValues(t, 1, committed.Outcome.Committed.BundleSeq)
	pullPath := fmt.Sprintf("/sync/pull?after_bundle_seq=0&max_bundles=10&target_bundle_seq=%d", committed.Outcome.Committed.BundleSeq)
	control := doAuditProcessRequest(t, f.directClient, http.MethodGet, f.process.URL()+pullPath, push.userID, push.sourceID, nil)
	interruptAuditHTTPResponseAfterEffect(t, f, auditResponseGatePull, http.MethodGet, "http://"+f.proxy.Address()+pullPath, push.userID, push.sourceID, nil, func() bool { return true })
	retry := doAuditProcessRequest(t, f.directClient, http.MethodGet, f.process.URL()+pullPath, push.userID, push.sourceID, nil)
	require.Equal(t, http.StatusOK, control.statusCode)
	require.JSONEq(t, string(control.body), string(retry.body))
	requireAuditMetadataIntegrity(t, ctx, f.assertionPool)
}
