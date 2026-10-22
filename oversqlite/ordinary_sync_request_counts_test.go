package oversqlite

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Ordinary public sync request-count regression for no-change and one-row work.
func TestOrdinarySync_RequestCounts(t *testing.T) {
	client, db := newA1SurvivorClient(t)
	server := client.HTTP.Transport.(*mockPushServer)
	initialAcks := len(server.ackRequests)
	var trace []string
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		trace = append(trace, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/sync/push" || r.URL.Path == "/sync/push-outcomes/ack" {
			return server.RoundTrip(r)
		}
		require.Equal(t, "GET /sync/pull", r.Method+" "+r.URL.Path)
		return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
	})}
	logState := func(label string) {
		raw, marshalErr := json.Marshal(map[string]any{
			"local":                  qualificationState(t, db, "nodes"),
			"wireRequests":           server.pushRequests,
			"remoteCommittedBundles": server.committedBySeq,
		})
		require.NoError(t, marshalErr)
		t.Logf("C03 %s complete state and wire: %s", label, raw)
	}
	beforeNoChange := qualificationState(t, db, "nodes")
	logState("before no-change")
	_, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"GET /sync/pull"}, trace)
	t.Logf("no-change public Sync: %v", trace)
	require.Equal(t, beforeNoChange, qualificationState(t, db, "nodes"))
	logState("after no-change")
	trace = nil
	_, err = db.Exec(`UPDATE nodes SET name='single-row edit' WHERE id='D'`)
	require.NoError(t, err)
	logState("before one-row")
	client.SetBeforePushReplayHook(func(ctx context.Context) error {
		var frozen, outcome int64
		if err := db.QueryRowContext(ctx, `SELECT length(request_body),length(outcome_body) FROM _sync_upload_journal`).Scan(&frozen, &outcome); err != nil {
			return err
		}
		t.Logf("ordinary one-row durable journal: F=%d bytes C=%d bytes (encoded bodies; excludes SQLite page/WAL overhead)", frozen, outcome)
		require.Positive(t, frozen)
		require.Positive(t, outcome)
		return nil
	})
	_, err = client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"POST /sync/push", "POST /sync/push-outcomes/ack", "GET /sync/pull"}, trace)
	t.Logf("one-row public Sync: %v", trace)
	logState("after one-row")
	dirty, outbound, err := client.pendingPushWorkCounts(context.Background())
	require.NoError(t, err)
	require.Zero(t, dirty)
	require.Zero(t, outbound)
	require.Len(t, server.ackRequests, initialAcks+1)
	require.Empty(t, server.resolveRequests)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
}
