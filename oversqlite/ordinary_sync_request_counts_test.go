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
	var trace []string
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		trace = append(trace, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/sync/push" {
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
		t.Logf("C07 %s complete state and wire: %s", label, raw)
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
	_, err = client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"POST /sync/push", "GET /sync/pull"}, trace)
	t.Logf("one-row public Sync: %v", trace)
	logState("after one-row")
	dirty, outbound, err := client.pendingPushWorkCounts(context.Background())
	require.NoError(t, err)
	require.Zero(t, dirty)
	require.Zero(t, outbound)
}
