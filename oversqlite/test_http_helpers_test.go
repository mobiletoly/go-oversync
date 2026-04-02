package oversqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func errorJSONResponse(status int, v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func requireAuthenticatedSyncHeaders(t *testing.T, r *http.Request, sourceID string) {
	t.Helper()
	require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
	if sourceID == "" {
		require.NotEmpty(t, r.Header.Get(oversync.SourceIDHeader))
		return
	}
	require.Equal(t, sourceID, r.Header.Get(oversync.SourceIDHeader))
}

type staticResolver struct {
	result MergeResult
}

func (r *staticResolver) Resolve(conflict ConflictContext) MergeResult {
	if r == nil || r.result == nil {
		return AcceptServer{}
	}
	return r.result
}

func attachTestClient(t *testing.T, client *Client, userID, sourceID string) AttachResult {
	t.Helper()
	if strings.TrimSpace(sourceID) != "" {
		client.sourceIDGenerator = func() string { return sourceID }
	}

	prevHTTP := client.HTTP
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})}
	t.Cleanup(func() {
		client.HTTP = prevHTTP
	})

	mustOpen(t, client, context.Background())
	result, err := client.Attach(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	return result
}

func tokenProviderForTests(_ context.Context) (string, error) {
	return "token", nil
}

func newTestRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		JitterFraction: 0,
	}
}
