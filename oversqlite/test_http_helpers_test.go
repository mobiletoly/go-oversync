package oversqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
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

type staticResolver struct {
	result MergeResult
}

func (r *staticResolver) Resolve(conflict ConflictContext) MergeResult {
	if r == nil || r.result == nil {
		return AcceptServer{}
	}
	return r.result
}

func attachTestClient(t *testing.T, client *Client, userID, sourceID string) ConnectResult {
	t.Helper()

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

	require.NoError(t, client.Open(context.Background(), sourceID))
	result, err := client.Connect(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)
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
