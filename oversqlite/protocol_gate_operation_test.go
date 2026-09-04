package oversqlite

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const hostileProtocolVersion = "HOSTILE_PROTOCOL_VERSION_SECRET"

func TestPushPending_UsesCachedAdmissionWithoutCapabilityPreflight(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "local-1", "Local", "local@example.com")
	require.NoError(t, err)
	server := newMockPushServer(t)
	var capabilityRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/generations/1" {
			capabilityRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "retired_route"}), nil
		}
		return server.RoundTrip(r)
	})}

	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Zero(t, capabilityRequests.Load())
	require.Len(t, server.pushRequests, 1)
}
