package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRetirement_MaintenanceAndUploadBoundaries(t *testing.T) {
	raw, err := os.ReadFile("../conformance/rollout-retirement/v1/boundaries.json")
	require.NoError(t, err)
	var corpus struct {
		Version int
		Cases   []struct {
			ID, Boundary, Error, Remediation string
			Status                           int
			Retryable                        bool
		}
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.Len(t, corpus.Cases, 8)
	for _, vector := range corpus.Cases {
		t.Run(vector.ID, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			source := client.sourceID
			_, err := db.Exec(`INSERT INTO users VALUES(?,?,?)`, uuid.NewString(), "Offline", "offline@example.invalid")
			require.NoError(t, err)
			remote := newMockPushServer(t)
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				hit := vector.Boundary == "push" && r.Method == http.MethodPost && r.URL.Path == "/sync/push"
				if hit {
					return errorJSONResponse(vector.Status, map[string]any{"error": vector.Error, "retryable": vector.Retryable, "remediation": vector.Remediation}), nil
				}
				return remote.RoundTrip(r)
			})}
			_, err = client.PushPending(context.Background())
			require.Error(t, err)
			var typed *oversync.SchemaGenerationRequestError
			require.Equal(t, vector.Status == 426, errors.As(err, &typed))
			if typed != nil {
				require.Equal(t, vector.Error, string(typed.Code))
				require.Equal(t, UpgradeOutcomeOldGenerationUnavailable, classifyUpgradeCause(err, false))
				require.Equal(t, UpgradeOutcomeTargetGenerationUnavailable, classifyUpgradeCause(err, true))
			}
			before := requireOutboxBundle(t, db)
			require.NotEmpty(t, before.CanonicalRequestHash)
			_, err = client.PushPending(context.Background())
			require.Error(t, err)
			after := requireOutboxBundle(t, db)
			require.Equal(t, before.CanonicalRequestHash, after.CanonicalRequestHash)
			require.Equal(t, before.SourceBundleID, after.SourceBundleID)
			require.Equal(t, source, client.sourceID)
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE name='Offline'`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}
