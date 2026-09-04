package oversqlite

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRetirement_ConflictAndUncertainIntentConformance(t *testing.T) {
	raw, err := os.ReadFile("../conformance/rollout-retirement/v1/intent.json")
	require.NoError(t, err)
	var corpus struct {
		Version, Status    int
		Error, Remediation string
		Retryable          bool
		Cases              []struct {
			ID       string
			Initial  string `json:"initial_state"`
			Requests int    `json:"requests_after_retirement"`
			Outcome  string
		}
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.Len(t, corpus.Cases, 2)
	for _, vector := range corpus.Cases {
		t.Run(vector.ID, func(t *testing.T) {
			ctx := context.Background()
			var client *Client
			var id string
			if vector.Initial == "conflicted" {
				client, id = pendingConflictFixture(t)
			} else {
				client, _ = newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
				client.config.RetryPolicy = &RetryPolicy{Enabled: false}
				id = uuid.NewString()
				_, err := client.db.Exec(`INSERT INTO users VALUES(?,'Offline','offline@example.invalid')`, id)
				require.NoError(t, err)
				remote := newMockPushServer(t)
				accepted := false
				client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if accepted {
						return nil, io.ErrUnexpectedEOF
					}
					response, err := remote.RoundTrip(r)
					if err == nil && r.Method == http.MethodPost && r.URL.Path == "/sync/push" {
						accepted = true
						if response != nil {
							response.Body.Close()
						}
						return nil, io.ErrUnexpectedEOF
					}
					return response, err
				})}
				_, err = client.PushPending(ctx)
				require.Error(t, err)
				require.True(t, accepted)
			}
			before := requireOutboxBundle(t, client.db)
			source := client.sourceID
			var body string
			require.NoError(t, client.db.QueryRow(`SELECT name FROM users WHERE id=?`, id).Scan(&body))
			db, cfg := client.db, cloneCoordinatorClientConfig(client.config)
			require.NoError(t, client.Close())
			restarted, err := newAdmittedFixtureClient(db, client.BaseURL, tokenProviderForTests, cfg)
			require.NoError(t, err)
			defer restarted.Close()
			// Reconstruct process-local attachment only after the original persistent
			// attachment has been read; no source is generated or file replaced.
			require.NoError(t, restarted.open(ctx))
			attachment, err := loadAttachmentState(ctx, db)
			require.NoError(t, err)
			restarted.sourceID = attachment.CurrentSourceID
			restarted.UserID = attachment.AttachedUserID
			restarted.sessionConnected = true
			requests := 0
			restarted.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
				requests++
				return errorJSONResponse(corpus.Status, map[string]any{"error": corpus.Error, "retryable": corpus.Retryable, "remediation": corpus.Remediation}), nil
			})}
			_, err = restarted.PushPending(ctx)
			if vector.Initial == "conflicted" {
				var conflictErr *ConflictNeedsResolutionError
				require.ErrorAs(t, err, &conflictErr)
				pending, err := restarted.PendingConflict(ctx)
				require.NoError(t, err)
				require.NotNil(t, pending)
			} else {
				var retired *oversync.SchemaGenerationRequestError
				require.ErrorAs(t, err, &retired)
				require.Equal(t, vector.Outcome, string(retired.Code))
			}
			require.Equal(t, vector.Requests, requests)
			require.Equal(t, before, requireOutboxBundle(t, db))
			require.Equal(t, source, restarted.sourceID)
			var after string
			require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, id).Scan(&after))
			require.Equal(t, body, after)
		})
	}
}
