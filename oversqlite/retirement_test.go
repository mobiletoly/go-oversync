package oversqlite

import (
	"context"
	"encoding/json"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetirement_TypedResponse(t *testing.T) {
	proof := oversync.SchemaGenerationProof{ID: 1, Fingerprint: strings.Repeat("a", 64)}
	body := []byte(`{"error":"retired_schema_generation","message":"untrusted payload","retryable":false,"remediation":"upgrade_application"}`)
	err := decodeSchemaGenerationRequestError(426, body, proof)
	var typed *oversync.SchemaGenerationRequestError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, oversync.SchemaGenerationRetired, typed.Code)
	require.NotContains(t, err.Error(), "untrusted")
	for _, code := range []int{200, 409, 410, 503} {
		require.Nil(t, decodeSchemaGenerationRequestError(code, body, proof))
	}
}

func TestRetirement_ConformanceStopsLoopsAndPreservesIntent(t *testing.T) {
	raw, err := os.ReadFile("../conformance/rollout-retirement/v1/retired-client.json")
	require.NoError(t, err)
	var corpus struct {
		Version int
		Cases   []struct {
			ID, Operation, Error, Remediation string
			Status, Requests                  int
			Retryable                         bool
		}
		Intent struct{ ID, Name, Email string } `json:"local_intent"`
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.Equal(t, 1, corpus.Version)
	require.Len(t, corpus.Cases, 4)
	for _, vector := range corpus.Cases {
		t.Run(vector.ID, func(t *testing.T) {
			cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
			cfg.RetryPolicy = newTestRetryPolicy()
			cfg.BackoffMin = time.Millisecond
			cfg.BackoffMax = time.Millisecond
			c, db := newBundleClientWithConfig(t, cfg, usersTestDDL)
			_, err := db.Exec(`INSERT INTO users VALUES(?,?,?)`, corpus.Intent.ID, corpus.Intent.Name, corpus.Intent.Email)
			require.NoError(t, err)
			source := c.sourceID
			before, err := c.LastBundleSeqSeen(context.Background())
			require.NoError(t, err)
			var requests atomic.Int32
			c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				return errorJSONResponse(vector.Status, map[string]any{"error": vector.Error, "retryable": vector.Retryable, "remediation": vector.Remediation}), nil
			})}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch vector.Operation {
				case "upload":
					c.uploaderLoop(ctx)
				case "download":
					c.downloaderLoop(ctx)
				case "watch":
					c.watchAwareDownloaderLoop(ctx)
				case "watch_auto":
					c.config.BundleChangeWatchMode = BundleChangeWatchAuto
					c.startDownloaderLoop(ctx)
				}
			}()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("retired generation did not stop automatic loop")
			}
			require.EqualValues(t, vector.Requests, requests.Load())
			require.Equal(t, source, c.sourceID)
			after, err := c.LastBundleSeqSeen(context.Background())
			require.NoError(t, err)
			require.Equal(t, before, after)
			var name, email string
			require.NoError(t, db.QueryRow(`SELECT name,email FROM users WHERE id=?`, corpus.Intent.ID).Scan(&name, &email))
			require.Equal(t, corpus.Intent.Name, name)
			require.Equal(t, corpus.Intent.Email, email)
			var pending int
			require.NoError(t, db.QueryRow(`SELECT (SELECT count(*) FROM _sync_dirty_rows)+(SELECT count(*) FROM _sync_outbox_rows)`).Scan(&pending))
			require.Positive(t, pending)
			action, err := c.ApplicationActionRequired(context.Background())
			require.NoError(t, err)
			require.True(t, action.Required())
			require.Equal(t, ApplicationActionUpgrade, action.Action)
			require.Equal(t, c.schemaGenerationProof, action.Generation)

			require.NoError(t, c.Close())
			restarted, err := newAdmittedFixtureClient(db, c.BaseURL, c.Token, c.config)
			require.NoError(t, err)
			restartedAction, err := restarted.ApplicationActionRequired(context.Background())
			require.NoError(t, err)
			require.Equal(t, action, restartedAction)
		})
	}
}
