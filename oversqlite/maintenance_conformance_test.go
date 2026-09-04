package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func maintenanceScenario(t *testing.T, id string) struct {
	Initial   map[string]json.RawMessage
	Expected  map[string]json.RawMessage
	Exchanges []struct {
		Status int
		Body   oversync.ErrorResponse
	}
} {
	t.Helper()
	data, err := os.ReadFile("../conformance/maintenance-conflicts/v2/scenarios.json")
	require.NoError(t, err)
	type scenario struct {
		ID        string
		Initial   map[string]json.RawMessage
		Expected  map[string]json.RawMessage
		Exchanges []struct {
			Status int
			Body   oversync.ErrorResponse
		}
	}
	var corpus struct{ Scenarios []scenario }
	require.NoError(t, json.Unmarshal(data, &corpus))
	for _, s := range corpus.Scenarios {
		if s.ID == id {
			return struct {
				Initial   map[string]json.RawMessage
				Expected  map[string]json.RawMessage
				Exchanges []struct {
					Status int
					Body   oversync.ErrorResponse
				}
			}{s.Initial, s.Expected, s.Exchanges}
		}
	}
	t.Fatal("missing scenario " + id)
	panic("unreachable")
}

func TestMaintenanceConformance_OrdinaryConflicts(t *testing.T) {
	fixture := maintenanceScenario(t, "M6-C01")
	var generations []int64
	var operations []string
	var wantsResolution bool
	require.NoError(t, json.Unmarshal(fixture.Initial["generations"], &generations))
	require.NoError(t, json.Unmarshal(fixture.Initial["operations"], &operations))
	require.NoError(t, json.Unmarshal(fixture.Expected["needs_resolution"], &wantsResolution))
	for _, generation := range generations {
		for _, op := range operations {
			t.Run(fmt.Sprintf("generation-%d/%s", generation, op), func(t *testing.T) {
				proof := testSchemaGenerationProof()
				proof.ID = oversync.SchemaGenerationID(generation)
				cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, proof)
				cfg.RetryPolicy = newTestRetryPolicy()
				c, db := newBundleClientWithConfig(t, cfg, usersTestDDL)
				id := uuid.NewString()
				if op != oversync.OpInsert {
					seedSyncedUserRow(t, c, db, id, "Original", 1)
				}
				var err error
				switch op {
				case oversync.OpInsert:
					_, err = db.Exec(`INSERT INTO users VALUES(?,?,?)`, id, "Offline", "ada@example.com")
				case oversync.OpUpdate:
					_, err = db.Exec(`UPDATE users SET name='Offline' WHERE id=?`, id)
				case oversync.OpDelete:
					_, err = db.Exec(`DELETE FROM users WHERE id=?`, id)
				}
				require.NoError(t, err)
				server := newMockPushServer(t)
				server.pushHook = func(s *mockDirectPush) *http.Response {
					return structuredConflictResponse(s.Rows[0], 2, false, map[string]any{"id": id, "name": "Server", "email": "ada@example.com"})
				}
				c.HTTP = &http.Client{Transport: server}
				_, err = c.PushPending(context.Background())
				var needs *ConflictNeedsResolutionError
				require.ErrorAs(t, err, &needs)
				status, err := c.SyncStatus(context.Background())
				require.NoError(t, err)
				require.Equal(t, wantsResolution, status.NeedsResolution)
				require.Equal(t, op, needs.Pending.Conflict.LocalOp)
				before := requireOutboxBundle(t, db)
				_, err = c.PushPending(context.Background())
				require.ErrorAs(t, err, &needs)
				require.Equal(t, before, requireOutboxBundle(t, db))
				var count int
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE id=?`, id).Scan(&count))
				if op == oversync.OpDelete {
					require.Zero(t, count)
				} else {
					name, _, _ := loadUserForKey(t, db, id)
					require.Equal(t, "Offline", name)
				}
			})
		}
	}
}

func TestMaintenanceConformance_TemporaryRejection(t *testing.T) {
	fixture := maintenanceScenario(t, "M6-C03")
	require.Len(t, fixture.Exchanges, 1)
	response := fixture.Exchanges[0]
	for _, boundary := range []string{"push"} {
		t.Run(boundary, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			_, err := db.Exec(`INSERT INTO users VALUES(?,?,?)`, uuid.NewString(), "Offline", "ada@example.com")
			require.NoError(t, err)
			server := newMockPushServer(t)
			var attempts int
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				hit := boundary == "push" && r.Method == http.MethodPost && r.URL.Path == "/sync/push"
				if hit {
					attempts++
					return errorJSONResponse(response.Status, response.Body), nil
				}
				return server.RoundTrip(r)
			})}
			_, err = c.PushPending(context.Background())
			require.Error(t, err)
			require.Positive(t, attempts)
			before := requireOutboxBundle(t, db)
			require.NotEmpty(t, before.CanonicalRequestHash)
			_, err = c.PushPending(context.Background())
			require.Error(t, err)
			after := requireOutboxBundle(t, db)
			require.Equal(t, before.CanonicalRequestHash, after.CanonicalRequestHash)
			require.Equal(t, before.SourceBundleID, after.SourceBundleID)
		})
	}
}
