package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// TestSchemaAwareHandlers verifies that table handlers are properly isolated by schema
// This test ensures that handlers for "schema1.users" and "schema2.users" don't collide
func TestSchemaAwareHandlers(t *testing.T) {
	// Setup test database
	ctx := context.Background()
	pool := setupTestDB(t, ctx)
	defer pool.Close()

	// Create two schemas with identical table names
	_, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS schema1;
		CREATE SCHEMA IF NOT EXISTS schema2;
		
		CREATE TABLE IF NOT EXISTS schema1.users (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT DEFAULT 'schema1'
		);
		
		CREATE TABLE IF NOT EXISTS schema2.users (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT DEFAULT 'schema2'
		);
	`)
	require.NoError(t, err)

	// Create test handlers that track which schema they handle
	schema1Handler := &TestSchemaHandler{schema: "schema1", calls: make([]string, 0)}
	schema2Handler := &TestSchemaHandler{schema: "schema2", calls: make([]string, 0)}

	// Create sync service with handlers registered in config
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	config := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "schema-collision-test",
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "schema1", Table: "users", Handler: schema1Handler},
			{Schema: "schema2", Table: "users", Handler: schema2Handler},
		},
		DisableAutoMigrateFKs: true, // Skip FK migration for this test
	}

	service, err := oversync.NewSyncService(pool, config, logger)
	require.NoError(t, err)
	defer service.Close()

	// Handlers are now registered via ServiceConfig - no need for separate registration

	// Test that changes to schema1.users only call schema1 handler
	userID1 := uuid.New()
	sourceID1 := uuid.New().String() // Use unique source ID
	payload1, _ := json.Marshal(map[string]interface{}{
		"id":   userID1.String(),
		"name": "User in Schema1",
		"type": "schema1",
	})

	change1 := oversync.ChangeUpload{
		SourceChangeID: 1,
		Schema:         "schema1",
		Table:          "users",
		PK:             userID1.String(),
		Op:             "INSERT",
		Payload:        payload1,
	}

	request1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           []oversync.ChangeUpload{change1},
	}

	// Process upload for schema1
	response, err := service.ProcessUpload(ctx, "test-user", sourceID1, request1)
	require.NoError(t, err)
	require.Len(t, response.Statuses, 1)
	require.Equal(t, "applied", response.Statuses[0].Status)

	// Verify only schema1 handler was called
	require.Len(t, schema1Handler.calls, 1, "Schema1 handler should be called once")
	require.Contains(t, schema1Handler.calls[0], "schema1.users")
	require.Len(t, schema2Handler.calls, 0, "Schema2 handler should not be called")

	// Test that changes to schema2.users only call schema2 handler
	userID2 := uuid.New()
	sourceID2 := uuid.New().String() // Use unique source ID
	payload2, _ := json.Marshal(map[string]interface{}{
		"id":   userID2.String(),
		"name": "User in Schema2",
		"type": "schema2",
	})

	change2 := oversync.ChangeUpload{
		SourceChangeID: 1, // Use same source_change_id but different source_id
		Schema:         "schema2",
		Table:          "users",
		PK:             userID2.String(),
		Op:             "INSERT",
		Payload:        payload2,
	}

	request2 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           []oversync.ChangeUpload{change2},
	}

	// Process upload for schema2
	response, err = service.ProcessUpload(ctx, "test-user", sourceID2, request2)
	require.NoError(t, err)
	require.Len(t, response.Statuses, 1)
	require.Equal(t, "applied", response.Statuses[0].Status)

	// Verify only schema2 handler was called (schema1 handler call count unchanged)
	require.Len(t, schema1Handler.calls, 1, "Schema1 handler should still have only 1 call")
	require.Len(t, schema2Handler.calls, 1, "Schema2 handler should be called once")
	require.Contains(t, schema2Handler.calls[0], "schema2.users")

	// Verify data was materialized to correct schemas by checking the specific records we created
	var schema1HasOurUser, schema2HasOurUser bool

	// Check if our specific user exists in schema1 with correct type
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema1.users WHERE id = $1 AND type = 'schema1')", userID1).Scan(&schema1HasOurUser)
	require.NoError(t, err)
	require.True(t, schema1HasOurUser, "Schema1 should have our user with correct type")

	// Check if our specific user exists in schema2 with correct type
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema2.users WHERE id = $1 AND type = 'schema2')", userID2).Scan(&schema2HasOurUser)
	require.NoError(t, err)
	require.True(t, schema2HasOurUser, "Schema2 should have our user with correct type")

	// Verify cross-contamination didn't happen
	var schema1HasWrongUser, schema2HasWrongUser bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema1.users WHERE id = $1)", userID2).Scan(&schema1HasWrongUser)
	require.NoError(t, err)
	require.False(t, schema1HasWrongUser, "Schema1 should NOT have schema2's user")

	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema2.users WHERE id = $1)", userID1).Scan(&schema2HasWrongUser)
	require.NoError(t, err)
	require.False(t, schema2HasWrongUser, "Schema2 should NOT have schema1's user")

	t.Log("✅ Schema-aware handlers working correctly - no cross-schema contamination")
}

// TestSchemaHandler is a test implementation that tracks which schema.table it handles
type TestSchemaHandler struct {
	schema string
	calls  []string
}

func (h *TestSchemaHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	call := fmt.Sprintf("INSERT/UPDATE %s.%s pk=%s", schema, table, pk.String())
	h.calls = append(h.calls, call)

	// Parse payload to get data
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("failed to parse payload: %w", err)
	}

	// Materialize to the correct schema
	query := fmt.Sprintf(`
		INSERT INTO %s.%s (id, name, type)
		VALUES (@id, @name, @type)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			type = EXCLUDED.type
	`, schema, table)

	_, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"id":   pk,
		"name": data["name"],
		"type": data["type"],
	})
	return err
}

func (h *TestSchemaHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	call := fmt.Sprintf("DELETE %s.%s pk=%s", schema, table, pk.String())
	h.calls = append(h.calls, call)

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE id = @id", schema, table)
	_, err := tx.Exec(ctx, query, pgx.NamedArgs{"id": pk})
	return err
}

func (h *TestSchemaHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *TestSchemaHandler) ParsePrimaryKey(pkString string) (uuid.UUID, error) {
	return uuid.Parse(pkString)
}

// setupTestDB creates a test database connection for schema collision tests
func setupTestDB(t *testing.T, ctx context.Context) *pgxpool.Pool {
	databaseURL := "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"

	config, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)

	// Test connection
	err = pool.Ping(ctx)
	require.NoError(t, err)

	return pool
}
