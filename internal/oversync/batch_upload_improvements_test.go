package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupBatchTestDB creates a test database with FK relationships for batch upload testing
func setupBatchTestDB(t *testing.T) (*pgxpool.Pool, *oversync.SyncService) {
	// Use test database connection
	pool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	require.NoError(t, err)

	// Clean up any existing test tables and sync data
	ctx := context.Background()
	cleanupSQL := []string{
		"DROP TABLE IF EXISTS batch_test.comments CASCADE",
		"DROP TABLE IF EXISTS batch_test.posts CASCADE",
		"DROP TABLE IF EXISTS batch_test.users CASCADE",
		"DROP SCHEMA IF EXISTS batch_test CASCADE",
		"DELETE FROM sync.server_change_log WHERE user_id LIKE 'test-user%'",
		"DELETE FROM sync.sync_state WHERE schema_name = 'batch_test'",
	}

	for _, sql := range cleanupSQL {
		_, _ = pool.Exec(ctx, sql)
	}

	// Create test schema
	_, err = pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS batch_test")
	require.NoError(t, err)

	// Create test tables with FK relationships
	// users (no dependencies)
	_, err = pool.Exec(ctx, `
		CREATE TABLE batch_test.users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL
		)`)
	require.NoError(t, err)

	// posts (depends on users)
	_, err = pool.Exec(ctx, `
		CREATE TABLE batch_test.posts (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id UUID NOT NULL REFERENCES batch_test.users(id) DEFERRABLE INITIALLY DEFERRED,
			title TEXT NOT NULL,
			content TEXT
		)`)
	require.NoError(t, err)

	// comments (depends on posts and users)
	_, err = pool.Exec(ctx, `
		CREATE TABLE batch_test.comments (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			post_id UUID NOT NULL REFERENCES batch_test.posts(id) DEFERRABLE INITIALLY DEFERRED,
			author_id UUID NOT NULL REFERENCES batch_test.users(id) DEFERRABLE INITIALLY DEFERRED,
			content TEXT NOT NULL
		)`)
	require.NoError(t, err)

	// Create simple table handler for materialization
	handler := &SimpleTableHandler{pool: pool}

	// Create SyncService with registered tables and handlers
	config := &oversync.ServiceConfig{
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "batch_test", Table: "users", Handler: handler},
			{Schema: "batch_test", Table: "posts", Handler: handler},
			{Schema: "batch_test", Table: "comments", Handler: handler},
		},
		DisableAutoMigrateFKs: true, // FKs are already deferrable
	}

	service, err := oversync.NewSyncService(pool, config, slog.Default())
	require.NoError(t, err)

	return pool, service
}

// SimpleTableHandler implements basic materialization to business tables for testing
type SimpleTableHandler struct {
	pool *pgxpool.Pool
}

// ConvertReferenceKey implements the TableHandler interface - no key conversion needed for this handler
func (h *SimpleTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *SimpleTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pkUUID uuid.UUID, payload []byte) error {

	// Parse payload
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// Build upsert query
	var columns []string
	var values []interface{}
	var placeholders []string

	i := 1
	for col, val := range data {
		columns = append(columns, col)
		values = append(values, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s",
		pgx.Identifier{schema}.Sanitize(),
		pgx.Identifier{table}.Sanitize(),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		func() string {
			var updates []string
			for _, col := range columns {
				if col != "id" { // Don't update the ID
					updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
				}
			}
			return strings.Join(updates, ", ")
		}(),
	)

	_, err := tx.Exec(ctx, query, values...)
	return err
}

func (h *SimpleTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pkUUID uuid.UUID) error {
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE id = $1",
		pgx.Identifier{schema}.Sanitize(),
		pgx.Identifier{table}.Sanitize())
	_, err := tx.Exec(ctx, query, pkUUID)
	return err
}

func TestBatchUpload_ParentChildInSameRequest(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	// Create parent and child in same request
	userUUID := uuid.New().String()
	postUUID := uuid.New().String()

	changes := []oversync.ChangeUpload{
		// Child first (should be reordered to come after parent)
		{
			SourceChangeID: 2,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             postUUID,
			Payload:        []byte(`{"id":"` + postUUID + `","user_id":"` + userUUID + `","title":"Test Post","content":"Content"}`),
			ServerVersion:  0,
		},
		// Parent second (should be reordered to come first)
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "users",
			Op:             "INSERT",
			PK:             userUUID,
			Payload:        []byte(`{"id":"` + userUUID + `","name":"Test User","email":"test@example.com"}`),
			ServerVersion:  0,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Debug: Print response statuses
	t.Logf("Response statuses:")
	for i, status := range resp.Statuses {
		t.Logf("  [%d] SourceChangeID: %d, Status: %s, NewServerVersion: %v",
			i, status.SourceChangeID, status.Status, status.NewServerVersion)
		if status.Invalid != nil {
			t.Logf("      Invalid: %+v", status.Invalid)
		}
	}

	// Both changes should be applied successfully
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 2)

	// Find statuses by source change ID
	statusMap := make(map[int64]oversync.ChangeUploadStatus)
	for _, status := range resp.Statuses {
		statusMap[status.SourceChangeID] = status
	}

	// User should be applied
	userStatus := statusMap[1]
	assert.Equal(t, "applied", userStatus.Status)
	assert.NotNil(t, userStatus.NewServerVersion)

	// Post should be applied (parent exists in same batch)
	postStatus := statusMap[2]
	assert.Equal(t, "applied", postStatus.Status)
	assert.NotNil(t, postStatus.NewServerVersion)

	// Verify data was actually inserted into business tables (via table handlers)
	var userCount, postCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.users WHERE id = $1", userUUID).Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount, "User should be in business table")

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", postUUID).Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 1, postCount, "Post should be in business table")
}

func TestBatchUpload_ChildOnlyParentExists(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	// First, create a parent user directly in business table
	userUUID := uuid.New().String()
	_, err := pool.Exec(ctx,
		"INSERT INTO batch_test.users (id, name, email) VALUES ($1, $2, $3)",
		userUUID, "Existing User", "existing@example.com")
	require.NoError(t, err)

	// Now upload a child that references the existing parent
	postUUID := uuid.New().String()
	changes := []oversync.ChangeUpload{
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             postUUID,
			Payload:        []byte(`{"id":"` + postUUID + `","user_id":"` + userUUID + `","title":"Child Post","content":"Content"}`),
			ServerVersion:  0,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Change should be applied successfully
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 1)

	status := resp.Statuses[0]
	assert.Equal(t, "applied", status.Status)
	assert.NotNil(t, status.NewServerVersion)

	// Verify data was inserted into business table
	var postCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", postUUID).Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 1, postCount, "Post should be in business table")
}

func TestBatchUpload_ChildOnlyParentMissing(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	// Upload a child that references a non-existent parent
	postUUID := uuid.New().String()
	missingUserUUID := uuid.New().String()

	changes := []oversync.ChangeUpload{
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             postUUID,
			Payload:        []byte(`{"id":"` + postUUID + `","user_id":"` + missingUserUUID + `","title":"Orphan Post","content":"Content"}`),
			ServerVersion:  0,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Debug: Print response statuses
	t.Logf("Response statuses:")
	for i, status := range resp.Statuses {
		t.Logf("  [%d] SourceChangeID: %d, Status: %s, NewServerVersion: %v",
			i, status.SourceChangeID, status.Status, status.NewServerVersion)
		if status.Invalid != nil {
			t.Logf("      Invalid: %+v", status.Invalid)
		}
	}

	// Request should be accepted but change should be invalid
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 1)

	status := resp.Statuses[0]
	assert.Equal(t, "invalid", status.Status)
	assert.NotNil(t, status.Invalid)

	// Should have fk_missing reason
	reason, ok := status.Invalid["reason"].(string)
	assert.True(t, ok)
	assert.Equal(t, "fk_missing", reason)

	// Should have details about missing parent
	details, ok := status.Invalid["details"].(map[string]any)
	assert.True(t, ok)
	missing, ok := details["missing"].([]string)
	assert.True(t, ok)
	assert.Len(t, missing, 1, "Should have one missing parent")
	assert.Contains(t, missing[0], "users:"+missingUserUUID)

	// Verify data was NOT inserted into business table
	var postCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", postUUID).Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 0, postCount, "Invalid post should not be in business table")
}

func TestBatchUpload_MixedBatch(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	// Create one existing user
	existingUserUUID := uuid.New().String()
	_, err := pool.Exec(ctx,
		"INSERT INTO batch_test.users (id, name, email) VALUES ($1, $2, $3)",
		existingUserUUID, "Existing User", "existing@example.com")
	require.NoError(t, err)

	// Mixed batch: some valid, some invalid
	newUserUUID := uuid.New().String()
	validPostUUID := uuid.New().String()
	invalidPostUUID := uuid.New().String()
	missingUserUUID := uuid.New().String()

	changes := []oversync.ChangeUpload{
		// Valid: new user
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "users",
			Op:             "INSERT",
			PK:             newUserUUID,
			Payload:        []byte(`{"id":"` + newUserUUID + `","name":"New User","email":"new@example.com"}`),
			ServerVersion:  0,
		},
		// Valid: post referencing existing user
		{
			SourceChangeID: 2,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             validPostUUID,
			Payload:        []byte(`{"id":"` + validPostUUID + `","user_id":"` + existingUserUUID + `","title":"Valid Post","content":"Content"}`),
			ServerVersion:  0,
		},
		// Invalid: post referencing missing user
		{
			SourceChangeID: 3,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             invalidPostUUID,
			Payload:        []byte(`{"id":"` + invalidPostUUID + `","user_id":"` + missingUserUUID + `","title":"Invalid Post","content":"Content"}`),
			ServerVersion:  0,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Debug: Print response statuses
	t.Logf("Response statuses:")
	for i, status := range resp.Statuses {
		t.Logf("  [%d] SourceChangeID: %d, Status: %s, NewServerVersion: %v",
			i, status.SourceChangeID, status.Status, status.NewServerVersion)
		if status.Invalid != nil {
			t.Logf("      Invalid: %+v", status.Invalid)
		}
	}

	// Request should be accepted with mixed results
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 3)

	// Build status map
	statusMap := make(map[int64]oversync.ChangeUploadStatus)
	for _, status := range resp.Statuses {
		statusMap[status.SourceChangeID] = status
	}

	// New user should be applied
	assert.Equal(t, "applied", statusMap[1].Status)

	// Valid post should be applied
	assert.Equal(t, "applied", statusMap[2].Status)

	// Invalid post should be rejected with fk_missing
	assert.Equal(t, "invalid", statusMap[3].Status)
	assert.Equal(t, "fk_missing", statusMap[3].Invalid["reason"])

	// Verify only valid data was inserted
	var userCount, validPostCount, invalidPostCount int

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.users WHERE id = $1", newUserUUID).Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", validPostUUID).Scan(&validPostCount)
	require.NoError(t, err)
	assert.Equal(t, 1, validPostCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", invalidPostUUID).Scan(&invalidPostCount)
	require.NoError(t, err)
	assert.Equal(t, 0, invalidPostCount)
}

func TestBatchUpload_IdempotentRetry(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user"
	sourceID := "device-1"

	userUUID := uuid.New().String()
	change := oversync.ChangeUpload{
		SourceChangeID: 1,
		Schema:         "batch_test",
		Table:          "users",
		Op:             "INSERT",
		PK:             userUUID,
		Payload:        []byte(`{"id":"` + userUUID + `","name":"Test User","email":"test@example.com"}`),
		ServerVersion:  0,
	}

	// First upload
	req := &oversync.UploadRequest{Changes: []oversync.ChangeUpload{change}}
	resp1, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp1)

	assert.True(t, resp1.Accepted)
	assert.Len(t, resp1.Statuses, 1)
	assert.Equal(t, "applied", resp1.Statuses[0].Status)
	assert.NotNil(t, resp1.Statuses[0].NewServerVersion)

	// Second upload (idempotent retry)
	resp2, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Debug: Print response statuses for second request
	t.Logf("Second response statuses:")
	for i, status := range resp2.Statuses {
		t.Logf("  [%d] SourceChangeID: %d, Status: %s, NewServerVersion: %v",
			i, status.SourceChangeID, status.Status, status.NewServerVersion)
	}

	assert.True(t, resp2.Accepted)
	assert.Len(t, resp2.Statuses, 1)
	assert.Equal(t, "applied", resp2.Statuses[0].Status)
	// NewServerVersion should be nil for idempotent responses
	assert.Nil(t, resp2.Statuses[0].NewServerVersion)

	// Verify only one record exists
	var userCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.users WHERE id = $1", userUUID).Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount)
}

func TestBatchUpload_ConflictOnUpsert(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	userUUID := uuid.New().String()

	// First, create a user with version 0
	change1 := oversync.ChangeUpload{
		SourceChangeID: 1,
		Schema:         "batch_test",
		Table:          "users",
		Op:             "INSERT",
		PK:             userUUID,
		Payload:        []byte(`{"id":"` + userUUID + `","name":"Original User","email":"original@example.com"}`),
		ServerVersion:  0,
	}

	req1 := &oversync.UploadRequest{Changes: []oversync.ChangeUpload{change1}}
	resp1, err := service.ProcessUpload(ctx, userID, sourceID, req1)
	require.NoError(t, err)
	assert.Equal(t, "applied", resp1.Statuses[0].Status)

	// Now try to update with wrong version (should conflict)
	change2 := oversync.ChangeUpload{
		SourceChangeID: 2,
		Schema:         "batch_test",
		Table:          "users",
		Op:             "UPDATE",
		PK:             userUUID,
		Payload:        []byte(`{"id":"` + userUUID + `","name":"Updated User","email":"updated@example.com"}`),
		ServerVersion:  0, // Wrong version - should be 1
	}

	req2 := &oversync.UploadRequest{Changes: []oversync.ChangeUpload{change2}}
	resp2, err := service.ProcessUpload(ctx, userID, sourceID, req2)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	assert.True(t, resp2.Accepted)
	assert.Len(t, resp2.Statuses, 1)
	assert.Equal(t, "conflict", resp2.Statuses[0].Status)
	assert.NotNil(t, resp2.Statuses[0].ServerRow)

	// Verify server row contains current state
	var serverRow map[string]interface{}
	err = json.Unmarshal(resp2.Statuses[0].ServerRow, &serverRow)
	require.NoError(t, err)

	// ServerRow contains sync_state structure with payload nested inside
	payload, ok := serverRow["payload"].(map[string]interface{})
	require.True(t, ok, "ServerRow should contain payload field")
	assert.Equal(t, "Original User", payload["name"])
}

func TestBatchUpload_DeleteOrdering(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user-" + uuid.New().String()
	sourceID := "device-" + uuid.New().String()

	// First, create test data through sync service to ensure sync_state entries exist
	userUUID := uuid.New().String()
	postUUID := uuid.New().String()
	commentUUID := uuid.New().String()

	// Create data through sync service
	setupChanges := []oversync.ChangeUpload{
		{
			SourceChangeID: 100,
			Schema:         "batch_test",
			Table:          "users",
			Op:             "INSERT",
			PK:             userUUID,
			Payload:        []byte(`{"id":"` + userUUID + `","name":"Test User","email":"test@example.com"}`),
			ServerVersion:  0,
		},
		{
			SourceChangeID: 101,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             postUUID,
			Payload:        []byte(`{"id":"` + postUUID + `","user_id":"` + userUUID + `","title":"Test Post","content":"Content"}`),
			ServerVersion:  0,
		},
		{
			SourceChangeID: 102,
			Schema:         "batch_test",
			Table:          "comments",
			Op:             "INSERT",
			PK:             commentUUID,
			Payload:        []byte(`{"id":"` + commentUUID + `","post_id":"` + postUUID + `","author_id":"` + userUUID + `","content":"Test Comment"}`),
			ServerVersion:  0,
		},
	}

	setupReq := &oversync.UploadRequest{Changes: setupChanges}
	setupResp, err := service.ProcessUpload(ctx, userID, sourceID, setupReq)
	require.NoError(t, err)
	require.True(t, setupResp.Accepted)
	for i, status := range setupResp.Statuses {
		require.Equal(t, "applied", status.Status, "Setup change %d should be applied", i)
	}

	// Extract server versions from setup response
	userVersion := *setupResp.Statuses[0].NewServerVersion
	postVersion := *setupResp.Statuses[1].NewServerVersion
	commentVersion := *setupResp.Statuses[2].NewServerVersion

	// Now delete in wrong order (parent first) - should be reordered
	changes := []oversync.ChangeUpload{
		// User first (should be reordered to last)
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "users",
			Op:             "DELETE",
			PK:             userUUID,
			ServerVersion:  userVersion,
		},
		// Comment second (should be reordered to first)
		{
			SourceChangeID: 2,
			Schema:         "batch_test",
			Table:          "comments",
			Op:             "DELETE",
			PK:             commentUUID,
			ServerVersion:  commentVersion,
		},
		// Post third (should be reordered to middle)
		{
			SourceChangeID: 3,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "DELETE",
			PK:             postUUID,
			ServerVersion:  postVersion,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// All deletes should succeed due to proper ordering
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 3)

	for _, status := range resp.Statuses {
		assert.Equal(t, "applied", status.Status, "Delete should be applied for change %d", status.SourceChangeID)
	}

	// Verify all data was deleted
	var userCount, postCount, commentCount int

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.users WHERE id = $1", userUUID).Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 0, userCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", postUUID).Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 0, postCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.comments WHERE id = $1", commentUUID).Scan(&commentCount)
	require.NoError(t, err)
	assert.Equal(t, 0, commentCount)
}

func TestBatchUpload_ConstraintDeferral(t *testing.T) {
	pool, service := setupBatchTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	userID := "test-user"
	sourceID := "device-1"

	// Test that we can insert child before parent in same transaction
	// This should work due to SET CONSTRAINTS ALL DEFERRED
	userUUID := uuid.New().String()
	postUUID := uuid.New().String()

	changes := []oversync.ChangeUpload{
		// Child first - would normally fail FK constraint
		{
			SourceChangeID: 1,
			Schema:         "batch_test",
			Table:          "posts",
			Op:             "INSERT",
			PK:             postUUID,
			Payload:        []byte(`{"id":"` + postUUID + `","user_id":"` + userUUID + `","title":"Child First","content":"Content"}`),
			ServerVersion:  0,
		},
		// Parent second - satisfies FK constraint at commit time
		{
			SourceChangeID: 2,
			Schema:         "batch_test",
			Table:          "users",
			Op:             "INSERT",
			PK:             userUUID,
			Payload:        []byte(`{"id":"` + userUUID + `","name":"Parent Second","email":"parent@example.com"}`),
			ServerVersion:  0,
		},
	}

	req := &oversync.UploadRequest{Changes: changes}
	resp, err := service.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Both should succeed due to constraint deferral and proper ordering
	assert.True(t, resp.Accepted)
	assert.Len(t, resp.Statuses, 2)

	for _, status := range resp.Statuses {
		assert.Equal(t, "applied", status.Status, "Change should be applied for change %d", status.SourceChangeID)
	}

	// Verify both records exist
	var userCount, postCount int

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.users WHERE id = $1", userUUID).Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM batch_test.posts WHERE id = $1", postUUID).Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 1, postCount)
}
