package oversync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// SimpleTestHarness provides basic test utilities without TestContainers
type SimpleTestHarness struct {
	t              *testing.T
	ctx            context.Context
	pool           *pgxpool.Pool
	service        *oversync.SyncService
	server         *Server
	jwtAuth        *oversync.JWTAuth
	logger         *slog.Logger
	client1ID      string
	client2ID      string
	client1Token   string
	client2Token   string
	generatedUUIDs map[string]string // For collision detection
}

// NewSimpleTestHarness creates a test harness using a real PostgreSQL database
func NewSimpleTestHarness(t *testing.T) *SimpleTestHarness {
	ctx := context.Background()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Use environment database URL or skip test
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
	}

	// Create database pool
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)

	// Create sync service config with registered tables and handlers
	config := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "go-oversync-test",
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "public", Table: "note", Handler: &SimpleNoteTableHandler{}},
			{Schema: "public", Table: "task", Handler: &SimpleTaskTableHandler{}},
			{Schema: "public", Table: "users"},
			{Schema: "public", Table: "posts"},
			{Schema: "crm", Table: "note"},      // For multi-schema tests
			{Schema: "business", Table: "note"}, // For multi-schema tests
		},
		DisableAutoMigrateFKs: false,
	}

	// Create sync service
	service, err := oversync.NewSyncService(pool, config, logger)
	require.NoError(t, err)

	// Setup JWT auth
	jwtSecret := "test-secret-key"
	jwtAuth := oversync.NewJWTAuth(jwtSecret)

	// Create server
	server := NewServer(service, jwtAuth, logger)

	// Generate test user and device IDs with tokens for multi-device sync
	testUserID := "user-" + uuid.New().String()
	client1ID := "device1-" + uuid.New().String()
	client2ID := "device2-" + uuid.New().String()

	// Both devices belong to the same user for multi-device sync testing
	client1Token, err := jwtAuth.GenerateToken(testUserID, client1ID, time.Hour)
	require.NoError(t, err)

	client2Token, err := jwtAuth.GenerateToken(testUserID, client2ID, time.Hour)
	require.NoError(t, err)

	harness := &SimpleTestHarness{
		t:            t,
		ctx:          ctx,
		pool:         pool,
		service:      service,
		server:       server,
		jwtAuth:      jwtAuth,
		logger:       logger,
		client1ID:    client1ID,
		client2ID:    client2ID,
		client1Token: client1Token,
		client2Token: client2Token,
	}

	// Setup business tables and handlers
	harness.setupBusinessTables()
	// Table handlers are now registered automatically from ServiceConfig

	return harness
}

// NewSimpleTestHarnessWithConfig allows overriding service config for tests (e.g., limits)
func NewSimpleTestHarnessWithConfig(t *testing.T, mutate func(cfg *oversync.ServiceConfig)) *SimpleTestHarness {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)

	cfg := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "go-oversync-test",
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "public", Table: "note", Handler: &SimpleNoteTableHandler{}},
			{Schema: "public", Table: "task", Handler: &SimpleTaskTableHandler{}},
			{Schema: "public", Table: "users"},
			{Schema: "public", Table: "posts"},
		},
		DisableAutoMigrateFKs: false,
	}
	if mutate != nil {
		mutate(cfg)
	}

	service, err := oversync.NewSyncService(pool, cfg, logger)
	require.NoError(t, err)

	jwtAuth := oversync.NewJWTAuth("test-secret-key")
	server := NewServer(service, jwtAuth, logger)

	testUserID := "user-" + uuid.New().String()
	client1ID := "device1-" + uuid.New().String()
	client2ID := "device2-" + uuid.New().String()
	client1Token, err := jwtAuth.GenerateToken(testUserID, client1ID, time.Hour)
	require.NoError(t, err)
	client2Token, err := jwtAuth.GenerateToken(testUserID, client2ID, time.Hour)
	require.NoError(t, err)

	h := &SimpleTestHarness{
		t:            t,
		ctx:          ctx,
		pool:         pool,
		service:      service,
		server:       server,
		jwtAuth:      jwtAuth,
		logger:       logger,
		client1ID:    client1ID,
		client2ID:    client2ID,
		client1Token: client1Token,
		client2Token: client2Token,
	}
	h.setupBusinessTables()
	return h
}

// Cleanup cleans up test resources
func (h *SimpleTestHarness) Cleanup() {
	if h.service != nil {
		h.service.Close()
	}
	if h.pool != nil {
		h.pool.Close()
	}
}

// Reset flushes all data between tests
func (h *SimpleTestHarness) Reset() {
	err := pgx.BeginFunc(h.ctx, h.service.Pool(), func(tx pgx.Tx) error {
		// Truncate all tables and reset sequences (schema-aware sidecar v3)
		syncTables := []string{"sync.server_change_log", "sync.sync_state", "sync.sync_row_meta", "sync.materialize_failures"}
		businessTables := []string{"note", "task"} // Only tables that actually exist

		// Clear sync schema tables
		for _, table := range syncTables {
			if _, err := tx.Exec(h.ctx, fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table)); err != nil {
				// Ignore errors for tables that don't exist
				h.t.Logf("Warning: Failed to truncate sync table %s: %v", table, err)
			}
		}

		// Clear business tables
		for _, table := range businessTables {
			if _, err := tx.Exec(h.ctx, fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table)); err != nil {
				// Ignore errors for tables that don't exist
				h.t.Logf("Warning: Failed to truncate business table %s: %v", table, err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)
}

// setupBusinessTables creates the business tables
func (h *SimpleTestHarness) setupBusinessTables() {
	migrations := []string{
		// Business tables (clean, no sync metadata)
		`CREATE TABLE IF NOT EXISTS note (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS task (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			done BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
	}

	err := pgx.BeginFunc(h.ctx, h.service.Pool(), func(tx pgx.Tx) error {
		for _, migration := range migrations {
			if _, err := tx.Exec(h.ctx, migration); err != nil {
				return fmt.Errorf("failed to create business table: %w", err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)
}

// DoUpload performs an upload request
func (h *SimpleTestHarness) DoUpload(clientToken string, req *oversync.UploadRequest) (*oversync.UploadResponse, *http.Response) {
	body, err := json.Marshal(req)
	require.NoError(h.t, err)

	httpReq := httptest.NewRequest("POST", "/sync/upload", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+clientToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var uploadResp oversync.UploadResponse
	if recorder.Code == 200 {
		err = json.NewDecoder(recorder.Body).Decode(&uploadResp)
		require.NoError(h.t, err)
	}

	return &uploadResp, recorder.Result()
}

// DoDownload performs a download request
func (h *SimpleTestHarness) DoDownload(clientToken string, after int64, limit int) (*oversync.DownloadResponse, *http.Response) {
	url := fmt.Sprintf("/sync/download?after=%d&limit=%d", after, limit)
	httpReq := httptest.NewRequest("GET", url, nil)
	httpReq.Header.Set("Authorization", "Bearer "+clientToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var downloadResp oversync.DownloadResponse
	if recorder.Code == 200 {
		err := json.NewDecoder(recorder.Body).Decode(&downloadResp)
		require.NoError(h.t, err)
	}

	return &downloadResp, recorder.Result()
}

// DoDownloadWithQuery performs a download request with additional query parameters
// Extra params may include: "schema", "include_self", "until"
func (h *SimpleTestHarness) DoDownloadWithQuery(clientToken string, after int64, limit int, extra map[string]string) (*oversync.DownloadResponse, *http.Response) {
	// Base query
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("/sync/download?after=%d&limit=%d", after, limit))

	// Append extra query params deterministically for test readability
	for k, v := range extra {
		if v == "" {
			continue
		}
		sb.WriteString("&")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v)
	}

	httpReq := httptest.NewRequest("GET", sb.String(), nil)
	httpReq.Header.Set("Authorization", "Bearer "+clientToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var downloadResp oversync.DownloadResponse
	if recorder.Code == 200 {
		err := json.NewDecoder(recorder.Body).Decode(&downloadResp)
		require.NoError(h.t, err)
	}

	return &downloadResp, recorder.Result()
}

// DoDownloadWindowed performs a windowed download with optional include_self, schema and until
// If until <= 0, the server computes a frozen window and returns it in WindowUntil
func (h *SimpleTestHarness) DoDownloadWindowed(clientToken string, after int64, limit int, schema string, includeSelf bool, until int64) (*oversync.DownloadResponse, *http.Response) {
	extra := map[string]string{}
	if schema != "" {
		extra["schema"] = schema
	}
	if includeSelf {
		extra["include_self"] = "true"
	}
	if until > 0 {
		extra["until"] = fmt.Sprintf("%d", until)
	}
	return h.DoDownloadWithQuery(clientToken, after, limit, extra)
}

// DoDownloadWithSchema performs a schema-filtered download request
func (h *SimpleTestHarness) DoDownloadWithSchema(clientToken string, after int64, limit int, schema string) (*oversync.DownloadResponse, *http.Response) {
	url := fmt.Sprintf("/sync/download?after=%d&limit=%d&schema=%s", after, limit, schema)
	httpReq := httptest.NewRequest("GET", url, nil)
	httpReq.Header.Set("Authorization", "Bearer "+clientToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var downloadResp oversync.DownloadResponse
	if recorder.Code == 200 {
		err := json.NewDecoder(recorder.Body).Decode(&downloadResp)
		require.NoError(h.t, err)
	}

	return &downloadResp, recorder.Result()
}

// Helper functions for schema-aware queries

// GetSyncRowMeta retrieves sync metadata for a record (schema-aware)
func (h *SimpleTestHarness) GetSyncRowMeta(schemaName, tableName string, pkUUID uuid.UUID) (*oversync.SyncRowMetaEntity, error) {
	var meta oversync.SyncRowMetaEntity
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT schema_name, table_name, pk_uuid::text, server_version, deleted, updated_at
		 FROM sync.sync_row_meta
		 WHERE schema_name = $1 AND table_name = $2 AND pk_uuid = $3`,
		schemaName, tableName, pkUUID).Scan(
		&meta.SchemaName, &meta.TableName, &meta.PkUUID,
		&meta.ServerVersion, &meta.Deleted, &meta.UpdatedAt)

	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetSyncState retrieves sync state for a record (schema-aware)
func (h *SimpleTestHarness) GetSyncState(schemaName, tableName string, pkUUID uuid.UUID) (*oversync.SyncStateEntity, error) {
	var state oversync.SyncStateEntity
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT schema_name, table_name, pk_uuid::text, payload
		 FROM sync.sync_state
		 WHERE schema_name = $1 AND table_name = $2 AND pk_uuid = $3`,
		schemaName, tableName, pkUUID).Scan(
		&state.SchemaName, &state.TableName, &state.PkUUID, &state.Payload)

	if err != nil {
		return nil, err
	}
	return &state, nil
}

// GetChangeLogEntry retrieves a change log entry by source info (schema-agnostic)
func (h *SimpleTestHarness) GetChangeLogEntry(sourceID string, sourceChangeID int64) (*oversync.ServerChangeLogEntity, error) {
	var change oversync.ServerChangeLogEntity
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_id, schema_name, table_name, op, pk_uuid::text, payload, source_id, source_change_id, ts
		 FROM sync.server_change_log
		 WHERE source_id = $1 AND source_change_id = $2`,
		sourceID, sourceChangeID).Scan(
		&change.ServerID, &change.SchemaName, &change.TableName, &change.Op,
		&change.PkUUID, &change.Payload, &change.SourceID, &change.SourceChangeID, &change.Timestamp)

	if err == pgx.ErrNoRows {
		return nil, nil // Return nil when not found (for pruning tests)
	}
	if err != nil {
		return nil, err
	}
	return &change, nil
}

// createHTTPRequest creates an HTTP request for manual testing
func (h *SimpleTestHarness) createHTTPRequest(method, url string, body []byte) *http.Request {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	httpReq := httptest.NewRequest(method, url, bodyReader)
	return httpReq
}

// executeHTTPRequest executes an HTTP request and returns the recorder
func (h *SimpleTestHarness) executeHTTPRequest(httpReq *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)
	return recorder
}

// MakeUUID creates deterministic UUIDs for testing
func (h *SimpleTestHarness) MakeUUID(suffix string) uuid.UUID {
	// Convert suffix to hex
	hexSuffix := fmt.Sprintf("%x", []byte(suffix))

	// Check if the hex representation is too long for the UUID format
	if len(hexSuffix) > 12 {
		// Use a hash to create a deterministic 12-character hex string
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(suffix)))
		hexSuffix = hash[:12]
	} else {
		// Pad with zeros to make exactly 12 hex characters
		hexSuffix = fmt.Sprintf("%-12s", hexSuffix)
		hexSuffix = strings.ReplaceAll(hexSuffix, " ", "0")
	}

	base := fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%s", hexSuffix)
	id, err := uuid.Parse(base)
	require.NoError(h.t, err)

	// Add collision detection for development
	if h.t != nil {
		// Store generated UUIDs to detect collisions
		if h.generatedUUIDs == nil {
			h.generatedUUIDs = make(map[string]string)
		}

		idStr := id.String()
		if existingSuffix, exists := h.generatedUUIDs[idStr]; exists && existingSuffix != suffix {
			panic(fmt.Sprintf("UUID collision detected! Suffix '%s' and '%s' both generate UUID %s",
				existingSuffix, suffix, idStr))
		}
		h.generatedUUIDs[idStr] = suffix
	}

	return id
}

// Simple table handlers for testing

// SimpleNoteTableHandler handles note table operations (sidecar v2)
type SimpleNoteTableHandler struct{}

// ConvertReferenceKey implements the TableHandler interface - no key conversion needed for this handler
func (h *SimpleNoteTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *SimpleNoteTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Parse the payload to extract the note data
	var note map[string]interface{}
	if err := json.Unmarshal(payload, &note); err != nil {
		return fmt.Errorf("invalid note payload: %w", err)
	}

	// Test-only hook: allow forcing a materializer failure via payload
	if v, ok := note["force_fail"].(bool); ok && v {
		return fmt.Errorf("forced materializer failure for test")
	}

	title, _ := note["title"].(string)
	content, _ := note["content"].(string)

	// Handle updated_at field (could be string or number)
	var updatedAt time.Time
	switch v := note["updated_at"].(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			updatedAt = parsed
		} else {
			updatedAt = time.Now()
		}
	case float64:
		updatedAt = time.Unix(int64(v), 0)
	default:
		updatedAt = time.Now()
	}

	// Materialize to business table (note table)
	_, err := tx.Exec(ctx, `
        INSERT INTO note (id, title, content, updated_at)
        VALUES (@id, @title, @content, @updated_at)
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            content = EXCLUDED.content,
            updated_at = EXCLUDED.updated_at`,
		pgx.NamedArgs{
			"id":         pk,
			"title":      title,
			"content":    content,
			"updated_at": updatedAt,
		})

	if err != nil {
		return fmt.Errorf("failed to materialize note: %w", err)
	}

	return nil
}

func (h *SimpleNoteTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	_, err := tx.Exec(ctx, `DELETE FROM note WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete note from business table: %w", err)
	}
	return nil
}

// SimpleTaskTableHandler handles task table operations (sidecar v2)
type SimpleTaskTableHandler struct{}

// ConvertReferenceKey implements the TableHandler interface - no key conversion needed for this handler
func (h *SimpleTaskTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *SimpleTaskTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Parse the payload to extract the task data
	var task map[string]interface{}
	if err := json.Unmarshal(payload, &task); err != nil {
		return fmt.Errorf("invalid task payload: %w", err)
	}

	title, _ := task["title"].(string)
	done, _ := task["done"].(bool)

	// Handle updated_at field (could be string or number)
	var updatedAt time.Time
	switch v := task["updated_at"].(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			updatedAt = parsed
		} else {
			updatedAt = time.Now()
		}
	case float64:
		updatedAt = time.Unix(int64(v), 0)
	default:
		updatedAt = time.Now()
	}

	// Materialize to business table (task table)
	_, err := tx.Exec(ctx, `
		INSERT INTO task (id, title, done, updated_at)
		VALUES (@id, @title, @done, @updated_at)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			done = EXCLUDED.done,
			updated_at = EXCLUDED.updated_at`,
		pgx.NamedArgs{
			"id":         pk,
			"title":      title,
			"done":       done,
			"updated_at": updatedAt,
		})

	if err != nil {
		return fmt.Errorf("failed to materialize task: %w", err)
	}

	return nil
}

func (h *SimpleTaskTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	_, err := tx.Exec(ctx, `DELETE FROM task WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete task from business table: %w", err)
	}
	return nil
}

// GetServerNote retrieves a note from the server (schema-aware sidecar v3)
func (h *SimpleTestHarness) GetServerNote(id uuid.UUID) (*ServerNote, error) {
	var note ServerNote

	// Get from business table + sidecar metadata (defaults to public schema)
	err := h.service.Pool().QueryRow(h.ctx, `
		SELECT
			n.id, n.title, n.content, n.updated_at,
			COALESCE(m.server_version, 0) as server_version
		FROM note n
		LEFT JOIN sync.sync_row_meta m
		  ON m.schema_name = 'public'
		 AND m.table_name = 'note'
		 AND m.pk_uuid = n.id
		WHERE n.id = @id`,
		pgx.NamedArgs{"id": id}).Scan(
		&note.ID, &note.Title, &note.Content, &note.UpdatedAt, &note.ServerVersion)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &note, nil
}

// GetServerTask retrieves a task from the server (schema-aware sidecar v3)
func (h *SimpleTestHarness) GetServerTask(id uuid.UUID) (*ServerTask, error) {
	var task ServerTask

	// Get from business table + sidecar metadata (defaults to public schema)
	err := h.service.Pool().QueryRow(h.ctx, `
		SELECT
			t.id, t.title, t.done, t.updated_at,
			COALESCE(m.server_version, 0) as server_version
		FROM task t
		LEFT JOIN sync.sync_row_meta m
		  ON m.schema_name = 'public'
		 AND m.table_name = 'task'
		 AND m.pk_uuid = t.id
		WHERE t.id = @id`,
		pgx.NamedArgs{"id": id}).Scan(
		&task.ID, &task.Title, &task.Done, &task.UpdatedAt, &task.ServerVersion)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// GenerateJWT generates a JWT token for testing
func (h *SimpleTestHarness) GenerateJWT(clientName string) string {
	userID := "user-" + uuid.New().String()
	deviceID := "device-" + uuid.New().String()
	token, err := h.jwtAuth.GenerateToken(userID, deviceID, time.Hour)
	require.NoError(h.t, err)
	return token
}

// ExtractClientIDFromToken extracts the device ID from a JWT token
func (h *SimpleTestHarness) ExtractClientIDFromToken(token string) string {
	claims, err := h.jwtAuth.ValidateToken(token)
	require.NoError(h.t, err)
	return claims.DeviceID
}

// ExtractUserIDFromToken extracts the user ID from a JWT token
func (h *SimpleTestHarness) ExtractUserIDFromToken(token string) string {
	claims, err := h.jwtAuth.ValidateToken(token)
	require.NoError(h.t, err)
	return claims.Subject
}

// DoUploadWithoutAuth performs upload without authentication
func (h *SimpleTestHarness) DoUploadWithoutAuth(req *oversync.UploadRequest) (*oversync.UploadResponse, *http.Response) {
	reqBody, err := json.Marshal(req)
	require.NoError(h.t, err)

	httpReq := httptest.NewRequest(http.MethodPost, "/sync/upload", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	// No Authorization header

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var resp oversync.UploadResponse
	if recorder.Code == http.StatusOK {
		err = json.Unmarshal(recorder.Body.Bytes(), &resp)
		require.NoError(h.t, err)
	}

	return &resp, recorder.Result()
}

// DoUploadWithInvalidAuth performs upload with invalid authentication
func (h *SimpleTestHarness) DoUploadWithInvalidAuth(req *oversync.UploadRequest, invalidToken string) (*oversync.UploadResponse, *http.Response) {
	reqBody, err := json.Marshal(req)
	require.NoError(h.t, err)

	httpReq := httptest.NewRequest(http.MethodPost, "/sync/upload", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+invalidToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var resp oversync.UploadResponse
	if recorder.Code == http.StatusOK {
		err = json.Unmarshal(recorder.Body.Bytes(), &resp)
		require.NoError(h.t, err)
	}

	return &resp, recorder.Result()
}

// DoDownloadWithoutAuth performs download without authentication
func (h *SimpleTestHarness) DoDownloadWithoutAuth(after int64, limit int) (*oversync.DownloadResponse, *http.Response) {
	url := fmt.Sprintf("/sync/download?after=%d&limit=%d", after, limit)
	httpReq := httptest.NewRequest(http.MethodGet, url, nil)
	// No Authorization header

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var resp oversync.DownloadResponse
	if recorder.Code == http.StatusOK {
		err := json.Unmarshal(recorder.Body.Bytes(), &resp)
		require.NoError(h.t, err)
	}

	return &resp, recorder.Result()
}

// DoDownloadWithInvalidAuth performs download with invalid authentication
func (h *SimpleTestHarness) DoDownloadWithInvalidAuth(invalidToken string, after int64, limit int) (*oversync.DownloadResponse, *http.Response) {
	url := fmt.Sprintf("/sync/download?after=%d&limit=%d", after, limit)
	httpReq := httptest.NewRequest(http.MethodGet, url, nil)
	httpReq.Header.Set("Authorization", "Bearer "+invalidToken)

	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, httpReq)

	var resp oversync.DownloadResponse
	if recorder.Code == http.StatusOK {
		err := json.Unmarshal(recorder.Body.Bytes(), &resp)
		require.NoError(h.t, err)
	}

	return &resp, recorder.Result()
}

// SeedServerNote creates a note with specific server version for testing
func (h *SimpleTestHarness) SeedServerNote(id uuid.UUID, title, content string, serverVersion int64) {
	// Insert into business table
	_, err := h.service.Pool().Exec(h.ctx, `
		INSERT INTO note (id, title, content, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			updated_at = EXCLUDED.updated_at`,
		id, title, content)
	require.NoError(h.t, err)

	// Insert/update sync metadata
	_, err = h.service.Pool().Exec(h.ctx, `
		INSERT INTO sync.sync_row_meta (schema_name, table_name, pk_uuid, server_version, deleted)
		VALUES ($1, $2, $3, $4, false)
		ON CONFLICT (schema_name, table_name, pk_uuid) DO UPDATE SET
			server_version = EXCLUDED.server_version,
			deleted = EXCLUDED.deleted,
			updated_at = now()`,
		"public", "note", id, serverVersion)
	require.NoError(h.t, err)

	// Insert/update sync state
	payload := json.RawMessage(fmt.Sprintf(`{
		"id": "%s",
		"title": "%s",
		"content": "%s",
		"updated_at": %d
	}`, id.String(), title, content, time.Now().Unix()))

	_, err = h.service.Pool().Exec(h.ctx, `
		INSERT INTO sync.sync_state (schema_name, table_name, pk_uuid, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (schema_name, table_name, pk_uuid) DO UPDATE SET
			payload = EXCLUDED.payload`,
		"public", "note", id, payload)
	require.NoError(h.t, err)
}

// ServerNote and ServerTask types are defined in integration_comprehensive_test.go
