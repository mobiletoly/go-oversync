package oversync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// IntegrationTestHarness provides comprehensive test utilities and fixtures
type IntegrationTestHarness struct {
	t         *testing.T
	ctx       context.Context
	container *postgres.PostgresContainer
	pool      *pgxpool.Pool
	service   *oversync.SyncService
	server    *Server
	jwtAuth   *oversync.JWTAuth
	logger    *slog.Logger

	// Test clients (C1, C2 from spec)
	client1ID    string
	client2ID    string
	client1Token string
	client2Token string
}

// NewIntegrationTestHarness creates a new test harness with TestContainer PostgreSQL
func NewIntegrationTestHarness(t *testing.T) *IntegrationTestHarness {
	ctx := context.Background()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Start PostgreSQL container
	container, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("clisync_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	// Get connection string
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Create database pool
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// Create sync service config with registered tables and handlers
	config := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "go-oversync-integration-test",
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "public", Table: "note", Handler: &NoteTableHandler{}},
			{Schema: "public", Table: "task", Handler: &TaskTableHandler{}},
			{Schema: "public", Table: "users"},
			{Schema: "public", Table: "posts"},
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

	harness := &IntegrationTestHarness{
		t:            t,
		ctx:          ctx,
		container:    container,
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

// Cleanup cleans up test resources
func (h *IntegrationTestHarness) Cleanup() {
	if h.service != nil {
		h.service.Close()
	}
	if h.pool != nil {
		h.pool.Close()
	}
	if h.container != nil {
		h.container.Terminate(h.ctx)
	}
}

// Reset flushes all data between tests (spec section 18)
func (h *IntegrationTestHarness) Reset() {
	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		// Truncate all tables and reset sequences
		tables := []string{"server_change_log", "note", "task"}
		for _, table := range tables {
			if _, err := tx.Exec(h.ctx, fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table)); err != nil {
				return fmt.Errorf("failed to truncate %s: %w", table, err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)
}

// setupBusinessTables creates the business tables from the spec
func (h *IntegrationTestHarness) setupBusinessTables() {
	// Create business tables as per spec section 1
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS note (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT,
			updated_at TIMESTAMPTZ NOT NULL,
			server_version BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS task (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			done BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at TIMESTAMPTZ NOT NULL,
			server_version BIGINT NOT NULL DEFAULT 0
		)`,
	}

	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		for _, migration := range migrations {
			if _, err := tx.Exec(h.ctx, migration); err != nil {
				return fmt.Errorf("failed to create business table: %w", err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)
}

// Test Harness Helpers (from spec section 18)

// MakeUUID creates deterministic UUIDs for testing (spec section 18)
func (h *IntegrationTestHarness) MakeUUID(suffix string) uuid.UUID {
	// Create deterministic UUIDs like aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
	base := fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%012s", suffix)
	id, err := uuid.Parse(base)
	require.NoError(h.t, err)
	return id
}

// SeedServerNote creates a note row on the server with specific server_version
func (h *IntegrationTestHarness) SeedServerNote(id uuid.UUID, title, content string, serverVersion int64) {
	updatedAt := time.Now().UTC()

	_, err := h.pool.Exec(h.ctx, `
		INSERT INTO note (id, title, content, updated_at, server_version)
		VALUES (@id, @title, @content, @updated_at, @server_version)
		ON CONFLICT (id) DO UPDATE SET
			title = @title,
			content = @content,
			updated_at = @updated_at,
			server_version = @server_version`,
		pgx.NamedArgs{
			"id":             id,
			"title":          title,
			"content":        content,
			"updated_at":     updatedAt,
			"server_version": serverVersion,
		})
	require.NoError(h.t, err)
}

// SeedServerTask creates a task row on the server with specific server_version
func (h *IntegrationTestHarness) SeedServerTask(id uuid.UUID, title string, done bool, serverVersion int64) {
	updatedAt := time.Now().UTC()

	_, err := h.pool.Exec(h.ctx, `
		INSERT INTO task (id, title, done, updated_at, server_version)
		VALUES (@id, @title, @done, @updated_at, @server_version)
		ON CONFLICT (id) DO UPDATE SET
			title = @title,
			done = @done,
			updated_at = @updated_at,
			server_version = @server_version`,
		pgx.NamedArgs{
			"id":             id,
			"title":          title,
			"done":           done,
			"updated_at":     updatedAt,
			"server_version": serverVersion,
		})
	require.NoError(h.t, err)
}

// GetServerNote retrieves a note from the server
func (h *IntegrationTestHarness) GetServerNote(id uuid.UUID) (*ServerNote, error) {
	var note ServerNote

	err := h.pool.QueryRow(h.ctx, `
		SELECT id, title, content, updated_at, server_version
		FROM note WHERE id = @id`,
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

// GetServerTask retrieves a task from the server
func (h *IntegrationTestHarness) GetServerTask(id uuid.UUID) (*ServerTask, error) {
	var task ServerTask

	err := h.pool.QueryRow(h.ctx, `
		SELECT id, title, done, updated_at, server_version
		FROM task WHERE id = @id`,
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

// GetChangeLogEntry retrieves a change log entry
func (h *IntegrationTestHarness) GetChangeLogEntry(sourceID string, sourceChangeID int64) (*oversync.ServerChangeLogEntity, error) {
	var change oversync.ServerChangeLogEntity

	err := h.pool.QueryRow(h.ctx, `
		SELECT server_id, table_name, op, pk_uuid::text, payload, source_id, source_change_id, ts
		FROM sync.server_change_log 
		WHERE source_id = @source_id AND source_change_id = @source_change_id`,
		pgx.NamedArgs{
			"source_id":        sourceID,
			"source_change_id": sourceChangeID,
		}).Scan(
		&change.ServerID, &change.TableName, &change.Op, &change.PkUUID,
		&change.Payload, &change.SourceID, &change.SourceChangeID, &change.Timestamp)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &change, nil
}

// GetHighestServerSeq returns the highest server_id from change log
func (h *IntegrationTestHarness) GetHighestServerSeq() int64 {
	var maxSeq sql.NullInt64

	err := h.pool.QueryRow(h.ctx, `SELECT MAX(server_id) FROM sync.server_change_log`).Scan(&maxSeq)
	require.NoError(h.t, err)

	if maxSeq.Valid {
		return maxSeq.Int64
	}
	return 0
}

// HTTP helper methods

// DoUpload performs an upload request with the specified client token
func (h *IntegrationTestHarness) DoUpload(clientToken string, req *oversync.UploadRequest) (*oversync.UploadResponse, *http.Response) {
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
func (h *IntegrationTestHarness) DoDownload(clientToken string, after int64, limit int) (*oversync.DownloadResponse, *http.Response) {
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

// Business table models

// ServerNote represents a note row on the server
type ServerNote struct {
	ID            uuid.UUID `db:"id"`
	Title         string    `db:"title"`
	Content       string    `db:"content"`
	UpdatedAt     time.Time `db:"updated_at"`
	ServerVersion int64     `db:"server_version"`
}

// ServerTask represents a task row on the server
type ServerTask struct {
	ID            uuid.UUID `db:"id"`
	Title         string    `db:"title"`
	Done          bool      `db:"done"`
	UpdatedAt     time.Time `db:"updated_at"`
	ServerVersion int64     `db:"server_version"`
}

// Table handlers implementing clisync.MaterializationHandler

// NoteTableHandler handles note table operations
type NoteTableHandler struct{}

// ConvertReferenceKey implements the MaterializationHandler interface - no key conversion needed for this handler
func (h *NoteTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *NoteTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Parse the payload to extract the note data
	var note map[string]interface{}
	if err := json.Unmarshal(payload, &note); err != nil {
		return fmt.Errorf("invalid note payload: %w", err)
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

func (h *NoteTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	_, err := tx.Exec(ctx, `DELETE FROM note WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete note from business table: %w", err)
	}
	return nil
}

// TaskTableHandler handles task table operations
type TaskTableHandler struct{}

// ConvertReferenceKey implements the MaterializationHandler interface - no key conversion needed for this handler
func (h *TaskTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *TaskTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
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

func (h *TaskTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	_, err := tx.Exec(ctx, `DELETE FROM task WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete task from business table: %w", err)
	}
	return nil
}
