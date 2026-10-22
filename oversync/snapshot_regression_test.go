package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type blockingSnapshotResponseWriter struct {
	header      http.Header
	started     chan struct{}
	release     <-chan struct{}
	onStarted   func()
	startedOnce sync.Once
	mu          sync.Mutex
	statusCode  int
	body        bytes.Buffer
	discardBody bool
}

func newBlockingSnapshotResponseWriter(release <-chan struct{}) *blockingSnapshotResponseWriter {
	return &blockingSnapshotResponseWriter{header: make(http.Header), started: make(chan struct{}), release: release}
}

func (w *blockingSnapshotResponseWriter) Header() http.Header { return w.header }

func (w *blockingSnapshotResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
}

func (w *blockingSnapshotResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	block := w.statusCode < http.StatusBadRequest
	w.mu.Unlock()
	if block {
		w.startedOnce.Do(func() {
			close(w.started)
			if w.onStarted != nil {
				w.onStarted()
			}
		})
		<-w.release
	}
	if w.discardBody {
		return len(p), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(p)
}

func (w *blockingSnapshotResponseWriter) StatusCode() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.statusCode
}

func (w *blockingSnapshotResponseWriter) BodyBytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body.Bytes()...)
}

func snapshotRequestWithActor(method, target, snapshotID string, actor Actor, service *SyncService) *http.Request {
	request := httptest.NewRequest(method, target, nil)
	setTestSchemaGenerationHeaders(request, service)
	if snapshotID != "" {
		request.SetPathValue("snapshot_id", snapshotID)
	}
	return request.WithContext(ContextWithActor(request.Context(), actor))
}

func waitForSnapshotWriter(t *testing.T, writer *blockingSnapshotResponseWriter) {
	t.Helper()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot response writer did not reach the blocked write")
	}
}

func recordedSnapshotAttrs(handler *recordingSlogHandler, message string) (map[string]any, bool) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for _, record := range handler.records {
		if record.Message != message {
			continue
		}
		attrs := make(map[string]any)
		record.Attrs(func(attr slog.Attr) bool {
			attrs[attr.Key] = attr.Value.Any()
			return true
		})
		return attrs, true
	}
	return nil, false
}

func TestSnapshotHTTP_ChunkPermitIsHeldThroughResponseWrite(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "http_write_capacity", snapshotSessionFixtureOptions{})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	beforeMetrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	release := make(chan struct{})
	writers := make([]*blockingSnapshotResponseWriter, cap(fixture.svc.snapshotChunkPermits))
	done := make(chan struct{}, len(writers))

	for i := range writers {
		writers[i] = newBlockingSnapshotResponseWriter(release)
		go func(writer *blockingSnapshotResponseWriter) {
			handlers.HandleGetSnapshotChunk(writer, snapshotRequestWithActor(http.MethodGet, "/sync/snapshot-sessions/"+session.SnapshotID, session.SnapshotID, fixture.reader, fixture.svc))
			done <- struct{}{}
		}(writers[i])
	}
	for _, writer := range writers {
		waitForSnapshotWriter(t, writer)
	}
	require.Equal(t, int64(len(writers)), fixture.svc.snapshotRuntimeMetricsSnapshot().ActiveChunks)

	rejected := httptest.NewRecorder()
	handlers.HandleGetSnapshotChunk(rejected, snapshotRequestWithActor(http.MethodGet, "/sync/snapshot-sessions/"+session.SnapshotID, session.SnapshotID, fixture.reader, fixture.svc))
	require.Equal(t, http.StatusTooManyRequests, rejected.Code)
	var response ErrorResponse
	require.NoError(t, json.Unmarshal(rejected.Body.Bytes(), &response))
	require.Equal(t, "snapshot_chunk_capacity", response.Error)
	metricsWhileBlocked := fixture.svc.snapshotRuntimeMetricsSnapshot()
	require.Equal(t, beforeMetrics.ChunkRequests+int64(len(writers))+1, metricsWhileBlocked.ChunkRequests)
	require.Equal(t, beforeMetrics.ChunkCompletions+int64(len(writers)), metricsWhileBlocked.ChunkCompletions)
	require.Equal(t, beforeMetrics.ChunkRejections+1, metricsWhileBlocked.ChunkRejections)

	close(release)
	for range writers {
		<-done
	}
	for _, writer := range writers {
		require.Equal(t, http.StatusOK, writer.StatusCode())
	}
	var chunk CompactSnapshotChunkResponse
	require.NoError(t, json.Unmarshal(writers[0].BodyBytes(), &chunk))
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, int64(0), fixture.svc.snapshotRuntimeMetricsSnapshot().ActiveChunks)
}

func TestSnapshotHTTP_SessionLimitIsStructuredConflict(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "http_row_wire_limit", snapshotSessionFixtureOptions{})
	fixture.pushUser(t, 1, uuid.New(), strings.Repeat("x", 512))
	fixture.svc.config.MaxBytesPerSnapshotRow = 128
	fixture.svc.config.SnapshotMaterializationBatchBytes = 128
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	recorder := httptest.NewRecorder()
	handlers.HandleCreateSnapshotSession(recorder, snapshotRequestWithActor(http.MethodPost, "/sync/snapshot-sessions", "", fixture.reader, fixture.svc))

	require.Equal(t, http.StatusConflict, recorder.Code)
	var response SnapshotSessionLimitResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "snapshot_session_limit_exceeded", response.Error)
	require.Equal(t, "row_decoded_body_bytes", response.Dimension)
	require.Greater(t, response.Actual, response.Limit)
	require.Equal(t, int64(128), response.Limit)
	require.Zero(t, fixture.snapshotSessionCountForUser(t))
}

func TestSnapshotCleanup_StartupPublicationIsAtomicWithClose(t *testing.T) {
	service, err := newTestRuntimeService(nil, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(runtimeServiceTestRegisteredTables()), SnapshotCleanupInterval: time.Hour, RegisteredTables: runtimeServiceTestRegisteredTables(),
		RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	service.bootstrapReadiness = bootstrapReadinessReady
	reachedPublish := make(chan struct{})
	releasePublish := make(chan struct{})
	service.snapshotHooks = &snapshotTestHooks{beforeSnapshotCleanupWorkerPublish: func() {
		close(reachedPublish)
		<-releasePublish
	}}
	startDone := make(chan struct{})
	go func() {
		service.startSnapshotCleanupWorker()
		close(startDone)
	}()
	<-reachedPublish

	closeDone := make(chan error, 1)
	go func() { closeDone <- service.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before cleanup worker publication: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releasePublish)
	<-startDone
	require.NoError(t, <-closeDone)
	require.NotNil(t, service.snapshotCleanupDone)
	select {
	case <-service.snapshotCleanupDone:
	case <-time.After(time.Second):
		t.Fatal("published cleanup worker did not stop during Close")
	}
}

func TestSnapshotSessions_DeletePreservesEarlierExpiry(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "expiry_order", snapshotSessionFixtureOptions{snapshotCleanupInterval: time.Hour})
	fixture.svc.stopSnapshotCleanupWorker()
	require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 hour' WHERE snapshot_id=$1::uuid`, session.SnapshotID)
	require.NoError(t, err)
	var before, after time.Time
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT expires_at FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid`, session.SnapshotID).Scan(&before))
	var expiredErr *SnapshotSessionExpiredError
	require.ErrorAs(t, fixture.svc.DeleteSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID), &expiredErr)
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT expires_at FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid`, session.SnapshotID).Scan(&after))
	require.True(t, before.Equal(after), "expired delete moved expiry from %s to %s", before, after)
}

func TestSnapshotObservability_EmitsPerBuildChunkAndCleanupFacts(t *testing.T) {
	t.Parallel()
	t.Run("build completion and rejection", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "build_observability", snapshotSessionFixtureOptions{snapshotMaterializationBatchRows: 1})
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		fixture.pushUser(t, 1, uuid.New(), "Alpha")
		fixture.pushUser(t, 2, uuid.New(), "Bravo")
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)

		attrs, ok := recordedSnapshotAttrs(logs, "Snapshot build completed")
		require.True(t, ok)
		require.Equal(t, fixture.userID, attrs["user_id"])
		require.Equal(t, session.SnapshotID, attrs["snapshot_id"])
		require.EqualValues(t, 2, attrs["materialization_batch_count"])
		require.EqualValues(t, 1, attrs["batch_rows_high_water"])
		require.Equal(t, "completed", attrs["outcome"])
		require.Equal(t, "admitted", attrs["admission_outcome"])
		require.Contains(t, attrs, "duration")

		for i := 0; i < cap(fixture.svc.snapshotBuildPermits); i++ {
			fixture.svc.snapshotBuildPermits <- struct{}{}
		}
		_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		for i := 0; i < cap(fixture.svc.snapshotBuildPermits); i++ {
			<-fixture.svc.snapshotBuildPermits
		}
		var capacity *SnapshotCapacityError
		require.ErrorAs(t, err, &capacity)
		attrs, ok = recordedSnapshotAttrs(logs, "Snapshot build rejected")
		require.True(t, ok)
		require.Equal(t, fixture.userID, attrs["user_id"])
		require.Equal(t, "rejected", attrs["outcome"])
		require.Equal(t, "rejected", attrs["admission_outcome"])
		require.Contains(t, attrs, "duration")
	})

	t.Run("failed build retains completed batch facts", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "build_fail_obs", snapshotSessionFixtureOptions{snapshotMaterializationBatchRows: 1})
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		fixture.pushUser(t, 1, uuid.New(), "Alpha")
		fixture.pushUser(t, 2, uuid.New(), "Bravo")
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		injectedErr := errors.New("injected second snapshot COPY failure")
		copyCalls := 0
		fixture.svc.snapshotHooks = &snapshotTestHooks{beforeSnapshotCopy: func(context.Context) error {
			copyCalls++
			if copyCalls == 2 {
				return injectedErr
			}
			return nil
		}}

		_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.ErrorIs(t, err, injectedErr)
		require.Equal(t, 2, copyCalls)
		require.Zero(t, fixture.snapshotSessionCountForUser(t))
		require.Zero(t, fixture.snapshotSessionRowCountForUser(t))

		attrs, ok := recordedSnapshotAttrs(logs, "Snapshot build failed")
		require.True(t, ok)
		require.Equal(t, fixture.userID, attrs["user_id"])
		require.NotEmpty(t, attrs["snapshot_id"])
		require.Equal(t, "failed", attrs["outcome"])
		require.Equal(t, "admitted", attrs["admission_outcome"])
		require.EqualValues(t, 1, attrs["materialization_batch_count"])
		require.EqualValues(t, 1, attrs["batch_rows_high_water"])
		require.Greater(t, attrs["batch_bytes_high_water"].(int64), int64(0))
		require.Equal(t, injectedErr, attrs["error"])
		require.Contains(t, attrs, "duration")

		metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, int64(1), metrics.MaterializationBatchRowsHighWater)
		require.Greater(t, metrics.MaterializationBatchBytesHighWater, int64(0))
	})

	t.Run("chunk completion", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "chunk_obs", snapshotSessionFixtureOptions{defaultRowsPerSnapshotChunk: 1, maxRowsPerSnapshotChunk: 1})
		fixture.pushUser(t, 1, uuid.New(), "Alpha")
		fixture.pushUser(t, 2, uuid.New(), "Bravo")
		session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		before := fixture.svc.snapshotRuntimeMetricsSnapshot()
		requestedRows := fixture.svc.maxRowsPerSnapshotChunk() + 7
		requestedBytes := fixture.svc.maxBytesPerSnapshotChunk() + 123

		chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, requestedRows, requestedBytes)
		require.NoError(t, err)
		require.Len(t, chunk.Rows, 1)
		require.True(t, chunk.HasMore)

		attrs, ok := recordedSnapshotAttrs(logs, "Snapshot chunk selection completed")
		require.True(t, ok)
		require.Equal(t, fixture.userID, attrs["user_id"])
		require.Equal(t, session.SnapshotID, attrs["snapshot_id"])
		require.EqualValues(t, requestedRows, attrs["requested_max_rows"])
		require.EqualValues(t, requestedBytes, attrs["requested_max_bytes"])
		require.EqualValues(t, fixture.svc.maxRowsPerSnapshotChunk(), attrs["effective_max_rows"])
		require.EqualValues(t, fixture.svc.maxBytesPerSnapshotChunk(), attrs["effective_max_bytes"])
		require.EqualValues(t, 1, attrs["returned_rows"])
		require.EqualValues(t, chunk.ByteCount, attrs["returned_bytes"])
		require.EqualValues(t, 1, attrs["selected_rows_high_water"])
		require.EqualValues(t, 2, attrs["retained_rows_high_water"])
		require.Greater(t, attrs["retained_bytes_high_water"].(int64), chunk.ByteCount)
		require.Greater(t, attrs["selection_duration"].(time.Duration), time.Duration(0))
		require.Equal(t, "admitted", attrs["admission_outcome"])
		require.Equal(t, "completed", attrs["outcome"])

		metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, before.ChunkRequests+1, metrics.ChunkRequests)
		require.Equal(t, before.ChunkCompletions+1, metrics.ChunkCompletions)
		require.Equal(t, before.ChunkRejections, metrics.ChunkRejections)
		require.Equal(t, before.ChunkFailures, metrics.ChunkFailures)
		require.Greater(t, metrics.ChunkSelectionDurationHighWater, int64(0))
		require.Equal(t, int64(0), metrics.ActiveChunks)
	})

	t.Run("chunk capacity rejection", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "chunk_reject_obs", snapshotSessionFixtureOptions{})
		session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		for i := 0; i < cap(fixture.svc.snapshotChunkPermits); i++ {
			require.True(t, fixture.svc.tryAcquireSnapshotChunk())
		}
		released := false
		defer func() {
			if released {
				return
			}
			for i := 0; i < cap(fixture.svc.snapshotChunkPermits); i++ {
				fixture.svc.releaseSnapshotChunk()
			}
		}()
		before := fixture.svc.snapshotRuntimeMetricsSnapshot()

		_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 3, defaultBytesPerSnapshotChunk)
		var capacity *SnapshotCapacityError
		require.ErrorAs(t, err, &capacity)
		for i := 0; i < cap(fixture.svc.snapshotChunkPermits); i++ {
			fixture.svc.releaseSnapshotChunk()
		}
		released = true

		attrs, ok := recordedSnapshotAttrs(logs, "Snapshot chunk selection completed")
		require.True(t, ok)
		require.Equal(t, "rejected", attrs["admission_outcome"])
		require.Equal(t, "rejected", attrs["outcome"])
		require.EqualValues(t, 0, attrs["returned_rows"])
		require.Equal(t, time.Duration(0), attrs["selection_duration"])
		metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, before.ChunkRequests+1, metrics.ChunkRequests)
		require.Equal(t, before.ChunkRejections+1, metrics.ChunkRejections)
		require.Equal(t, before.ChunkQueries, metrics.ChunkQueries)
		require.Equal(t, int64(0), metrics.ActiveChunks)
	})

	t.Run("missing session fails before chunk admission", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "chunk_fail_obs", snapshotSessionFixtureOptions{})
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		missingSnapshotID := uuid.NewString()
		before := fixture.svc.snapshotRuntimeMetricsSnapshot()

		_, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), missingSnapshotID, 0, 1, defaultBytesPerSnapshotChunk)
		var notFound *SnapshotSessionNotFoundError
		require.ErrorAs(t, err, &notFound)
		_, ok := recordedSnapshotAttrs(logs, "Snapshot chunk selection completed")
		require.False(t, ok)
		metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, before.ChunkRequests, metrics.ChunkRequests)
		require.Equal(t, before.ChunkFailures, metrics.ChunkFailures)
		require.Equal(t, int64(0), metrics.ActiveChunks)
	})

	t.Run("cleanup batch and run", func(t *testing.T) {
		fixture := newSnapshotSessionFixture(t, "cleanup_observability", snapshotSessionFixtureOptions{snapshotCleanupInterval: time.Hour})
		fixture.svc.stopSnapshotCleanupWorker()
		require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(fixture.ctx))
		session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
		_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '2 seconds' WHERE snapshot_id=$1::uuid`, session.SnapshotID)
		require.NoError(t, err)
		logs := &recordingSlogHandler{}
		fixture.svc.logger = slog.New(logs)
		beforeMetrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		fixture.svc.runSnapshotCleanup(fixture.ctx, "test_observability")

		batchAttrs, ok := recordedSnapshotAttrs(logs, "Snapshot cleanup batch completed")
		require.True(t, ok)
		require.Equal(t, "completed", batchAttrs["outcome"])
		require.Contains(t, batchAttrs, "duration")
		require.Contains(t, batchAttrs, "oldest_selected_expiry_age")
		runAttrs, ok := recordedSnapshotAttrs(logs, "Snapshot cleanup run completed")
		require.True(t, ok)
		require.Equal(t, "completed", runAttrs["outcome"])
		require.Contains(t, runAttrs, "batch_count")
		require.Contains(t, runAttrs, "active_run_high_water")

		metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, int64(0), metrics.ActiveCleanupRuns)
		require.Equal(t, int64(1), metrics.CleanupRunHighWater)
		require.Equal(t, beforeMetrics.CleanupRuns+1, metrics.CleanupRuns)
		require.Greater(t, metrics.CleanupBatches, beforeMetrics.CleanupBatches)
		require.Greater(t, metrics.CleanupBatchDurationHighWater, int64(0))
		require.Greater(t, metrics.CleanupOldestExpiryAgeHighWater, int64(0))
		require.Equal(t, int64(1), metrics.CleanupDeletedSessionsHighWater)

		cancelledCtx, cancel := context.WithCancel(fixture.ctx)
		cancel()
		beforeCancellations := metrics.CleanupCancellations
		fixture.svc.runSnapshotCleanup(cancelledCtx, "test_cancelled")
		metrics = fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.Equal(t, beforeCancellations+1, metrics.CleanupCancellations)

		fixture.svc.config.SnapshotCleanupBatchTimeout = time.Nanosecond
		beforeFailures := metrics.CleanupFailures
		fixture.svc.runSnapshotCleanup(context.Background(), "test_failed")
		metrics = fixture.svc.snapshotRuntimeMetricsSnapshot()
		require.GreaterOrEqual(t, metrics.CleanupFailures, beforeFailures+1)
	})
}

func TestSnapshotPublicContract_LockedCapabilityAndEndpointDocumentation(t *testing.T) {
	type schema struct {
		Type        string   `yaml:"type"`
		Format      string   `yaml:"format"`
		Pattern     string   `yaml:"pattern"`
		MinLength   int      `yaml:"minLength"`
		MaxLength   int      `yaml:"maxLength"`
		Description string   `yaml:"description"`
		Required    []string `yaml:"required"`
		Properties  map[string]struct {
			Description string `yaml:"description"`
			Ref         string `yaml:"$ref"`
			MinItems    int    `yaml:"minItems"`
			MaxItems    int    `yaml:"maxItems"`
			Enum        []any  `yaml:"enum"`
			AllOf       []struct {
				Ref string `yaml:"$ref"`
			} `yaml:"allOf"`
		} `yaml:"properties"`
	}
	type response struct {
		Content map[string]struct {
			Schema struct {
				Ref   string `yaml:"$ref"`
				OneOf []struct {
					Ref string `yaml:"$ref"`
				} `yaml:"oneOf"`
				AnyOf []struct {
					Ref string `yaml:"$ref"`
				} `yaml:"anyOf"`
			} `yaml:"schema"`
		} `yaml:"content"`
	}
	type parameter struct {
		Name   string `yaml:"name"`
		Schema struct {
			Ref string `yaml:"$ref"`
		} `yaml:"schema"`
	}
	type operation struct {
		Parameters []parameter         `yaml:"parameters"`
		Responses  map[string]response `yaml:"responses"`
	}
	var spec struct {
		Components struct {
			Schemas    map[string]schema `yaml:"schemas"`
			Parameters map[string]struct {
				Schema struct {
					Ref       string `yaml:"$ref"`
					Type      string `yaml:"type"`
					Pattern   string `yaml:"pattern"`
					MinLength int    `yaml:"minLength"`
				} `yaml:"schema"`
			} `yaml:"parameters"`
		} `yaml:"components"`
		Paths map[string]struct {
			Get    operation `yaml:"get"`
			Post   operation `yaml:"post"`
			Delete operation `yaml:"delete"`
		} `yaml:"paths"`
	}
	swaggerBytes, err := os.ReadFile("../swagger/two_way_sync.yaml")
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(swaggerBytes, &spec))
	sourceID := spec.Components.Parameters["SourceIDHeader"].Schema
	require.Equal(t, "string", sourceID.Type)
	require.Equal(t, "^[!-~]{1,256}$", sourceID.Pattern)
	require.NotContains(t, spec.Paths, "/sync/push-sessions/{push_id}/chunks")
	require.NotContains(t, spec.Paths, "/sync/push-sessions/{push_id}/commit")
	require.NotContains(t, spec.Paths, "/sync/push-sessions/{push_id}")
	require.Contains(t, spec.Paths, "/sync/push")

	limits := spec.Components.Schemas["WireLimits"]
	discoverySchema := spec.Components.Schemas["DiscoveryResponse"]
	require.Contains(t, discoverySchema.Required, "limits")
	require.Contains(t, discoverySchema.Required, "schema_generation")
	require.NotContains(t, discoverySchema.Required, "registered_table_specs")
	require.NotContains(t, discoverySchema.Required, "registered_tables")
	require.NotContains(t, discoverySchema.Required, "schema_version")
	generationCapabilities := spec.Components.Schemas["SchemaGeneration"]
	for _, name := range []string{"id", "fingerprint", "tables"} {
		require.Contains(t, generationCapabilities.Required, name)
	}
	for _, name := range []string{"max_push_rows", "max_push_decoded_bytes", "max_pull_bundles", "max_pull_decoded_bytes", "max_snapshot_rows_per_chunk", "max_snapshot_decoded_bytes_per_chunk", "max_snapshot_rows", "max_snapshot_logical_bytes", "max_row_decoded_bytes"} {
		require.Contains(t, limits.Required, name)
		_, ok := limits.Properties[name]
		require.True(t, ok, "missing capability property %s", name)
	}
	_, hasOldName := limits.Properties["max_concurrent_snapshot_chunks"]
	require.False(t, hasOldName)
	require.Empty(t, limits.Properties["max_snapshot_logical_bytes"].Description)
	errorResponse := spec.Components.Schemas["ErrorResponse"]
	for _, name := range []string{"error", "message"} {
		require.Contains(t, errorResponse.Required, name)
	}

	for name, operation := range map[string]operation{
		"POST /sync/connect":           spec.Paths["/sync/connect"].Post,
		"POST /sync/push":              spec.Paths["/sync/push"].Post,
		"GET /sync/pull":               spec.Paths["/sync/pull"].Get,
		"GET /sync/watch":              spec.Paths["/sync/watch"].Get,
		"POST /sync/snapshot-sessions": spec.Paths["/sync/snapshot-sessions"].Post,
	} {
		for _, status := range []string{"400", "500"} {
			_, ok := operation.Responses[status]
			require.True(t, ok, "%s missing status %s", name, status)
		}
	}

	zeroDiscovery, err := json.Marshal(GenerationDiscoveryResponse{})
	require.NoError(t, err)
	var zeroWire map[string]any
	require.NoError(t, json.Unmarshal(zeroDiscovery, &zeroWire))
	require.Contains(t, zeroWire, "schema_generation")
	zeroLimits, ok := zeroWire["limits"].(map[string]any)
	require.True(t, ok, "zero-value discovery must encode limits as an object")
	for _, name := range []string{"max_snapshot_rows_per_chunk", "max_snapshot_decoded_bytes_per_chunk", "max_row_decoded_bytes"} {
		require.Contains(t, zeroLimits, name)
	}
	discovery, err := json.Marshal(GenerationDiscoveryResponse{ProtocolVersion: SyncProtocolVersion, SchemaGeneration: SchemaGenerationCapabilities{ID: "1", Fingerprint: strings.Repeat("a", 64), Tables: []SchemaGenerationTableCapabilities{}}, Limits: WireLimits{MaxSnapshotRowsPerChunk: 3}})
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(discovery, &wire))
	wireLimits := wire["limits"].(map[string]any)
	require.Contains(t, wireLimits, "max_snapshot_rows_per_chunk")
	require.NotContains(t, wireLimits, "max_concurrent_snapshot_chunks")

	apiBytes, err := os.ReadFile("../docs/documentation/api.md")
	require.NoError(t, err)
	api := string(apiBytes)
	section := func(heading string) string {
		start := strings.Index(api, heading)
		require.NotEqual(t, -1, start, "missing heading %s", heading)
		rest := api[start+len(heading):]
		if end := strings.Index(rest, "\n## "); end >= 0 {
			rest = rest[:end]
		}
		return rest
	}
	require.NotContains(t, api, "/sync/committed-bundles/")
	require.Contains(t, section("## GET `/sync/snapshot-sessions/{snapshot_id}`"), "`max_bytes`")
	require.NotContains(t, api, "## POST `/sync/push-sessions`")
	require.Contains(t, section("## POST `/sync/push`"), "compact")
	snapshotCreate := section("## POST `/sync/snapshot-sessions`")
	require.Contains(t, snapshotCreate, "snapshot_build_capacity")
	require.Contains(t, snapshotCreate, "snapshot_session_limit_exceeded")
}
