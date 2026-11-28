package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// A pull over history-projection capacity must be refused before it takes a
// pooled connection: with every pool connection busy and every permit held, the
// refusal is immediate instead of waiting on the pool.
func TestProcessPull_AdmissionPrecedesPooledConnections(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "pull_admission_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations:               testSchemaGenerationsForRegisteredTables(registered),
		RegisteredTables:                registered,
		RegisteredTableCatalogRevision:  1,
		MaxConcurrentHistoryProjections: 1,
	}, integrationTestLogger(slog.LevelWarn))
	reader := Actor{UserID: "pull-admission-user", SourceID: "reader"}
	writer := Actor{UserID: reader.UserID, SourceID: "writer"}
	mustPushUserBundle(t, ctx, service, writer, schema, 1, uuid.New(), "Admission")
	mustInitializeEmptyScope(t, ctx, service, reader.UserID, reader.SourceID)
	proof := testSchemaGenerationProofForService(service)
	handlers := NewHTTPSyncHandlers(service, integrationTestLogger(slog.LevelWarn))

	requireNoHeldPermit := func(t *testing.T) {
		t.Helper()
		require.Zero(t, service.historyProjectionRuntimeMetricsSnapshot().Active)
		require.Zero(t, len(service.historyProjectionPermits))
	}

	t.Run("refused without a pooled connection", func(t *testing.T) {
		releasePermit, err := service.acquireHistoryProjection(ctx)
		require.NoError(t, err)
		defer releasePermit()
		releasePool := exhaustPool(t, pool)
		defer releasePool()

		pullCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		acquiresBefore := pool.Stat().AcquireCount()
		_, err = service.ProcessPull(pullCtx, reader, proof, 0, 10, 0)
		var projectionErr *GenerationProjectionError
		require.True(t, errors.As(err, &projectionErr), "pull waited for the pool instead of refusing: %v", err)
		require.Equal(t, HistoryProjectionCapacityExceeded, projectionErr.Code)
		require.Equal(t, acquiresBefore, pool.Stat().AcquireCount(), "refused pull acquired a pooled connection")

		request := httptest.NewRequest(http.MethodGet, "/sync/pull?after_bundle_seq=0", nil)
		request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		request = request.WithContext(ContextWithActor(pullCtx, reader))
		recorder := httptest.NewRecorder()
		handlers.HandlePull(recorder, request)
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		require.Equal(t, "1", recorder.Header().Get("Retry-After"))
		var response GenerationErrorResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Equal(t, string(HistoryProjectionCapacityExceeded), response.Error)
		require.True(t, response.Retryable)
		require.Equal(t, "retry", response.Remediation)
	})
	requireNoHeldPermit(t)

	t.Run("admitted pull returns its page and releases the permit", func(t *testing.T) {
		response, err := service.ProcessPull(ctx, reader, proof, 0, 10, 0)
		require.NoError(t, err)
		require.Len(t, response.Bundles, 1)
		require.Equal(t, int64(1), response.StableBundleSeq)
		requireNoHeldPermit(t)
	})

	t.Run("errors after admission release the permit", func(t *testing.T) {
		_, err := service.ProcessPull(ctx, reader, proof, 5, 10, 0)
		var aheadErr *CheckpointAheadError
		require.ErrorAs(t, err, &aheadErr)
		requireNoHeldPermit(t)

		_, err = service.ProcessPull(ctx, Actor{UserID: reader.UserID, SourceID: "never-connected"}, proof, 0, 10, 0)
		var sourceErr *SourceGenerationError
		require.ErrorAs(t, err, &sourceErr)
		require.Equal(t, SourceGenerationUnbound, sourceErr.Code)
		requireNoHeldPermit(t)
	})

	t.Run("admitted pull cancelled while waiting for the pool releases the permit", func(t *testing.T) {
		releasePool := exhaustPool(t, pool)
		defer releasePool()
		pullCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			_, err := service.ProcessPull(pullCtx, reader, proof, 0, 10, 0)
			done <- err
		}()
		require.Eventually(t, func() bool {
			return service.historyProjectionRuntimeMetricsSnapshot().Active == 1
		}, 5*time.Second, time.Millisecond, "pull was not admitted")
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled pull did not return")
		}
		requireNoHeldPermit(t)
	})
	requireNoHeldPermit(t)
}

// exhaustPool holds every connection the pool can hand out, waiting for
// connections that background workers hold briefly, and returns their release.
// The watch listener is not counted: it takes its connection out of the pool.
func exhaustPool(t *testing.T, pool *pgxpool.Pool) func() {
	t.Helper()
	var held []*pgxpool.Conn
	release := func() {
		for _, conn := range held {
			conn.Release()
		}
		held = nil
	}
	for len(held) < int(pool.Config().MaxConns) {
		acquireCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := pool.Acquire(acquireCtx)
		cancel()
		if err != nil {
			release()
			require.NoError(t, err)
		}
		held = append(held, conn)
	}
	return release
}
