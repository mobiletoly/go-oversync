package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type collectedStageMetrics struct {
	mu      sync.Mutex
	records []StageTiming
}

func (c *collectedStageMetrics) ObserveStage(ctx context.Context, timing StageTiming) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, timing)
}

func (c *collectedStageMetrics) stagesForOperation(op string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.records))
	for _, rec := range c.records {
		if rec.Operation == op {
			out = append(out, rec.Stage)
		}
	}
	return out
}

func (c *collectedStageMetrics) recordsForOperation(op string) map[string][]StageTiming {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]StageTiming)
	for _, rec := range c.records {
		if rec.Operation == op {
			out[rec.Stage] = append(out[rec.Stage], rec)
		}
	}
	return out
}

func requireContainsAllStages(t *testing.T, got []string, expected ...string) {
	t.Helper()
	set := make(map[string]struct{}, len(got))
	for _, stage := range got {
		set[stage] = struct{}{}
	}
	for _, stage := range expected {
		_, ok := set[stage]
		require.Truef(t, ok, "expected stage %q in %v", stage, got)
	}
}

func TestBootstrapProgress_IsStageAwareBoundedAndSecretSafe(t *testing.T) {
	var logs bytes.Buffer
	service := &SyncService{
		config: &ServiceConfig{LogStageTimings: true,
			RegisteredTableCatalogRevision: 1},
		logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	progress := newBootstrapProgress(service, 17)
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	progress.now = func() time.Time { return now }
	progress.interval = 5 * time.Second
	progress.lastLogAt = now

	progress.maybeLog("business_row_scan_canonicalization", 1, 3, 0)
	require.Empty(t, logs.String(), "fast work must not emit periodic progress noise")

	now = now.Add(5 * time.Second)
	progress.maybeLog("business_row_scan_canonicalization", 2, 8, 0)
	progress.maybeLog("coherent_state_history_validation", 2, 8, 0)
	require.Equal(t, 1, strings.Count(logs.String(), "Sync bootstrap adoption progress"), "the heartbeat must be rate bounded")

	now = now.Add(5 * time.Second)
	progress.maybeLog("coherent_state_history_validation", 4, 13, 0)
	now = now.Add(5 * time.Second)
	progress.maybeLog("pristine_baseline_persistence", 5, 15, 1)

	output := logs.String()
	require.Contains(t, output, `"stage":"business_row_scan_canonicalization"`)
	require.Contains(t, output, `"stage":"coherent_state_history_validation"`)
	require.Contains(t, output, `"stage":"pristine_baseline_persistence"`)
	require.Contains(t, output, `"completed_scope_count":5`)
	require.Contains(t, output, `"total_scope_count":17`)
	require.Contains(t, output, `"business_row_count":15`)
	require.Contains(t, output, `"adopted_scope_count":1`)
	for _, secret := range []string{
		"scope-secret", "row-key-secret", "payload-secret", "hash-secret",
		"postgres://secret", "jwt-secret",
	} {
		require.NotContains(t, output, secret)
	}
}

func TestObserveStageDuration_RecordsZeroDuration(t *testing.T) {
	recorder := &collectedStageMetrics{}
	service := &SyncService{config: &ServiceConfig{StageMetrics: recorder,
		RegisteredTableCatalogRevision: 1}}
	service.observeStageDuration(context.Background(), "bootstrap", "business_row_scan_canonicalization", 0, 0, 1, true)

	records := recorder.recordsForOperation("bootstrap")["business_row_scan_canonicalization"]
	require.Len(t, records, 1)
	require.Zero(t, records[0].Duration)
	require.True(t, records[0].Error)
}

func TestProcessPull_EmitsStageMetrics(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_metrics_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	recorder := &collectedStageMetrics{}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-metrics-test",
		StageMetrics: recorder,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "pull-metrics-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)

	rowID := uuid.New()
	_, err := pushRowsOneShot(t, ctx, svc, writer, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice","email":"alice@example.com"}`, rowID)),
	}})
	require.NoError(t, err)

	resp, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, resp.Bundles, 1)

	stages := recorder.stagesForOperation("pull")
	requireContainsAllStages(t, stages,
		"total",
		"transaction",
		"retained_floor_check",
		"resolve_stable_bundle_seq",
		"list_bundle_seqs",
		"load_bundles",
	)
}
