package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

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

func TestProcessPull_EmitsStageMetrics(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_metrics_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	recorder := &collectedStageMetrics{}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "pull-metrics-test",
		StageMetrics:              recorder,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "pull-metrics-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}

	rowID := uuid.New()
	_, err := pushRowsViaSession(t, ctx, svc, writer, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice","email":"alice@example.com"}`, rowID)),
	}})
	require.NoError(t, err)

	resp, err := svc.ProcessPull(ctx, reader, 0, 10, 0)
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
