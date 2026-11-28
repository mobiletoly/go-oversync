// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryProjectionAdmission_IsFiniteAndReleasesExactlyOnce(t *testing.T) {
	service := &SyncService{historyProjectionPermits: make(chan struct{}, 1)}
	release, err := service.acquireHistoryProjection(context.Background())
	require.NoError(t, err)

	_, err = service.acquireHistoryProjection(context.Background())
	var projectionErr *GenerationProjectionError
	require.ErrorAs(t, err, &projectionErr)
	require.Equal(t, HistoryProjectionCapacityExceeded, projectionErr.Code)

	metrics := service.historyProjectionRuntimeMetricsSnapshot()
	require.Equal(t, int64(1), metrics.Active)
	require.Equal(t, int64(1), metrics.ActiveHighWater)
	require.Equal(t, int64(1), metrics.CapacityFailures)

	release()
	release()
	require.Equal(t, int64(0), service.historyProjectionRuntimeMetricsSnapshot().Active)
	releaseAgain, err := service.acquireHistoryProjection(context.Background())
	require.NoError(t, err)
	releaseAgain()
}

func TestHistoryProjectionConfig_DefaultsAndRejectsInvalidBounds(t *testing.T) {
	defaults := ServiceConfig{}
	require.NoError(t, normalizeHistoryProjectionConfig(&defaults))
	require.Equal(t, defaultMaxRowsPerBundle, defaults.MaxRowsPerBundle)
	require.Equal(t, defaultMaxBytesPerBundle, defaults.MaxBytesPerBundle)
	require.Equal(t, defaultMaxBytesPerPullResponse, defaults.MaxBytesPerPullResponse)
	require.Equal(t, defaultMaxConcurrentHistoryProjections, defaults.MaxConcurrentHistoryProjections)

	tests := []ServiceConfig{
		{MaxRowsPerBundle: -1},
		{MaxRowsPerBundle: maxMaxRowsPerBundle + 1},
		{MaxBytesPerBundle: -1},
		{MaxBytesPerBundle: maxMaxBytesPerBundle + 1},
		{MaxBytesPerBundle: 1024, MaxBytesPerPullResponse: 1023},
		{MaxBytesPerBundle: 1024, MaxBytesPerPullResponse: maxMaxBytesPerPullResponse + 1},
		{MaxConcurrentHistoryProjections: -1},
		{MaxConcurrentHistoryProjections: maxMaxConcurrentHistoryProjections + 1},
	}
	for _, config := range tests {
		require.Error(t, normalizeHistoryProjectionConfig(&config), "%+v", config)
	}
}

func TestHistoryProjectionMetrics_CountBoundedDimensions(t *testing.T) {
	service := &SyncService{}
	canonical := &Bundle{RowCount: 7, ByteCount: 101}
	projected := &Bundle{RowCount: 3, ByteCount: 44}
	service.observeHistoryProjection(canonical, projected, nil)
	for _, code := range []GenerationProjectionErrorCode{
		GenerationHistoryUnavailable,
		GenerationProjectionIntegrityError,
		BundleLimitExceeded,
		HistoryProjectionCapacityExceeded,
	} {
		service.observeHistoryProjection(nil, nil, projectionError(code, "test", "safe"))
	}

	metrics := service.historyProjectionRuntimeMetricsSnapshot()
	require.Equal(t, int64(7), metrics.CanonicalRows)
	require.Equal(t, int64(101), metrics.CanonicalBytes)
	require.Equal(t, int64(3), metrics.ProjectedRows)
	require.Equal(t, int64(44), metrics.ProjectedBytes)
	require.Equal(t, int64(2), metrics.RetainedBuffersHigh)
	require.Equal(t, int64(10), metrics.RetainedRowsHigh)
	require.Equal(t, int64(145), metrics.RetainedBytesHigh)
	require.Equal(t, int64(1), metrics.HistoryUnavailable)
	require.Equal(t, int64(1), metrics.IntegrityFailures)
	require.Equal(t, int64(1), metrics.BundleLimitFailures)
	require.Equal(t, int64(1), metrics.CapacityFailures)
}

func TestHistoryProjectionResourceBound_PureProjectorKeepsFixedHighWater(t *testing.T) {
	selection := projectionTestSelection(t, GenerationTableContract{
		Schema: "app", Table: "items", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}, {Name: "value", WireType: GenerationWireTypeText}},
	})

	run := func(population int) historyProjectionRuntimeMetricsSnapshot {
		service := &SyncService{historyProjectionPermits: make(chan struct{}, 1)}
		release, err := service.acquireHistoryProjection(context.Background())
		require.NoError(t, err)
		defer release()
		rows := make([]BundleRow, 8)
		for row := range rows {
			id := fmt.Sprintf("item-%02d", row)
			rows[row] = BundleRow{
				Schema: "app", Table: "items", Key: SyncKey{"id": id}, Op: OpInsert, RowVersion: 9,
				Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"value":%q}`, id, strings.Repeat("v", 256))),
			}
		}
		canonical := canonicalBundleForProjectionTest(t, rows...)
		for range population {
			projected, err := service.projectCommittedBundle(selection, canonical)
			require.NoError(t, err)
			require.Len(t, projected.Rows, 8)
		}
		return service.historyProjectionRuntimeMetricsSnapshot()
	}

	small := run(16)
	large := run(512)
	t.Logf("population=16 retained_buffers_high=%d retained_rows_high=%d retained_bytes_high=%d permits_high=%d canonical_rows_total=%d",
		small.RetainedBuffersHigh, small.RetainedRowsHigh, small.RetainedBytesHigh, small.ActiveHighWater, small.CanonicalRows)
	t.Logf("population=512 retained_buffers_high=%d retained_rows_high=%d retained_bytes_high=%d permits_high=%d canonical_rows_total=%d",
		large.RetainedBuffersHigh, large.RetainedRowsHigh, large.RetainedBytesHigh, large.ActiveHighWater, large.CanonicalRows)
	require.Equal(t, small.CanonicalRowsHigh, large.CanonicalRowsHigh)
	require.Equal(t, small.CanonicalBytesHigh, large.CanonicalBytesHigh)
	require.Equal(t, small.ProjectedRowsHigh, large.ProjectedRowsHigh)
	require.Equal(t, small.ProjectedBytesHigh, large.ProjectedBytesHigh)
	require.Equal(t, small.RetainedBuffersHigh, large.RetainedBuffersHigh)
	require.Equal(t, small.RetainedRowsHigh, large.RetainedRowsHigh)
	require.Equal(t, small.RetainedBytesHigh, large.RetainedBytesHigh)
	require.Equal(t, int64(2), large.RetainedBuffersHigh)
	require.Equal(t, int64(1), large.ActiveHighWater)
	require.Equal(t, int64(8*512), large.CanonicalRows)
	require.Equal(t, int64(8*16), small.CanonicalRows)
}
