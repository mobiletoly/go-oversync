// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"time"
)

const (
	MetricsOpUpload   = "upload"
	MetricsOpDownload = "download"

	MetricsStageTotal = "total"

	// Upload transaction (service-level) stages.
	MetricsStageUploadTxBatched = "tx_batched"
	MetricsStageUploadTxSlow    = "tx_slow"

	// Upload per-batch stages (tx-level).
	MetricsStageUpsertsValidate     = "upserts_validate"
	MetricsStageUpsertsInBatchIndex = "upserts_in_batch_index"
	MetricsStageUpsertsFKPrecheck   = "upserts_fk_precheck"
	MetricsStageUpsertsApplyBatched = "upserts_apply_batched"
	MetricsStageUpsertsApplySlow    = "upserts_apply_slow"
	MetricsStageUpsertsMaterialize  = "upserts_materialize"

	MetricsStageDeletesValidate     = "deletes_validate"
	MetricsStageDeletesApplyBatched = "deletes_apply_batched"
	MetricsStageDeletesApplySlow    = "deletes_apply_slow"
	MetricsStageDeletesMaterialize  = "deletes_materialize"

	// Download stages.
	MetricsStageDownloadWatermark = "watermark"
	MetricsStageDownloadFetch     = "fetch"
)

type StageTiming struct {
	Operation string
	Stage     string
	Duration  time.Duration
	Count     int
	Attempt   int
	Error     bool
}

type StageMetricsRecorder interface {
	ObserveStage(ctx context.Context, timing StageTiming)
}

type StageMetricsRecorderFunc func(ctx context.Context, timing StageTiming)

func (f StageMetricsRecorderFunc) ObserveStage(ctx context.Context, timing StageTiming) {
	f(ctx, timing)
}

func (s *SyncService) stageTimingEnabled() bool {
	if s == nil || s.config == nil {
		return false
	}
	return s.config.StageMetrics != nil || s.config.LogStageTimings
}

func (s *SyncService) stageStart() time.Time {
	if !s.stageTimingEnabled() {
		return time.Time{}
	}
	return time.Now()
}

func (s *SyncService) observeStage(ctx context.Context, op, stage string, start time.Time, count, attempt int, hadError bool) {
	if start.IsZero() || s == nil || s.config == nil {
		return
	}

	d := time.Since(start)
	timing := StageTiming{
		Operation: op,
		Stage:     stage,
		Duration:  d,
		Count:     count,
		Attempt:   attempt,
		Error:     hadError,
	}

	if s.config.StageMetrics != nil {
		s.config.StageMetrics.ObserveStage(ctx, timing)
	}
	if s.config.LogStageTimings && s.logger != nil {
		s.logger.Debug("Stage timing",
			"op", timing.Operation,
			"stage", timing.Stage,
			"duration", timing.Duration,
			"count", timing.Count,
			"attempt", timing.Attempt,
			"error", timing.Error,
		)
	}
}
