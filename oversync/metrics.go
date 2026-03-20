// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"time"
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

func (s *SyncService) observeStageErr(ctx context.Context, op, stage string, start time.Time, count, attempt int, err error) {
	s.observeStage(ctx, op, stage, start, count, attempt, err != nil && !errors.Is(err, errServiceShuttingDown))
}
