// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// historyProjectionRuntimeMetrics deliberately uses a fixed field per public
// failure code. This keeps metric cardinality bounded: request identities,
// generations, sources, keys, and bundle sequences never become labels.
type historyProjectionRuntimeMetrics struct {
	checkpointOnly      atomic.Int64
	active              atomic.Int64
	activeHighWater     atomic.Int64
	canonicalRows       atomic.Int64
	canonicalBytes      atomic.Int64
	projectedRows       atomic.Int64
	projectedBytes      atomic.Int64
	canonicalRowsHigh   atomic.Int64
	canonicalBytesHigh  atomic.Int64
	projectedRowsHigh   atomic.Int64
	projectedBytesHigh  atomic.Int64
	retainedBuffersHigh atomic.Int64
	retainedRowsHigh    atomic.Int64
	retainedBytesHigh   atomic.Int64
	ownedBuffersHigh    atomic.Int64
	ownedBytesHigh      atomic.Int64
	historyUnavailable  atomic.Int64
	integrityFailures   atomic.Int64
	bundleLimitFailures atomic.Int64
	capacityFailures    atomic.Int64
}

type historyProjectionRuntimeMetricsSnapshot struct {
	Active, ActiveHighWater                                      int64
	CanonicalRows, CanonicalBytes, ProjectedRows, ProjectedBytes int64
	CanonicalRowsHigh, CanonicalBytesHigh                        int64
	ProjectedRowsHigh, ProjectedBytesHigh                        int64
	RetainedBuffersHigh, RetainedRowsHigh, RetainedBytesHigh     int64
	OwnedBuffersHigh, OwnedBytesHigh                             int64
	HistoryUnavailable, IntegrityFailures                        int64
	BundleLimitFailures, CapacityFailures                        int64
}

func (s *SyncService) historyProjectionRuntimeMetricsSnapshot() historyProjectionRuntimeMetricsSnapshot {
	m := &s.historyProjectionMetrics
	return historyProjectionRuntimeMetricsSnapshot{
		Active: m.active.Load(), ActiveHighWater: m.activeHighWater.Load(),
		CanonicalRows: m.canonicalRows.Load(), CanonicalBytes: m.canonicalBytes.Load(),
		ProjectedRows: m.projectedRows.Load(), ProjectedBytes: m.projectedBytes.Load(),
		CanonicalRowsHigh: m.canonicalRowsHigh.Load(), CanonicalBytesHigh: m.canonicalBytesHigh.Load(),
		ProjectedRowsHigh: m.projectedRowsHigh.Load(), ProjectedBytesHigh: m.projectedBytesHigh.Load(),
		RetainedBuffersHigh: m.retainedBuffersHigh.Load(), RetainedRowsHigh: m.retainedRowsHigh.Load(), RetainedBytesHigh: m.retainedBytesHigh.Load(),
		OwnedBuffersHigh: m.ownedBuffersHigh.Load(), OwnedBytesHigh: m.ownedBytesHigh.Load(),
		HistoryUnavailable: m.historyUnavailable.Load(), IntegrityFailures: m.integrityFailures.Load(),
		BundleLimitFailures: m.bundleLimitFailures.Load(), CapacityFailures: m.capacityFailures.Load(),
	}
}

func (s *SyncService) observeHistoryProjection(canonical, projected *Bundle, err error) {
	m := &s.historyProjectionMetrics
	if canonical != nil {
		m.canonicalRows.Add(canonical.RowCount)
		m.canonicalBytes.Add(canonical.ByteCount)
		observeAtomicHighWater(&m.canonicalRowsHigh, canonical.RowCount)
		observeAtomicHighWater(&m.canonicalBytesHigh, canonical.ByteCount)
	}
	if projected != nil && projected.RowCount == 0 {
		m.checkpointOnly.Add(1)
	}
	if projected != nil {
		m.projectedRows.Add(projected.RowCount)
		m.projectedBytes.Add(projected.ByteCount)
		observeAtomicHighWater(&m.projectedRowsHigh, projected.RowCount)
		observeAtomicHighWater(&m.projectedBytesHigh, projected.ByteCount)
	}
	if canonical != nil && projected != nil {
		s.observeHistoryProjectionRetention(2, canonical.RowCount+projected.RowCount, canonical.ByteCount+projected.ByteCount)
		buffers, bytes := retainedBundleAllocations(canonical, projected)
		s.observeHistoryProjectionOwnedRetention(buffers, bytes)
	}
	var projectionErr *GenerationProjectionError
	if !errors.As(err, &projectionErr) {
		return
	}
	switch projectionErr.Code {
	case GenerationHistoryUnavailable:
		m.historyUnavailable.Add(1)
	case GenerationProjectionIntegrityError:
		m.integrityFailures.Add(1)
	case BundleLimitExceeded:
		m.bundleLimitFailures.Add(1)
	case HistoryProjectionCapacityExceeded:
		m.capacityFailures.Add(1)
	}
}

func retainedBundleAllocations(bundles ...*Bundle) (int64, int64) {
	var buffers, bytes int64
	for _, bundle := range bundles {
		if bundle == nil {
			continue
		}
		if bundle.Rows != nil {
			buffers++
			bytes += int64(cap(bundle.Rows)) * int64(unsafe.Sizeof(BundleRow{}))
		}
		for i := range bundle.Rows {
			row := &bundle.Rows[i]
			if row.Payload != nil {
				buffers++
				bytes += int64(cap(row.Payload))
			}
			if row.Key != nil {
				buffers++
				for key, value := range row.Key {
					bytes += int64(len(key))
					if text, ok := value.(string); ok {
						bytes += int64(len(text))
					}
				}
			}
		}
	}
	return buffers, bytes
}

func retainedPullAllocations(admitted []Bundle, current ...*Bundle) (int64, int64) {
	var buffers, bytes int64
	if admitted != nil {
		buffers++
		bytes += int64(cap(admitted)) * int64(unsafe.Sizeof(Bundle{}))
	}
	for i := range admitted {
		rowBuffers, rowBytes := retainedBundleAllocations(&admitted[i])
		buffers += rowBuffers
		bytes += rowBytes
	}
	currentBuffers, currentBytes := retainedBundleAllocations(current...)
	return buffers + currentBuffers, bytes + currentBytes
}

func (s *SyncService) observeHistoryProjectionOwnedRetention(buffers, bytes int64) {
	m := &s.historyProjectionMetrics
	observeAtomicHighWater(&m.ownedBuffersHigh, buffers)
	observeAtomicHighWater(&m.ownedBytesHigh, bytes)
}

func (s *SyncService) observeHistoryProjectionRetention(buffers, rows, bytes int64) {
	m := &s.historyProjectionMetrics
	observeAtomicHighWater(&m.retainedBuffersHigh, buffers)
	observeAtomicHighWater(&m.retainedRowsHigh, rows)
	observeAtomicHighWater(&m.retainedBytesHigh, bytes)
}

func (s *SyncService) projectCommittedBundle(selection generationSelection, canonical *Bundle) (*Bundle, error) {
	projected, err := projectCommittedBundle(selection, canonical)
	s.observeHistoryProjection(canonical, projected, err)
	return projected, err
}
