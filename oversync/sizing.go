// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Machine describes the resources available to one server process (one
// container or VM). See the "Sizing and deployment" documentation.
type Machine struct {
	// CPUs is the number of CPUs the process may use (the container CPU limit,
	// which Go 1.25 also uses for GOMAXPROCS). Must be at least 1.
	CPUs int
	// MemoryBytes is the memory limit of the process or container. Must be at
	// least 512 MiB.
	MemoryBytes int64
}

// Presets are ordinary Machine values; ApplySizing applies one rule to all of them.
var (
	MachineSmall  = Machine{CPUs: 2, MemoryBytes: 2 << 30}  // 2 vCPU / 2 GiB
	MachineMedium = Machine{CPUs: 4, MemoryBytes: 8 << 30}  // 4 vCPU / 8 GiB
	MachineLarge  = Machine{CPUs: 8, MemoryBytes: 16 << 30} // 8 vCPU / 16 GiB
)

const (
	minimumMachineMemoryBytes int64 = 512 << 20
	// Share of the machine memory recommended as the Go soft memory limit; the
	// rest covers goroutine stacks, runtime overhead and the operating system.
	goMemoryLimitPercent = 75
	// Share of the Go memory limit that concurrent pushes may use.
	historyProjectionMemoryPercent = 40
	// Estimated memory per busy permit, from laptop measurements (see the
	// sizing documentation): a push holds about six copies of its body, a
	// build about 32 MiB, a chunk request about 2.5 copies of its chunk.
	pushMemoryMultiple                = 6
	snapshotBuildMemoryBytes    int64 = 32 << 20
	snapshotChunkMemoryTenths         = 25
	processBaseMemoryBytes      int64 = 64 << 20
	maxSizedSnapshotBuilds            = 16
	maxSizedSnapshotChunks            = 32
	backgroundWorkerConnections       = 2
	shortRequestConnections           = 4
)

// RuntimeMachine reports runtime.GOMAXPROCS(0) and derives MemoryBytes from
// the current Go memory limit, assuming that limit follows the recommended 75%
// of the machine memory. MemoryBytes is 0 when no Go memory limit is set; the
// host must then fill it in. RuntimeMachine reads these settings and changes
// nothing.
func RuntimeMachine() Machine {
	machine := Machine{CPUs: runtime.GOMAXPROCS(0)}
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 {
		machine.MemoryBytes = math.MaxInt64
		if share := limit / goMemoryLimitPercent; share <= math.MaxInt64/100 {
			machine.MemoryBytes = share * 100
		}
	}
	return machine
}

// Sizing is the per-process capacity the sizing rule derives for one Machine.
type Sizing struct {
	Machine                            Machine
	MaxConcurrentHistoryProjections    int
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
	// GoMemoryLimitBytes is the recommended Go soft memory limit: 75% of
	// Machine.MemoryBytes. Oversync never sets it; the host sets GOMEMLIMIT or
	// calls debug.SetMemoryLimit before serving.
	GoMemoryLimitBytes int64
	// EstimatedPeakBytes estimates the heap when every permit is busy with
	// maximum-size work. It is an estimate from laptop measurements.
	EstimatedPeakBytes int64
}

// ApplySizing fills MaxConcurrentHistoryProjections, MaxConcurrentSnapshotBuilds
// and MaxConcurrentSnapshotChunkRequests where they are zero (explicit values
// are kept) and returns the resulting Sizing. It reads the configured bundle
// and chunk byte limits (zero means their defaults) and leaves every other
// field unchanged. It returns an error when the machine cannot admit one
// maximum-size push.
//
// The rule: builds = CPUs (at most 16); chunk requests = 2 × CPUs (2..32);
// history projections = the smaller of 2 × CPUs and the number of
// maximum-size pushes that fit in 40% of the recommended Go memory limit
// (1..32).
func (c *ServiceConfig) ApplySizing(m Machine) (Sizing, error) {
	if c == nil {
		return Sizing{}, fmt.Errorf("service config cannot be nil")
	}
	if m.CPUs < 1 {
		return Sizing{}, fmt.Errorf("machine CPUs must be at least 1")
	}
	if m.MemoryBytes < minimumMachineMemoryBytes {
		return Sizing{}, fmt.Errorf("machine memory must be at least %d bytes", minimumMachineMemoryBytes)
	}
	bundleBytes, _ := c.sizingByteLimits()
	goLimit := m.MemoryBytes / 100 * goMemoryLimitPercent
	pushCost := pushMemoryMultiple * bundleBytes
	fittingPushes := goLimit / 100 * historyProjectionMemoryPercent / pushCost
	if fittingPushes < 1 {
		return Sizing{}, fmt.Errorf("machine memory %d bytes cannot admit one %d-byte push; lower MaxBytesPerBundle or use a larger machine", m.MemoryBytes, bundleBytes)
	}
	if c.MaxConcurrentHistoryProjections == 0 {
		c.MaxConcurrentHistoryProjections = int(min(fittingPushes, int64(2*m.CPUs), int64(maxMaxConcurrentHistoryProjections)))
	}
	if c.MaxConcurrentSnapshotBuilds == 0 {
		c.MaxConcurrentSnapshotBuilds = min(m.CPUs, maxSizedSnapshotBuilds)
	}
	if c.MaxConcurrentSnapshotChunkRequests == 0 {
		c.MaxConcurrentSnapshotChunkRequests = min(2*m.CPUs, maxSizedSnapshotChunks)
	}
	return Sizing{
		Machine:                            m,
		MaxConcurrentHistoryProjections:    c.MaxConcurrentHistoryProjections,
		MaxConcurrentSnapshotBuilds:        c.MaxConcurrentSnapshotBuilds,
		MaxConcurrentSnapshotChunkRequests: c.MaxConcurrentSnapshotChunkRequests,
		GoMemoryLimitBytes:                 goLimit,
		EstimatedPeakBytes:                 c.estimatedPeakBytes(),
	}, nil
}

// sizingByteLimits returns the bundle and chunk byte limits, defaulting zero.
func (c *ServiceConfig) sizingByteLimits() (bundleBytes, chunkBytes int64) {
	bundleBytes, chunkBytes = int64(c.MaxBytesPerBundle), c.MaxBytesPerSnapshotChunk
	if bundleBytes <= 0 {
		bundleBytes = int64(defaultMaxBytesPerBundle)
	}
	if chunkBytes <= 0 {
		chunkBytes = defaultMaxBytesPerSnapshotChunk
	}
	return bundleBytes, chunkBytes
}

// estimatedPeakBytes estimates the heap with every permit busy; zero permit
// fields count as their defaults.
func (c *ServiceConfig) estimatedPeakBytes() int64 {
	projections, builds, chunks := c.effectivePermits()
	bundleBytes, chunkBytes := c.sizingByteLimits()
	return processBaseMemoryBytes +
		int64(projections)*pushMemoryMultiple*bundleBytes +
		int64(builds)*snapshotBuildMemoryBytes +
		int64(chunks)*chunkBytes*snapshotChunkMemoryTenths/10
}

func (c *ServiceConfig) effectivePermits() (projections, builds, chunks int) {
	projections, builds, chunks = c.MaxConcurrentHistoryProjections, c.MaxConcurrentSnapshotBuilds, c.MaxConcurrentSnapshotChunkRequests
	if projections <= 0 {
		projections = defaultMaxConcurrentHistoryProjections
	}
	if builds <= 0 {
		builds = defaultMaxConcurrentSnapshotBuilds
	}
	if chunks <= 0 {
		chunks = defaultMaxConcurrentSnapshotChunkRequests
	}
	return projections, builds, chunks
}

// ConnectionBudget is the PostgreSQL connections one service instance can use
// at once, derived from its permits.
type ConnectionBudget struct {
	// HistoryProjections, SnapshotBuilds and SnapshotChunks are the permits:
	// each admitted request holds at most one connection.
	HistoryProjections int
	SnapshotBuilds     int
	SnapshotChunks     int
	// BackgroundWorkers covers snapshot cleanup and scope-receipt cleanup.
	BackgroundWorkers int
	// ShortRequests is a reserve for requests that take a connection without a
	// permit: pull planning, push-outcome acknowledgment, source binding,
	// snapshot deletion, watch subscription and status.
	ShortRequests int
	// PoolMaxConns is the sum above: the minimum pgxpool MaxConns.
	PoolMaxConns int32
	// Listener is 1 when the bundle change watch is enabled: that connection is
	// taken out of the pool, so PostgreSQL sees it in addition to PoolMaxConns.
	Listener int
	// PerInstance is PoolMaxConns + Listener. Add the readiness admission
	// pool's size when the host uses NewReadinessPoolPair. Across a fleet,
	// instances × PerInstance must fit PostgreSQL max_connections.
	PerInstance int
}

// ConnectionBudget returns the budget for this configuration; zero permit
// fields count as their defaults.
func (c ServiceConfig) ConnectionBudget() ConnectionBudget {
	projections, builds, chunks := c.effectivePermits()
	budget := ConnectionBudget{
		HistoryProjections: projections,
		SnapshotBuilds:     builds,
		SnapshotChunks:     chunks,
		BackgroundWorkers:  backgroundWorkerConnections,
		ShortRequests:      shortRequestConnections,
	}
	budget.PoolMaxConns = int32(projections + builds + chunks + backgroundWorkerConnections + shortRequestConnections)
	if c.BundleChangeWatch.Enabled {
		budget.Listener = 1
	}
	budget.PerInstance = int(budget.PoolMaxConns) + budget.Listener
	return budget
}

// EffectiveSettings is the normalized configuration one instance runs with.
type EffectiveSettings struct {
	MaxConcurrentHistoryProjections    int
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
	// Limits is exactly what generation discovery advertises.
	// Limits.MaxPushOutcomeDecodedBytes is 0 until Bootstrap has read the store
	// (discovery answers 503 service_not_ready until then).
	Limits                  WireLimits
	SnapshotSessionTTL      time.Duration
	InitializationLeaseTTL  time.Duration
	RetainedBundlesPerUser  int64
	MaxClientSourcesPerUser int
	UploadLockTimeout       time.Duration
	BundleChangeWatch       BundleChangeWatchConfig
	ConnectionBudget        ConnectionBudget
	// PoolMaxConns is observed from the pool; 0 without a pool.
	PoolMaxConns int32
	// AdmissionPoolMaxConns is observed from a readiness pool pair; 0 without one.
	AdmissionPoolMaxConns int32
	// GoMemoryLimitBytes is the process's Go memory limit; 0 when unset.
	GoMemoryLimitBytes int64
	// EstimatedPeakBytes estimates the heap with every permit busy.
	EstimatedPeakBytes int64
	// SharedSettingsFingerprint is a SHA-256 over the settings that must be
	// identical on every instance: the advertised limits, TTLs, retention, the
	// client source limit and the watch channel. Compare it across instances.
	SharedSettingsFingerprint string
}

// EffectiveSettings returns the settings in force for this instance.
func (s *SyncService) EffectiveSettings() EffectiveSettings {
	cfg := s.config
	watch := s.effectiveBundleChangeWatchConfig()
	limits := s.wireLimits()
	if _, known := s.pushOutcomeCeilingBytes(); !known {
		limits.MaxPushOutcomeDecodedBytes = 0
	}
	settings := EffectiveSettings{
		MaxConcurrentHistoryProjections:    cfg.MaxConcurrentHistoryProjections,
		MaxConcurrentSnapshotBuilds:        cfg.MaxConcurrentSnapshotBuilds,
		MaxConcurrentSnapshotChunkRequests: cfg.MaxConcurrentSnapshotChunkRequests,
		Limits:                             limits,
		SnapshotSessionTTL:                 s.snapshotSessionTTL(),
		InitializationLeaseTTL:             s.initializationLeaseTTL(),
		RetainedBundlesPerUser:             s.retainedBundlesPerUser(),
		MaxClientSourcesPerUser:            cfg.MaxClientSourcesPerUser,
		UploadLockTimeout:                  cfg.UploadLockTimeout,
		BundleChangeWatch:                  watch,
		ConnectionBudget:                   cfg.ConnectionBudget(),
		PoolMaxConns:                       poolMaxConns(s.pool),
		GoMemoryLimitBytes:                 currentGoMemoryLimit(),
		EstimatedPeakBytes:                 cfg.estimatedPeakBytes(),
	}
	if cfg.Readiness != nil && cfg.Readiness.PoolPair != nil && cfg.Readiness.PoolPair.admission != nil {
		settings.AdmissionPoolMaxConns = cfg.Readiness.PoolPair.admission.Config().MaxConns
	}
	settings.SharedSettingsFingerprint = sharedSettingsFingerprint(settings)
	return settings
}

func sharedSettingsFingerprint(settings EffectiveSettings) string {
	shared := struct {
		Limits                  WireLimits
		SnapshotSessionTTL      time.Duration
		InitializationLeaseTTL  time.Duration
		RetainedBundlesPerUser  int64
		MaxClientSourcesPerUser int
		WatchEnabled            bool
		NotifyChannel           string
	}{settings.Limits, settings.SnapshotSessionTTL, settings.InitializationLeaseTTL, settings.RetainedBundlesPerUser,
		settings.MaxClientSourcesPerUser, settings.BundleChangeWatch.Enabled, settings.BundleChangeWatch.NotifyChannel}
	encoded, err := json.Marshal(shared)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *SyncService) logEffectiveSettings() {
	settings := s.EffectiveSettings()
	s.logger.Info("Oversync effective settings",
		"history_projections", settings.MaxConcurrentHistoryProjections,
		"snapshot_builds", settings.MaxConcurrentSnapshotBuilds,
		"snapshot_chunk_requests", settings.MaxConcurrentSnapshotChunkRequests,
		"pool_max_conns", settings.PoolMaxConns,
		"connection_budget_pool", settings.ConnectionBudget.PoolMaxConns,
		"connection_budget_per_instance", settings.ConnectionBudget.PerInstance,
		"go_memory_limit_bytes", settings.GoMemoryLimitBytes,
		"estimated_peak_bytes", settings.EstimatedPeakBytes,
		"max_push_decoded_bytes", settings.Limits.MaxPushDecodedBytes,
		"max_push_outcome_decoded_bytes", settings.Limits.MaxPushOutcomeDecodedBytes,
		"max_pull_decoded_bytes", settings.Limits.MaxPullDecodedBytes,
		"snapshot_session_ttl", settings.SnapshotSessionTTL,
		"retained_bundles_per_user", settings.RetainedBundlesPerUser,
		"shared_settings_fingerprint", settings.SharedSettingsFingerprint,
	)
}

func poolMaxConns(pool *pgxpool.Pool) int32 {
	if pool == nil {
		return 0
	}
	return pool.Config().MaxConns
}

// currentGoMemoryLimit returns the Go memory limit, or 0 when none is set.
func currentGoMemoryLimit() int64 {
	if limit := debug.SetMemoryLimit(-1); limit < math.MaxInt64 {
		return limit
	}
	return 0
}

var unsetGoMemoryLimitWarning sync.Once

// warnOnCapacityMismatch reports configurations that are valid but likely to
// misbehave under load. It never refuses: the host decides.
func warnOnCapacityMismatch(logger *slog.Logger, pool *pgxpool.Pool, cfg *ServiceConfig) {
	budget := cfg.ConnectionBudget()
	if maxConns := poolMaxConns(pool); pool != nil && maxConns < budget.PoolMaxConns {
		logger.Warn("PostgreSQL pool is smaller than the Oversync connection budget; admitted requests may wait for a connection",
			"pool_max_conns", maxConns, "connection_budget_pool", budget.PoolMaxConns)
	}
	limit := currentGoMemoryLimit()
	if limit == 0 {
		unsetGoMemoryLimitWarning.Do(func() {
			logger.Warn("Go memory limit is not set; set GOMEMLIMIT (recommended: 75% of the container memory)")
		})
		return
	}
	if peak := cfg.estimatedPeakBytes(); peak > limit/10*8 {
		logger.Warn("Estimated Oversync peak memory exceeds 80% of the Go memory limit; lower the concurrency permits or MaxBytesPerBundle",
			"estimated_peak_bytes", peak, "go_memory_limit_bytes", limit)
	}
}
