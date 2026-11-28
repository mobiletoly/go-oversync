// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"log/slog"
	"runtime/debug"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func sizingTestConfig() *ServiceConfig {
	tables := runtimeServiceTestRegisteredTables()
	return &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(tables), RegisteredTables: tables, RegisteredTableCatalogRevision: 1}
}

func TestApplySizing_PresetsFollowOneRule(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		machine                       Machine
		projections, builds, chunks   int
		goLimit                       int64
		poolMaxConns                  int32
		perInstanceWithWatch          int
		maxEstimatedPeakFractionOfMem float64
	}{
		{"small", MachineSmall, 4, 2, 4, 2 << 30 / 100 * 75, 16, 17, 0.4},
		{"medium", MachineMedium, 8, 4, 8, 8 << 30 / 100 * 75, 26, 27, 0.2},
		{"large", MachineLarge, 16, 8, 16, 16 << 30 / 100 * 75, 46, 47, 0.2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sizingTestConfig()
			cfg.BundleChangeWatch.Enabled = true
			sizing, err := cfg.ApplySizing(tc.machine)
			require.NoError(t, err)
			require.Equal(t, tc.machine, sizing.Machine)
			require.Equal(t, []int{tc.projections, tc.builds, tc.chunks}, []int{sizing.MaxConcurrentHistoryProjections, sizing.MaxConcurrentSnapshotBuilds, sizing.MaxConcurrentSnapshotChunkRequests})
			require.Equal(t, []int{tc.projections, tc.builds, tc.chunks}, []int{cfg.MaxConcurrentHistoryProjections, cfg.MaxConcurrentSnapshotBuilds, cfg.MaxConcurrentSnapshotChunkRequests})
			require.Equal(t, tc.goLimit, sizing.GoMemoryLimitBytes)
			require.Less(t, float64(sizing.EstimatedPeakBytes), tc.maxEstimatedPeakFractionOfMem*float64(tc.machine.MemoryBytes))
			budget := cfg.ConnectionBudget()
			require.Equal(t, tc.poolMaxConns, budget.PoolMaxConns)
			require.Equal(t, 1, budget.Listener)
			require.Equal(t, tc.perInstanceWithWatch, budget.PerInstance)
			_, err = newTestRuntimeService(nil, cfg, nil)
			require.NoError(t, err, "a sized configuration must be valid")
		})
	}
}

func TestApplySizing_ExplicitValuesWinAndOtherFieldsAreUntouched(t *testing.T) {
	cfg := sizingTestConfig()
	cfg.MaxConcurrentSnapshotBuilds = 1
	cfg.MaxRowsPerBundle = 77
	sizing, err := cfg.ApplySizing(MachineLarge)
	require.NoError(t, err)
	require.Equal(t, 1, sizing.MaxConcurrentSnapshotBuilds)
	require.Equal(t, 1, cfg.MaxConcurrentSnapshotBuilds)
	require.Equal(t, 16, cfg.MaxConcurrentHistoryProjections)
	require.Equal(t, 77, cfg.MaxRowsPerBundle)
	require.Zero(t, cfg.MaxBytesPerBundle, "sizing reads but never sets wire limits")
}

func TestApplySizing_MemoryBoundsPushesAndRejectsTooSmallMachines(t *testing.T) {
	cfg := sizingTestConfig()
	cfg.MaxBytesPerBundle = 64 << 20
	sizing, err := cfg.ApplySizing(Machine{CPUs: 8, MemoryBytes: 2 << 30})
	require.NoError(t, err)
	require.Equal(t, 1, sizing.MaxConcurrentHistoryProjections, "2 GiB fits one 64 MiB push in 40% of its Go limit")

	for _, machine := range []Machine{{CPUs: 0, MemoryBytes: 2 << 30}, {CPUs: 2, MemoryBytes: 256 << 20}, {CPUs: 2, MemoryBytes: 1 << 30}} {
		cfg := sizingTestConfig()
		cfg.MaxBytesPerBundle = 64 << 20
		_, err := cfg.ApplySizing(machine)
		require.Error(t, err, "%+v", machine)
	}
	var nilConfig *ServiceConfig
	_, err = nilConfig.ApplySizing(MachineSmall)
	require.Error(t, err)
}

func TestConnectionBudget_DefaultsAndWatchListener(t *testing.T) {
	budget := ServiceConfig{}.ConnectionBudget()
	require.Equal(t, ConnectionBudget{
		HistoryProjections: defaultMaxConcurrentHistoryProjections,
		SnapshotBuilds:     defaultMaxConcurrentSnapshotBuilds,
		SnapshotChunks:     defaultMaxConcurrentSnapshotChunkRequests,
		BackgroundWorkers:  2,
		ShortRequests:      4,
		PoolMaxConns:       18,
		PerInstance:        18,
	}, budget)
	require.Equal(t, 4, defaultMaxConcurrentSnapshotBuilds)
	withWatch := ServiceConfig{BundleChangeWatch: BundleChangeWatchConfig{Enabled: true}}.ConnectionBudget()
	require.Equal(t, 19, withWatch.PerInstance)
}

func TestRuntimeMachine_ReadsWithoutChanging(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	machine := RuntimeMachine()
	require.Equal(t, previous, debug.SetMemoryLimit(-1))
	require.GreaterOrEqual(t, machine.CPUs, 1)
	if previous == 0 || previous >= 1<<62 {
		require.Zero(t, machine.MemoryBytes)
	}
}

// Sizing changes only per-process permits; the limits clients read stay
// byte-identical to the defaults for every preset.
func TestSizing_AdvertisedWireLimitsAreUnchanged(t *testing.T) {
	expected := WireLimits{
		MaxPushRows:                     10000,
		MaxPushDecodedBytes:             16<<20 + 4096,
		MaxPullBundles:                  5000,
		MaxPullDecodedBytes:             32 << 20,
		MaxSnapshotRowsPerChunk:         5000,
		MaxSnapshotDecodedBytesPerChunk: 16 << 20,
		MaxSnapshotRows:                 1 << 50,
		MaxSnapshotLogicalBytes:         1 << 50,
		MaxRowDecodedBytes:              4 << 20,
		MaxPushOutcomeDecodedBytes:      16<<20 + 8192,
	}
	for _, machine := range []*Machine{nil, &MachineSmall, &MachineMedium, &MachineLarge} {
		cfg := sizingTestConfig()
		if machine != nil {
			_, err := cfg.ApplySizing(*machine)
			require.NoError(t, err)
		}
		svc, err := newTestRuntimeService(nil, cfg, nil)
		require.NoError(t, err)
		require.Equal(t, expected, svc.wireLimits())
	}
}

func TestEffectiveSettings_FingerprintCoversSharedSettingsOnly(t *testing.T) {
	newService := func(mutate func(*ServiceConfig)) *SyncService {
		cfg := sizingTestConfig()
		mutate(cfg)
		svc, err := newTestRuntimeService(nil, cfg, nil)
		require.NoError(t, err)
		return svc
	}
	base := newService(func(*ServiceConfig) {}).EffectiveSettings()
	require.Len(t, base.SharedSettingsFingerprint, 64)
	require.Equal(t, defaultMaxConcurrentSnapshotBuilds, base.MaxConcurrentSnapshotBuilds)
	require.Equal(t, defaultSnapshotSessionTTL, base.SnapshotSessionTTL)
	require.Equal(t, defaultInitializationLeaseTTL, base.InitializationLeaseTTL)
	require.Equal(t, int64(16<<20+8192), base.Limits.MaxPushOutcomeDecodedBytes, "without a database the new-store ceiling is advertised, as discovery does")

	permits := newService(func(c *ServiceConfig) { c.MaxConcurrentSnapshotBuilds = 1 }).EffectiveSettings()
	require.Equal(t, base.SharedSettingsFingerprint, permits.SharedSettingsFingerprint, "per-process permits may differ between instances")
	for name, mutate := range map[string]func(*ServiceConfig){
		"retention":  func(c *ServiceConfig) { c.RetainedBundlesPerUser = 5 },
		"bundle":     func(c *ServiceConfig) { c.MaxRowsPerBundle = 5 },
		"ttl":        func(c *ServiceConfig) { c.SnapshotSessionTTL = 1 },
		"watch":      func(c *ServiceConfig) { c.BundleChangeWatch.NotifyChannel = "other_channel" },
		"sources":    func(c *ServiceConfig) { c.MaxClientSourcesPerUser = 2 },
		"chunk_rows": func(c *ServiceConfig) { c.MaxRowsPerSnapshotChunk = 7 },
	} {
		require.NotEqual(t, base.SharedSettingsFingerprint, newService(mutate).EffectiveSettings().SharedSettingsFingerprint, name)
	}
}

func TestNewRuntimeService_WarnsOnSmallPoolAndMemoryLimit(t *testing.T) {
	poolConfig, err := pgxpool.ParseConfig("postgres://oversync@127.0.0.1:1/unused?connect_timeout=1")
	require.NoError(t, err)
	poolConfig.MaxConns = 3
	poolConfig.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	previous := debug.SetMemoryLimit(100 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })
	logs := &recordingSlogHandler{}
	svc, err := NewRuntimeService(pool, sizingTestConfig(), slog.New(logs))
	require.NoError(t, err, "a small pool or memory limit warns but never refuses")
	attrs, ok := recordedSnapshotAttrs(logs, "PostgreSQL pool is smaller than the Oversync connection budget; admitted requests may wait for a connection")
	require.True(t, ok)
	require.EqualValues(t, 3, attrs["pool_max_conns"])
	require.EqualValues(t, 18, attrs["connection_budget_pool"])
	_, ok = recordedSnapshotAttrs(logs, "Estimated Oversync peak memory exceeds 80% of the Go memory limit; lower the concurrency permits or MaxBytesPerBundle")
	require.True(t, ok)
	settings := svc.EffectiveSettings()
	require.EqualValues(t, 3, settings.PoolMaxConns)
	require.EqualValues(t, 100<<20, settings.GoMemoryLimitBytes)
}

func TestGenerationDiscovery_WaitsForStoredPushOutcomeCeiling(t *testing.T) {
	poolConfig, err := pgxpool.ParseConfig("postgres://oversync@127.0.0.1:1/unused?connect_timeout=1")
	require.NoError(t, err)
	poolConfig.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	svc, err := NewRuntimeService(pool, sizingTestConfig(), slog.New(&recordingSlogHandler{}))
	require.NoError(t, err)
	_, known := svc.pushOutcomeCeilingBytes()
	require.False(t, known)
	_, err = svc.GetGeneration(context.Background(), Actor{UserID: "user", SourceID: "device"}, svc.schemaGenerationRegistry.GenerationIDs()[0])
	require.ErrorIs(t, err, errServiceNotReady)
	require.Zero(t, svc.EffectiveSettings().Limits.MaxPushOutcomeDecodedBytes, "the stored ceiling is unknown before Bootstrap")
	require.NoError(t, svc.validateGenerationResponseSizes(), "size checks measure the widest ceiling before it is known")

	svc.pushOutcomeCeiling.Store(16<<20 + 8192)
	response, err := svc.GetGeneration(context.Background(), Actor{UserID: "user", SourceID: "device"}, svc.schemaGenerationRegistry.GenerationIDs()[0])
	require.NoError(t, err)
	require.Equal(t, int64(16<<20+8192), response.Limits.MaxPushOutcomeDecodedBytes)
	require.Equal(t, response.Limits, svc.EffectiveSettings().Limits)
}
