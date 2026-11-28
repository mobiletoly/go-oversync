// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package serverenv

import (
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestLoad_EmptyEnvironmentKeepsLibraryDefaults(t *testing.T) {
	settings, err := Load(env(nil))
	require.NoError(t, err)
	require.Nil(t, settings.Machine)
	require.Equal(t, slog.LevelInfo, settings.LogLevel)

	cfg := &oversync.ServiceConfig{}
	sizing, err := settings.ApplyService(cfg)
	require.NoError(t, err)
	require.Nil(t, sizing)
	require.Zero(t, cfg.MaxConcurrentSnapshotBuilds)

	poolConfig, err := pgxpool.ParseConfig("postgres://u@localhost/db")
	require.NoError(t, err)
	settings.ApplyPool(poolConfig, "postgres://u@localhost/db", *cfg)
	require.Equal(t, cfg.ConnectionBudget().PoolMaxConns, poolConfig.MaxConns)
	require.EqualValues(t, 2, poolConfig.MinConns)
}

func TestLoad_MachinePresetSizesAndExplicitPermitsWin(t *testing.T) {
	settings, err := Load(env(map[string]string{
		"OVERSYNC_MACHINE":                        "small",
		"OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS": "1",
		"OVERSYNC_LOG_STAGE_TIMINGS":              "yes",
		"OVERSYNC_LOG_LEVEL":                      "debug",
	}))
	require.NoError(t, err)
	require.Equal(t, oversync.MachineSmall, *settings.Machine)
	require.Equal(t, slog.LevelDebug, settings.LogLevel)

	cfg := &oversync.ServiceConfig{}
	sizing, err := settings.ApplyService(cfg)
	require.NoError(t, err)
	require.NotNil(t, sizing)
	require.Equal(t, []int{4, 1, 4}, []int{cfg.MaxConcurrentHistoryProjections, cfg.MaxConcurrentSnapshotBuilds, cfg.MaxConcurrentSnapshotChunkRequests})
	require.True(t, cfg.LogStageTimings)
	require.Equal(t, int64(2<<30)/100*75, sizing.GoMemoryLimitBytes)
}

func TestLoad_CustomMachineAndMemorySuffixes(t *testing.T) {
	settings, err := Load(env(map[string]string{"OVERSYNC_CPUS": "3", "OVERSYNC_MEMORY": "1536MiB"}))
	require.NoError(t, err)
	require.Equal(t, oversync.Machine{CPUs: 3, MemoryBytes: 1536 << 20}, *settings.Machine)

	settings, err = Load(env(map[string]string{"OVERSYNC_MACHINE": "large", "OVERSYNC_MEMORY": "32GiB"}))
	require.NoError(t, err)
	require.Equal(t, oversync.Machine{CPUs: 8, MemoryBytes: 32 << 30}, *settings.Machine)
}

func TestLoad_RejectsInvalidValues(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"unknown machine":   {"OVERSYNC_MACHINE": "huge"},
		"cpus without mem":  {"OVERSYNC_CPUS": "2"},
		"bad memory":        {"OVERSYNC_MEMORY": "2GB", "OVERSYNC_CPUS": "2"},
		"zero permit":       {"OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS": "0"},
		"negative projects": {"OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS": "-1"},
		"zero pool":         {"OVERSYNC_DB_POOL_MAX_CONNS": "0"},
		"negative min":      {"OVERSYNC_DB_POOL_MIN_CONNS": "-1"},
		"min exceeds max":   {"OVERSYNC_DB_POOL_MAX_CONNS": "3", "OVERSYNC_DB_POOL_MIN_CONNS": "4"},
		"unknown log level": {"OVERSYNC_LOG_LEVEL": "trace"},
		"too small machine": {"OVERSYNC_CPUS": "2", "OVERSYNC_MEMORY": "1KiB"},
	} {
		settings, err := Load(env(values))
		if err == nil {
			_, err = settings.ApplyService(&oversync.ServiceConfig{})
		}
		require.Error(t, err, name)
	}
}

func TestApplyPool_PrefersEnvironmentThenDSNThenBudget(t *testing.T) {
	cfg := oversync.ServiceConfig{MaxConcurrentHistoryProjections: 8}
	dsn := "postgres://u@localhost/db?pool_max_conns=7&pool_min_conns=1"
	poolConfig, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	Settings{}.ApplyPool(poolConfig, dsn, cfg)
	require.EqualValues(t, 7, poolConfig.MaxConns)
	require.EqualValues(t, 1, poolConfig.MinConns)

	settings, err := Load(env(map[string]string{"OVERSYNC_DB_POOL_MAX_CONNS": "30", "OVERSYNC_DB_POOL_MIN_CONNS": "0"}))
	require.NoError(t, err)
	settings.ApplyPool(poolConfig, dsn, cfg)
	require.EqualValues(t, 30, poolConfig.MaxConns)
	require.EqualValues(t, 0, poolConfig.MinConns)
}

func TestApplyGoMemoryLimit_RespectsGOMEMLIMIT(t *testing.T) {
	sizing := &oversync.Sizing{GoMemoryLimitBytes: 1 << 40}
	before := ApplyGoMemoryLimit(nil, env(nil))
	require.Equal(t, before, ApplyGoMemoryLimit(sizing, env(map[string]string{"GOMEMLIMIT": "1GiB"})), "an explicit GOMEMLIMIT is left alone")
}
