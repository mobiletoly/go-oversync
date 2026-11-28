// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// Package serverenv reads the environment variables of the example servers and
// applies them through the oversync sizing helpers. It is a reference for host
// applications; the oversync library itself never reads the environment.
//
// Variables (all optional):
//
//	OVERSYNC_MACHINE      small | medium | large | auto: size the permits for that machine
//	OVERSYNC_CPUS         CPUs of the machine (overrides the preset or auto value)
//	OVERSYNC_MEMORY       memory of the machine, e.g. 2GiB or 2147483648 (required for
//	                      auto when GOMEMLIMIT is unset)
//	OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS, OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS,
//	OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS
//	                      explicit permits; they win over the machine sizing
//	OVERSYNC_DB_POOL_MAX_CONNS, OVERSYNC_DB_POOL_MIN_CONNS
//	                      pool size; default: pool_max_conns from DATABASE_URL, else
//	                      the oversync connection budget
//	OVERSYNC_LOG_STAGE_TIMINGS  1 | true | yes
//	OVERSYNC_LOG_LEVEL    debug | info | warn | error (default info)
package serverenv

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Settings are the parsed example-server variables.
type Settings struct {
	// Machine is nil when no machine variable is set: the library defaults apply.
	Machine                            *oversync.Machine
	MaxConcurrentHistoryProjections    int
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
	PoolMaxConns                       int32
	PoolMinConns                       int32
	poolMinConnsSet                    bool
	LogStageTimings                    bool
	LogLevel                           slog.Level
}

// Load parses the variables through getenv (normally os.Getenv).
func Load(getenv func(string) string) (Settings, error) {
	var settings Settings
	var err error
	if settings.Machine, err = loadMachine(getenv); err != nil {
		return Settings{}, err
	}
	for _, permit := range []struct {
		name   string
		target *int
	}{
		{"OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", &settings.MaxConcurrentHistoryProjections},
		{"OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", &settings.MaxConcurrentSnapshotBuilds},
		{"OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", &settings.MaxConcurrentSnapshotChunkRequests},
	} {
		if *permit.target, err = positiveInt(getenv, permit.name); err != nil {
			return Settings{}, err
		}
	}
	maxConns, err := positiveInt(getenv, "OVERSYNC_DB_POOL_MAX_CONNS")
	if err != nil {
		return Settings{}, err
	}
	settings.PoolMaxConns = int32(maxConns)
	if value := strings.TrimSpace(getenv("OVERSYNC_DB_POOL_MIN_CONNS")); value != "" {
		parsed, parseErr := strconv.ParseInt(value, 10, 32)
		if parseErr != nil || parsed < 0 {
			return Settings{}, fmt.Errorf("OVERSYNC_DB_POOL_MIN_CONNS must be a non-negative integer")
		}
		settings.PoolMinConns, settings.poolMinConnsSet = int32(parsed), true
	}
	if settings.PoolMaxConns > 0 && settings.PoolMinConns > settings.PoolMaxConns {
		return Settings{}, fmt.Errorf("OVERSYNC_DB_POOL_MIN_CONNS cannot exceed OVERSYNC_DB_POOL_MAX_CONNS")
	}
	switch strings.ToLower(strings.TrimSpace(getenv("OVERSYNC_LOG_STAGE_TIMINGS"))) {
	case "1", "true", "yes":
		settings.LogStageTimings = true
	}
	if settings.LogLevel, err = logLevel(getenv("OVERSYNC_LOG_LEVEL")); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

// ApplyService sets the explicit permits, then sizes the remaining ones for
// the machine. It returns nil when no machine is configured.
func (s Settings) ApplyService(cfg *oversync.ServiceConfig) (*oversync.Sizing, error) {
	if s.MaxConcurrentHistoryProjections > 0 {
		cfg.MaxConcurrentHistoryProjections = s.MaxConcurrentHistoryProjections
	}
	if s.MaxConcurrentSnapshotBuilds > 0 {
		cfg.MaxConcurrentSnapshotBuilds = s.MaxConcurrentSnapshotBuilds
	}
	if s.MaxConcurrentSnapshotChunkRequests > 0 {
		cfg.MaxConcurrentSnapshotChunkRequests = s.MaxConcurrentSnapshotChunkRequests
	}
	if s.LogStageTimings {
		cfg.LogStageTimings = true
	}
	if s.Machine == nil {
		return nil, nil
	}
	sizing, err := cfg.ApplySizing(*s.Machine)
	if err != nil {
		return nil, err
	}
	return &sizing, nil
}

// ApplyPool sizes the pool: OVERSYNC_DB_POOL_MAX_CONNS, else pool_max_conns in
// the DSN, else the connection budget of cfg. MinConns defaults to 2.
func (s Settings) ApplyPool(poolConfig *pgxpool.Config, dsn string, cfg oversync.ServiceConfig) {
	switch {
	case s.PoolMaxConns > 0:
		poolConfig.MaxConns = s.PoolMaxConns
	case strings.Contains(dsn, "pool_max_conns="):
		// pgxpool.ParseConfig already applied it.
	default:
		poolConfig.MaxConns = cfg.ConnectionBudget().PoolMaxConns
	}
	switch {
	case s.poolMinConnsSet:
		poolConfig.MinConns = s.PoolMinConns
	case !strings.Contains(dsn, "pool_min_conns="):
		poolConfig.MinConns = min(2, poolConfig.MaxConns)
	}
	poolConfig.MinConns = min(poolConfig.MinConns, poolConfig.MaxConns)
}

// ApplyGoMemoryLimit sets the recommended Go memory limit when the process has
// none from GOMEMLIMIT. It is the host's decision; the library never does it.
// It returns the limit now in force, or 0.
func ApplyGoMemoryLimit(sizing *oversync.Sizing, getenv func(string) string) int64 {
	if strings.TrimSpace(getenv("GOMEMLIMIT")) != "" || sizing == nil {
		if limit := debug.SetMemoryLimit(-1); limit < 1<<62 {
			return limit
		}
		return 0
	}
	debug.SetMemoryLimit(sizing.GoMemoryLimitBytes)
	return sizing.GoMemoryLimitBytes
}

func loadMachine(getenv func(string) string) (*oversync.Machine, error) {
	name := strings.ToLower(strings.TrimSpace(getenv("OVERSYNC_MACHINE")))
	cpus, err := positiveInt(getenv, "OVERSYNC_CPUS")
	if err != nil {
		return nil, err
	}
	memory, err := byteSize(getenv("OVERSYNC_MEMORY"))
	if err != nil {
		return nil, fmt.Errorf("OVERSYNC_MEMORY: %w", err)
	}
	var machine oversync.Machine
	switch name {
	case "":
		if cpus == 0 && memory == 0 {
			return nil, nil
		}
		if cpus == 0 || memory == 0 {
			return nil, fmt.Errorf("set both OVERSYNC_CPUS and OVERSYNC_MEMORY, or OVERSYNC_MACHINE")
		}
	case "small":
		machine = oversync.MachineSmall
	case "medium":
		machine = oversync.MachineMedium
	case "large":
		machine = oversync.MachineLarge
	case "auto":
		machine = oversync.RuntimeMachine()
	default:
		return nil, fmt.Errorf("OVERSYNC_MACHINE must be small, medium, large or auto")
	}
	if cpus > 0 {
		machine.CPUs = cpus
	}
	if memory > 0 {
		machine.MemoryBytes = memory
	}
	if machine.MemoryBytes == 0 {
		return nil, fmt.Errorf("OVERSYNC_MACHINE=auto needs OVERSYNC_MEMORY or GOMEMLIMIT")
	}
	return &machine, nil
}

func positiveInt(getenv func(string) string, name string) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

// byteSize parses a byte count with an optional KiB, MiB or GiB suffix.
func byteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	multiplier := int64(1)
	for suffix, factor := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(value, suffix) {
			value, multiplier = strings.TrimSuffix(value, suffix), factor
			break
		}
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 1 || parsed > (1<<62)/multiplier {
		return 0, fmt.Errorf("must be a positive byte count such as 2GiB")
	}
	return parsed * multiplier, nil
}

func logLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("OVERSYNC_LOG_LEVEL must be debug, info, warn or error")
}
