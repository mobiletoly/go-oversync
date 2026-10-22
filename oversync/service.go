// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type serviceLifecycleState string
type bootstrapReadinessState string

const (
	serviceLifecycleRunning                serviceLifecycleState   = "running"
	serviceLifecycleShuttingDown           serviceLifecycleState   = "shutting_down"
	serviceLifecycleClosed                 serviceLifecycleState   = "closed"
	bootstrapReadinessNotReady             bootstrapReadinessState = "not_ready"
	bootstrapReadinessBootstrapping        bootstrapReadinessState = "bootstrapping"
	bootstrapReadinessReady                bootstrapReadinessState = "ready"
	defaultMaxBundlesPerPull               int                     = 5000
	defaultMaxRowsPerBundle                int                     = 10000
	maxMaxRowsPerBundle                    int                     = 100000
	defaultMaxBytesPerBundle               int                     = 16 << 20
	maxMaxBytesPerBundle                   int                     = 64 << 20
	defaultMaxBytesPerPullResponse         int64                   = 32 << 20
	maxMaxBytesPerPullResponse             int64                   = 128 << 20
	defaultMaxConcurrentHistoryProjections int                     = 4
	maxMaxConcurrentHistoryProjections     int                     = 32
	historyEnvelopeAllowance               int64                   = 4096
	// maxCompactSingleRowEnvelopeBytes is a conservative upper bound for the
	// fixed JSON members, maximum escaped source token, hashes, and int64
	// counters surrounding one compact row. Runtime codec sizing remains exact.
	maxCompactSingleRowEnvelopeBytes          int64         = 1024
	defaultPullBundlesPerRequest              int           = 1000
	defaultMaxRowsPerSnapshotChunk            int           = 5000
	defaultRowsPerSnapshotChunk               int           = 1000
	defaultSnapshotSessionTTL                 time.Duration = 15 * time.Minute
	defaultSnapshotMaterializationBatchRows   int           = 512
	defaultSnapshotMaterializationBatchBytes  int64         = 4 << 20
	defaultMaxConcurrentSnapshotBuilds        int           = 8
	defaultMaxConcurrentSnapshotChunkRequests int           = 4
	defaultBytesPerSnapshotChunk              int64         = 4 << 20
	defaultMaxBytesPerSnapshotChunk           int64         = 16 << 20
	defaultMaxBytesPerSnapshotRow             int64         = 4 << 20
	defaultSnapshotCleanupInterval            time.Duration = time.Minute
	defaultSnapshotCleanupBatchRows           int           = 5000
	defaultSnapshotCleanupBatchSessions       int           = 32
	defaultSnapshotCleanupMaxBatchesPerRun    int           = 8
	defaultSnapshotCleanupBatchTimeout        time.Duration = 5 * time.Second
	defaultRetainedBundlesPerUser             int64         = 10000
	defaultRetentionPruneBatchSize            int64         = 1000
	defaultBundleChangeNotifyChannel          string        = "oversync_bundle_change_v1"
	defaultBundleChangeHeartbeatInterval      time.Duration = 25 * time.Second
	defaultMaxClientSourcesPerUser            int           = 64
	maxMaxClientSourcesPerUser                int           = 4096
)

var (
	errServiceShuttingDown = errors.New("sync service is shutting down")
	errServiceNotReady     = errors.New("sync service bootstrap is incomplete")
)

const (
	syncScopeColumnName = "_sync_scope_id"
	syncKeyTypeUUID     = "uuid"
	syncKeyTypeText     = "text"
)

type registeredTableRuntimeInfo struct {
	schemaName    string
	tableName     string
	tableID       int32
	syncKeyColumn string
	syncKeyType   string
	syncKeyKind   int16
}

// BundleChangeWatchConfig controls optional process-local bundle change watches.
type BundleChangeWatchConfig struct {
	Enabled           bool
	NotifyChannel     string
	HeartbeatInterval time.Duration
}

// RegisteredTable represents a table that is registered for sync operations
type RegisteredTable struct {
	Schema         string   `json:"schema"`                     // Schema name (e.g., "public", "crm", "business")
	Table          string   `json:"table"`                      // Table name (e.g., "users", "posts")
	SyncKeyColumns []string `json:"sync_key_columns,omitempty"` // Ordered sync key columns for the target bundle-based protocol
}

func (t RegisteredTable) normalizedSchema() string {
	schema := strings.ToLower(strings.TrimSpace(t.Schema))
	if schema == "" {
		return "public"
	}
	return schema
}

func (t RegisteredTable) normalizedTable() string {
	return strings.ToLower(strings.TrimSpace(t.Table))
}

func (t RegisteredTable) normalizedKey() string {
	return t.normalizedSchema() + "." + t.normalizedTable()
}

func (t RegisteredTable) normalizedSyncKeyColumns() []string {
	if len(t.SyncKeyColumns) == 0 {
		return nil
	}

	columns := make([]string, 0, len(t.SyncKeyColumns))
	seen := make(map[string]struct{}, len(t.SyncKeyColumns))
	for _, col := range t.SyncKeyColumns {
		normalized := strings.ToLower(strings.TrimSpace(col))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		columns = append(columns, normalized)
	}
	return columns
}

// SyncService provides the core synchronization functionality
// This is the main SDK component that developers integrate into their applications
type SyncService struct {
	pool                     *pgxpool.Pool
	logger                   *slog.Logger
	config                   *ServiceConfig
	bundleChangeHub          *bundleChangeHub
	registeredTables         map[string]bool // Set of "schema.table" combinations allowed in sync operations
	registeredTableInfo      map[string]registeredTableRuntimeInfo
	registeredTableByID      map[int32]registeredTableRuntimeInfo
	retainedTableByID        map[int32]registeredTableRuntimeInfo
	catalogMetadata          registeredTableCatalogMetadata
	columnTypesByTable       map[string]map[string]string
	schemaGenerationRegistry *SchemaGenerationRegistry
	generationViews          map[SchemaGenerationID]*schemaGenerationView

	// Internal adoption instrumentation is nil/empty in normal runtime use. It
	// keeps retry, progress, and owned-spool tests on the production Bootstrap
	// path without exposing a public configuration surface.
	adoptionSpoolDir          string
	adoptionHooks             *adoptionTestHooks
	catalogHooks              *registeredTableCatalogTestHooks
	uploadConnHooks           *userUploadConnTestHooks
	bootstrapProgressNow      func() time.Time
	bootstrapProgressInterval time.Duration

	// Schema discovery snapshot for bootstrap validation and FK-safe bundle ordering.
	discoveredSchema *DiscoveredSchema

	// Runtime lifecycle tracking.
	readinessReport             ReadinessReport
	readinessWithdrawn          bool
	readinessContentionHook     func()
	bootstrapMu                 sync.Mutex
	mu                          sync.RWMutex
	lifecycle                   serviceLifecycleState
	bootstrapReadiness          bootstrapReadinessState
	inFlightOps                 int
	userUploadGatesMu           sync.Mutex
	userUploadGates             map[string]*userUploadGate
	drainedCh                   chan struct{}
	drainStartedCh              chan struct{}
	bundleChangeListenerRunning bool
	bootstrapDrainedCh          chan struct{}
	closedCh                    chan struct{}
	closeOnce                   sync.Once

	snapshotBuildPermits       chan struct{}
	snapshotChunkPermits       chan struct{}
	historyProjectionPermits   chan struct{}
	snapshotCleanupMu          sync.Mutex
	snapshotCleanupCancel      context.CancelFunc
	snapshotCleanupDone        chan struct{}
	snapshotCleanupTrigger     chan string
	scopeReceiptCleanupMu      sync.Mutex
	scopeReceiptCleanupCancel  context.CancelFunc
	scopeReceiptCleanupDone    chan struct{}
	scopeReceiptCleanupTrigger chan string
	generationMetrics          generationRuntimeMetrics
	snapshotMetrics            snapshotRuntimeMetrics
	historyProjectionMetrics   historyProjectionRuntimeMetrics
	snapshotHooks              *snapshotTestHooks
}

type snapshotTestHooks struct {
	afterBuildPermit                   func(context.Context) error
	afterChunkPermit                   func(context.Context) error
	afterChunkSessionRead              func(context.Context) error
	afterDeleteSessionRead             func(context.Context) error
	afterSnapshotFence                 func(context.Context) error
	afterSnapshotCommit                func(context.Context) error
	afterSnapshotRowRead               func(context.Context) error
	beforeSnapshotCanonicalize         func(context.Context) error
	beforeSnapshotCopy                 func(context.Context) error
	beforeSnapshotFinalize             func(context.Context) error
	beforeSnapshotCleanupWorkerPublish func()
	afterSnapshotMaterializedRowScan   func(snapshotPayloadScanObservation)
	afterSnapshotChunkRowScan          func(snapshotPayloadScanObservation)
}

// ServiceConfig holds configuration for the sync service
type ServiceConfig struct {
	// ScopeTransactionAdmission optionally fences each scoped mutation inside its
	// SQL transaction. Nil preserves ordinary SDK behavior.
	ScopeTransactionAdmission      ScopeTransactionAdmission
	Readiness                      *ReadinessConfig   // Required application-owned deployment evidence
	SchemaGenerations              []SchemaGeneration // Required immutable client generation contracts
	AppName                        string             // Application name for connection tracking
	RegisteredTables               []RegisteredTable  // Schema.table combinations allowed for sync (required)
	RegisteredTableCatalogRevision int64              // Required target revision for the durable table catalog
	MaxClientSourcesPerUser        int                // Zero selects 64; explicit values must be in 1..4096
	// ReservedServerSourceIDs is the exact frozen set of SourceIDs rejected at
	// client boundaries and available to trusted ScopeManager writes.
	ReservedServerSourceIDs []string

	MaxRowsPerBundle                int   // Zero selects 10,000; hard maximum 100,000.
	MaxBytesPerBundle               int   // Zero selects 16 MiB; hard maximum 64 MiB.
	MaxPushOutcomeDecodedBytes      int64 // Zero selects MaxBytesPerBundle+8KiB; immutable per fresh store.
	MaxBytesPerPullResponse         int64
	MaxConcurrentHistoryProjections int
	InitializationLeaseTTL          time.Duration
	// Snapshot chunking limits. Zero uses the runtime defaults.
	DefaultRowsPerSnapshotChunk        int
	MaxRowsPerSnapshotChunk            int
	SnapshotSessionTTL                 time.Duration
	MaxRowsPerSnapshotSession          int64
	MaxBytesPerSnapshotSession         int64
	SnapshotMaterializationBatchRows   int
	SnapshotMaterializationBatchBytes  int64
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
	DefaultBytesPerSnapshotChunk       int64
	MaxBytesPerSnapshotChunk           int64
	MaxBytesPerSnapshotRow             int64
	SnapshotCleanupInterval            time.Duration
	SnapshotCleanupBatchRows           int
	SnapshotCleanupBatchSessions       int
	SnapshotCleanupMaxBatchesPerRun    int
	SnapshotCleanupBatchTimeout        time.Duration
	RetainedBundlesPerUser             int64
	RetentionPruneBatchSize            int64
	// UploadLockTimeout bounds lock waits inside upload transactions.
	// Zero disables SET LOCAL lock_timeout so lock waits are governed only by the request context,
	// which is the reliability-first default.
	UploadLockTimeout time.Duration

	// DependencyOverrides optionally adds explicit ordering constraints on top of discovered
	// DB FKs. Keys and values are "schema.table". Only affects ordering, not FK validation.
	DependencyOverrides map[string][]string

	// StageMetrics optionally records per-stage timings for sync hot paths.
	// The recorder is called synchronously; it must be fast and concurrency-safe.
	StageMetrics          StageMetricsRecorder
	GenerationObservation GenerationObservationConfig

	// LogStageTimings logs per-stage timings via the service logger at DEBUG.
	// Useful for profiling; keep disabled in production.
	LogStageTimings bool

	// BundleChangeWatch optionally enables metadata-only bundle change wakeups.
	BundleChangeWatch BundleChangeWatchConfig
}

func normalizeHistoryProjectionConfig(cfg *ServiceConfig) error {
	if cfg.MaxRowsPerBundle == 0 {
		cfg.MaxRowsPerBundle = defaultMaxRowsPerBundle
	}
	if cfg.MaxBytesPerBundle == 0 {
		cfg.MaxBytesPerBundle = defaultMaxBytesPerBundle
	}
	if cfg.MaxPushOutcomeDecodedBytes == 0 {
		cfg.MaxPushOutcomeDecodedBytes = int64(cfg.MaxBytesPerBundle) + 8192
	}
	if cfg.MaxPushOutcomeDecodedBytes < int64(cfg.MaxBytesPerBundle)+8192 || cfg.MaxPushOutcomeDecodedBytes > int64(maxMaxBytesPerBundle)+8192 {
		return fmt.Errorf("MaxPushOutcomeDecodedBytes must be between MaxBytesPerBundle+8192 and %d", int64(maxMaxBytesPerBundle)+8192)
	}
	if cfg.MaxBytesPerPullResponse == 0 {
		cfg.MaxBytesPerPullResponse = defaultMaxBytesPerPullResponse
	}
	if cfg.MaxConcurrentHistoryProjections == 0 {
		cfg.MaxConcurrentHistoryProjections = defaultMaxConcurrentHistoryProjections
	}
	if cfg.MaxRowsPerBundle < 1 || cfg.MaxRowsPerBundle > maxMaxRowsPerBundle {
		return fmt.Errorf("MaxRowsPerBundle must be in 1..%d", maxMaxRowsPerBundle)
	}
	if cfg.MaxBytesPerBundle < 2 || cfg.MaxBytesPerBundle > maxMaxBytesPerBundle {
		return fmt.Errorf("MaxBytesPerBundle must be in 2..%d", maxMaxBytesPerBundle)
	}
	if cfg.MaxBytesPerPullResponse < int64(cfg.MaxBytesPerBundle) || cfg.MaxBytesPerPullResponse > maxMaxBytesPerPullResponse {
		return fmt.Errorf("MaxBytesPerPullResponse must be between MaxBytesPerBundle and %d", maxMaxBytesPerPullResponse)
	}
	if cfg.MaxConcurrentHistoryProjections < 1 || cfg.MaxConcurrentHistoryProjections > maxMaxConcurrentHistoryProjections {
		return fmt.Errorf("MaxConcurrentHistoryProjections must be in 1..%d", maxMaxConcurrentHistoryProjections)
	}
	return nil
}

func normalizeBundleChangeWatchConfig(cfg BundleChangeWatchConfig) (BundleChangeWatchConfig, error) {
	if cfg.NotifyChannel == "" {
		cfg.NotifyChannel = defaultBundleChangeNotifyChannel
	}
	if _, err := quotePostgresNotificationChannel(cfg.NotifyChannel); err != nil {
		return BundleChangeWatchConfig{}, err
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultBundleChangeHeartbeatInterval
	}
	if cfg.Enabled && cfg.HeartbeatInterval <= 0 {
		return BundleChangeWatchConfig{}, fmt.Errorf("bundle change watch heartbeat interval must be positive when enabled")
	}
	return cfg, nil
}

func normalizeSnapshotConfig(cfg *ServiceConfig) error {
	if cfg.DefaultRowsPerSnapshotChunk < 0 || cfg.MaxRowsPerSnapshotChunk < 0 ||
		cfg.MaxRowsPerSnapshotSession < 0 || cfg.MaxBytesPerSnapshotSession < 0 || cfg.SnapshotSessionTTL < 0 {
		return fmt.Errorf("snapshot row and session limits must not be negative")
	}
	if cfg.DefaultRowsPerSnapshotChunk == 0 {
		cfg.DefaultRowsPerSnapshotChunk = defaultRowsPerSnapshotChunk
	}
	if cfg.MaxRowsPerSnapshotChunk == 0 {
		cfg.MaxRowsPerSnapshotChunk = defaultMaxRowsPerSnapshotChunk
	}
	if cfg.SnapshotSessionTTL == 0 {
		cfg.SnapshotSessionTTL = defaultSnapshotSessionTTL
	}
	if cfg.SnapshotMaterializationBatchRows == 0 {
		cfg.SnapshotMaterializationBatchRows = defaultSnapshotMaterializationBatchRows
	}
	if cfg.SnapshotMaterializationBatchBytes == 0 {
		cfg.SnapshotMaterializationBatchBytes = defaultSnapshotMaterializationBatchBytes
	}
	if cfg.MaxConcurrentSnapshotBuilds == 0 {
		cfg.MaxConcurrentSnapshotBuilds = defaultMaxConcurrentSnapshotBuilds
	}
	if cfg.MaxConcurrentSnapshotChunkRequests == 0 {
		cfg.MaxConcurrentSnapshotChunkRequests = defaultMaxConcurrentSnapshotChunkRequests
	}
	if cfg.DefaultBytesPerSnapshotChunk == 0 {
		cfg.DefaultBytesPerSnapshotChunk = defaultBytesPerSnapshotChunk
	}
	if cfg.MaxBytesPerSnapshotChunk == 0 {
		cfg.MaxBytesPerSnapshotChunk = defaultMaxBytesPerSnapshotChunk
	}
	if cfg.MaxBytesPerSnapshotRow == 0 {
		maximumAdvertisableRow := min(defaultMaxBytesPerSnapshotRow, int64(cfg.MaxBytesPerBundle))
		maximumAdvertisableRow = min(maximumAdvertisableRow, cfg.MaxBytesPerPullResponse-maxCompactSingleRowEnvelopeBytes)
		maximumAdvertisableRow = min(maximumAdvertisableRow, cfg.MaxBytesPerSnapshotChunk-maxCompactSingleRowEnvelopeBytes)
		if maximumAdvertisableRow <= 0 {
			return fmt.Errorf("wire response limits cannot represent one row plus its mandatory envelope")
		}
		cfg.MaxBytesPerSnapshotRow = maximumAdvertisableRow
	}
	if cfg.SnapshotCleanupInterval == 0 {
		cfg.SnapshotCleanupInterval = defaultSnapshotCleanupInterval
	}
	if cfg.SnapshotCleanupBatchRows == 0 {
		cfg.SnapshotCleanupBatchRows = defaultSnapshotCleanupBatchRows
	}
	if cfg.SnapshotCleanupBatchSessions == 0 {
		cfg.SnapshotCleanupBatchSessions = defaultSnapshotCleanupBatchSessions
	}
	if cfg.SnapshotCleanupMaxBatchesPerRun == 0 {
		cfg.SnapshotCleanupMaxBatchesPerRun = defaultSnapshotCleanupMaxBatchesPerRun
	}
	if cfg.SnapshotCleanupBatchTimeout == 0 {
		cfg.SnapshotCleanupBatchTimeout = defaultSnapshotCleanupBatchTimeout
	}
	if cfg.SnapshotMaterializationBatchRows <= 0 || cfg.SnapshotMaterializationBatchBytes <= 0 ||
		cfg.MaxConcurrentSnapshotBuilds <= 0 || cfg.MaxConcurrentSnapshotChunkRequests <= 0 ||
		cfg.DefaultBytesPerSnapshotChunk <= 0 || cfg.MaxBytesPerSnapshotChunk <= 0 ||
		cfg.MaxBytesPerSnapshotRow <= 0 || cfg.SnapshotCleanupInterval <= 0 ||
		cfg.SnapshotCleanupBatchRows <= 0 || cfg.SnapshotCleanupBatchSessions <= 0 ||
		cfg.SnapshotCleanupMaxBatchesPerRun <= 0 || cfg.SnapshotCleanupBatchTimeout <= 0 {
		return fmt.Errorf("snapshot bounded-work configuration values must be positive")
	}
	if cfg.DefaultRowsPerSnapshotChunk > cfg.MaxRowsPerSnapshotChunk {
		return fmt.Errorf("default rows per snapshot chunk must be <= max rows per snapshot chunk")
	}
	if cfg.SnapshotMaterializationBatchBytes < cfg.MaxBytesPerSnapshotRow {
		return fmt.Errorf("snapshot materialization batch bytes must be >= max bytes per snapshot row")
	}
	if cfg.DefaultBytesPerSnapshotChunk > cfg.MaxBytesPerSnapshotChunk {
		return fmt.Errorf("default bytes per snapshot chunk must be <= max bytes per snapshot chunk")
	}
	if cfg.MaxBytesPerSnapshotRow > cfg.MaxBytesPerSnapshotChunk {
		return fmt.Errorf("max bytes per snapshot row must be <= max bytes per snapshot chunk")
	}
	return nil
}

func validateAdvertisedWireLimits(cfg *ServiceConfig) error {
	if cfg.MaxBytesPerSnapshotRow > int64(cfg.MaxBytesPerBundle) {
		return fmt.Errorf("MaxBytesPerSnapshotRow must be <= MaxBytesPerBundle")
	}
	minimumOneRowEnvelope := cfg.MaxBytesPerSnapshotRow + maxCompactSingleRowEnvelopeBytes
	if minimumOneRowEnvelope < cfg.MaxBytesPerSnapshotRow {
		return fmt.Errorf("advertised one-row envelope size overflows")
	}
	if cfg.MaxBytesPerPullResponse < minimumOneRowEnvelope {
		return fmt.Errorf("MaxBytesPerPullResponse must represent MaxBytesPerSnapshotRow plus its mandatory envelope")
	}
	if cfg.MaxBytesPerSnapshotChunk < minimumOneRowEnvelope {
		return fmt.Errorf("MaxBytesPerSnapshotChunk must represent MaxBytesPerSnapshotRow plus its mandatory envelope")
	}
	return nil
}

func (s *SyncService) effectiveBundleChangeWatchConfig() BundleChangeWatchConfig {
	if s == nil || s.config == nil {
		cfg, _ := normalizeBundleChangeWatchConfig(BundleChangeWatchConfig{})
		return cfg
	}
	cfg, err := normalizeBundleChangeWatchConfig(s.config.BundleChangeWatch)
	if err != nil {
		return s.config.BundleChangeWatch
	}
	return cfg
}

func (s *SyncService) bundleChangeWatchEnabled() bool {
	return s != nil && s.config != nil && s.config.BundleChangeWatch.Enabled
}

func (s *SyncService) defaultRowsPerSnapshotChunk() int {
	if s != nil && s.config != nil && s.config.DefaultRowsPerSnapshotChunk > 0 {
		return s.config.DefaultRowsPerSnapshotChunk
	}
	return defaultRowsPerSnapshotChunk
}

func (s *SyncService) maxRowsPerSnapshotChunk() int {
	if s != nil && s.config != nil && s.config.MaxRowsPerSnapshotChunk > 0 {
		return s.config.MaxRowsPerSnapshotChunk
	}
	return defaultMaxRowsPerSnapshotChunk
}

func (s *SyncService) snapshotSessionTTL() time.Duration {
	if s != nil && s.config != nil && s.config.SnapshotSessionTTL > 0 {
		return s.config.SnapshotSessionTTL
	}
	return defaultSnapshotSessionTTL
}

func (s *SyncService) maxRowsPerSnapshotSession() int64 {
	if s != nil && s.config != nil && s.config.MaxRowsPerSnapshotSession > 0 {
		return s.config.MaxRowsPerSnapshotSession
	}
	return 0
}

func (s *SyncService) maxBytesPerSnapshotSession() int64 {
	if s != nil && s.config != nil && s.config.MaxBytesPerSnapshotSession > 0 {
		return s.config.MaxBytesPerSnapshotSession
	}
	return 0
}

func (s *SyncService) defaultBytesPerSnapshotChunk() int64 {
	return s.config.DefaultBytesPerSnapshotChunk
}
func (s *SyncService) maxBytesPerSnapshotChunk() int64 { return s.config.MaxBytesPerSnapshotChunk }
func (s *SyncService) maxBytesPerSnapshotRow() int64   { return s.config.MaxBytesPerSnapshotRow }

func (s *SyncService) retainedBundlesPerUser() int64 {
	if s != nil && s.config != nil && s.config.RetainedBundlesPerUser > 0 {
		return s.config.RetainedBundlesPerUser
	}
	return defaultRetainedBundlesPerUser
}

func (s *SyncService) retentionPruneBatchSize() int64 {
	if s != nil && s.config != nil && s.config.RetentionPruneBatchSize > 0 {
		return s.config.RetentionPruneBatchSize
	}
	return defaultRetentionPruneBatchSize
}

// NewRuntimeService creates a runtime-only sync service instance from an existing pool.
// It does not mutate database schema or discover runtime topology.
func NewRuntimeService(pool *pgxpool.Pool, config *ServiceConfig, logger *slog.Logger) (*SyncService, error) {
	if config == nil {
		return nil, fmt.Errorf("service config cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := validateAdmissionPool(pool, config.Readiness); err != nil {
		return nil, err
	}
	configCopy := *config
	configCopy.Readiness = cloneReadinessConfig(config.Readiness)
	configCopy.RegisteredTables = cloneRegisteredTables(config.RegisteredTables)
	configCopy.SchemaGenerations = cloneSchemaGenerations(config.SchemaGenerations)
	sort.Slice(configCopy.RegisteredTables, func(i, j int) bool {
		return compareRegisteredTables(configCopy.RegisteredTables[i], configCopy.RegisteredTables[j]) < 0
	})
	configCopy.ReservedServerSourceIDs = append([]string(nil), config.ReservedServerSourceIDs...)
	configCopy.DependencyOverrides = cloneDependencyOverrides(config.DependencyOverrides)
	config = &configCopy
	if config.RegisteredTableCatalogRevision < 1 {
		return nil, &RegisteredTableCatalogError{
			Code:           RegisteredTableCatalogInvalidTargetRevision,
			Field:          "RegisteredTableCatalogRevision",
			Value:          strconv.FormatInt(config.RegisteredTableCatalogRevision, 10),
			TargetRevision: config.RegisteredTableCatalogRevision,
		}
	}
	if err := validateRegisteredTables(config.RegisteredTables); err != nil {
		return nil, err
	}
	registry, err := CompileSchemaGenerationRegistry(config.RegisteredTables, config.SchemaGenerations)
	if err != nil {
		return nil, fmt.Errorf("compile schema generation registry: %w", err)
	}
	if config.MaxClientSourcesPerUser == 0 {
		config.MaxClientSourcesPerUser = defaultMaxClientSourcesPerUser
	} else if config.MaxClientSourcesPerUser < 1 || config.MaxClientSourcesPerUser > maxMaxClientSourcesPerUser {
		return nil, fmt.Errorf("MaxClientSourcesPerUser must be in 1..%d", maxMaxClientSourcesPerUser)
	}
	if err := normalizeHistoryProjectionConfig(config); err != nil {
		return nil, err
	}
	if err := validateReservedServerSourceIDs(config.ReservedServerSourceIDs); err != nil {
		return nil, err
	}
	normalizedWatchConfig, err := normalizeBundleChangeWatchConfig(config.BundleChangeWatch)
	if err != nil {
		return nil, err
	}
	config.BundleChangeWatch = normalizedWatchConfig
	if err := normalizeSnapshotConfig(config); err != nil {
		return nil, err
	}
	if err := validateAdvertisedWireLimits(config); err != nil {
		return nil, err
	}

	service := &SyncService{
		pool:                       pool,
		readinessReport:            ReadinessReport{ProcessStartID: uuid.NewString()},
		logger:                     logger,
		config:                     config,
		bundleChangeHub:            newBundleChangeHub(),
		registeredTables:           make(map[string]bool),
		registeredTableInfo:        make(map[string]registeredTableRuntimeInfo),
		registeredTableByID:        make(map[int32]registeredTableRuntimeInfo),
		retainedTableByID:          make(map[int32]registeredTableRuntimeInfo),
		columnTypesByTable:         make(map[string]map[string]string),
		schemaGenerationRegistry:   registry,
		generationViews:            make(map[SchemaGenerationID]*schemaGenerationView),
		lifecycle:                  serviceLifecycleRunning,
		bootstrapReadiness:         bootstrapReadinessNotReady,
		closedCh:                   make(chan struct{}),
		snapshotBuildPermits:       make(chan struct{}, config.MaxConcurrentSnapshotBuilds),
		snapshotChunkPermits:       make(chan struct{}, config.MaxConcurrentSnapshotChunkRequests),
		historyProjectionPermits:   make(chan struct{}, config.MaxConcurrentHistoryProjections),
		snapshotCleanupTrigger:     make(chan string, 1),
		scopeReceiptCleanupTrigger: make(chan string, 1),
	}

	return service, nil
}

// Bootstrap initializes sync metadata and the runtime topology snapshot. A fresh layout adopts
// populated registered tables atomically. An existing marked layout is attached after catalog and
// managed-definition validation without auditing business rows or managed operational state.
// Topology is prepared at bootstrap time and is restart-only for now; runtime schema changes are
// not re-discovered automatically by SyncService.
func (s *SyncService) Bootstrap(ctx context.Context) (err error) {
	if err := rejectRetryableCallbackContext(ctx, "Bootstrap"); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.pool == nil {
		return fmt.Errorf("bootstrap requires a database pool")
	}
	startedAt := time.Now()
	s.logger.Info("Sync bootstrap started", "registered_table_count", len(s.config.RegisteredTables))
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()

	started, err := s.beginBootstrap(ctx)
	if !started {
		return err
	}
	succeeded := false
	bootstrapMode := ""
	defer func() {
		if !succeeded {
			s.recordReadinessFailure(err)
		}
		s.finishBootstrap(succeeded)
		if succeeded {
			s.startBackgroundCleanupWorkers()
			s.logger.Info("Sync bootstrap completed", "duration", time.Since(startedAt), "registered_table_count", len(s.config.RegisteredTables), "bootstrap_mode", bootstrapMode)
			return
		}
		s.logger.Warn("Sync bootstrap rejected or rolled back", "duration", time.Since(startedAt), "error", bootstrapErrorClass(err))
	}()
	if err != nil {
		return err
	}

	if err := s.validateGenerationResponseSizes(); err != nil {
		return err
	}

	if err := validateReadinessConfig(s.config.Readiness, s.schemaGenerationRegistry); err != nil {
		return err
	}
	authorityDeadline := time.Now().Add(s.config.Readiness.LockTimeout)
	attempt := 0
	var committedCandidate *SyncService
	var committedMetadata registeredTableCatalogMetadata
	if err := runRetryableTx(ctx, 3, 50*time.Millisecond, func() error {
		attempt++
		currentAttempt := attempt
		return s.withReadinessTransaction(ctx, authorityDeadline, func(tx pgx.Tx) error {
			if s.catalogHooks != nil && s.catalogHooks.inspectTransaction != nil {
				if err := s.catalogHooks.inspectTransaction(ctx, tx); err != nil {
					return err
				}
			}
			lockStartedAt := s.stageStart()
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey); err != nil {
				err = fmt.Errorf("acquire sync bootstrap lock: %w", err)
				return err
			}
			s.logger.Debug("Sync bootstrap advisory lock acquired")

			managedStartedAt := s.stageStart()
			freshLayout, err := classifyRegisteredTableCatalogLayout(ctx, tx)
			if err != nil {
				s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, err)
				return err
			}
			if !freshLayout {
				if err := s.validatePushOutcomeStore(ctx, tx); err != nil {
					return err
				}
				if err := validateStaticManagedLayout(ctx, tx); err != nil {
					s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, err)
					return err
				}
			}

			if freshLayout {
				if s.config.RegisteredTableCatalogRevision != 1 {
					err := &RegisteredTableCatalogError{
						Code:            RegisteredTableCatalogRevisionConflict,
						Field:           "RegisteredTableCatalogRevision",
						CatalogRevision: 0,
						TargetRevision:  s.config.RegisteredTableCatalogRevision,
						Expected:        "1",
						Actual:          strconv.FormatInt(s.config.RegisteredTableCatalogRevision, 10),
					}
					s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, err)
					return err
				}
				candidate, err := s.prepareCatalogBootstrapCandidate(ctx, tx)
				if err != nil {
					return err
				}
				metadata, entries, err := seedRegisteredTableCatalog(s.config.RegisteredTables, candidate.registeredTableInfo)
				if err != nil {
					return err
				}
				if err := candidate.applyCatalogEntries(entries); err != nil {
					return err
				}
				if err := candidate.discoverSchemaRelationships(ctx, tx); err != nil {
					return fmt.Errorf("failed to discover schema relationships: %w", err)
				}
				if err := candidate.lockRegisteredTablesForAdoption(ctx, tx); err != nil {
					s.observeStageErr(ctx, "bootstrap", "advisory_table_lock_wait", lockStartedAt, len(s.config.RegisteredTables), currentAttempt, err)
					return err
				}
				s.observeStageErr(ctx, "bootstrap", "advisory_table_lock_wait", lockStartedAt, len(s.config.RegisteredTables), currentAttempt, nil)
				candidate, err = s.prepareCatalogBootstrapCandidate(ctx, tx)
				if err != nil {
					return err
				}
				metadata, entries, err = seedRegisteredTableCatalog(s.config.RegisteredTables, candidate.registeredTableInfo)
				if err != nil {
					return err
				}
				if err := candidate.applyCatalogEntries(entries); err != nil {
					return err
				}
				if err := candidate.discoverSchemaRelationships(ctx, tx); err != nil {
					return fmt.Errorf("failed to discover schema relationships: %w", err)
				}
				if err := candidate.buildAndValidateGenerationViews(ctx, tx); err != nil {
					return err
				}
				if err := candidate.createFreshSyncLayoutInTx(ctx, tx); err != nil {
					return fmt.Errorf("initialize first durable sync layout: %w", err)
				}
				if err := candidate.installServerSourceReservations(ctx, tx); err != nil {
					return err
				}
				if err := candidate.installRegisteredTableCaptureTriggersInTx(ctx, tx); err != nil {
					return fmt.Errorf("install registered table capture triggers: %w", err)
				}
				if err := persistRegisteredTableCatalogSeed(ctx, tx, metadata, entries); err != nil {
					return err
				}
				if err := candidate.validateManagedLayout(ctx, tx); err != nil {
					return err
				}
				if err := candidate.adoptFreshPopulatedRegisteredTables(ctx, tx, currentAttempt); err != nil {
					return fmt.Errorf("adopt populated registered tables: %w", err)
				}
				if err := candidate.validateManagedLayoutVolatileFacts(ctx, tx); err != nil {
					return err
				}
				if err := candidate.buildAndValidateGenerationViews(ctx, tx); err != nil {
					return err
				}
				committedCandidate = candidate
				committedMetadata = metadata
				bootstrapMode = "fresh_seed"
				s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, nil)
				return nil
			}

			if err := s.runRegisteredTableCatalogMutationHook("before_metadata_lock", 0); err != nil {
				return err
			}
			metadata, entries, err := loadRegisteredTableCatalogForTarget(ctx, tx, s.config.RegisteredTableCatalogRevision)
			if err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("after_metadata_lock", 0); err != nil {
				return err
			}
			if err := validateLoadedRegisteredTableCatalog(metadata, entries); err != nil {
				return err
			}
			identityKinds, err := loadConfiguredCatalogIdentityKinds(
				ctx,
				tx,
				s.config.RegisteredTables,
				entries,
			)
			if err != nil {
				return err
			}
			appendTables, err := reconcileRegisteredTableCatalog(
				s.config.RegisteredTables,
				s.config.RegisteredTableCatalogRevision,
				metadata,
				entries,
				identityKinds,
			)
			if err != nil {
				return err
			}
			activeEntries := activeRegisteredTableCatalogEntries(entries)
			activeTables := registeredTableDeclarationsForCatalogEntries(activeEntries)
			candidate, err := s.prepareCatalogBootstrapCandidateForTables(ctx, tx, activeTables)
			if err != nil {
				return err
			}
			if err := candidate.applyCatalogEntries(entries); err != nil {
				return err
			}
			if err := candidate.discoverSchemaRelationships(ctx, tx); err != nil {
				return fmt.Errorf("validate active schema relationships: %w", err)
			}
			if err := candidate.validateManagedLayout(ctx, tx); err != nil {
				return err
			}
			if err := candidate.validateServerSourceReservations(ctx, tx); err != nil {
				return err
			}
			planMode, err := registeredTableCatalogPlanModeForTarget(
				metadata,
				s.config.RegisteredTableCatalogRevision,
				len(appendTables),
			)
			if err != nil {
				return err
			}
			if planMode == registeredTableCatalogPlanAttach {
				if err := candidate.buildAndValidateGenerationViews(ctx, tx); err != nil {
					return err
				}
				committedCandidate = candidate
				committedMetadata = metadata
				bootstrapMode = "existing_attach"
				s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, nil)
				return nil
			}

			if err := validateRegisteredTableCatalogAppendDrain(ctx, tx); err != nil {
				return err
			}
			if err := lockRegisteredTables(ctx, tx, appendTables); err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("after_append_table_locks", 0); err != nil {
				return err
			}
			candidate, err = s.prepareCatalogBootstrapCandidate(ctx, tx)
			if err != nil {
				return err
			}
			metadata, entries, err = loadRegisteredTableCatalog(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateLoadedRegisteredTableCatalog(metadata, entries); err != nil {
				return err
			}
			appendTables, err = reconcileRegisteredTableCatalog(
				s.config.RegisteredTables,
				s.config.RegisteredTableCatalogRevision,
				metadata,
				entries,
				candidate.registeredTableInfo,
			)
			if err != nil {
				return err
			}
			planMode, err = registeredTableCatalogPlanModeForTarget(
				metadata,
				s.config.RegisteredTableCatalogRevision,
				len(appendTables),
			)
			if err != nil {
				return err
			}
			if planMode != registeredTableCatalogPlanAppend {
				return fmt.Errorf("catalog append revalidation produced non-append plan")
			}
			if err := candidate.discoverSchemaRelationships(ctx, tx); err != nil {
				return fmt.Errorf("validate append schema relationships: %w", err)
			}
			if err := validateNoReservedRegisteredTriggersForTables(ctx, tx, appendTables); err != nil {
				return err
			}
			if err := validateRegisteredTableCatalogAppendTablesEmpty(ctx, tx, appendTables); err != nil {
				return err
			}
			sort.Slice(appendTables, func(i, j int) bool { return compareRegisteredTables(appendTables[i], appendTables[j]) < 0 })
			plan, err := allocateRegisteredTableCatalogAppend(
				metadata,
				entries,
				appendTables,
				candidate.registeredTableInfo,
				s.config.RegisteredTableCatalogRevision,
			)
			if err != nil {
				return err
			}
			if err := candidate.applyCatalogEntries(plan.entries); err != nil {
				return err
			}
			if err := candidate.discoverSchemaRelationships(ctx, tx); err != nil {
				return fmt.Errorf("validate allocated append schema relationships: %w", err)
			}
			if err := candidate.buildAndValidateGenerationViews(ctx, tx); err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("after_plan_validation", 0); err != nil {
				return err
			}
			for index, entry := range plan.appended {
				if err := insertRegisteredTableCatalogEntries(ctx, tx, []registeredTableCatalogEntry{entry}); err != nil {
					return err
				}
				if err := s.runRegisteredTableCatalogMutationHook("after_catalog_insert", index); err != nil {
					return err
				}
			}
			if err := candidate.installRegisteredTableCaptureTriggersForTablesInTx(ctx, tx, appendTables); err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("before_metadata_update", 0); err != nil {
				return err
			}
			if err := publishRegisteredTableCatalogMetadata(ctx, tx, plan.metadata); err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("after_metadata_update", 0); err != nil {
				return err
			}
			if err := s.runRegisteredTableCatalogMutationHook("before_final_validation", 0); err != nil {
				return err
			}
			persistedMetadata, persistedEntries, err := loadRegisteredTableCatalog(ctx, tx)
			if err != nil {
				return err
			}
			persistedPlan, err := planExistingRegisteredTableCatalog(
				s.config.RegisteredTables,
				s.config.RegisteredTableCatalogRevision,
				persistedMetadata,
				persistedEntries,
				candidate.registeredTableInfo,
			)
			if err != nil {
				return fmt.Errorf("validate persisted registered table catalog: %w", err)
			}
			if persistedPlan.mode != registeredTableCatalogPlanAttach {
				return fmt.Errorf("validate persisted registered table catalog: expected exact attach plan")
			}
			if err := s.runRegisteredTableCatalogMutationHook("during_final_managed_validation", 0); err != nil {
				return err
			}
			if err := candidate.validateManagedLayout(ctx, tx); err != nil {
				return err
			}
			if err := candidate.buildAndValidateGenerationViews(ctx, tx); err != nil {
				return err
			}
			committedCandidate = candidate
			committedMetadata = persistedMetadata
			bootstrapMode = "catalog_append"
			s.observeStageErr(ctx, "bootstrap", "managed_layout_validation", managedStartedAt, len(s.config.RegisteredTables), currentAttempt, nil)
			if err := s.runRegisteredTableCatalogMutationHook("before_commit_known_rollback", 0); err != nil {
				return err
			}
			return nil
		})
	}); err != nil {
		return err
	}
	if err := s.runRegisteredTableCatalogMutationHook("after_commit_ambiguous", 0); err != nil {
		return err
	}
	if committedCandidate == nil {
		return fmt.Errorf("bootstrap committed without a catalog topology")
	}
	s.publishCatalogBootstrapCandidate(committedCandidate, committedMetadata)
	s.publishReadinessReport()
	succeeded = true
	return nil
}

func (s *SyncService) startBackgroundCleanupWorkers() {
	if s.config.Readiness != nil && s.config.Readiness.DisableBackgroundWorkers {
		return
	}
	s.startScopeReceiptCleanupWorker()
	s.requestScopeReceiptCleanup("post_bootstrap")
	s.startSnapshotCleanupWorker()
	s.requestSnapshotCleanup("post_bootstrap")
}

func bootstrapErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}
	var adoptionErr *PopulatedTableAdoptionError
	if errors.As(err, &adoptionErr) {
		return "populated_table_adoption_" + adoptionErr.Reason
	}
	return "bootstrap_error"
}

func (s *SyncService) validateRegisteredIdentityNullability(ctx context.Context, q syncCatalogQuerier) error {
	if s == nil || s.config == nil || len(s.config.RegisteredTables) == 0 {
		return nil
	}

	schemas := make([]string, 0, len(s.config.RegisteredTables))
	tables := make([]string, 0, len(s.config.RegisteredTables))
	syncKeys := make([]string, 0, len(s.config.RegisteredTables))
	for _, table := range s.config.RegisteredTables {
		keyColumns := table.normalizedSyncKeyColumns()
		if len(keyColumns) != 1 {
			return unsupportedSchemaf("registered table %s must resolve to exactly one sync key column for nullable identity validation", table.normalizedKey())
		}
		schemas = append(schemas, table.normalizedSchema())
		tables = append(tables, table.normalizedTable())
		syncKeys = append(syncKeys, keyColumns[0])
	}

	rows, err := q.Query(ctx, `
WITH RECURSIVE registered_roots AS (
  SELECT schema_name, table_name, sync_key_column
  FROM unnest(@schemas::text[], @tables::text[], @sync_keys::text[])
    AS configured(schema_name, table_name, sync_key_column)
), roots AS (
  SELECT configured.*, relation.oid AS root_oid
  FROM registered_roots AS configured
  JOIN pg_namespace AS namespace ON namespace.nspname = configured.schema_name
  JOIN pg_class AS relation
    ON relation.relnamespace = namespace.oid
   AND relation.relname = configured.table_name
), relation_tree AS (
  SELECT schema_name, table_name, sync_key_column, root_oid, root_oid AS target_oid
  FROM roots

  UNION ALL

  SELECT tree.schema_name, tree.table_name, tree.sync_key_column, tree.root_oid, inheritance.inhrelid
  FROM relation_tree AS tree
  JOIN pg_inherits AS inheritance ON inheritance.inhparent = tree.target_oid
)
SELECT
  tree.schema_name AS root_schema,
  tree.table_name AS root_table,
  target_namespace.nspname AS target_schema,
  target.relname AS target_table,
  required.role,
  required.column_name,
  type.typname AS udt_name,
  attribute.attnotnull
FROM relation_tree AS tree
JOIN pg_class AS target ON target.oid = tree.target_oid
JOIN pg_namespace AS target_namespace ON target_namespace.oid = target.relnamespace
CROSS JOIN LATERAL (
  VALUES
    ('scope'::text, @scope_column::text),
    ('sync key'::text, tree.sync_key_column)
) AS required(role, column_name)
LEFT JOIN pg_attribute AS attribute
  ON attribute.attrelid = tree.target_oid
 AND lower(attribute.attname) = lower(required.column_name)
 AND attribute.attnum > 0
 AND NOT attribute.attisdropped
LEFT JOIN pg_type AS type ON type.oid = attribute.atttypid
ORDER BY
  tree.schema_name,
  tree.table_name,
  target_namespace.nspname,
  target.relname,
  required.role,
  required.column_name
`, pgx.NamedArgs{
		"scope_column": syncScopeColumnName,
		"schemas":      schemas,
		"tables":       tables,
		"sync_keys":    syncKeys,
	})
	if err != nil {
		return fmt.Errorf("load registered identity nullability: %w", err)
	}
	defer rows.Close()

	offenders := make([]string, 0)
	for rows.Next() {
		var rootSchema, rootTable, targetSchema, targetTable, role, columnName string
		var udtName *string
		var notNull *bool
		if err := rows.Scan(&rootSchema, &rootTable, &targetSchema, &targetTable, &role, &columnName, &udtName, &notNull); err != nil {
			return fmt.Errorf("scan registered identity nullability: %w", err)
		}
		root := Key(rootSchema, rootTable)
		target := Key(targetSchema, targetTable)
		if notNull == nil {
			offenders = append(offenders, fmt.Sprintf("logical root %s, physical relation %s, %s column %s is missing", root, target, role, columnName))
			continue
		}
		if !*notNull {
			offenders = append(offenders, fmt.Sprintf("logical root %s, physical relation %s, %s column %s is nullable", root, target, role, columnName))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate registered identity nullability: %w", err)
	}
	if len(offenders) > 0 {
		return unsupportedSchemaf(
			"nullable identity declarations are unsupported: %s; resolve existing NULL values according to application policy, add explicit NOT NULL constraints to every reported root/descendant column, and retry bootstrap",
			strings.Join(offenders, "; "),
		)
	}
	return nil
}

func (s *SyncService) beginBootstrap(ctx context.Context) (bool, error) {
	s.mu.Lock()
	if s.readinessWithdrawn {
		s.mu.Unlock()
		return false, errServiceNotReady
	}
	if s.lifecycle != serviceLifecycleRunning {
		s.mu.Unlock()
		return false, errServiceShuttingDown
	}
	s.bootstrapReadiness = bootstrapReadinessBootstrapping
	s.inFlightOps++
	if s.inFlightOps == 1 {
		s.mu.Unlock()
		return true, nil
	}
	if s.bootstrapDrainedCh == nil {
		s.bootstrapDrainedCh = make(chan struct{})
	}
	drainedCh := s.bootstrapDrainedCh
	s.mu.Unlock()

	select {
	case <-drainedCh:
		s.mu.RLock()
		running := s.lifecycle == serviceLifecycleRunning
		s.mu.RUnlock()
		if !running {
			return true, errServiceShuttingDown
		}
		return true, nil
	case <-ctx.Done():
		return true, fmt.Errorf("wait for in-flight operations before bootstrap: %w", ctx.Err())
	}
}

func (s *SyncService) finishBootstrap(succeeded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bootstrapDrainedCh != nil {
		close(s.bootstrapDrainedCh)
		s.bootstrapDrainedCh = nil
	}
	if succeeded && s.lifecycle == serviceLifecycleRunning {
		s.bootstrapReadiness = bootstrapReadinessReady
	} else {
		s.bootstrapReadiness = bootstrapReadinessNotReady
	}
	s.finishOperationLocked()
}

func (s *SyncService) normalizeAndValidateRegisteredSyncKeys(ctx context.Context, q syncCatalogQuerier) error {
	if s == nil || s.pool == nil || s.config == nil || len(s.config.RegisteredTables) == 0 {
		return nil
	}

	schemas := make([]string, 0, len(s.config.RegisteredTables))
	tables := make([]string, 0, len(s.config.RegisteredTables))
	for _, tbl := range s.config.RegisteredTables {
		schemas = append(schemas, tbl.normalizedSchema())
		tables = append(tables, tbl.normalizedTable())
	}

	colRows, err := q.Query(ctx, `
WITH configured AS (
  SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name)
)
SELECT
  table_namespace.nspname AS table_schema,
  relation.relname AS table_name,
  lower(attribute.attname) AS column_name,
  type_namespace.nspname AS type_schema,
  lower(type.typname) AS udt_name,
  type.typtype::text AS type_kind,
  type.typcategory::text AS type_category,
  COALESCE(base_namespace.nspname, '') AS base_type_schema,
  lower(COALESCE(base_type.typname, '')) AS base_type_name
FROM configured
JOIN pg_namespace AS table_namespace
  ON table_namespace.nspname = configured.schema_name
JOIN pg_class AS relation
  ON relation.relnamespace = table_namespace.oid
 AND relation.relname = configured.table_name
JOIN pg_attribute AS attribute
  ON attribute.attrelid = relation.oid
 AND attribute.attnum > 0
 AND NOT attribute.attisdropped
JOIN pg_type AS type ON type.oid = attribute.atttypid
JOIN pg_namespace AS type_namespace ON type_namespace.oid = type.typnamespace
LEFT JOIN pg_type AS base_type ON base_type.oid = NULLIF(type.typbasetype, 0)
LEFT JOIN pg_namespace AS base_namespace ON base_namespace.oid = base_type.typnamespace
ORDER BY table_namespace.nspname, relation.relname, attribute.attnum
`, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
	})
	if err != nil {
		return fmt.Errorf("load registered table column types: %w", err)
	}
	defer colRows.Close()

	columnTypes := make(map[string]map[string]string, len(s.config.RegisteredTables))
	for colRows.Next() {
		var schemaName, tableName, columnName, typeSchema, udtName, typeKind, typeCategory, baseTypeSchema, baseTypeName string
		if err := colRows.Scan(
			&schemaName,
			&tableName,
			&columnName,
			&typeSchema,
			&udtName,
			&typeKind,
			&typeCategory,
			&baseTypeSchema,
			&baseTypeName,
		); err != nil {
			return fmt.Errorf("scan registered table column types: %w", err)
		}
		tableKey := Key(schemaName, tableName)
		if err := validateRegisteredPayloadColumnType(
			tableKey,
			columnName,
			typeSchema,
			udtName,
			typeKind,
			typeCategory,
			baseTypeSchema,
			baseTypeName,
		); err != nil {
			return err
		}
		cols := columnTypes[tableKey]
		if cols == nil {
			cols = make(map[string]string)
			columnTypes[tableKey] = cols
		}
		cols[columnName] = udtName
	}
	if colRows.Err() != nil {
		return fmt.Errorf("iterate registered table column types: %w", colRows.Err())
	}

	type uniqueIndexInfo struct {
		name           string
		columns        []string
		isPartial      bool
		hasExpressions bool
	}

	uniqueRows, err := q.Query(ctx, `
WITH t AS (
  SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name)
)
SELECT
  n.nspname AS table_schema,
  c.relname AS table_name,
  i.relname AS index_name,
  idx.indpred IS NOT NULL AS is_partial,
  idx.indexprs IS NOT NULL AS has_expressions,
  ord.ordinality AS column_ordinal,
  lower(COALESCE(a.attname, '')) AS column_name
FROM pg_class c
JOIN pg_namespace n
  ON n.oid = c.relnamespace
JOIN t
  ON n.nspname = t.schema_name
 AND c.relname = t.table_name
JOIN pg_index idx
  ON idx.indrelid = c.oid
JOIN pg_class i
  ON i.oid = idx.indexrelid
LEFT JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
  ON TRUE
LEFT JOIN pg_attribute a
  ON a.attrelid = c.oid
 AND a.attnum = ord.attnum
WHERE idx.indisunique
ORDER BY n.nspname, c.relname, i.relname, ord.ordinality
`, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
	})
	if err != nil {
		return fmt.Errorf("load registered table unique indexes: %w", err)
	}
	defer uniqueRows.Close()

	type uniqueIndexKey struct {
		tableKey  string
		indexName string
	}
	uniqueIndexes := make(map[string][]uniqueIndexInfo, len(s.config.RegisteredTables))
	indexPosByKey := make(map[uniqueIndexKey]int)
	for uniqueRows.Next() {
		var schemaName, tableName, indexName, columnName string
		var (
			isPartial      bool
			hasExpressions bool
			columnOrdinal  *int64
		)
		if err := uniqueRows.Scan(&schemaName, &tableName, &indexName, &isPartial, &hasExpressions, &columnOrdinal, &columnName); err != nil {
			return fmt.Errorf("scan registered table unique indexes: %w", err)
		}
		tableKey := Key(schemaName, tableName)
		lookupKey := uniqueIndexKey{tableKey: tableKey, indexName: indexName}
		indexPos, exists := indexPosByKey[lookupKey]
		if !exists {
			indexPos = len(uniqueIndexes[tableKey])
			indexPosByKey[lookupKey] = indexPos
			uniqueIndexes[tableKey] = append(uniqueIndexes[tableKey], uniqueIndexInfo{
				name:           indexName,
				isPartial:      isPartial,
				hasExpressions: hasExpressions,
			})
		}
		if columnOrdinal != nil {
			uniqueIndexes[tableKey][indexPos].columns = append(uniqueIndexes[tableKey][indexPos].columns, columnName)
		}
	}
	if uniqueRows.Err() != nil {
		return fmt.Errorf("iterate registered table unique indexes: %w", uniqueRows.Err())
	}

	registeredInfo := make(map[string]registeredTableRuntimeInfo, len(s.config.RegisteredTables))
	normalizedTableKeys := make([]string, 0, len(s.config.RegisteredTables))
	for i := range s.config.RegisteredTables {
		tbl := s.config.RegisteredTables[i]
		tableKey := tbl.normalizedKey()
		keyColumns := tbl.normalizedSyncKeyColumns()
		if len(keyColumns) != 1 {
			return unsupportedSchemaf("registered table %s must resolve to exactly one sync key column for the current server runtime", tableKey)
		}
		if keyColumns[0] == syncScopeColumnName {
			return unsupportedSchemaf("registered table %s cannot use hidden scope column %s as its visible sync key", tableKey, syncScopeColumnName)
		}

		cols := columnTypes[tableKey]
		if cols == nil {
			return fmt.Errorf("registered table %s could not load column metadata for sync key validation", tableKey)
		}
		if cols[syncScopeColumnName] != syncKeyTypeText {
			return unsupportedSchemaf("registered table %s must define %s TEXT", tableKey, syncScopeColumnName)
		}
		keyType := cols[keyColumns[0]]
		if keyType != syncKeyTypeUUID && keyType != syncKeyTypeText {
			return unsupportedSchemaf("registered table %s uses unsupported sync key column type %s for %s; the current server runtime allows only uuid and text", tableKey, keyType, keyColumns[0])
		}

		indexes := uniqueIndexes[tableKey]
		if len(indexes) == 0 {
			return unsupportedSchemaf("registered table %s must provide unique indexes or constraints for scope-aware identity", tableKey)
		}
		hasOwnerScopedIdentity := false
		for _, idx := range indexes {
			if idx.isPartial || idx.hasExpressions {
				return unsupportedSchemaf("registered table %s uses unsupported partial or expression unique index %s", tableKey, idx.name)
			}
			if len(idx.columns) == 0 {
				return unsupportedSchemaf("registered table %s uses unsupported unique index %s without column entries", tableKey, idx.name)
			}
			if idx.columns[0] != syncScopeColumnName {
				return unsupportedSchemaf("registered table %s has unique index %s that does not begin with %s", tableKey, idx.name, syncScopeColumnName)
			}
			if len(idx.columns) == 2 && idx.columns[0] == syncScopeColumnName && idx.columns[1] == keyColumns[0] {
				hasOwnerScopedIdentity = true
			}
		}
		if !hasOwnerScopedIdentity {
			return unsupportedSchemaf("registered table %s must provide unique identity (%s, %s)", tableKey, syncScopeColumnName, keyColumns[0])
		}

		syncKeyKind, err := syncKeyKindCode(keyType)
		if err != nil {
			return err
		}
		registeredInfo[tableKey] = registeredTableRuntimeInfo{
			schemaName:    tbl.normalizedSchema(),
			tableName:     tbl.normalizedTable(),
			syncKeyColumn: keyColumns[0],
			syncKeyType:   keyType,
			syncKeyKind:   syncKeyKind,
		}
		normalizedTableKeys = append(normalizedTableKeys, tableKey)
	}

	sort.Strings(normalizedTableKeys)
	registeredByID := make(map[int32]registeredTableRuntimeInfo, len(normalizedTableKeys))
	for i, tableKey := range normalizedTableKeys {
		info := registeredInfo[tableKey]
		info.tableID = int32(i + 1)
		registeredInfo[tableKey] = info
		registeredByID[info.tableID] = info
	}

	clonedColumnTypes := make(map[string]map[string]string, len(columnTypes))
	for tableKey, cols := range columnTypes {
		cloned := make(map[string]string, len(cols))
		for columnName, udtName := range cols {
			cloned[columnName] = udtName
		}
		clonedColumnTypes[tableKey] = cloned
	}

	s.mu.Lock()
	s.registeredTableInfo = registeredInfo
	s.registeredTableByID = registeredByID
	s.columnTypesByTable = clonedColumnTypes
	s.mu.Unlock()

	return nil
}

func validateRegisteredPayloadColumnType(
	tableKey string,
	columnName string,
	typeSchema string,
	typeName string,
	typeKind string,
	typeCategory string,
	baseTypeSchema string,
	baseTypeName string,
) error {
	if typeKind == "d" {
		if typeCategory == "N" || isBuiltInNumericWireType(baseTypeSchema, baseTypeName) {
			return unsupportedSchemaf(
				"registered payload column %s.%s uses unsupported numeric domain %s.%s over %s.%s; use a supported built-in PostgreSQL numeric type directly",
				tableKey,
				columnName,
				typeSchema,
				typeName,
				baseTypeSchema,
				baseTypeName,
			)
		}
		return nil
	}

	if typeKind == "c" || typeKind == "r" || typeKind == "m" || typeCategory == "A" {
		return unsupportedSchemaf(
			"registered payload column %s.%s uses unsupported PostgreSQL array, range, or composite type %s.%s",
			tableKey,
			columnName,
			typeSchema,
			typeName,
		)
	}

	if typeCategory != "N" {
		return nil
	}
	if !isBuiltInNumericWireType(typeSchema, typeName) {
		return unsupportedSchemaf(
			"registered payload column %s.%s uses unsupported numeric type %s.%s; supported built-in families are int2, int4, int8, numeric, float4, and float8",
			tableKey,
			columnName,
			typeSchema,
			typeName,
		)
	}
	return nil
}

func isBuiltInNumericWireType(typeSchema, typeName string) bool {
	if typeSchema != "pg_catalog" {
		return false
	}
	switch typeName {
	case "int2", "int4", "int8", "numeric", "float4", "float8":
		return true
	default:
		return false
	}
}

// Close gracefully shuts down the sync service.
// It rejects new runtime operations, waits for in-flight work to drain, and is safe to call multiple times.
// Note: This does NOT close the database pool - the caller is responsible for pool lifecycle.
func (s *SyncService) Close(ctx context.Context) error {
	if err := rejectRetryableCallbackContext(ctx, "Close"); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	s.signalDrainStartedLocked()
	switch s.lifecycle {
	case serviceLifecycleClosed:
		s.closeServiceSignalLocked()
		s.mu.Unlock()
		return s.waitSyncMaintenanceWorkers(ctx)
	case serviceLifecycleRunning:
		s.logger.Debug("Shutting down sync service")
		s.lifecycle = serviceLifecycleShuttingDown
		s.stopSnapshotCleanupWorker()
		s.stopScopeReceiptCleanupWorker()
		if s.inFlightOps == 0 {
			s.lifecycle = serviceLifecycleClosed
			s.closeServiceSignalLocked()
			s.mu.Unlock()
			s.logger.Debug("Sync service shutdown complete")
			return s.waitSyncMaintenanceWorkers(ctx)
		}
		if s.drainedCh == nil {
			s.drainedCh = make(chan struct{})
		}
	case serviceLifecycleShuttingDown:
		if s.inFlightOps == 0 {
			s.lifecycle = serviceLifecycleClosed
			s.closeServiceSignalLocked()
			s.mu.Unlock()
			return s.waitSyncMaintenanceWorkers(ctx)
		}
		if s.drainedCh == nil {
			s.drainedCh = make(chan struct{})
		}
	}
	drainedCh := s.drainedCh
	s.mu.Unlock()

	select {
	case <-drainedCh:
		s.logger.Debug("Sync service shutdown complete")
		return s.waitSyncMaintenanceWorkers(ctx)
	case <-ctx.Done():
		return fmt.Errorf("wait for in-flight operations to drain: %w", ctx.Err())
	}
}

// Pool returns the underlying database connection pool
// This allows advanced users to execute custom queries
func (s *SyncService) Pool() *pgxpool.Pool {
	return s.pool
}

// IsTableRegistered checks if a schema.table combination is registered for sync operations
func (s *SyncService) IsTableRegistered(schemaName, tableName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := schemaName + "." + tableName
	return s.registeredTables[key]
}

func (s *SyncService) beginLocalOperation() (func(), error) {
	return s.beginTrackedOperation(true)
}

func (s *SyncService) beginLifecycleOperation() (func(), error) {
	return s.beginTrackedOperation(false)
}

func (s *SyncService) beginTrackedOperation(requireReady bool) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.lifecycle {
	case serviceLifecycleShuttingDown:
		return nil, errServiceShuttingDown
	case serviceLifecycleClosed:
		return nil, errors.New("sync service has been closed")
	}
	if requireReady && (s.readinessWithdrawn || (s.pool != nil && s.bootstrapReadiness != bootstrapReadinessReady)) {
		return nil, errServiceNotReady
	}

	s.inFlightOps++

	var once sync.Once
	return func() {
		once.Do(func() {
			s.finishOperation()
		})
	}, nil
}

func (s *SyncService) finishOperation() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishOperationLocked()
}

func (s *SyncService) finishOperationLocked() {
	if s.inFlightOps > 0 {
		s.inFlightOps--
	}
	if s.inFlightOps == 1 && s.bootstrapReadiness == bootstrapReadinessBootstrapping && s.bootstrapDrainedCh != nil {
		close(s.bootstrapDrainedCh)
		s.bootstrapDrainedCh = nil
	}
	if s.inFlightOps == 0 && s.lifecycle == serviceLifecycleShuttingDown {
		s.lifecycle = serviceLifecycleClosed
		s.closeServiceSignalLocked()
		if s.drainedCh != nil {
			close(s.drainedCh)
			s.drainedCh = nil
		}
	}
}

func (s *SyncService) closeServiceSignalLocked() {
	if s.closedCh == nil {
		s.closedCh = make(chan struct{})
	}
	ch := s.closedCh
	s.closeOnce.Do(func() {
		close(ch)
	})
}

func (s *SyncService) serviceClosedChannel() <-chan struct{} {
	if s == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closedCh == nil {
		s.closedCh = make(chan struct{})
		if s.lifecycle == serviceLifecycleClosed {
			s.closeServiceSignalLocked()
		}
	}
	return s.closedCh
}

func (s *SyncService) lifecycleSnapshot() (serviceLifecycleState, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	accepting := s.lifecycle == serviceLifecycleRunning &&
		!s.readinessWithdrawn &&
		(s.pool == nil || s.bootstrapReadiness == bootstrapReadinessReady)
	return s.lifecycle, s.inFlightOps, accepting
}

func (s *SyncService) requestReadinessError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lifecycle != serviceLifecycleRunning {
		return errServiceShuttingDown
	}
	if s.bootstrapReadiness != bootstrapReadinessReady {
		return errServiceNotReady
	}
	return nil
}

type operationalInvariantSnapshot struct {
	UserStateRetentionFloorAheadCount int64
	LatestBundleSeqMax                int64
	RetainedBundleFloorMin            int64
	RetainedBundleFloorMax            int64
	RetainedBundleWindowMin           int64
	RetainedBundleWindowMax           int64
	HistoryPrunedErrorCount           int64
	AcceptedPushReplayCount           int64
	RejectedRegisteredWriteCount      int64
	CommittedBundleCount              int64
	CommittedBundleBytes              int64
}

func (s *SyncService) operationalInvariantStats(ctx context.Context) (*operationalInvariantSnapshot, error) {
	if s.pool == nil {
		return &operationalInvariantSnapshot{}, nil
	}

	var stats operationalInvariantSnapshot
	err := s.pool.QueryRow(ctx, `
		SELECT
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COUNT(*) FROM sync.user_state WHERE retained_bundle_floor > next_bundle_seq - 1)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(GREATEST(next_bundle_seq - 1, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MIN(retained_bundle_floor), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(retained_bundle_floor), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MIN(GREATEST((next_bundle_seq - 1) - retained_bundle_floor, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(GREATEST((next_bundle_seq - 1) - retained_bundle_floor, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.history_pruned_error_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.history_pruned_error_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.accepted_push_replay_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.accepted_push_replay_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.rejected_registered_write_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.rejected_registered_write_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.bundle_log') IS NULL THEN 0
				ELSE (SELECT COUNT(*) FROM sync.bundle_log)
			END,
			CASE
				WHEN to_regclass('sync.bundle_log') IS NULL THEN 0
				ELSE (SELECT COALESCE(SUM(byte_count), 0) FROM sync.bundle_log)
			END
	`).Scan(
		&stats.UserStateRetentionFloorAheadCount,
		&stats.LatestBundleSeqMax,
		&stats.RetainedBundleFloorMin,
		&stats.RetainedBundleFloorMax,
		&stats.RetainedBundleWindowMin,
		&stats.RetainedBundleWindowMax,
		&stats.HistoryPrunedErrorCount,
		&stats.AcceptedPushReplayCount,
		&stats.RejectedRegisteredWriteCount,
		&stats.CommittedBundleCount,
		&stats.CommittedBundleBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("query operational invariant stats: %w", err)
	}
	return &stats, nil
}

// GetStatus returns the current service lifecycle and bundle-era operability snapshot.
func (s *SyncService) GetStatus(ctx context.Context) (*StatusResponse, error) {
	if err := rejectRetryableCallbackContext(ctx, "GetStatus"); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	lifecycle, inFlightOps, accepting := s.lifecycleSnapshot()
	if !accepting {
		return &StatusResponse{
			Status:              "unhealthy",
			Version:             SyncProtocolVersion,
			AppName:             s.config.AppName,
			Lifecycle:           string(lifecycle),
			AcceptingOperations: false,
			InFlightOperations:  inFlightOps,
			RegisteredTables:    s.statusRegisteredTables(),
			Features:            s.serviceFeatures(),
		}, nil
	}
	invariantStats, err := s.operationalInvariantStats(ctx)
	if err != nil {
		return nil, err
	}

	status := "healthy"
	switch lifecycle {
	case serviceLifecycleShuttingDown, serviceLifecycleClosed:
		status = "unhealthy"
	default:
		if invariantStats.UserStateRetentionFloorAheadCount > 0 {
			status = "unhealthy"
		}
	}

	return &StatusResponse{
		Status:                            status,
		Version:                           SyncProtocolVersion,
		AppName:                           s.config.AppName,
		Lifecycle:                         string(lifecycle),
		AcceptingOperations:               accepting,
		InFlightOperations:                inFlightOps,
		RegisteredTables:                  s.statusRegisteredTables(),
		Features:                          s.serviceFeatures(),
		UserStateRetentionFloorAheadCount: invariantStats.UserStateRetentionFloorAheadCount,
		LatestBundleSeqMax:                invariantStats.LatestBundleSeqMax,
		RetainedBundleFloorMin:            invariantStats.RetainedBundleFloorMin,
		RetainedBundleFloorMax:            invariantStats.RetainedBundleFloorMax,
		RetainedBundleWindowMin:           invariantStats.RetainedBundleWindowMin,
		RetainedBundleWindowMax:           invariantStats.RetainedBundleWindowMax,
		HistoryPrunedErrorCount:           invariantStats.HistoryPrunedErrorCount,
		AcceptedPushReplayCount:           invariantStats.AcceptedPushReplayCount,
		RejectedRegisteredWriteCount:      invariantStats.RejectedRegisteredWriteCount,
		CommittedBundleCount:              invariantStats.CommittedBundleCount,
		CommittedBundleBytes:              invariantStats.CommittedBundleBytes,
	}, nil
}

func (s *SyncService) serviceFeatures() map[string]bool {
	return map[string]bool{
		"bundle_pull":                         true,
		"snapshot_chunking":                   true,
		"status_endpoint":                     true,
		"graceful_shutdown":                   true,
		"server_checkpoint_tracking":          false,
		"history_pruned_errors":               true,
		"bundle_push":                         true,
		"structured_sync_keys":                true,
		"registered_write_rejection_enforced": true,
		"accepted_push_replay_visibility":     true,
		"committed_bundle_visibility":         true,
		"retained_floor_visibility":           true,
		"retained_window_visibility":          true,
		"history_pruned_visibility":           true,
		"connect_lifecycle":                   true,
		"scope_initialization_leases":         true,
		"bundle_change_watch":                 s.bundleChangeWatchEnabled(),
	}
}

func (s *SyncService) statusRegisteredTables() []string {
	tables := make([]string, 0, len(s.config.RegisteredTables))
	for _, tbl := range s.config.RegisteredTables {
		tables = append(tables, tbl.normalizedKey())
	}
	slices.Sort(tables)
	return tables
}

func (s *SyncService) uploadLockTimeoutMillis() (int64, bool) {
	if s == nil || s.config == nil || s.config.UploadLockTimeout <= 0 {
		return 0, false
	}
	ms := s.config.UploadLockTimeout.Milliseconds()
	if ms == 0 {
		ms = 1
	}
	return ms, true
}

func (s *SyncService) configureUploadTx(ctx context.Context, tx pgx.Tx) error {
	lockTimeoutMs, ok := s.uploadLockTimeoutMillis()
	if !ok {
		return nil
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeoutMs)); err != nil {
		return fmt.Errorf("failed to set upload lock timeout: %w", err)
	}
	return nil
}

func (s *SyncService) signalDrainStartedLocked() {
	if s.drainStartedCh == nil {
		s.drainStartedCh = make(chan struct{})
	}
	select {
	case <-s.drainStartedCh:
	default:
		close(s.drainStartedCh)
	}
}
func (s *SyncService) drainStartedChannel() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drainStartedCh == nil {
		s.drainStartedCh = make(chan struct{})
	}
	if s.lifecycle != serviceLifecycleRunning {
		s.signalDrainStartedLocked()
	}
	return s.drainStartedCh
}
