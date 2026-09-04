// Package oversqlite provides a SQLite-based client service for go-oversync
// single-user multi-device synchronization.
//
// This package implements the client-side service according to the technical
// specification in docs/client_techreq/01_client_sqlite.md
// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Client manages SQLite database and two-way sync operations
type Client struct {
	db *sql.DB
	// ownsDB is false only for package-internal lower-level test fixtures.
	// Every client published by Opening owns its pool.
	ownsDB       bool
	BaseURL      string
	Token        func(context.Context) (string, error) // returns JWT
	UserID       string
	Resolver     Resolver
	HTTP         *http.Client
	config       *Config
	a3Stats      a3AccountingStats
	syncWorkUsed int64 // owned by writeMu; one allowance across all bundles in an invocation
	logger       *slog.Logger
	writeMu      sync.Mutex // Serialize write operations to prevent SQLite locking issues
	pkByTable    map[string]string
	keyByTable   map[string][]string
	tableOrder   map[string]int
	tableInfo    *TableInfoProvider
	closed       uint32
	loopState    uint32
	loopMu       sync.Mutex
	loopCancel   context.CancelFunc
	loopDone     chan struct{}

	// Pause switches (atomic): allow callers to suspend sync activity deterministically
	uploadPaused             int32
	downloadPaused           int32
	snapshotStats            snapshotTransferStats
	snapshotObserver         func(snapshotRestoreObservation)
	pushStats                pushTransferStats
	beforePushFreezeHook     func(context.Context) error
	beforePushCommitHook     func(context.Context) error
	beforePushReplayHook     func(context.Context) error
	beforeSnapshotApplyHook  func(context.Context) error
	afterSnapshotRowsHook    func()
	beforeStartAdmissionHook func()
	beforeFinalWorkReadHook  func()
	dbOwnershipKey           string
	pendingInitializationID  string
	sessionConnected         bool
	sourceID                 string
	schemaGenerationProof    oversync.SchemaGenerationProof
	localGeneration          *compiledLocalContract
	discovery                *oversync.GenerationDiscoveryResponse
	sourceIDGenerator        func() string
}

const (
	clientLoopStateLive uint32 = iota
	clientLoopStateUninstalling
	clientLoopStateTerminal
)

// DatabaseAlreadyOwnedError indicates that another oversqlite client already owns the same SQLite DB.
type DatabaseAlreadyOwnedError struct {
	Database string
}

// Error implements error.
func (e *DatabaseAlreadyOwnedError) Error() string {
	if e == nil || strings.TrimSpace(e.Database) == "" {
		return "another oversqlite client is already active for this SQLite database"
	}
	return fmt.Sprintf("another oversqlite client is already active for SQLite database %s", e.Database)
}

// ClientClosedError indicates that Close() has already been called on the client.
type ClientClosedError struct{}

// Error implements error.
func (e *ClientClosedError) Error() string {
	return "oversqlite client has been closed"
}

type clientDBIdentity struct {
	Key   string
	Label string
	File  *os.File
}

type clientDBOwner struct {
	label string
	db    *sql.DB
	lock  *os.File
	kind  string
}

var activeClientDBOwnership = struct {
	mu     sync.Mutex
	owners map[string]clientDBOwner
}{
	owners: make(map[string]clientDBOwner),
}

const clientDBOwnershipLockRoot = "/var/tmp"

type snapshotTransferStats struct {
	sessionsCreated          int64
	chunksFetched            int64
	maxChunkRows             int64
	maxChunkWireBytes        int64
	maxChunkDecodedBodyBytes int64
	maxLiveStagedApplyRows   int64
	maxLiveStagedApplyText   int64
	maxAppliedInMemoryRows   int64
	capacityResponses        int64
	capacityRetries          int64
	capacityWaitNanos        int64
}

// SnapshotTransferStats exposes snapshot-session transfer counters for diagnostics.
type SnapshotTransferStats struct {
	SessionsCreated             int64
	ChunksFetched               int64
	MaxChunkRows                int64
	MaxChunkWireBytes           int64
	MaxChunkDecodedBodyBytes    int64
	MaxLiveStagedApplyRows      int64
	MaxLiveStagedApplyTextBytes int64
	MaxAppliedInMemoryRows      int64
	CapacityResponses           int64
	CapacityRetries             int64
	CapacityWait                time.Duration
}

type snapshotRestoreObservation struct {
	StagedRows        int64
	DeclaredWireBytes int64
	AppliedRows       int64
	Duration          time.Duration
}

type pushTransferStats struct {
	capacityResponses int64
	capacityRetries   int64
	capacityWaitNanos int64
}

// PushTransferStats exposes whole-request push retry counters for diagnostics.
type PushTransferStats struct {
	CapacityResponses int64
	CapacityRetries   int64
	CapacityWait      time.Duration
}

var syncMetadataTableNames = []string{
	"_sync_admitted_contracts",
	"_sync_contract_identity",
	"_sync_source_state",
	"_sync_attachment_state",
	"_sync_operation_state",
	"_sync_action_required",
	"_sync_apply_state",
	"_sync_apply_plan",
	"_sync_apply_rows",
	"_sync_apply_edges",
	"_sync_row_state",
	"_sync_relationships",
	"_sync_dirty_rows",
	"_sync_snapshot_stage",
	"_sync_outbox_bundle",
	"_sync_outbox_rows",
	"_sync_push_stage",
	"_sync_push_plan", "_sync_push_nodes", "_sync_push_edges",
	"_sync_managed_tables",
	"_sync_upgrade_journal",
	"_sync_upgrade_identity",
	"_sync_conflict_state",
	"_sync_push_rejection_state",
}

func (c *Client) tryBeginSyncOperation() error {
	if c == nil {
		return nil
	}
	if atomic.LoadUint32(&c.closed) == 1 {
		return &ClientClosedError{}
	}
	if !c.writeMu.TryLock() {
		return &SyncOperationInProgressError{}
	}
	if atomic.LoadUint32(&c.closed) == 1 {
		c.writeMu.Unlock()
		return &ClientClosedError{}
	}
	c.syncWorkUsed = 0
	c.a3Stats = a3AccountingStats{}
	return nil
}

func detectClientDBIdentity(db *sql.DB) (clientDBIdentity, error) {
	if db == nil {
		return clientDBIdentity{}, fmt.Errorf("db cannot be nil")
	}

	rows, err := db.Query(`PRAGMA database_list`)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to inspect SQLite database identity: %w", err)
	}
	defer rows.Close()

	var mainFile string
	for rows.Next() {
		var (
			seq  int
			name string
			file string
		)
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return clientDBIdentity{}, fmt.Errorf("failed to scan SQLite database identity: %w", err)
		}
		if name == "main" {
			mainFile = strings.TrimSpace(file)
			break
		}
	}
	if err := rows.Err(); err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to iterate SQLite database identity: %w", err)
	}

	if mainFile == "" {
		label := fmt.Sprintf("in-memory:%p", db)
		return clientDBIdentity{
			Key:   label,
			Label: label,
		}, nil
	}

	if !strings.HasPrefix(mainFile, "file:") {
		resolvedPath := mainFile
		if !filepath.IsAbs(resolvedPath) {
			if absPath, err := filepath.Abs(resolvedPath); err == nil {
				resolvedPath = absPath
			}
		}
		if evalPath, err := filepath.EvalSymlinks(resolvedPath); err == nil {
			resolvedPath = evalPath
		}
		mainFile = resolvedPath
	}

	databaseFile, err := os.OpenFile(mainFile, os.O_RDONLY, 0)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to open SQLite file for ownership: %w", err)
	}
	info, err := databaseFile.Stat()
	if err != nil {
		_ = databaseFile.Close()
		return clientDBIdentity{}, fmt.Errorf("failed to inspect SQLite file identity: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		_ = databaseFile.Close()
		return clientDBIdentity{}, fmt.Errorf("SQLite file identity is unavailable")
	}
	_ = databaseFile.Close()
	key := fmt.Sprintf("sqlite:%v:%v", stat.Dev, stat.Ino)
	lockPath := filepath.Join(clientDBOwnershipLockRoot, fmt.Sprintf("oversqlite-%v-%v.lock", stat.Dev, stat.Ino))
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o666)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to open SQLite ownership lock: %w", err)
	}
	lockFile := os.NewFile(uintptr(fd), lockPath)
	if lockFile == nil {
		_ = syscall.Close(fd)
		return clientDBIdentity{}, fmt.Errorf("failed to wrap SQLite ownership lock")
	}
	if err := syscall.Fchmod(fd, 0o666); err != nil {
		_ = lockFile.Close()
		return clientDBIdentity{}, fmt.Errorf("failed to make SQLite ownership lock shareable: %w", err)
	}
	lockInfo, err := lockFile.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() {
		_ = lockFile.Close()
		if err != nil {
			return clientDBIdentity{}, fmt.Errorf("failed to inspect SQLite ownership lock: %w", err)
		}
		return clientDBIdentity{}, fmt.Errorf("SQLite ownership lock is not a regular file")
	}
	return clientDBIdentity{
		Key:   key,
		Label: mainFile,
		File:  lockFile,
	}, nil
}

func claimClientDBOwnership(db *sql.DB) (clientDBIdentity, error) {
	identity, err := detectClientDBIdentity(db)
	if err != nil {
		return clientDBIdentity{}, err
	}
	if err := claimClientDBIdentity(identity, db, "client"); err != nil {
		return clientDBIdentity{}, err
	}
	return identity, nil
}

func claimClientDBIdentity(identity clientDBIdentity, db *sql.DB, kind string) error {

	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()

	if _, exists := activeClientDBOwnership.owners[identity.Key]; exists {
		if identity.File != nil {
			_ = identity.File.Close()
		}
		return &DatabaseAlreadyOwnedError{Database: identity.Label}
	}
	lockFile := identity.File
	if lockFile != nil {
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = lockFile.Close()
			return &DatabaseAlreadyOwnedError{Database: identity.Label}
		}
	}
	activeClientDBOwnership.owners[identity.Key] = clientDBOwner{
		label: identity.Label,
		db:    db,
		lock:  lockFile,
		kind:  kind,
	}
	return nil
}

func canonicalOpeningDatabasePath(path string) (string, bool, error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return "", false, fmt.Errorf("DatabasePath must be a non-empty filesystem path")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", false, fmt.Errorf("resolve DatabasePath: %w", err)
	}
	absPath = filepath.Clean(absPath)
	info, err := os.Lstat(absPath)
	if err == nil {
		resolved, err := filepath.EvalSymlinks(absPath)
		if err != nil {
			return "", false, fmt.Errorf("resolve DatabasePath symlinks: %w", err)
		}
		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			return "", false, fmt.Errorf("DatabasePath must identify a regular file")
		}
		return resolved, true, nil
	}
	if !os.IsNotExist(err) {
		return "", false, fmt.Errorf("inspect DatabasePath: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absPath))
	if err != nil {
		return "", false, fmt.Errorf("resolve DatabasePath parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(absPath)), false, nil
}

func openingPathLock(path string) (clientDBIdentity, error) {
	digest := sha256.Sum256([]byte(path))
	key := fmt.Sprintf("sqlite-path:%x", digest[:])
	lockPath := filepath.Join(clientDBOwnershipLockRoot, fmt.Sprintf("oversqlite-path-%x.lock", digest[:]))
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o666)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("open SQLite path ownership lock: %w", err)
	}
	lockFile := os.NewFile(uintptr(fd), lockPath)
	if lockFile == nil {
		_ = syscall.Close(fd)
		return clientDBIdentity{}, fmt.Errorf("wrap SQLite path ownership lock")
	}
	if err := syscall.Fchmod(fd, 0o666); err != nil {
		_ = lockFile.Close()
		return clientDBIdentity{}, fmt.Errorf("make SQLite path ownership lock shareable: %w", err)
	}
	return clientDBIdentity{Key: key, Label: path, File: lockFile}, nil
}

// claimOpeningDBOwnership reserves the canonical path before a new file is
// created, then binds the claim to the file's device/inode before SQLite opens.
func claimOpeningDBOwnership(path string) (clientDBIdentity, string, error) {
	canonical, exists, err := canonicalOpeningDatabasePath(path)
	if err != nil {
		return clientDBIdentity{}, "", err
	}
	if exists {
		file, err := os.Open(canonical)
		if err != nil {
			return clientDBIdentity{}, "", err
		}
		_ = file.Close()
		identity, err := detectClientDBIdentityFromPath(canonical)
		if err != nil {
			return clientDBIdentity{}, "", err
		}
		if err := claimClientDBIdentity(identity, nil, "opening"); err != nil {
			return clientDBIdentity{}, "", err
		}
		return identity, canonical, nil
	}

	pathIdentity, err := openingPathLock(canonical)
	if err != nil {
		return clientDBIdentity{}, "", err
	}
	if err := claimClientDBIdentity(pathIdentity, nil, "opening-path"); err != nil {
		return clientDBIdentity{}, "", err
	}
	created, err := os.OpenFile(canonical, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		releaseClientDBOwnership(pathIdentity.Key)
		return clientDBIdentity{}, "", fmt.Errorf("create package-owned SQLite file: %w", err)
	}
	if err := created.Close(); err != nil {
		releaseClientDBOwnership(pathIdentity.Key)
		return clientDBIdentity{}, "", fmt.Errorf("close newly created SQLite file: %w", err)
	}
	identity, err := detectClientDBIdentityFromPath(canonical)
	if err != nil {
		releaseClientDBOwnership(pathIdentity.Key)
		return clientDBIdentity{}, "", err
	}
	if err := claimClientDBIdentity(identity, nil, "opening"); err != nil {
		releaseClientDBOwnership(pathIdentity.Key)
		return clientDBIdentity{}, "", err
	}
	releaseClientDBOwnership(pathIdentity.Key)
	return identity, canonical, nil
}

func detectClientDBIdentityFromPath(path string) (clientDBIdentity, error) {
	databaseFile, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("open SQLite file for ownership: %w", err)
	}
	defer databaseFile.Close()
	info, err := databaseFile.Stat()
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("inspect SQLite file identity: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return clientDBIdentity{}, fmt.Errorf("SQLite file identity is unavailable")
	}
	key := fmt.Sprintf("sqlite:%v:%v", stat.Dev, stat.Ino)
	lockPath := filepath.Join(clientDBOwnershipLockRoot, fmt.Sprintf("oversqlite-%v-%v.lock", stat.Dev, stat.Ino))
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o666)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("open SQLite ownership lock: %w", err)
	}
	lockFile := os.NewFile(uintptr(fd), lockPath)
	if lockFile == nil {
		_ = syscall.Close(fd)
		return clientDBIdentity{}, fmt.Errorf("wrap SQLite ownership lock")
	}
	if err := syscall.Fchmod(fd, 0o666); err != nil {
		_ = lockFile.Close()
		return clientDBIdentity{}, fmt.Errorf("make SQLite ownership lock shareable: %w", err)
	}
	return clientDBIdentity{Key: key, Label: path, File: lockFile}, nil
}

func bindOpeningDBOwnership(key string, db *sql.DB) error {
	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()
	owner, ok := activeClientDBOwnership.owners[key]
	if !ok || owner.kind != "opening" || owner.db != nil {
		return fmt.Errorf("opening database ownership changed before pool binding")
	}
	owner.db = db
	activeClientDBOwnership.owners[key] = owner
	return nil
}

func transferOpeningDBOwnership(key string, db *sql.DB) error {
	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()
	owner, ok := activeClientDBOwnership.owners[key]
	if !ok || owner.kind != "opening" || owner.db != db {
		return fmt.Errorf("opening database ownership changed before ready transfer")
	}
	owner.kind = "client"
	activeClientDBOwnership.owners[key] = owner
	return nil
}

func releaseClientDBOwnership(key string) {
	if strings.TrimSpace(key) == "" {
		return
	}

	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()
	owner, ok := activeClientDBOwnership.owners[key]
	if !ok {
		return
	}
	delete(activeClientDBOwnership.owners, key)
	if owner.lock != nil {
		_ = syscall.Flock(int(owner.lock.Fd()), syscall.LOCK_UN)
		_ = owner.lock.Close()
	}
}

// SyncTable represents a table configuration for synchronization
type SyncTable struct {
	TableName         string   // Table name (e.g., "users", "posts")
	SyncKeyColumnName string   // Single-column convenience field for the current supported envelope
	SyncKeyColumns    []string // Ordered sync key columns for the bundle-based runtime
}

type BundleChangeWatchMode string

const (
	BundleChangeWatchOff  BundleChangeWatchMode = "off"
	BundleChangeWatchAuto BundleChangeWatchMode = "auto"
)

// Config holds configuration for the SQLite sync client
type Config struct {
	MaxSyncWorkspaceEntries       int64                                 // zero selects 100000
	MaxSyncWorkspaceBytes         int64                                 // zero selects 256 MiB
	MaxSyncWorkUnitsPerInvocation int64                                 // zero selects 1000000
	GenerationID                  oversync.SchemaGenerationID           // Explicit generation resolved on attachment; no network during construction.
	generationProof               oversync.SchemaGenerationProof        // Internal admitted identity
	localContract                 *LocalGenerationContract              // Internal derived local conversions
	discovery                     *oversync.GenerationDiscoveryResponse // Internal immutable opening-time wire contract
	DatabaseID                    string                                // Stable application database identity
	ServiceID                     string                                // Stable service/application namespace
	LocalVersion                  int64                                 // Durable application migration version
	LocalOnlyTables               []string                              // Application-owned tables preserved by upgrades
	schema                        string                                // Internal lifecycle namespace
	Tables                        []SyncTable                           // Table configurations with explicit primary key columns
	UploadLimit                   int                                   // Soft row target; complete FK groups may exceed it up to the admitted hard limit.
	UploadByteLimit               int64                                 // Soft decoded JSON target for a frozen push bundle; zero uses the admitted hard maximum.
	DownloadLimit                 int                                   // e.g., 1000
	SnapshotChunkRows             int                                   // e.g., 1000
	SnapshotChunkBytes            int64                                 // e.g., 4 MiB
	BackoffMin                    time.Duration                         // 1s
	BackoffMax                    time.Duration                         // 60s
	RetryPolicy                   *RetryPolicy                          // nil => conservative built-in retry policy; explicit disabled policy turns retries off
	SnapshotCapacityRetryPolicy   *SnapshotCapacityRetryPolicy          // nil => enabled with a 30-second elapsed-time budget
	PushCapacityRetryPolicy       *PushCapacityRetryPolicy              // nil => enabled with a 30-second elapsed-time budget
	BundleChangeWatchMode         BundleChangeWatchMode                 // off by default; auto uses /sync/watch only when advertised by server
	WatchFallbackInterval         time.Duration                         // 0 => BackoffMax, or 60s when BackoffMax is unset
	testAfterOwnership            func()                                // tests may pause construction after exclusive ownership
}

// DefaultConfig returns transport defaults for an explicitly selected generation.
// Set DatabaseID and ServiceID to stable application identities before NewOpening.
func DefaultConfig(generationID oversync.SchemaGenerationID, tables []SyncTable) *Config {
	return &Config{
		MaxSyncWorkspaceEntries:       defaultSyncWorkspaceEntries,
		MaxSyncWorkspaceBytes:         defaultSyncWorkspaceBytes,
		MaxSyncWorkUnitsPerInvocation: defaultSyncWorkUnits,
		GenerationID:                  generationID, Tables: tables,
		UploadLimit: 1000, UploadByteLimit: 1 << 20, DownloadLimit: 1000,
		SnapshotChunkRows: 1000, SnapshotChunkBytes: 4 << 20,
		BackoffMin: time.Second, BackoffMax: 60 * time.Second,
		BundleChangeWatchMode: BundleChangeWatchOff,
	}
}

// PauseUploads suspends push operations and background sync loops.
func (c *Client) PauseUploads() { atomic.StoreInt32(&c.uploadPaused, 1) }

// ResumeUploads resumes upload operations
func (c *Client) ResumeUploads() { atomic.StoreInt32(&c.uploadPaused, 0) }

// PauseDownloads suspends pull, hydrate, recover, and background download-loop work.
func (c *Client) PauseDownloads() { atomic.StoreInt32(&c.downloadPaused, 1) }

// ResumeDownloads resumes download operations
func (c *Client) ResumeDownloads() { atomic.StoreInt32(&c.downloadPaused, 0) }

// SnapshotTransferDiagnostics returns counters for chunked hydrate/recover transport activity.
func (c *Client) SnapshotTransferDiagnostics() SnapshotTransferStats {
	if c == nil {
		return SnapshotTransferStats{}
	}
	return SnapshotTransferStats{
		SessionsCreated:             atomic.LoadInt64(&c.snapshotStats.sessionsCreated),
		ChunksFetched:               atomic.LoadInt64(&c.snapshotStats.chunksFetched),
		MaxChunkRows:                atomic.LoadInt64(&c.snapshotStats.maxChunkRows),
		MaxChunkWireBytes:           atomic.LoadInt64(&c.snapshotStats.maxChunkWireBytes),
		MaxChunkDecodedBodyBytes:    atomic.LoadInt64(&c.snapshotStats.maxChunkDecodedBodyBytes),
		MaxLiveStagedApplyRows:      atomic.LoadInt64(&c.snapshotStats.maxLiveStagedApplyRows),
		MaxLiveStagedApplyTextBytes: atomic.LoadInt64(&c.snapshotStats.maxLiveStagedApplyText),
		MaxAppliedInMemoryRows:      atomic.LoadInt64(&c.snapshotStats.maxAppliedInMemoryRows),
		CapacityResponses:           atomic.LoadInt64(&c.snapshotStats.capacityResponses),
		CapacityRetries:             atomic.LoadInt64(&c.snapshotStats.capacityRetries),
		CapacityWait:                time.Duration(atomic.LoadInt64(&c.snapshotStats.capacityWaitNanos)),
	}
}

// ResetSnapshotTransferDiagnostics clears the local chunked snapshot counters.
func (c *Client) ResetSnapshotTransferDiagnostics() {
	if c == nil {
		return
	}
	atomic.StoreInt64(&c.snapshotStats.sessionsCreated, 0)
	atomic.StoreInt64(&c.snapshotStats.chunksFetched, 0)
	atomic.StoreInt64(&c.snapshotStats.maxChunkRows, 0)
	atomic.StoreInt64(&c.snapshotStats.maxChunkWireBytes, 0)
	atomic.StoreInt64(&c.snapshotStats.maxChunkDecodedBodyBytes, 0)
	atomic.StoreInt64(&c.snapshotStats.maxLiveStagedApplyRows, 0)
	atomic.StoreInt64(&c.snapshotStats.maxLiveStagedApplyText, 0)
	atomic.StoreInt64(&c.snapshotStats.maxAppliedInMemoryRows, 0)
	atomic.StoreInt64(&c.snapshotStats.capacityResponses, 0)
	atomic.StoreInt64(&c.snapshotStats.capacityRetries, 0)
	atomic.StoreInt64(&c.snapshotStats.capacityWaitNanos, 0)
}

func atomicRecordMax(target *int64, value int64) {
	for current := atomic.LoadInt64(target); value > current; current = atomic.LoadInt64(target) {
		if atomic.CompareAndSwapInt64(target, current, value) {
			return
		}
	}
}

// PushTransferDiagnostics returns cumulative push transfer counters for the current client instance.
func (c *Client) PushTransferDiagnostics() PushTransferStats {
	if c == nil {
		return PushTransferStats{}
	}
	return PushTransferStats{
		CapacityResponses: atomic.LoadInt64(&c.pushStats.capacityResponses),
		CapacityRetries:   atomic.LoadInt64(&c.pushStats.capacityRetries),
		CapacityWait:      time.Duration(atomic.LoadInt64(&c.pushStats.capacityWaitNanos)),
	}
}

// ResetPushTransferDiagnostics resets the push transfer counters.
func (c *Client) ResetPushTransferDiagnostics() {
	if c == nil {
		return
	}
	atomic.StoreInt64(&c.pushStats.capacityResponses, 0)
	atomic.StoreInt64(&c.pushStats.capacityRetries, 0)
	atomic.StoreInt64(&c.pushStats.capacityWaitNanos, 0)
}

// SetBeforePushReplayHook installs a test or debug hook that runs after remote commit and before local replay.
func (c *Client) SetBeforePushReplayHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushReplayHook = hook
}

// SetBeforePushFreezeHook installs a test or debug hook that runs after freezing the local outbox and before commit.
func (c *Client) SetBeforePushFreezeHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushFreezeHook = hook
}

// SetBeforePushCommitHook installs a test or debug hook immediately before the
// complete direct push request is sent.
func (c *Client) SetBeforePushCommitHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushCommitHook = hook
}

// Validate and copy caller configuration before taking database ownership.
func prepareClientConfiguration(config *Config) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	config = cloneCoordinatorClientConfig(config)
	if config.MaxSyncWorkspaceEntries < 0 || config.MaxSyncWorkspaceBytes < 0 || config.MaxSyncWorkUnitsPerInvocation < 0 {
		return nil, fmt.Errorf("sync workspace limits must be positive")
	}
	if config.MaxSyncWorkspaceEntries == 0 {
		config.MaxSyncWorkspaceEntries = defaultSyncWorkspaceEntries
	}
	if config.MaxSyncWorkspaceBytes == 0 {
		config.MaxSyncWorkspaceBytes = defaultSyncWorkspaceBytes
	}
	if config.MaxSyncWorkUnitsPerInvocation == 0 {
		config.MaxSyncWorkUnitsPerInvocation = defaultSyncWorkUnits
	}
	if config.GenerationID < 1 {
		return nil, fmt.Errorf("an explicit positive generation ID is required")
	}
	config.schema = unconfirmedLocalSchema
	config.generationProof = oversync.SchemaGenerationProof{ID: config.GenerationID}
	config.localContract = nil
	if config.SnapshotChunkRows < 0 || config.SnapshotChunkBytes < 0 {
		return nil, fmt.Errorf("snapshot chunk limits must be positive")
	}
	if config.SnapshotChunkRows == 0 {
		config.SnapshotChunkRows = 1000
	}
	if config.SnapshotChunkBytes == 0 {
		config.SnapshotChunkBytes = 4 << 20
	}
	return config, nil
}

// newClientFromDB constructs a borrowed lower-level client for package tests.
func newClientFromDB(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config) (client *Client, err error) {
	return newClientFromDBWithOwnership(db, baseURL, tok, config, false)
}

// newBorrowedClientFromDB is restricted to package tests that exercise a
// lower-level algorithm against a pool owned by their fixture or Opening.
func newBorrowedClientFromDB(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config) (client *Client, err error) {
	return newClientFromDBWithOwnership(db, baseURL, tok, config, false)
}

func newClientFromDBWithOwnership(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config, claimOwnership bool) (client *Client, err error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	config, err = prepareClientConfiguration(config)
	if err != nil {
		return nil, err
	}
	tableInfoProvider := NewTableInfoProvider()
	validatedTables, err := validateSyncTables(db, tableInfoProvider, config.Tables)
	if err != nil {
		return nil, err
	}

	var identity clientDBIdentity
	if claimOwnership {
		identity, err = claimClientDBOwnership(db)
		if err != nil {
			return nil, err
		}
	}
	releaseOwnershipOnError := claimOwnership
	defer func() {
		if releaseOwnershipOnError {
			releaseClientDBOwnership(identity.Key)
		}
	}()
	if config.testAfterOwnership != nil {
		config.testAfterOwnership()
	}
	client = assembleClientRuntime(db, baseURL, tok, config, tableInfoProvider, validatedTables, identity.Key, nil)
	client.ownsDB = claimOwnership
	if err := prepareRetainedContractWithFinalize(context.Background(), db, config, func(ctx context.Context, tx *immediateTx) error {
		localGeneration, err := compileClientContract(config)
		if err != nil {
			return err
		}
		client.localGeneration = localGeneration
		client.schemaGenerationProof = config.generationProof
		managedReady, err := inspectUpgradeConstructionGate(ctx, tx, config.generationProof)
		if err != nil {
			return err
		}
		if managedReady {
			if err := validateReadyClientIdentity(ctx, tx, config); err != nil {
				return err
			}
		}
		return installClientRuntime(ctx, tx, client, validatedTables)
	}, claimOwnership); err != nil {
		return nil, err
	}

	releaseOwnershipOnError = false
	return client, nil
}

func assembleClientRuntime(db *sql.DB, baseURL string, tok func(context.Context) (string, error), config *Config, tableInfoProvider *TableInfoProvider, validatedTables *validatedSyncTables, ownershipKey string, localGeneration *compiledLocalContract) *Client {
	client := &Client{
		db:                    db,
		ownsDB:                true,
		BaseURL:               baseURL,
		Token:                 tok,
		UserID:                "",
		Resolver:              &DefaultResolver{},
		HTTP:                  &http.Client{Timeout: 120 * time.Second}, // Increased for large batch uploads
		config:                config,
		logger:                slog.Default(),
		pkByTable:             validatedTables.pkByTable,
		keyByTable:            validatedTables.keyByTable,
		tableOrder:            validatedTables.tableOrder,
		tableInfo:             tableInfoProvider,
		dbOwnershipKey:        ownershipKey,
		sourceID:              "",
		schemaGenerationProof: config.generationProof,
		localGeneration:       localGeneration,
		discovery:             config.discovery,
		sourceIDGenerator: func() string {
			return uuid.NewString()
		},
	}
	return client
}

func installClientRuntime(ctx context.Context, db triggerSQLDB, client *Client, validatedTables *validatedSyncTables) error {
	if err := validateRuntimeTriggerInventory(db, client, validatedTables); err != nil {
		return fmt.Errorf("validate runtime trigger inventory: %w", err)
	}
	// Create triggers for all registered tables (after sync tables are created)
	for _, syncTable := range client.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if err := createTriggersForTableWithInfo(db, client.schemaForTable(tableName), syncTable, validatedTables.tableInfoByKey[tableName]); err != nil {
			return fmt.Errorf("failed to create triggers for table %s: %w", syncTable.TableName, err)
		}
	}
	if err := client.recordManagedSyncTablesUsing(ctx, db); err != nil {
		return err
	}
	// Opening validates through its reserved writer. Bind those same immutable
	// metadata objects to the client database scope before publishing the client.
	// Planner pages then reuse them without retaining a fresh cache per writer.
	provider := client.tableInfoProvider()
	provider.mutex.Lock()
	for table, info := range validatedTables.tableInfoByKey {
		provider.cache[tableInfoCacheKey{scope: reflect.ValueOf(client.db).Pointer(), table: table}] = info
	}
	provider.mutex.Unlock()

	return nil
}

// doHTTP applies the sync client's redirect policy even when callers replace
// Client.HTTP with a custom client. Redirects are not part of the protocol and
// no authenticated request is ever replayed to a Location target.
func (c *Client) doHTTP(req *http.Request) (*http.Response, error) {
	if c == nil || c.HTTP == nil {
		return nil, fmt.Errorf("HTTP client is not configured")
	}
	client := *c.HTTP
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		return fmt.Errorf("sync protocol redirects are not permitted")
	}
	return client.Do(req)
}

// A fresh offline client has no admitted descriptor yet.
func compileClientContract(config *Config) (*compiledLocalContract, error) {
	if config.localContract == nil {
		return nil, nil
	}
	compiled, err := compileLocalContract(*config.localContract)
	if err != nil {
		return nil, err
	}
	if compiled.proof != config.generationProof {
		return nil, fmt.Errorf("local contract does not match installed proof")
	}
	if err := validateClientTablesMatchLocalContract(config, compiled); err != nil {
		return nil, err
	}
	return compiled, nil
}

func validateClientTablesMatchLocalContract(config *Config, local *compiledLocalContract) error {
	if config == nil || local == nil {
		return fmt.Errorf("client configuration and compiled local contract are required")
	}
	if _, err := validateLocalOnlyTableScope(config.LocalOnlyTables, local); err != nil {
		return err
	}
	expectedTables := local.localTables
	if len(config.Tables) != len(expectedTables) {
		return fmt.Errorf("config.Tables count does not match Config.localContract")
	}
	expectedKeys := make(map[string][]string, len(expectedTables))
	for _, table := range expectedTables {
		expectedKeys[table.Table] = append([]string(nil), table.SyncKeyColumns...)
	}
	seen := make(map[string]struct{}, len(config.Tables))
	for _, table := range config.Tables {
		if table.TableName == "" || table.TableName != strings.TrimSpace(table.TableName) || table.TableName != strings.ToLower(table.TableName) {
			return fmt.Errorf("config.Tables table name %q is not canonical", table.TableName)
		}
		if _, exists := seen[table.TableName]; exists {
			return fmt.Errorf("config.Tables contains duplicate table %q", table.TableName)
		}
		seen[table.TableName] = struct{}{}
		wantKeys, exists := expectedKeys[table.TableName]
		if !exists {
			return fmt.Errorf("config.Tables contains table %q absent from Config.localContract", table.TableName)
		}
		gotKeys, err := normalizedSyncKeyColumns(table)
		if err != nil {
			return err
		}
		for _, key := range gotKeys {
			if key == "" || key != strings.TrimSpace(key) || key != strings.ToLower(key) {
				return fmt.Errorf("config.Tables sync key %q for table %q is not canonical", key, table.TableName)
			}
		}
		if !stringSlicesEqual(gotKeys, wantKeys) {
			return fmt.Errorf("config.Tables sync keys for table %q do not match Config.localContract", table.TableName)
		}
	}
	return nil
}

func validSchemaGenerationFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

// Database returns the borrowed application pool while the ready client is live.
// The client retains ownership and closes the pool during Close or successful uninstall.
func (c *Client) Database() *sql.DB {
	if c == nil || atomic.LoadUint32(&c.closed) == 1 {
		return nil
	}
	return c.db
}

// Close stops package-owned activity, closes the pool, and releases ownership.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	atomic.StoreUint32(&c.loopState, clientLoopStateTerminal)
	stopErr := c.Stop(context.Background())

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if atomic.LoadUint32(&c.closed) == 1 {
		return stopErr
	}
	atomic.StoreUint32(&c.closed, 1)
	if !c.ownsDB {
		return stopErr
	}
	closeErr := c.db.Close()
	releaseClientDBOwnership(c.dbOwnershipKey)
	c.dbOwnershipKey = ""
	return errors.Join(stopErr, closeErr)
}

func (c *Client) recordManagedSyncTables() error {
	if c == nil || c.db == nil || c.config == nil {
		return nil
	}
	return c.recordManagedSyncTablesUsing(context.Background(), c.db)
}

func (c *Client) recordManagedSyncTablesUsing(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) error {
	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO _sync_managed_tables (schema_name, table_name)
			VALUES (?, ?)
			ON CONFLICT(schema_name, table_name) DO NOTHING
		`, c.schemaForTable(tableName), tableName); err != nil {
			return fmt.Errorf("failed to record managed sync table %s: %w", tableName, err)
		}
	}
	return nil
}

func (c *Client) tableInfoProvider() *TableInfoProvider {
	if c.tableInfo == nil {
		c.tableInfo = NewTableInfoProvider()
	}
	return c.tableInfo
}

func (c *Client) getTableInfo(tableName string) (*TableInfo, error) {
	return c.tableInfoProvider().Get(c.db, strings.ToLower(tableName))
}

func (c *Client) getTableInfoTx(tx tableInfoQueryer, tableName string) (*TableInfo, error) {
	if owned, ok := tx.(*statementTransaction); ok {
		tx = owned.sqliteTransaction
	}

	return c.tableInfoProvider().Get(tx, strings.ToLower(tableName))
}

type validatedSyncTables struct {
	pkByTable      map[string]string
	keyByTable     map[string][]string
	tableOrder     map[string]int
	tableInfoByKey map[string]*TableInfo
}

func validateSyncTables(db tableInfoQueryer, provider *TableInfoProvider, tables []SyncTable) (*validatedSyncTables, error) {
	if provider == nil {
		provider = NewTableInfoProvider()
	}
	pkByTable := make(map[string]string, len(tables))
	keyByTable := make(map[string][]string, len(tables))
	tableInfoByKey := make(map[string]*TableInfo, len(tables))
	managedTables := make(map[string]struct{}, len(tables))
	for _, syncTable := range tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			return nil, fmt.Errorf("sync table name must be provided")
		}
		if strings.Contains(tableName, ".") {
			return nil, fmt.Errorf("table %s must not include a schema qualifier; oversqlite supports exactly one config.schema per local database", syncTable.TableName)
		}
		if _, exists := pkByTable[tableName]; exists {
			return nil, fmt.Errorf("duplicate sync table registration for %s", tableName)
		}
		managedTables[tableName] = struct{}{}

		keyColumns, err := normalizedSyncKeyColumns(syncTable)
		if err != nil {
			return nil, err
		}
		if len(keyColumns) != 1 {
			return nil, fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", syncTable.TableName)
		}

		tableInfo, err := provider.Get(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect table %s: %w", syncTable.TableName, err)
		}
		for _, column := range tableInfo.Columns {
			if column.Hidden != 0 {
				return nil, fmt.Errorf("table %s contains unsupported generated or hidden column %s", syncTable.TableName, column.Name)
			}
		}

		resolvedPK, err := configuredPrimaryKeyColumn(tableInfo, syncTable)
		if err != nil {
			return nil, err
		}

		pkByTable[tableName] = resolvedPK
		keyByTable[tableName] = append([]string(nil), keyColumns...)
		tableInfoByKey[tableName] = tableInfo
	}
	if err := validateManagedForeignKeyClosure(managedTables, tableInfoByKey); err != nil {
		return nil, err
	}
	tableOrder, err := computeManagedTableOrder(tables, tableInfoByKey)
	if err != nil {
		return nil, err
	}
	return &validatedSyncTables{
		pkByTable:      pkByTable,
		keyByTable:     keyByTable,
		tableOrder:     tableOrder,
		tableInfoByKey: tableInfoByKey,
	}, nil
}

func normalizedSyncKeyColumns(syncTable SyncTable) ([]string, error) {
	if len(syncTable.SyncKeyColumns) == 0 {
		pkColumn := strings.TrimSpace(syncTable.SyncKeyColumnName)
		if pkColumn == "" {
			return nil, fmt.Errorf("table %s must declare SyncKeyColumnName or SyncKeyColumns explicitly", syncTable.TableName)
		}
		return []string{pkColumn}, nil
	}

	columns := make([]string, 0, len(syncTable.SyncKeyColumns))
	seen := make(map[string]struct{}, len(syncTable.SyncKeyColumns))
	for _, col := range syncTable.SyncKeyColumns {
		normalized := strings.TrimSpace(col)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		columns = append(columns, normalized)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s must declare at least one non-empty sync key column", syncTable.TableName)
	}
	return columns, nil
}

func validateManagedForeignKeyClosure(managedTables map[string]struct{}, tableInfoByKey map[string]*TableInfo) error {
	for tableName := range managedTables {
		tableInfo := tableInfoByKey[tableName]
		if tableInfo == nil {
			return fmt.Errorf("missing table metadata for %s", tableName)
		}
		var unsupportedCompositeRefs []string
		var missingRefs []string
		for _, fk := range tableInfo.ForeignKeys {
			refKey := strings.ToLower(strings.TrimSpace(fk.RefTable))
			if refKey == "" {
				continue
			}
			if fk.Seq > 0 {
				unsupportedCompositeRefs = append(unsupportedCompositeRefs, fmt.Sprintf("%s -> %s", tableName, refKey))
				continue
			}
			if _, ok := managedTables[refKey]; ok {
				continue
			}

			missingRefs = append(missingRefs, fmt.Sprintf("%s.%s -> %s.%s", tableName, fk.FromCol, refKey, fk.ToCol))
		}

		if len(unsupportedCompositeRefs) > 0 {
			sort.Strings(unsupportedCompositeRefs)
			return fmt.Errorf("managed tables contain unsupported composite foreign keys: %s", strings.Join(unsupportedCompositeRefs, "; "))
		}
		if len(missingRefs) > 0 {
			sort.Strings(missingRefs)
			return fmt.Errorf("managed tables are not FK-closed: %s", strings.Join(missingRefs, "; "))
		}
	}

	return nil
}

func computeManagedTableOrder(tables []SyncTable, tableInfoByKey map[string]*TableInfo) (map[string]int, error) {
	orderByTable := make(map[string]int, len(tables))
	originalOrder := make(map[string]int, len(tables))
	managed := make(map[string]struct{}, len(tables))
	dependents := make(map[string]map[string]struct{}, len(tables))
	inDegree := make(map[string]int, len(tables))

	for i, table := range tables {
		name := strings.ToLower(strings.TrimSpace(table.TableName))
		originalOrder[name] = i
		managed[name] = struct{}{}
		if _, ok := dependents[name]; !ok {
			dependents[name] = make(map[string]struct{})
		}
	}

	for _, table := range tables {
		tableName := strings.ToLower(strings.TrimSpace(table.TableName))
		tableInfo := tableInfoByKey[tableName]
		if tableInfo == nil {
			return nil, fmt.Errorf("missing table metadata for %s", tableName)
		}
		for _, fk := range tableInfo.ForeignKeys {
			refName := strings.ToLower(strings.TrimSpace(fk.RefTable))
			if refName == "" || refName == tableName {
				continue
			}
			if _, ok := managed[refName]; !ok {
				continue
			}
			if _, ok := dependents[refName][tableName]; ok {
				continue
			}
			dependents[refName][tableName] = struct{}{}
			inDegree[tableName]++
		}
	}
	queue := make([]string, 0, len(tables))
	for _, table := range tables {
		name := strings.ToLower(strings.TrimSpace(table.TableName))
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		return originalOrder[queue[i]] < originalOrder[queue[j]]
	})

	ordered := make([]string, 0, len(tables))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		ordered = append(ordered, current)

		children := make([]string, 0, len(dependents[current]))
		for child := range dependents[current] {
			children = append(children, child)
		}
		sort.Slice(children, func(i, j int) bool {
			return originalOrder[children[i]] < originalOrder[children[j]]
		})

		for _, child := range children {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
		sort.Slice(queue, func(i, j int) bool {
			return originalOrder[queue[i]] < originalOrder[queue[j]]
		})
	}

	if len(ordered) < len(tables) {
		remaining := make([]string, 0, len(tables)-len(ordered))
		seen := make(map[string]struct{}, len(ordered))
		for _, tableName := range ordered {
			seen[tableName] = struct{}{}
		}
		for _, table := range tables {
			name := strings.ToLower(strings.TrimSpace(table.TableName))
			if _, ok := seen[name]; ok {
				continue
			}
			remaining = append(remaining, name)
		}
		sort.Slice(remaining, func(i, j int) bool {
			return originalOrder[remaining[i]] < originalOrder[remaining[j]]
		})
		ordered = append(ordered, remaining...)
	}

	for i, tableName := range ordered {
		orderByTable[tableName] = i
	}
	return orderByTable, nil
}

func (c *Client) primaryKeyColumnForTable(tableName string) (string, error) {
	pkColumn, ok := c.pkByTable[strings.ToLower(tableName)]
	if !ok {
		return "", fmt.Errorf("table %s is not configured for sync", tableName)
	}
	return pkColumn, nil
}

func (c *Client) syncKeyColumnsForTable(tableName string) ([]string, error) {
	keyColumns, ok := c.keyByTable[strings.ToLower(tableName)]
	if !ok || len(keyColumns) == 0 {
		return nil, fmt.Errorf("table %s is not configured for sync", tableName)
	}
	return append([]string(nil), keyColumns...), nil
}

// initializeDatabase creates sync metadata tables according to the spec (private function)
func initializeDatabase(db *sql.DB) error {
	if err := configureSQLiteConnection(db); err != nil {
		return err
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin sync metadata transaction: %w", err)
	}
	defer tx.Rollback()
	if err := initializeDatabaseInTx(context.Background(), tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit sync metadata transaction: %w", err)
	}
	return nil
}

func configureSQLiteConnection(db *sql.DB) error {
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}
	return nil
}

type syncMetadataExecutor interface {
	execer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func initializeDatabaseInTx(ctx context.Context, e syncMetadataExecutor) error {
	tables := []string{
		pushPlanDDL, pushNodesDDL, pushEdgesDDL,
		applyPlanDDL,
		applyRowsDDL,
		`CREATE INDEX IF NOT EXISTS _sync_apply_rows_role_ordinal_idx ON _sync_apply_rows(role,ordinal)`,
		applyEdgesDDL,
		`CREATE INDEX IF NOT EXISTS _sync_apply_rows_pending_idx ON _sync_apply_rows(pending,ordinal)`,
		`CREATE TABLE IF NOT EXISTS _sync_source_state (
				source_id             TEXT NOT NULL PRIMARY KEY,
				next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
				replaced_by_source_id TEXT NOT NULL DEFAULT '',
				created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
			)`,

		`CREATE TABLE IF NOT EXISTS _sync_attachment_state (
				singleton_key            INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
				attachment_epoch         INTEGER NOT NULL DEFAULT 1 CHECK(typeof(attachment_epoch)='integer' AND attachment_epoch > 0),
				current_source_id        TEXT NOT NULL DEFAULT '',
				binding_state            TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
				attached_user_id         TEXT NOT NULL DEFAULT '',
				schema_name              TEXT NOT NULL DEFAULT '',
				last_bundle_seq_seen     INTEGER NOT NULL DEFAULT 0,
				rebuild_required         INTEGER NOT NULL DEFAULT 0,
				pending_initialization_id TEXT NOT NULL DEFAULT ''
			)`,

		`CREATE TABLE IF NOT EXISTS _sync_operation_state (
				singleton_key        INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
				kind                 TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
				target_user_id       TEXT NOT NULL DEFAULT '',
				reason               TEXT NOT NULL DEFAULT '',
				replacement_source_id TEXT NOT NULL DEFAULT '',
				staged_snapshot_id   TEXT NOT NULL DEFAULT '',
				snapshot_bundle_seq  INTEGER NOT NULL DEFAULT 0,
				snapshot_row_count   INTEGER NOT NULL DEFAULT 0,
                workspace_context TEXT NOT NULL DEFAULT '',
                reserved_entries INTEGER NOT NULL DEFAULT 0 CHECK(typeof(reserved_entries)='integer' AND reserved_entries>=0),
                reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(reserved_bytes)='integer' AND reserved_bytes>=0),
                snapshot_stage_entries INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_stage_entries)='integer' AND snapshot_stage_entries>=0),
                snapshot_stage_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_stage_bytes)='integer' AND snapshot_stage_bytes>=0),
                snapshot_cleanup_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_cleanup_ordinal)='integer' AND snapshot_cleanup_ordinal>=0),
                push_stage_entries INTEGER NOT NULL DEFAULT 0 CHECK(typeof(push_stage_entries)='integer' AND push_stage_entries>=0),
                push_stage_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(push_stage_bytes)='integer' AND push_stage_bytes>=0),
                push_cleanup_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(typeof(push_cleanup_ordinal)='integer' AND push_cleanup_ordinal>=0)
            )`,

		`CREATE TABLE IF NOT EXISTS _sync_contract_identity (singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), database_id TEXT NOT NULL, service_id TEXT NOT NULL, generation_id INTEGER NOT NULL, local_version INTEGER NOT NULL, physical_shape TEXT NOT NULL, fingerprint TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS _sync_admitted_contracts (generation_id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL, descriptor TEXT NOT NULL, descriptor_hash TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS _sync_action_required (
				singleton_key          INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
				action                 TEXT NOT NULL CHECK (action IN ('', 'upgrade_application')),
				generation_id          INTEGER NOT NULL DEFAULT 0,
				generation_fingerprint TEXT NOT NULL DEFAULT '',
				observed_at             TEXT NOT NULL DEFAULT ''
			)`,

		`CREATE TABLE IF NOT EXISTS _sync_apply_state (
				singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
				apply_mode    INTEGER NOT NULL DEFAULT 0
			)`,

		authoritativeRowStateDDL,
		relationshipProjectionDDL,

		`CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
			schema_name      TEXT NOT NULL,
			table_name       TEXT NOT NULL,
			key_json         TEXT NOT NULL,
			op               TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			base_row_version INTEGER NOT NULL DEFAULT 0,
                base_kind TEXT NOT NULL CHECK(base_kind IN ('present','deleted','absent','unavailable')),
                base_payload TEXT,
                base_provenance TEXT NOT NULL CHECK(json_valid(base_provenance) AND json_type(base_provenance)='object'),
			payload          TEXT,
			dirty_ordinal    INTEGER NOT NULL DEFAULT 0,
			updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (schema_name, table_name, key_json),
 CHECK(typeof(base_row_version)='integer' AND CASE base_kind WHEN 'present' THEN base_row_version>0 AND base_payload IS NOT NULL AND json_valid(base_payload) AND json_type(base_payload)='object' WHEN 'deleted' THEN base_row_version>0 AND base_payload IS NULL ELSE base_row_version=0 AND base_payload IS NULL END),
 CHECK((op='DELETE' AND payload IS NULL) OR (op!='DELETE' AND payload IS NOT NULL AND json_valid(payload) AND json_type(payload)='object'))
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
			snapshot_id  TEXT NOT NULL,
			row_ordinal  INTEGER NOT NULL,
			schema_name  TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			key_json     TEXT NOT NULL,
			row_version  INTEGER NOT NULL,
			payload      TEXT NOT NULL,
            charge INTEGER NOT NULL DEFAULT 0 CHECK(typeof(charge)='integer' AND charge>=0),
            PRIMARY KEY (snapshot_id, row_ordinal)
		)`,

		`CREATE INDEX IF NOT EXISTS _sync_snapshot_stage_identity_idx
			ON _sync_snapshot_stage (snapshot_id, schema_name, table_name, key_json)`,

		`CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
				singleton_key           INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
				canonical_json_contract TEXT NOT NULL CHECK (canonical_json_contract = 'jcs_uniform_numeric_strings_v1'),
				state                   TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared')),
				source_id               TEXT NOT NULL DEFAULT '',
				source_bundle_id        INTEGER NOT NULL DEFAULT 0,
				canonical_request_hash  TEXT NOT NULL DEFAULT '',
				row_count               INTEGER NOT NULL DEFAULT 0,
				initialization_id       TEXT NOT NULL DEFAULT ''
			)`,

		`CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
				source_bundle_id INTEGER NOT NULL,
				row_ordinal      INTEGER NOT NULL,
				schema_name      TEXT NOT NULL,
				table_name       TEXT NOT NULL,
				key_json         TEXT NOT NULL,
				wire_key_json    TEXT NOT NULL,
				op               TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
				base_row_version INTEGER NOT NULL DEFAULT 0,
                base_kind TEXT NOT NULL CHECK(base_kind IN ('present','deleted','absent','unavailable')),
                base_payload TEXT,
                base_provenance TEXT NOT NULL CHECK(json_valid(base_provenance) AND json_type(base_provenance)='object'),
				local_payload    TEXT,
				wire_payload     TEXT,
				PRIMARY KEY (source_bundle_id, row_ordinal),
 UNIQUE(source_bundle_id,schema_name,table_name,key_json),
 CHECK(typeof(base_row_version)='integer' AND CASE base_kind WHEN 'present' THEN base_row_version>0 AND base_payload IS NOT NULL AND json_valid(base_payload) AND json_type(base_payload)='object' WHEN 'deleted' THEN base_row_version>0 AND base_payload IS NULL ELSE base_row_version=0 AND base_payload IS NULL END),
 CHECK((op='DELETE' AND local_payload IS NULL) OR (op!='DELETE' AND local_payload IS NOT NULL AND json_valid(local_payload) AND json_type(local_payload)='object'))
			)`,

		`CREATE TABLE IF NOT EXISTS _sync_push_stage (
			bundle_seq   INTEGER NOT NULL,
			row_ordinal  INTEGER NOT NULL,
			schema_name  TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			key_json     TEXT NOT NULL,
			op           TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			row_version  INTEGER NOT NULL,
			payload      TEXT,
            charge INTEGER NOT NULL DEFAULT 0 CHECK(typeof(charge)='integer' AND charge>=0),
            PRIMARY KEY (bundle_seq, row_ordinal)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_managed_tables (
			schema_name TEXT NOT NULL,
			table_name  TEXT NOT NULL,
			PRIMARY KEY (schema_name, table_name)
		)`,
	}

	for _, table := range tables {
		if _, err := e.ExecContext(ctx, table); err != nil {
			return fmt.Errorf("failed to create sync table: %w", err)
		}
	}
	if err := requireCurrentOutboxBundleSchema(ctx, e); err != nil {
		return err
	}
	if err := ensureConflictMetadata(ctx, e); err != nil {
		return err
	}
	if err := ensureBaselineMetadata(ctx, e); err != nil {
		return err
	}
	if err := ensurePushRejectionMetadata(ctx, e); err != nil {
		return err
	}
	if err := ensureUpgradeMetadata(ctx, e); err != nil {
		return err
	}

	for _, ddl := range stageAccountingDDL() {
		if _, err := e.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	for _, ddl := range pushIndexDDL {
		if _, err := e.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}

	if _, err := e.ExecContext(ctx, `
			INSERT INTO _sync_attachment_state (
				singleton_key, current_source_id, binding_state, attached_user_id, schema_name, last_bundle_seq_seen, rebuild_required, pending_initialization_id
			) VALUES (1, '', 'anonymous', '', '', 0, 0, '')
			ON CONFLICT(singleton_key) DO NOTHING
		`); err != nil {
		return fmt.Errorf("failed to initialize attachment state row: %w", err)
	}
	if _, err := e.ExecContext(ctx, `
			INSERT INTO _sync_operation_state (
				singleton_key, kind, target_user_id, reason, replacement_source_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
			) VALUES (1, 'none', '', '', '', '', 0, 0)
			ON CONFLICT(singleton_key) DO NOTHING
		`); err != nil {
		return fmt.Errorf("failed to initialize operation state row: %w", err)
	}
	if _, err := e.ExecContext(ctx, `
			INSERT INTO _sync_action_required (
				singleton_key, action, generation_id, generation_fingerprint, observed_at
			) VALUES (1, '', 0, '', '')
			ON CONFLICT(singleton_key) DO NOTHING
		`); err != nil {
		return fmt.Errorf("failed to initialize application action state row: %w", err)
	}
	if _, err := e.ExecContext(ctx, `
			INSERT INTO _sync_apply_state (
				singleton_key, apply_mode
			) VALUES (1, 0)
			ON CONFLICT(singleton_key) DO NOTHING
		`); err != nil {
		return fmt.Errorf("failed to initialize apply state row: %w", err)
	}
	if _, err := e.ExecContext(ctx, `
			INSERT INTO _sync_outbox_bundle (
				singleton_key, canonical_json_contract, state, source_id, source_bundle_id, canonical_request_hash,
				row_count, initialization_id
			) VALUES (1, 'jcs_uniform_numeric_strings_v1', 'none', '', 0, '', 0, '')
			ON CONFLICT(singleton_key) DO NOTHING
		`); err != nil {
		return fmt.Errorf("failed to initialize outbox bundle row: %w", err)
	}

	if _, err := e.ExecContext(ctx, `UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1 AND apply_mode = 1`); err != nil {
		return fmt.Errorf("failed to reset bundle apply mode: %w", err)
	}

	return nil
}

func requireCurrentOutboxBundleSchema(ctx context.Context, e syncMetadataExecutor) error {
	rows, err := e.QueryContext(ctx, `PRAGMA table_info(_sync_outbox_bundle)`)
	if err != nil {
		return fmt.Errorf("inspect outbox bundle columns: %w", err)
	}
	var names []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	expected := []string{"singleton_key", "canonical_json_contract", "state", "source_id", "source_bundle_id", "canonical_request_hash", "row_count", "initialization_id"}
	if stringSlicesEqual(names, expected) {
		return nil
	}
	return fmt.Errorf("unsupported sync control schema: _sync_outbox_bundle does not match the whole-request protocol layout")
}

// Start starts one client-owned uploader/downloader pair.
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return &ClientClosedError{}
	}
	if err := c.loopStartAdmissionError(); err != nil {
		return err
	}
	if c.beforeStartAdmissionHook != nil {
		c.beforeStartAdmissionHook()
	}
	c.loopMu.Lock()
	defer c.loopMu.Unlock()
	if err := c.loopStartAdmissionError(); err != nil {
		return err
	}
	if c.loopCancel != nil {
		select {
		case <-c.loopDone:
			c.loopCancel = nil
			c.loopDone = nil
		default:
			return fmt.Errorf("oversqlite background sync is already running")
		}
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.loopCancel = cancel
	c.loopDone = done
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		c.uploaderLoop(loopCtx)
	}()
	go func() {
		defer loops.Done()
		c.startDownloaderLoop(loopCtx)
	}()
	go func() {
		loops.Wait()
		close(done)
	}()
	return nil
}

func (c *Client) loopStartAdmissionError() error {
	if atomic.LoadUint32(&c.closed) == 1 || atomic.LoadUint32(&c.loopState) == clientLoopStateTerminal {
		return &ClientClosedError{}
	}
	if atomic.LoadUint32(&c.loopState) == clientLoopStateUninstalling {
		return fmt.Errorf("oversqlite sync uninstall is in progress")
	}
	return nil
}

// Stop cancels and joins package-owned sync loops.
func (c *Client) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.loopMu.Lock()
	cancel, done := c.loopCancel, c.loopDone
	if cancel == nil {
		c.loopMu.Unlock()
		return nil
	}
	cancel()
	c.loopMu.Unlock()
	select {
	case <-done:
		c.loopMu.Lock()
		if c.loopDone == done {
			c.loopCancel = nil
			c.loopDone = nil
		}
		c.loopMu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PullToStable pulls complete committed bundles until the client reaches the server's frozen stable ceiling.
func (c *Client) PullToStable(ctx context.Context) (report RemoteSyncReport, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return RemoteSyncReport{}, err
	}
	defer c.writeMu.Unlock()
	ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if err := c.ensureConnectedSessionLocked(ctx, "PullToStable()"); err != nil {
		return RemoteSyncReport{}, err
	}
	if sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx); err != nil {
		return RemoteSyncReport{}, err
	} else if sourceRecoveryErr != nil {
		return RemoteSyncReport{}, sourceRecoveryErr
	}
	if err := c.validateProtocolGateLocked(ctx, "pull_discovery"); err != nil {
		return RemoteSyncReport{}, err
	}
	return c.pullToStableLocked(ctx)
}

// Sync pushes local changes and then pulls to a stable committed-bundle ceiling.
func (c *Client) Sync(ctx context.Context) (report SyncReport, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return SyncReport{}, err
	}
	defer c.writeMu.Unlock()
	if err := c.ensureConnectedSessionLocked(ctx, "Sync()"); err != nil {
		return SyncReport{}, err
	}
	if sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx); err != nil {
		return SyncReport{}, err
	} else if sourceRecoveryErr != nil {
		return SyncReport{}, sourceRecoveryErr
	}
	if err := c.validateProtocolGateLocked(ctx, "sync_discovery"); err != nil {
		return SyncReport{}, err
	}
	ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	return c.syncLocked(ctx)
}

func (c *Client) syncLocked(ctx context.Context) (SyncReport, error) {
	invocation := invocationFromContext(ctx)
	if invocation == nil {
		invocation = &syncInvocation{}
		ctx = context.WithValue(ctx, syncInvocationKey{}, invocation)
	}
	for {
		if err := ctx.Err(); err != nil {
			return SyncReport{}, err
		}
		dirty, err := c.pendingChangeCount(ctx)
		if err != nil {
			return SyncReport{}, err
		}
		outbound, err := c.pendingPushOutboundCount(ctx)
		if err != nil {
			return SyncReport{}, err
		}
		recovering, err := c.rebuildRequired(ctx)
		if err != nil {
			return SyncReport{}, err
		}
		if dirty > 0 || outbound > 0 {
			if atomic.LoadInt32(&c.uploadPaused) == 1 {
				return SyncReport{}, c.incompleteSync(ctx, SyncIncompleteUploadsPaused)
			}
			before, err := c.pendingWorkIdentity(ctx)
			if err != nil {
				return SyncReport{}, err
			}
			var push PushReport
			if recovering {
				push, err = c.pushPendingLockedWithRecovery(ctx, 0, true)
			} else {
				push, err = c.pushPendingLocked(ctx, 0)
			}
			if err != nil {
				return SyncReport{}, err
			}
			if push.Outcome == PushOutcomeSkippedPaused {
				return SyncReport{}, c.incompleteSync(ctx, SyncIncompleteUploadsPaused)
			}
			if push.Outcome == PushOutcomeNoChange {
				remainingDirty, err := c.pendingChangeCount(ctx)
				if err != nil {
					return SyncReport{}, err
				}
				remainingOutbound, err := c.pendingPushOutboundCount(ctx)
				if err != nil {
					return SyncReport{}, err
				}
				if remainingDirty > 0 || remainingOutbound > 0 {
					after, err := c.pendingWorkIdentity(ctx)
					if err != nil {
						return SyncReport{}, err
					}
					if before == after {
						return SyncReport{}, c.incompleteSync(ctx, SyncIncompleteNoProgress)
					}
				}
			}
			continue
		}

		var remote RemoteSyncReport
		if recovering {
			var resumed bool
			remote, resumed, err = c.resumeRequiredRecoveryLocked(ctx)
			if !resumed && err == nil {
				continue
			}
		} else {
			remote, err = c.pullToStableLocked(ctx)
		}
		if err != nil {
			var recoveryBlocked *CheckpointRecoveryBlockedError
			if errors.As(err, &recoveryBlocked) && recoveryBlocked.Reason == CheckpointRecoveryBlockedPendingWork {
				stillRecovering, stateErr := c.rebuildRequired(ctx)
				if stateErr != nil {
					return SyncReport{}, stateErr
				}
				if stillRecovering {
					continue
				}
			}
			var dirtyErr *DirtyStateRejectedError
			var outboundErr *PendingPushReplayError
			if errors.As(err, &dirtyErr) || errors.As(err, &outboundErr) {
				continue
			}
			return SyncReport{}, err
		}
		if c.beforeFinalWorkReadHook != nil {
			c.beforeFinalWorkReadHook()
		}
		dirty, outbound, err = c.pendingPushWorkCounts(ctx)
		if err != nil {
			return SyncReport{}, err
		}
		if dirty > 0 || outbound > 0 {
			continue
		}
		pushOutcome := invocation.pushOutcome()
		if pushOutcome == PushOutcomeNoChange && atomic.LoadInt32(&c.uploadPaused) == 1 {
			pushOutcome = PushOutcomeSkippedPaused
		}
		return SyncReport{
			PushOutcome: pushOutcome, RemoteOutcome: remote.Outcome,
			Status: remote.Status, Restore: remote.Restore,
		}, nil
	}
}

// Rebuild rebuilds the managed tables from the current server snapshot.
func (c *Client) Rebuild(ctx context.Context) (report RemoteSyncReport, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return RemoteSyncReport{}, err
	}
	defer c.writeMu.Unlock()
	ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if err := c.requireNoPendingConflict(ctx, c.db); err != nil {
		return RemoteSyncReport{}, err
	}
	if err := c.ensureConnectedSessionLocked(ctx, "Rebuild()"); err != nil {
		return RemoteSyncReport{}, err
	}
	if err := c.validateProtocolGateLocked(ctx, "rebuild_discovery"); err != nil {
		return RemoteSyncReport{}, err
	}
	sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if sourceRecoveryErr != nil {
		return c.rebuildSourceRecoveryLocked(ctx)
	}
	return c.rebuildKeepSourceLocked(ctx)
}

// SerializeRow loads a row by (table, pk) and serializes it to JSON payload for upload
// This follows the KMP approach for handling BLOB data
func (c *Client) SerializeRow(ctx context.Context, table, pk string) (json.RawMessage, error) {
	tableLc := strings.ToLower(table)

	// Get table information to determine primary key column and BLOB columns
	tableInfo, err := c.getTableInfo(tableLc)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	// Determine primary key column
	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return nil, err
	}

	// Convert PK value for query (handles BLOB primary keys)
	pkValue, err := c.convertPKForQuery(table, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	// Build query with correct primary key column
	query := fmt.Sprintf("SELECT %s FROM %s AS captured WHERE %s = ?", buildJsonObjectExprHexAware(tableInfo, "captured"), quoteIdent(tableLc), quoteIdent(pkColumn))
	var payload string
	if err := scanTransactionRow(ctx, c.db, query, []any{pkValue}, &payload); err != nil {
		return nil, err
	}
	return json.RawMessage(payload), nil
}

func (c *Client) serializeRowInTx(ctx context.Context, tx sqliteTransaction, table, pk string) (json.RawMessage, error) {
	tableLc := strings.ToLower(table)

	tableInfo, err := c.getTableInfoTx(tx, tableLc)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return nil, err
	}

	pkValue, err := c.convertPKForQueryInTx(tx, table, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	query := fmt.Sprintf("SELECT %s FROM %s AS captured WHERE %s = ?", buildJsonObjectExprHexAware(tableInfo, "captured"), quoteIdent(tableLc), quoteIdent(pkColumn))
	var payload string
	if err := scanTransactionRow(ctx, tx, query, []any{pkValue}, &payload); err != nil {
		return nil, err
	}
	return json.RawMessage(payload), nil
}
