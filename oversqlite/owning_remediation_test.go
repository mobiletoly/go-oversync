package oversqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// Reuse the complete typed-column witness, adding opening/staging owners rather
// than replacing their payloads, identities or charges with row-count checks.
func owningRemediationState(t *testing.T, db *sql.DB, business ...string) qualificationVector {
	t.Helper()
	result := qualificationState(t, db, business[0])
	tables := append(append([]string{}, business...), "_sync_snapshot_stage", "_sync_push_stage",
		"_sync_upgrade_identity", "_sync_upgrade_journal", "_sync_apply_state", "_sync_contract_identity",
		"_sync_admitted_contracts")
	for _, table := range tables {
		result[table] = qualificationState(t, db, table)[table]
	}
	return result
}

func owningRemediationPrimaryState(t *testing.T, db *sql.DB, business ...string) qualificationVector {
	t.Helper()
	result := owningRemediationState(t, db, business...)
	for _, row := range result["_sync_operation_state"] {
		for _, column := range []string{"workspace_context", "reserved_entries", "reserved_bytes", "snapshot_stage_entries", "snapshot_stage_bytes", "snapshot_cleanup_ordinal", "push_stage_entries", "push_stage_bytes", "push_cleanup_ordinal"} {
			delete(row, column)
		}
	}
	return result
}

func TestOpening_RetainedLocalOnlyGuardsUseCanonicalOwner(t *testing.T) {
	for _, scenario := range []string{"canonical", "modified-body", "renamed", "case-forgery", "wrong-table", "retained-table"} {
		t.Run(scenario, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "retained-guards.sqlite")
			plan := prepareU8ProcessTestDatabase(t, file)
			db, err := sql.Open("sqlite3", file)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			guard := localOnlyUpgradeGuards([]string{"local_notes"})[0]
			if scenario != "canonical" {
				_, err = db.Exec("DROP TRIGGER " + quoteIdent(guard.Name))
				require.NoError(t, err)
				body := guard.SQL
				switch scenario {
				case "modified-body":
					body = "CREATE TRIGGER " + quoteIdent(guard.Name) + " BEFORE INSERT ON local_notes BEGIN SELECT 1; END"
				case "renamed":
					body = strings.Replace(body, quoteIdent(guard.Name), quoteIdent("_sync_upgrade_guard_forged_insert"), 1)
				case "case-forgery":
					body = "CREATE TRIGGER " + quoteIdent(strings.ToUpper(guard.Name)) + " BEFORE INSERT ON local_notes BEGIN SELECT 1; END"
				case "wrong-table":
					body = strings.Replace(body, "ON "+quoteIdent("local_notes"), "ON "+quoteIdent("people"), 1)
				case "retained-table":
					body = localOnlyUpgradeGuards([]string{"_sync_operation_state"})[0].SQL
				}
				_, err = db.Exec(body)
				require.NoError(t, err)
			}
			before := owningRemediationState(t, db, "people", "local_notes")
			config := publicOpeningConfigForPlan(file, plan)
			var callbacks atomic.Int64
			config.Progress = func(OpeningStatus) { callbacks.Add(1) }
			opening, err := NewOpening(config)
			if scenario == "canonical" {
				require.NoError(t, err)
				require.NoError(t, opening.Close())
			} else {
				require.ErrorContains(t, err, "local-only upgrade guard")
				require.Nil(t, opening)
			}
			require.Zero(t, callbacks.Load())
			require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
			// A second attempt must reach the same admission result, never Busy:
			// failed construction and normal Close both release the lifetime lease.
			reopened, retryErr := NewOpening(config)
			if scenario == "canonical" {
				require.NoError(t, retryErr)
				require.NoError(t, reopened.Close())
			} else {
				require.ErrorContains(t, retryErr, "local-only upgrade guard")
				require.Nil(t, reopened)
			}
		})
	}
}

func TestSnapshotApply_RealCommitFailurePreservesCompleteState(t *testing.T) {
	// A connection-local SQLite commit hook rejects a real COMMIT after all
	// valid snapshot rows and metadata have been written. No production hook.
	var rejectCommit atomic.Bool
	var rejectedCommits atomic.Int64
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "commit-failure.sqlite"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(usersTestDDL)
	require.NoError(t, err)
	config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	client, err := newAdmittedFixtureClient(db, "http://commit.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	attachTestClient(t, client, "bundle-user", "bundle-device")
	_, err = db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com'); DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	ctx := context.Background()
	setCurrentSourceBundleState(t, db, 1, 4)
	require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
	options := snapshotApplyOptions{}
	options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)
	session := stageUserSnapshotForGate(t, client, "real-commit-failure", "remote", 9, mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
	before := owningRemediationState(t, db, "users")
	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NoError(t, connection.Raw(func(driverConnection any) error {
		driverConnection.(*sqlite3.SQLiteConn).RegisterCommitHook(func() int {
			if rejectCommit.Load() {
				rejectedCommits.Add(1)
				return 1
			}
			return 0
		})
		return nil
	}))
	require.NoError(t, connection.Close())
	rejectCommit.Store(true)
	err = client.applyStagedSnapshotLocked(ctx, session, options)
	rejectCommit.Store(false)
	require.ErrorContains(t, err, "failed to commit staged snapshot apply")
	var sqliteErr sqlite3.Error
	require.ErrorAs(t, err, &sqliteErr)
	require.Equal(t, sqlite3.ErrConstraintCommitHook, sqliteErr.ExtendedCode)
	require.EqualValues(t, 1, rejectedCommits.Load())
	require.Equal(t, before, owningRemediationState(t, db, "users"))
	require.NoError(t, client.applyStagedSnapshotLocked(ctx, session, options))
	requireUserCount(t, db, "old", 0)
	requireUserCount(t, db, "remote", 1)
	requireLastBundleSeqSeen(t, client, ctx, 9)
	require.Zero(t, snapshotStageCount(t, db))
}

func TestSnapshotApply_SourceAbsenceRequiresConfirmedUpgrade(t *testing.T) {
	for _, scenario := range []string{"confirmed", "missing-journal", "wrong-phase", "corrupt-journal", "wrong-source", "wrong-target", "stray-source"} {
		t.Run(scenario, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "source-absence.sqlite")
			plan := prepareU8ProcessTestDatabase(t, file)
			db, err := sql.Open("sqlite3", file)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			opening, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, opening.Close()) })
			client, err := opening.newPhysicalClient(opening.targetContract, opening.config.TargetClientConfig)
			require.NoError(t, err)
			client.sourceID = "source-new"
			client.UserID = plan.UserID
			_, err = db.Exec(`DELETE FROM _sync_source_state`)
			require.NoError(t, err)
			switch scenario {
			case "missing-journal":
				_, err = db.Exec(`DELETE FROM _sync_upgrade_journal`)
			case "wrong-phase", "wrong-target":
				record, loadErr := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, loadErr)
				if scenario == "wrong-phase" {
					record.Phase = UpgradePhaseReattachPending
					record.ReplacementConfirmed = false
					record.SnapshotID = ""
					record.SnapshotBundleSeq = 0
					record.SnapshotRowCount = 0
					record.SnapshotByteCount = 0
					record.SnapshotRowsStaged = 0
					record.SnapshotBytesStaged = 0
					_, err = db.Exec(`DELETE FROM _sync_snapshot_stage`)
					require.NoError(t, err)
				} else {
					record.TargetGenerationID++
				}
				err = persistUpgradeJournal(context.Background(), db, record)
			case "corrupt-journal":
				_, err = db.Exec(`UPDATE _sync_upgrade_journal SET integrity_digest='corrupt'`)
			case "wrong-source":
				client.sourceID = "another-target-source"
			case "stray-source":
				_, err = db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('stray',1)`)
			}
			require.NoError(t, err)
			before := owningRemediationState(t, db, "people", "local_notes")
			tx, err := beginImmediateTx(context.Background(), db)
			require.NoError(t, err)
			unit := applyUnit("snapshot-source-absence", 7, "")
			guard, guardErr := client.applyGuard(context.Background(), tx, unit)
			if scenario == "confirmed" {
				require.NoError(t, guardErr)
				require.Len(t, guard, 64)
				_, err = tx.ExecContext(context.Background(), `INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('source-old',1)`)
				require.NoError(t, err)
				presentGuard, err := client.applyGuard(context.Background(), tx, unit)
				require.NoError(t, err)
				require.NotEqual(t, guard, presentGuard, "absence is pinned, never invented as source ID 1")
			} else {
				require.Error(t, guardErr)
				require.Empty(t, guard)
			}
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		})
	}
}

func TestSnapshotApply_MissingOrdinarySourceFailsClosed(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('local','Keep','keep@example.com'); DELETE FROM _sync_source_state`)
	require.NoError(t, err)
	before := owningRemediationState(t, db, "users")
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	guard, err := client.applyGuard(context.Background(), tx, applyUnit("ordinary-source-absence", 1, ""))
	require.ErrorContains(t, err, "source counters are missing outside a confirmed generation replacement")
	require.Empty(t, guard)
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, owningRemediationState(t, db, "users"))
}

type upgradeProofReadProbe struct {
	sqliteTransaction
	journalReads int
	guardReads   int
}

func (p *upgradeProofReadProbe) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	normalized := strings.Join(strings.Fields(query), " ")
	if strings.HasPrefix(normalized, "SELECT format_version, mode, phase, revision, plan_id, plan_digest,") {
		p.journalReads++
	}
	if strings.HasPrefix(normalized, "SELECT i.database_id,i.service_id,i.generation_id,i.local_version,i.physical_shape,i.fingerprint,") {
		p.guardReads++
	}
	return p.sqliteTransaction.QueryRowContext(ctx, query, args...)
}

func newUpgradeProofWitness(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "proof-capacity.sqlite")
	plan := prepareU8ProcessTestDatabase(t, file)
	db, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	opening, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, opening.Close()) })
	client, err := opening.newPhysicalClient(opening.targetContract, opening.config.TargetClientConfig)
	require.NoError(t, err)
	client.sourceID = "source-new"
	client.UserID = plan.UserID
	_, err = db.Exec(`DELETE FROM _sync_source_state`)
	require.NoError(t, err)
	return client, db
}

func TestSnapshotApply_UpgradeProofOversizedJournalRefusesBeforeRead(t *testing.T) {
	client, db := newUpgradeProofWitness(t)
	_, err := db.Exec(`UPDATE _sync_upgrade_journal SET failure_detail=?`, strings.Repeat("untrusted", 256<<10))
	require.NoError(t, err)
	before := owningRemediationState(t, db, "people", "local_notes")
	client.config.MaxSyncWorkspaceBytes = 64 << 10
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	probe := &upgradeProofReadProbe{sqliteTransaction: tx}
	unit := applyUnit("oversized-upgrade-proof", 7, "")
	guard, err := client.applyGuard(context.Background(), probe, unit)
	var capacity *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "bytes", capacity.Dimension)
	require.Equal(t, unit, capacity.Unit)
	require.Greater(t, capacity.Reserved, client.config.MaxSyncWorkspaceBytes)
	require.Empty(t, guard)
	require.Zero(t, probe.journalReads, "invalid digest must not allocate oversized journal before refusing capacity")
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
}

func TestSnapshotApply_UpgradeProofCapacityBoundariesPreserveOwners(t *testing.T) {
	client, db := newUpgradeProofWitness(t)
	ctx := context.Background()
	journal, err := loadUpgradeJournal(ctx, db)
	require.NoError(t, err)
	journal.FailureDetail = strings.Repeat("\"\\\t\x01é", 256)
	require.NoError(t, persistUpgradeJournal(ctx, db, journal))
	unit := applyUnit("upgrade-proof-capacity", 7, "")
	// A separate live copy and the retained snapshot stage must stay charged.
	token, err := client.reserveWorkspace(ctx, applyUnit("other-live-copy", 1, ""), workspaceCharge{3, 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.releaseWorkspace(ctx, token)) })
	before := owningRemediationState(t, db, "people", "local_notes")
	attempt := func() (string, error, int, workspaceCharge) {
		tx, beginErr := beginImmediateTx(ctx, db)
		require.NoError(t, beginErr)
		probe := &upgradeProofReadProbe{sqliteTransaction: tx}
		used, usageErr := client.workspaceUsage(ctx, tx, unit, nil, nil)
		require.NoError(t, usageErr)
		guard, applyErr := client.applyGuard(ctx, probe, unit)
		require.NoError(t, tx.Rollback())
		require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		return guard, applyErr, probe.journalReads, used
	}
	client.config.MaxSyncWorkspaceBytes = 64 << 10
	guard, err, reads, used := attempt()
	var capacity *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "bytes", capacity.Dimension)
	require.Equal(t, used.bytes, capacity.Used)
	require.Greater(t, used.bytes, int64(4096), "stage and live ledger owners are counted")
	require.Zero(t, reads)
	require.Empty(t, guard)
	byteBoundary := capacity.Used + capacity.Reserved
	client.config.MaxSyncWorkspaceBytes = byteBoundary - 1
	_, err, reads, _ = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, byteBoundary, capacity.Used+capacity.Reserved)
	require.Zero(t, reads)
	client.config.MaxSyncWorkspaceBytes = byteBoundary
	guard, err, reads, _ = attempt()
	require.NoError(t, err)
	require.Len(t, guard, 64)
	require.Equal(t, 1, reads, "valid integrity proof executes at the exact capacity boundary")
	// Both dimensions now fail in proof admission: entries has precedence.
	client.config.MaxSyncWorkspaceEntries = 100
	client.config.MaxSyncWorkspaceBytes = byteBoundary - 1
	_, err, reads, used = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "entries", capacity.Dimension)
	require.Equal(t, used.entries, capacity.Used)
	require.Zero(t, reads)
	entryBoundary := capacity.Used + capacity.Reserved
	client.config.MaxSyncWorkspaceEntries = entryBoundary - 1
	client.config.MaxSyncWorkspaceBytes = byteBoundary
	_, err, reads, _ = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "entries", capacity.Dimension)
	require.Zero(t, reads)
	client.config.MaxSyncWorkspaceEntries = entryBoundary
	guard, err, reads, _ = attempt()
	require.NoError(t, err)
	require.Len(t, guard, 64)
	require.Equal(t, 1, reads)
	require.NoError(t, client.releaseWorkspace(ctx, token))
	var reservations int64
	require.NoError(t, db.QueryRow(`SELECT reserved_entries+reserved_bytes FROM _sync_operation_state`).Scan(&reservations))
	require.Zero(t, reservations, "the proof retains no reservation or persistence owner")
}

func TestSnapshotApply_UpgradeProofOtherJournalFieldsRefuseBeforeRead(t *testing.T) {
	for _, column := range []string{"plan_id", "database_id", "old_generation_fingerprint", "snapshot_id", "failure_code", "pending_request_hash", "resolution_payload", "integrity_digest"} {
		t.Run(column, func(t *testing.T) {
			client, db := newUpgradeProofWitness(t)
			_, err := db.Exec(`UPDATE _sync_upgrade_journal SET `+quoteIdent(column)+`=?`, strings.Repeat("é\x00\"", 16<<10))
			require.NoError(t, err)
			before := owningRemediationState(t, db, "people", "local_notes")
			client.config.MaxSyncWorkspaceBytes = 64 << 10
			tx, err := beginImmediateTx(context.Background(), db)
			require.NoError(t, err)
			probe := &upgradeProofReadProbe{sqliteTransaction: tx}
			guard, err := client.applyGuard(context.Background(), probe, applyUnit("journal-field-capacity", 7, ""))
			var capacity *SyncWorkspaceLimitError
			require.ErrorAs(t, err, &capacity)
			require.Equal(t, "bytes", capacity.Dimension)
			require.Zero(t, probe.journalReads)
			require.Empty(t, guard)
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		})
	}
}

func TestSnapshotApply_GuardMetadataRefusesBeforeRead(t *testing.T) {
	for _, witness := range []struct {
		name, table, column string
		withSource          bool
	}{
		{"last_bundle_seq_seen", "_sync_attachment_state", "last_bundle_seq_seen", false},
		{"generation_id", "_sync_contract_identity", "generation_id", false},
		{"local_version", "_sync_contract_identity", "local_version", false},
		{"ordinary_last_bundle_seq_seen", "_sync_attachment_state", "last_bundle_seq_seen", true},
		{"ordinary_next_source_bundle_id", "_sync_source_state", "next_source_bundle_id", true},
	} {
		t.Run(witness.name, func(t *testing.T) {
			client, db := newUpgradeProofWitness(t)
			if witness.withSource {
				_, err := db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('source-old',1)`)
				require.NoError(t, err)
			}
			// SQLite INTEGER affinity accepts nonnumeric TEXT. Include byte-length
			// traps as well as the review's 2 MiB counterexample.
			_, err := db.Exec("UPDATE "+quoteIdent(witness.table)+" SET "+quoteIdent(witness.column)+"=?", strings.Repeat("é\x00\"\\", 512<<10))
			require.NoError(t, err)
			before := owningRemediationState(t, db, "people", "local_notes")
			client.config.MaxSyncWorkspaceBytes = 64 << 10
			tx, err := beginImmediateTx(context.Background(), db)
			require.NoError(t, err)
			probe := &upgradeProofReadProbe{sqliteTransaction: tx}
			unit := applyUnit("guard-metadata-capacity", 7, "")
			guard, err := client.applyGuard(context.Background(), probe, unit)
			var capacity *SyncWorkspaceLimitError
			require.ErrorAs(t, err, &capacity)
			require.Equal(t, "bytes", capacity.Dimension)
			require.Equal(t, unit, capacity.Unit)
			require.Greater(t, capacity.Reserved, client.config.MaxSyncWorkspaceBytes)
			require.Empty(t, guard)
			require.Zero(t, probe.guardReads, "capacity refusal must precede the first guard vector read")
			require.Zero(t, probe.journalReads, "journal validation is reached only after guard admission")
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		})
	}
}

func TestSnapshotApply_GuardMetadataCapacityBoundariesPreserveOwners(t *testing.T) {
	client, db := newUpgradeProofWitness(t)
	_, err := db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('source-old',1)`)
	require.NoError(t, err)
	ctx := context.Background()
	token, err := client.reserveWorkspace(ctx, applyUnit("other-guard-copy", 1, ""), workspaceCharge{3, 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.releaseWorkspace(ctx, token)) })
	before := owningRemediationState(t, db, "people", "local_notes")
	unit := applyUnit("guard-capacity-boundary", 7, "")
	attempt := func() (string, error, int, workspaceCharge) {
		tx, beginErr := beginImmediateTx(ctx, db)
		require.NoError(t, beginErr)
		used, usageErr := client.workspaceUsage(ctx, tx, unit, nil, nil)
		require.NoError(t, usageErr)
		probe := &upgradeProofReadProbe{sqliteTransaction: tx}
		guard, applyErr := client.applyGuard(ctx, probe, unit)
		require.Zero(t, probe.journalReads, "ordinary source state needs no replacement proof")
		require.NoError(t, tx.Rollback())
		require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		return guard, applyErr, probe.guardReads, used
	}
	client.config.MaxSyncWorkspaceBytes = 8192
	guard, err, reads, used := attempt()
	var capacity *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "bytes", capacity.Dimension)
	require.Equal(t, unit, capacity.Unit)
	require.Equal(t, used.bytes, capacity.Used)
	require.Greater(t, used.bytes, int64(4096), "retained staging and the live owner coexist")
	require.Empty(t, guard)
	require.Zero(t, reads)
	byteBoundary := capacity.Used + capacity.Reserved
	client.config.MaxSyncWorkspaceBytes = byteBoundary - 1
	_, err, reads, _ = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, byteBoundary, capacity.Used+capacity.Reserved)
	require.Zero(t, reads)
	client.config.MaxSyncWorkspaceBytes = byteBoundary
	guard, err, reads, _ = attempt()
	require.NoError(t, err)
	require.Len(t, guard, 64)
	require.Equal(t, 1, reads)
	repeated, err, reads, _ := attempt()
	require.NoError(t, err)
	require.Equal(t, guard, repeated, "unchanged guarded input has stable identity")
	require.Equal(t, 1, reads)
	client.config.MaxSyncWorkspaceEntries = used.entries + 1
	client.config.MaxSyncWorkspaceBytes = byteBoundary - 1
	_, err, reads, used = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "entries", capacity.Dimension, "entries precedes bytes when both fail")
	require.Equal(t, used.entries, capacity.Used)
	require.Zero(t, reads)
	entryBoundary := capacity.Used + capacity.Reserved
	client.config.MaxSyncWorkspaceEntries = entryBoundary - 1
	client.config.MaxSyncWorkspaceBytes = byteBoundary
	_, err, reads, _ = attempt()
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "entries", capacity.Dimension)
	require.Zero(t, reads)
	client.config.MaxSyncWorkspaceEntries = entryBoundary
	guard, err, reads, _ = attempt()
	require.NoError(t, err)
	require.Len(t, guard, 64)
	require.Equal(t, 1, reads)
	require.NoError(t, client.releaseWorkspace(ctx, token))
	var reservations int64
	require.NoError(t, db.QueryRow(`SELECT reserved_entries+reserved_bytes FROM _sync_operation_state`).Scan(&reservations))
	require.Zero(t, reservations, "guard validation leaves no new reservation owner")
}

func TestSnapshotApply_GuardMetadataCancellationPreservesOwners(t *testing.T) {
	for _, scenario := range []string{"canceled", "valid"} {
		t.Run(scenario, func(t *testing.T) {
			client, db := newUpgradeProofWitness(t)
			_, err := db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('source-old',1)`)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			before := owningRemediationState(t, db, "people", "local_notes")
			tx, err := beginImmediateTx(context.Background(), db)
			require.NoError(t, err)
			probe := &upgradeProofReadProbe{sqliteTransaction: tx}
			guard, err := client.applyGuard(ctx, probe, applyUnit("guard-cancellation", 7, ""))
			if scenario == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, guard)
				require.Zero(t, probe.guardReads)
			} else {
				require.NoError(t, err)
				require.Len(t, guard, 64)
				require.Equal(t, 1, probe.guardReads)
			}
			require.Zero(t, probe.journalReads)
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
		})
	}
}
