package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const (
	upgradeProcessHelperEnv   = "OVERSQLITE_UPGRADE_PROCESS_HELPER"
	upgradeProcessDBEnv       = "OVERSQLITE_UPGRADE_PROCESS_DB"
	upgradeProcessBoundaryEnv = "OVERSQLITE_UPGRADE_PROCESS_BOUNDARY"
	upgradeProcessCaseEnv     = "OVERSQLITE_UPGRADE_PROCESS_CASE"
	upgradeProcessEvidenceEnv = "OVERSQLITE_UPGRADE_PROCESS_EVIDENCE"
)

func processTestContract(t *testing.T) LocalGenerationContract {
	return testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
}

func processTestGenerationPlan(t *testing.T) UpgradePlan {
	oldContract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	targetContract := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	return UpgradePlan{
		PlanID: "process-generation-v3", Mode: UpgradeModeGeneration,
		DatabaseID: "process-db", ServiceID: "service", UserID: "user-1",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: oldContract.Proof.ID, TargetGenerationID: targetContract.Proof.ID, testInstalled: oldContract, testTarget: targetContract,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `PRAGMA user_version=2`)
			return err
		}},
	}
}

func processTestLocalPlan(t *testing.T) UpgradePlan {
	contract := processTestContract(t)
	return UpgradePlan{
		PlanID: "process-local-v2", Mode: UpgradeModeLocalOnly,
		DatabaseID: "process-db", ServiceID: "service", UserID: "user-1",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `UPDATE local_notes SET note='version two' WHERE id='sentinel'; PRAGMA user_version=2;`)
			return err
		}},
	}
}

func processTestFreshPlan(t *testing.T) UpgradePlan {
	contract := processTestContract(t)
	return UpgradePlan{
		PlanID: "process-fresh-bootstrap-v1", Mode: UpgradeModeFresh,
		DatabaseID: "process-fresh-db", ServiceID: "service", TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel', 'fresh');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}
}

func prepareProcessTestDatabase(t *testing.T, path string) {
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	contract := processTestContract(t)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "process-fresh-v1", Mode: UpgradeModeFresh,
		DatabaseID: "process-db", ServiceID: "service", TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel', 'version one');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	require.NoError(t, db.Close())
}

func TestUpgradeProcessHelper(t *testing.T) {
	if os.Getenv(upgradeProcessHelperEnv) != "1" {
		return
	}
	path := os.Getenv(upgradeProcessDBEnv)
	boundary := os.Getenv(upgradeProcessBoundaryEnv)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	config := &openingRuntimeConfig{Plan: processTestLocalPlan(t)}
	if os.Getenv(upgradeProcessCaseEnv) == "FRESH_BOOTSTRAP" {
		config = &openingRuntimeConfig{Plan: processTestFreshPlan(t)}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "U8" {
		plan := processTestGenerationPlan(t)
		config = &openingRuntimeConfig{
			Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
			HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
					return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
				}
				return nil, fmt.Errorf("unexpected process request %s %s", request.Method, request.URL.Path)
			})},
		}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "U6" {
		plan := processTestGenerationPlan(t)
		config = &openingRuntimeConfig{
			Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
			HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
					return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
				case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
					body, err := io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(os.Getenv(upgradeProcessEvidenceEnv), body, 0o600); err != nil {
						t.Fatal(err)
					}
					os.Exit(94)
				}
				return nil, fmt.Errorf("unexpected process request %s %s", request.Method, request.URL.Path)
			})},
		}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "GENERATION_BOUNDARY" {
		plan := processTestGenerationPlan(t)
		row := processSnapshotRow()
		config = &openingRuntimeConfig{
			Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
			HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
					if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
						return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
					}
					return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
				case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
					return jsonResponse(oversync.PullResponse{StableBundleSeq: 0}), nil
				case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
					return jsonResponse(oversync.SnapshotSession{
						SnapshotID: "boundary-snapshot", SnapshotBundleSeq: 7,
						RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z",
					}), nil
				case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/process-snapshot":
					return jsonResponse(oversync.SnapshotChunkResponse{
						SnapshotID: "process-snapshot", SnapshotBundleSeq: 7,
						Rows: []oversync.SnapshotRow{row}, NextRowOrdinal: 1,
					}), nil
				case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/sync/snapshot-sessions/"):
					return jsonResponse(map[string]string{"status": "deleted"}), nil
				default:
					return nil, fmt.Errorf("unexpected generation boundary request %s %s", request.Method, request.URL.Path)
				}
			})},
		}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "PORTABLE_U4" {
		config = portableGenerationU4Config(t, db)
	}
	if os.Getenv(upgradeProcessCaseEnv) == "RESOLUTION" {
		plan := processTestGenerationPlan(t)
		config = &openingRuntimeConfig{
			Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
			HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("resolution process crossed its committed-decision boundary: %s %s", request.Method, request.URL.Path)
			})},
		}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "PUSH" {
		plan := processTestGenerationPlan(t)
		config = &openingRuntimeConfig{
			Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
			HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
					if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
						return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
					}
					return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
				case request.Method == http.MethodPost && request.URL.Path == "/sync/push":
					var push oversync.CompactPushRequest
					if err := json.NewDecoder(request.Body).Decode(&push); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(os.Getenv(upgradeProcessEvidenceEnv), []byte(push.CanonicalRequestHash), 0o600); err != nil {
						t.Fatal(err)
					}
					os.Exit(95)
				}
				return nil, fmt.Errorf("unexpected process request %s %s", request.Method, request.URL.Path)
			})},
		}
	}
	config.testBoundary = func(reached string) {
		if reached == boundary {
			os.Exit(93)
		}
	}
	coordinator, err := newTestOpening(db, config)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(upgradeProcessCaseEnv) == "LOCK" {
		if err := os.WriteFile(os.Getenv(upgradeProcessEvidenceEnv), []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	if os.Getenv(upgradeProcessCaseEnv) == "RESOLUTION" {
		rec, err := loadUpgradeJournal(context.Background(), db)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.ResolvePendingIntent(context.Background(), PendingIntentResolution{
			Action: PendingIntentDiscard, PlanID: rec.PlanID, Revision: rec.Revision, RequestHash: rec.PendingRequestHash,
		}); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	if _, err := coordinator.Advance(context.Background()); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func prepareU8ProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := processTestGenerationPlan(t)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "u8-process-fresh-target", Mode: UpgradeModeFresh,
		DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID,
		TargetVersion: plan.TargetVersion, TargetGenerationID: plan.testTarget.Proof.ID, testTarget: plan.testTarget,
		LocalOnlyTables: append([]string(nil), plan.LocalOnlyTables...),
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel', 'preserved');
				PRAGMA user_version=2;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	retainTestDiscoveredGenerationDescriptor(t, db, plan.testInstalled)
	guardTx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	require.NoError(t, installLocalOnlyUpgradeGuards(context.Background(), guardTx, plan.LocalOnlyTables))
	require.NoError(t, guardTx.Commit(context.Background()))
	_, err = db.Exec(`UPDATE _sync_attachment_state SET current_source_id='source-old', binding_state='attached', attached_user_id='user-1', schema_name='main', rebuild_required=1 WHERE singleton_key=1`)
	require.NoError(t, err)
	row := processSnapshotRow()
	payload := string(row.Payload)
	keyJSON := `{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}`
	byteCount := testSnapshotRowsByteCount([]oversync.SnapshotRow{row})
	_, err = db.Exec(`INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES ('process-snapshot',1,'main','people',?,7,?)`, keyJSON, payload)
	require.NoError(t, err)
	digest, err := digestUpgradePlan(
		plan,
		&plan.testInstalled.Proof,
		&plan.testTarget.Proof,
		plan.testInstalled.Tables,
		plan.testTarget.Tables,
	)
	require.NoError(t, err)
	preservation, err := localOnlyDigestWithUpgradeGuards(context.Background(), db, plan.LocalOnlyTables)
	require.NoError(t, err)
	require.NoError(t, persistUpgradeIdentity(context.Background(), db, upgradeIdentityRecord{
		DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, UserID: plan.UserID,
		GenerationID: int64(plan.testInstalled.Proof.ID), GenerationFingerprint: plan.testInstalled.Proof.Fingerprint,
		LocalVersion: plan.TargetVersion, Ready: false,
	}))
	require.NoError(t, persistUpgradeJournal(context.Background(), db, &upgradeJournalRecord{
		Mode: plan.Mode, Phase: UpgradePhaseRebuildRunning, Revision: 7,
		PlanID: plan.PlanID, PlanDigest: digest, DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, UserID: plan.UserID,
		OldGenerationID: int64(plan.testInstalled.Proof.ID), OldGenerationFingerprint: plan.testInstalled.Proof.Fingerprint,
		OldVersion: plan.InstalledVersion, TargetGenerationID: int64(plan.testTarget.Proof.ID), TargetGenerationFingerprint: plan.testTarget.Proof.Fingerprint,
		TargetVersion: plan.TargetVersion, OldSourceID: "source-old", NewSourceID: "source-new", ReplacementReason: "schema_generation_upgrade",
		DrainComplete: true, MigrationComplete: true, ValidatedTargetFingerprint: plan.testTarget.Proof.Fingerprint,
		LocalOnlyDigest: preservation, ReplacementConfirmed: true, SnapshotID: "process-snapshot",
		SnapshotBundleSeq: 7, SnapshotRowCount: 1, SnapshotByteCount: byteCount,
		SnapshotRowsStaged: 1, SnapshotBytesStaged: byteCount,
	}))
	require.NoError(t, db.Close())
	return plan
}

func processSnapshotRow() oversync.SnapshotRow {
	return oversync.SnapshotRow{
		Schema: "main", Table: "people",
		Key:        oversync.SyncKey{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		RowVersion: 7,
		Payload:    []byte(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"after restart"}`),
	}
}

func prepareGenerationReadyProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	prepareProcessTestDatabase(t, path)
	plan := processTestGenerationPlan(t)
	return plan
}

func prepareGenerationBoundaryPhase(t *testing.T, path string, phase UpgradePhase) UpgradePlan {
	plan := prepareGenerationReadyProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			}
			return nil, fmt.Errorf("unexpected preparation request %s %s", request.Method, request.URL.Path)
		})},
	})
	require.NoError(t, err)
	rec, err := coordinator.loadOrStart(context.Background())
	require.NoError(t, err)
	if phase == UpgradePhaseResolvingOldIntent {
		require.NoError(t, coordinator.transitionPhase(context.Background(), rec, UpgradePhaseResolvingOldIntent))
	} else {
		require.Equal(t, UpgradePhaseStarted, phase)
	}
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
	return plan
}

func prepareGenerationDrainedProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := prepareGenerationBoundaryPhase(t, path, UpgradePhaseResolvingOldIntent)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	rec.DrainComplete = true
	rec.Outcome = ""
	rec.FailureCode = ""
	rec.FailureDetail = ""
	rec.PendingRequestHash = ""
	rec.ResolutionAction = ""
	rec.ResolutionRequestHash = ""
	rec.ResolutionPayload = ""
	rec.Revision++
	require.NoError(t, persistUpgradeJournal(context.Background(), db, rec))
	require.NoError(t, db.Close())
	return plan
}

func prepareLocalMigratedProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	rec.Phase = UpgradePhaseLocalMigrated
	rec.NewSourceID = ""
	rec.ReplacementConfirmed = false
	rec.SnapshotID = ""
	rec.SnapshotBundleSeq = 0
	rec.SnapshotRowCount = 0
	rec.SnapshotByteCount = 0
	rec.SnapshotRowsStaged = 0
	rec.SnapshotBytesStaged = 0
	require.NoError(t, persistUpgradeJournal(context.Background(), db, rec))
	_, err = db.Exec(`DELETE FROM _sync_snapshot_stage`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return plan
}

func clearUpgradeSnapshotStageForTest(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	defer tx.Rollback()
	rec, err := loadUpgradeJournal(context.Background(), tx)
	require.NoError(t, err)
	require.NotNil(t, rec)
	_, err = tx.ExecContext(context.Background(), `DELETE FROM _sync_snapshot_stage`)
	require.NoError(t, err)
	rec.SnapshotRowsStaged = 0
	rec.SnapshotBytesStaged = 0
	require.NoError(t, persistUpgradeJournal(context.Background(), tx, rec))
	require.NoError(t, tx.Commit(context.Background()))
}

func prepareU7ProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	clearUpgradeSnapshotStageForTest(t, db)
	require.NoError(t, db.Close())
	return plan
}

func prepareU6ProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	rec.Phase = UpgradePhaseReattachPending
	rec.ReplacementConfirmed = false
	rec.SnapshotID = ""
	rec.SnapshotBundleSeq = 0
	rec.SnapshotRowCount = 0
	rec.SnapshotByteCount = 0
	rec.SnapshotRowsStaged = 0
	rec.SnapshotBytesStaged = 0
	require.NoError(t, persistUpgradeJournal(context.Background(), db, rec))
	_, err = db.Exec(`DELETE FROM _sync_snapshot_stage`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return plan
}

func preparePushProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := processTestGenerationPlan(t)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	contract := plan.testInstalled
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "push-process-fresh", Mode: UpgradeModeFresh,
		DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		LocalOnlyTables: plan.LocalOnlyTables,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel', 'preserved');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	_, err = db.Exec(`INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8','offline intent')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return plan
}

func prepareNeedsResolutionProcessTestDatabase(t *testing.T, path string) UpgradePlan {
	plan := preparePushProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
					return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
				}
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/push":
				return errorJSONResponse(http.StatusUnprocessableEntity, oversync.ErrorResponse{Error: "payload_invalid", Message: "application resolution required"}), nil
			default:
				return nil, fmt.Errorf("unexpected resolution preparation request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeNeedsResolution, result.Status.Outcome)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
	return plan
}
func TestOpening_I10FreshProcessRecovery(t *testing.T) {
	for _, boundary := range []string{"before_U4_commit", "after_U4_commit"} {
		t.Run(boundary, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "same-file.sqlite")
			prepareProcessTestDatabase(t, path)
			command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
			command.Env = append(os.Environ(),
				upgradeProcessHelperEnv+"=1",
				upgradeProcessDBEnv+"="+path,
				upgradeProcessBoundaryEnv+"="+boundary,
			)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			t.Log(string(output))
			require.Equal(t, 93, exit.ExitCode())

			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			rec, err := loadUpgradeJournal(context.Background(), db)
			require.NoError(t, err)
			var version int64
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
			var note string
			require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
			if boundary == "before_U4_commit" {
				require.Equal(t, UpgradePhaseMigratingLocal, rec.Phase)
				require.Equal(t, int64(1), version)
				require.Equal(t, "version one", note)
				coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
				require.NoError(t, err)
				result, err := coordinator.Advance(context.Background())
				require.NoError(t, err)
				require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
				require.NoError(t, coordinator.Close())
			} else {
				require.Equal(t, UpgradePhaseReady, rec.Phase)
				require.Equal(t, int64(2), version)
				require.Equal(t, "version two", note)
			}
			require.NoError(t, db.Close())
		})
	}
}
func TestOpening_I10FreshBootstrapProcessRecovery(t *testing.T) {
	for _, boundary := range []string{"before_fresh_bootstrap_commit", "after_fresh_bootstrap_commit"} {
		t.Run(boundary, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fresh.sqlite")
			command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
			command.Env = append(os.Environ(),
				upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=FRESH_BOOTSTRAP",
				upgradeProcessDBEnv+"="+path, upgradeProcessBoundaryEnv+"="+boundary,
			)
			err := command.Run()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 93, exit.ExitCode())

			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			if boundary == "before_fresh_bootstrap_commit" {
				var tables int
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables))
				require.Zero(t, tables, "a pre-commit death must leave an empty business database")
			} else {
				rec, err := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, err)
				require.NotNil(t, rec)
				require.Equal(t, UpgradePhaseMigratingLocal, rec.Phase)
			}

			coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestFreshPlan(t)})
			require.NoError(t, err)
			result, err := coordinator.Advance(context.Background())
			require.NoError(t, err)
			require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
			require.NotNil(t, result.Ready)
			require.NoError(t, coordinator.Close())
			require.NoError(t, db.Close())
		})
	}
}

func TestOpening_I01ResumeRevalidatesVersionAndPhysicalContract(t *testing.T) {
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{name: "version", sql: `PRAGMA user_version=7`},
		{name: "schema", sql: `ALTER TABLE people ADD COLUMN undeclared TEXT`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "same-file.sqlite")
			prepareProcessTestDatabase(t, path)
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
			require.NoError(t, err)
			rec, err := coordinator.loadOrStart(context.Background())
			require.NoError(t, err)
			require.Equal(t, UpgradePhaseMigratingLocal, rec.Phase)
			require.NoError(t, coordinator.Close())
			_, err = db.Exec(mutation.sql)
			require.NoError(t, err)
			_, err = newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
			var upgradeErr *UpgradeError
			require.ErrorAs(t, err, &upgradeErr)
			if mutation.name == "version" {
				require.Equal(t, UpgradeOutcomePlanMismatch, upgradeErr.Outcome)
			} else {
				require.Equal(t, UpgradeOutcomeLocalContractMismatch, upgradeErr.Outcome)
			}
			require.NoError(t, db.Close())
		})
	}
}

func TestOpening_I01CompletedPlanRevalidatesVersionAndPhysicalContract(t *testing.T) {
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{name: "version", sql: `PRAGMA user_version=7`},
		{name: "schema", sql: `ALTER TABLE people ADD COLUMN undeclared TEXT`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "same-file.sqlite")
			prepareProcessTestDatabase(t, path)
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
			require.NoError(t, err)
			result, err := coordinator.Advance(context.Background())
			require.NoError(t, err)
			require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
			require.NoError(t, coordinator.Close())

			_, err = db.Exec(mutation.sql)
			require.NoError(t, err)
			if mutation.name == "version" {
				reopened, reopenErr := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
				require.NoError(t, reopenErr)
				result, advanceErr := reopened.Advance(context.Background())
				require.NoError(t, advanceErr)
				require.NotNil(t, result.Ready)
				require.NoError(t, result.Ready.Close())
			} else {
				beforeJournal, err := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, err)
				var beforeSchema, beforeRows string
				const schemaSQL = `SELECT json_group_array(json_object('type',type,'name',name,'sql',sql)) FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`
				const rowsSQL = `SELECT json_group_array(json_object('id',id,'name',name,'undeclared',undeclared)) FROM (SELECT * FROM people ORDER BY id)`
				require.NoError(t, db.QueryRow(schemaSQL).Scan(&beforeSchema))
				require.NoError(t, db.QueryRow(rowsSQL).Scan(&beforeRows))
				reopened, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
				require.Nil(t, reopened)
				var upgradeErr *UpgradeError
				require.ErrorAs(t, err, &upgradeErr)
				// Old capture no longer describes the altered physical schema, so
				// retained admission must reject before the later contract check.
				require.Equal(t, UpgradeOutcomeIntegrityFailed, upgradeErr.Outcome)
				require.ErrorContains(t, err, "incompatible retained capture trigger")
				afterJournal, err := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, err)
				require.Equal(t, beforeJournal, afterJournal)
				var afterSchema, afterRows string
				require.NoError(t, db.QueryRow(schemaSQL).Scan(&afterSchema))
				require.NoError(t, db.QueryRow(rowsSQL).Scan(&afterRows))
				require.Equal(t, beforeSchema, afterSchema)
				require.Equal(t, beforeRows, afterRows)
			}
			require.NoError(t, db.Close())
		})
	}
}

func TestOpening_I02ExclusiveProcessOwnershipAndDeathRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	alias := filepath.Join(t.TempDir(), "hard-link.sqlite")
	evidence := filepath.Join(t.TempDir(), "owned")
	childTemp := t.TempDir()
	prepareProcessTestDatabase(t, path)
	require.NoError(t, os.Link(path, alias))
	command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
	command.Env = append(os.Environ(),
		upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=LOCK",
		upgradeProcessDBEnv+"="+path, upgradeProcessEvidenceEnv+"="+evidence,
		"TMPDIR="+childTemp,
	)
	require.NoError(t, command.Start())
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(evidence); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			t.Fatal("child did not acquire the file lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	db, err := sql.Open("sqlite3", alias)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomeBusy, upgradeErr.Outcome)
	require.NoError(t, command.Process.Kill())
	_ = command.Wait()

	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
	require.NoError(t, err, "the OS must release ownership when the process dies")
	require.NoError(t, coordinator.Close(), "a hard-link alias must share file ownership and be released after death")
	require.NoError(t, db.Close())
}

func TestOpening_I10FinalReadyProcessDeath(t *testing.T) {
	for _, boundary := range []string{"before_U8_commit", "after_U8_commit"} {
		t.Run(boundary, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "same-file.sqlite")
			plan := prepareU8ProcessTestDatabase(t, path)
			command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
			command.Env = append(os.Environ(),
				upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=U8",
				upgradeProcessDBEnv+"="+path, upgradeProcessBoundaryEnv+"="+boundary,
			)
			output, err := command.CombinedOutput()
			t.Logf("U8 child output: %s", output)
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 93, exit.ExitCode())

			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			rec, err := loadUpgradeJournal(context.Background(), db)
			require.NoError(t, err)
			if boundary == "before_U8_commit" {
				require.Equal(t, UpgradePhaseRebuildRunning, rec.Phase)
				var staged int
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&staged))
				require.Equal(t, 1, staged)
				coordinator, err := newTestOpening(db, &openingRuntimeConfig{
					Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
					HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
						if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
							return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
						}
						return nil, fmt.Errorf("unexpected recovery request %s %s", request.Method, request.URL.Path)
					})},
				})
				require.NoError(t, err)
				result, err := coordinator.Advance(context.Background())
				require.NoError(t, err)
				require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
				require.NoError(t, coordinator.Close())
			} else {
				require.Equal(t, UpgradePhaseReady, rec.Phase)
			}
			var source string
			var checkpoint int64
			require.NoError(t, db.QueryRow(`SELECT current_source_id,last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&source, &checkpoint))
			require.Equal(t, "source-new", source)
			require.Equal(t, int64(7), checkpoint)
			var note string
			require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
			require.Equal(t, "preserved", note)
			require.NoError(t, db.Close())
			if boundary == "after_U8_commit" {
				target := DefaultConfig(plan.TargetGenerationID, cloneSyncTables(plan.testTarget.Tables))
				target.DatabaseID = plan.DatabaseID
				target.ServiceID = plan.ServiceID
				target.LocalVersion = plan.TargetVersion
				target.LocalOnlyTables = append([]string(nil), plan.LocalOnlyTables...)
				reopened, reopenErr := NewOpening(&OpeningConfig{
					DatabasePath: path, BaseURL: "http://process.invalid", Token: tokenProviderForTests, TargetClientConfig: target,
				})
				require.NoError(t, reopenErr)
				result, advanceErr := reopened.Advance(context.Background())
				require.NoError(t, advanceErr)
				require.NotNil(t, result.Ready)
				var count int
				require.NoError(t, result.Ready.Database().QueryRow(`SELECT COUNT(*) FROM people`).Scan(&count))
				require.Equal(t, 1, count)
				require.NoError(t, result.Ready.Close())
			}
		})
	}
}

func TestOpening_I09SnapshotCleanupStartsFreshTargetSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	clearUpgradeSnapshotStageForTest(t, db)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/process-snapshot":
				return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "snapshot_session_not_found"}), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				if request.Body != nil {
					var body map[string]any
					require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
					require.NotContains(t, body, "source_replacement", "U7 recovery must not rotate the already replaced source")
				}
				return jsonResponse(oversync.SnapshotSession{
					SnapshotID: "replacement-target-snapshot", SnapshotBundleSeq: 7,
					RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z",
				}), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/replacement-target-snapshot":
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected recovery request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.Equal(t, "source-new", result.Status.NewSourceID)
	require.Equal(t, "replacement-target-snapshot", result.Status.SnapshotID)
	require.Equal(t, plan.testTarget.Proof, result.Status.TargetProof)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func TestOpening_I10EveryLocalBoundaryProcessDeath(t *testing.T) {
	type boundaryCase struct {
		name        string
		beforePhase UpgradePhase
		afterPhase  UpgradePhase
		prepare     func(*testing.T, string) UpgradePlan
		assert      func(*testing.T, *sql.DB, string, *upgradeJournalRecord)
	}
	cases := []boundaryCase{
		{
			name: "U0", beforePhase: UpgradePhaseReady, afterPhase: UpgradePhaseStarted,
			prepare: prepareGenerationReadyProcessTestDatabase,
			assert: func(t *testing.T, db *sql.DB, boundary string, rec *upgradeJournalRecord) {
				var ready int
				require.NoError(t, db.QueryRow(`SELECT ready FROM _sync_upgrade_identity WHERE singleton_key=1`).Scan(&ready))
				if boundary == "before_U0_commit" {
					require.Equal(t, 1, ready)
				} else {
					require.Zero(t, ready)
					require.True(t, rec.PlanID == "process-generation-v3")
				}
			},
		},
		{name: "U1", beforePhase: UpgradePhaseStarted, afterPhase: UpgradePhaseResolvingOldIntent, prepare: func(t *testing.T, path string) UpgradePlan {
			return prepareGenerationBoundaryPhase(t, path, UpgradePhaseStarted)
		}},
		{
			name: "U2", beforePhase: UpgradePhaseResolvingOldIntent, afterPhase: UpgradePhaseResolvingOldIntent,
			prepare: func(t *testing.T, path string) UpgradePlan {
				return prepareGenerationBoundaryPhase(t, path, UpgradePhaseResolvingOldIntent)
			},
			assert: func(t *testing.T, _ *sql.DB, boundary string, rec *upgradeJournalRecord) {
				require.Equal(t, boundary == "after_U2_commit", rec.DrainComplete)
			},
		},
		{
			name: "U3", beforePhase: UpgradePhaseResolvingOldIntent, afterPhase: UpgradePhaseMigratingLocal,
			prepare: prepareGenerationDrainedProcessTestDatabase,
			assert: func(t *testing.T, _ *sql.DB, _ string, rec *upgradeJournalRecord) {
				require.True(t, rec.DrainComplete)
			},
		},
		{
			name: "U5", beforePhase: UpgradePhaseLocalMigrated, afterPhase: UpgradePhaseReattachPending,
			prepare: prepareLocalMigratedProcessTestDatabase,
			assert: func(t *testing.T, _ *sql.DB, boundary string, rec *upgradeJournalRecord) {
				if boundary == "before_U5_commit" {
					require.Empty(t, rec.NewSourceID)
				} else {
					require.NotEmpty(t, rec.NewSourceID)
				}
			},
		},
		{
			name: "U6", beforePhase: UpgradePhaseReattachPending, afterPhase: UpgradePhaseRebuildRunning,
			prepare: prepareU6ProcessTestDatabase,
			assert: func(t *testing.T, _ *sql.DB, boundary string, rec *upgradeJournalRecord) {
				require.Equal(t, boundary == "after_U6_commit", rec.ReplacementConfirmed)
			},
		},
		{
			name: "U7", beforePhase: UpgradePhaseRebuildRunning, afterPhase: UpgradePhaseRebuildRunning,
			prepare: prepareU7ProcessTestDatabase,
			assert: func(t *testing.T, db *sql.DB, boundary string, rec *upgradeJournalRecord) {
				var rows int64
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id='process-snapshot'`).Scan(&rows))
				if boundary == "before_U7_commit" {
					require.Zero(t, rows)
					require.Zero(t, rec.SnapshotRowsStaged)
				} else {
					require.Equal(t, int64(1), rows)
					require.Equal(t, int64(1), rec.SnapshotRowsStaged)
				}
			},
		},
	}
	for _, item := range cases {
		for _, side := range []string{"before", "after"} {
			boundary := side + "_" + item.name + "_commit"
			t.Run(boundary, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "same-file.sqlite")
				item.prepare(t, path)
				command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
				command.Env = append(os.Environ(),
					upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=GENERATION_BOUNDARY",
					upgradeProcessDBEnv+"="+path, upgradeProcessBoundaryEnv+"="+boundary,
				)
				output, err := command.CombinedOutput()
				t.Logf("generation-boundary child output: %s", output)
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 93, exit.ExitCode())

				db, err := sql.Open("sqlite3", path)
				require.NoError(t, err)
				db.SetMaxOpenConns(1)
				rec, err := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, err)
				if side == "before" {
					require.Equal(t, item.beforePhase, rec.Phase)
				} else {
					require.Equal(t, item.afterPhase, rec.Phase)
				}
				if item.assert != nil {
					item.assert(t, db, boundary, rec)
				}
				require.NoError(t, db.Close())
			})
		}
	}
}

func TestOpening_I08StableReplacementTupleAfterProcessReplyLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	evidence := filepath.Join(t.TempDir(), "replacement-request.json")
	plan := prepareU6ProcessTestDatabase(t, path)
	command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
	command.Env = append(os.Environ(),
		upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=U6",
		upgradeProcessDBEnv+"="+path, upgradeProcessEvidenceEnv+"="+evidence,
	)
	output, err := command.CombinedOutput()
	t.Logf("U6 child output: %s", output)
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 94, exit.ExitCode())

	body, err := os.ReadFile(evidence)
	require.NoError(t, err)
	var lostRequest oversync.SnapshotSessionCreateRequest
	require.NoError(t, json.Unmarshal(body, &lostRequest))
	require.NotNil(t, lostRequest.SourceReplacement)

	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, UpgradePhaseReattachPending, rec.Phase)
	require.Equal(t, rec.OldSourceID, lostRequest.SourceReplacement.PreviousSourceID)
	require.Equal(t, rec.NewSourceID, lostRequest.SourceReplacement.NewSourceID)
	require.Equal(t, rec.ReplacementReason, lostRequest.SourceReplacement.Reason)

	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				var replay oversync.SnapshotSessionCreateRequest
				require.NoError(t, json.NewDecoder(request.Body).Decode(&replay))
				require.Equal(t, lostRequest, replay)
				return jsonResponse(oversync.SnapshotSession{SnapshotID: "replacement-replay", SnapshotBundleSeq: 7, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/replacement-replay":
				return jsonResponse(map[string]string{"status": "deleted"}), nil
			default:
				return nil, fmt.Errorf("unexpected recovery request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func TestOpening_I12MalformedReplacementResponseIsIntegrityFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	plan := prepareU6ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://process.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				return rawJSONResponse(http.StatusOK, `{"snapshot_id":"snapshot-1","snapshot_bundle_seq":7,"row_count":0,"expires_at":"2030-01-01T00:00:00Z"}`), nil
			default:
				return nil, fmt.Errorf("unexpected malformed-response request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeIntegrityFailed, result.Status.Outcome)
	require.Equal(t, UpgradePhaseReattachPending, result.Status.Phase)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.False(t, rec.ReplacementConfirmed)
	require.NotEmpty(t, rec.NewSourceID)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func TestOpening_I03PermanentRejectionRequiresBoundResolution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	plan := preparePushProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	base := newMockPushServer(t)
	reject := true
	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
			if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
			}
			return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/push" && reject:
			return errorJSONResponse(http.StatusUnprocessableEntity, oversync.ErrorResponse{Error: "payload_invalid", Message: "application resolution required"}), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: base.nextBundleSeq - 1}), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "after-resolution", SnapshotBundleSeq: max(int64(1), base.nextBundleSeq-1), RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/after-resolution":
			return jsonResponse(map[string]string{"status": "deleted"}), nil
		default:
			return base.RoundTrip(request)
		}
	})}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan, BaseURL: "http://resolution.invalid", Token: tokenProviderForTests, HTTP: httpClient})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeNeedsResolution, result.Status.Outcome)
	require.NotEmpty(t, result.Status.PlanID)
	require.NotEmpty(t, result.Status.PlanDigest)
	require.NotEmpty(t, result.Status.PendingRequestHash)

	_, err = coordinator.ResolvePendingIntent(context.Background(), PendingIntentResolution{
		Action: PendingIntentRetry, PlanID: result.Status.PlanID, Revision: result.Status.Revision, RequestHash: "wrong",
	})
	require.Error(t, err)
	status, err := coordinator.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, result.Status.Revision, status.Revision)

	reject = false
	result, err = coordinator.ResolvePendingIntent(context.Background(), PendingIntentResolution{
		Action: PendingIntentRetry, PlanID: status.PlanID, Revision: status.Revision,
		RequestHash: status.PendingRequestHash,
	})
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func TestOpening_I04HistoryPrunedRecoveryCompletesUnderFrozenCoordinator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history-pruned-upgrade.sqlite")
	plan := preparePushProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	base := newMockPushServer(t)
	createPruned := true
	var recoveredOldSource string
	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
			if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
			}
			return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/push" && createPruned:
			createPruned = false
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{
				Error: "history_pruned", Message: "old source tuple is below the retained floor",
			}), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions" && generation == fmt.Sprint(plan.testInstalled.Proof.ID):
			var create oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(request.Body).Decode(&create))
			require.NotNil(t, create.SourceReplacement)
			require.Equal(t, "source-old", create.SourceReplacement.PreviousSourceID)
			require.Equal(t, "history_pruned", create.SourceReplacement.Reason)
			recoveredOldSource = create.SourceReplacement.NewSourceID
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID: "old-recovery", SnapshotBundleSeq: 0, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z",
			}), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/old-recovery":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "old-recovery", SnapshotBundleSeq: 0, NextRowOrdinal: 0, HasMore: false, ByteCount: 0,
			}), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/old-recovery":
			return jsonResponse(map[string]string{"status": "deleted"}), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/pull" && generation == fmt.Sprint(plan.testInstalled.Proof.ID):
			return jsonResponse(oversync.PullResponse{StableBundleSeq: base.nextBundleSeq - 1}), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions" && generation == fmt.Sprint(plan.testTarget.Proof.ID):
			var create oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(request.Body).Decode(&create))
			require.NotNil(t, create.SourceReplacement)
			require.Equal(t, recoveredOldSource, create.SourceReplacement.PreviousSourceID)
			require.Equal(t, "schema_generation_upgrade", create.SourceReplacement.Reason)
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID: "target-rebuild", SnapshotBundleSeq: 1, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z",
			}), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/target-rebuild":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "target-rebuild", SnapshotBundleSeq: 1, NextRowOrdinal: 0, HasMore: false, ByteCount: 0,
			}), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/target-rebuild":
			return jsonResponse(map[string]string{"status": "deleted"}), nil
		default:
			return base.RoundTrip(request)
		}
	})}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://history-pruned.invalid", Token: tokenProviderForTests, HTTP: httpClient,
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NotEmpty(t, recoveredOldSource)
	require.Equal(t, recoveredOldSource, result.Status.OldSourceID)
	require.NotEqual(t, recoveredOldSource, result.Status.NewSourceID)
	require.Len(t, base.pushRequests, 1, "the preserved request must replay after same-generation recovery")
	require.Equal(t, operationKindNone, requireOperationKind(t, db))
}

func TestOpening_I03ResolutionDecisionSurvivesProcessDeath(t *testing.T) {
	// Historical session-resolution boundary: direct application rejections are
	// discarded rather than persisted as an accept-server resolver decision.
	t.Skip("historical accept-server resolution process-death fixture")
	for _, boundary := range []string{"before_resolution_commit", "after_resolution_commit"} {
		t.Run(boundary, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "same-file.sqlite")
			prepareNeedsResolutionProcessTestDatabase(t, path)
			command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
			command.Env = append(os.Environ(),
				upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"=RESOLUTION",
				upgradeProcessDBEnv+"="+path, upgradeProcessBoundaryEnv+"="+boundary,
			)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			t.Log(string(output))
			require.Equal(t, 93, exit.ExitCode())

			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			rec, err := loadUpgradeJournal(context.Background(), db)
			require.NoError(t, err)
			if boundary == "before_resolution_commit" {
				require.Equal(t, UpgradeOutcomeNeedsResolution, rec.Outcome)
				require.Empty(t, rec.ResolutionAction)
			} else {
				require.Empty(t, rec.Outcome)
				require.Equal(t, PendingIntentDiscard, rec.ResolutionAction)
				require.Equal(t, rec.PendingRequestHash, rec.ResolutionRequestHash)
				resolver, err := resolverForDurableDecision(rec)
				require.NoError(t, err)
				require.IsType(t, AcceptServer{}, resolver.Resolve(ConflictContext{}))
			}
			require.NoError(t, db.Close())
		})
	}
}
