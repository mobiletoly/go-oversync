package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const (
	upgradeProcessHelperEnv = "OVERSQLITE_UPGRADE_E2E_HELPER"
	upgradeProcessCaseEnv   = "OVERSQLITE_UPGRADE_E2E_CASE"
	upgradeProcessDBEnv     = "OVERSQLITE_UPGRADE_E2E_DB"
	upgradeProcessURLEnv    = "OVERSQLITE_UPGRADE_E2E_URL"
	upgradeProcessTokenEnv  = "OVERSQLITE_UPGRADE_E2E_TOKEN"
	upgradeProcessUserEnv   = "OVERSQLITE_UPGRADE_E2E_USER"
)

type upgradeMigrator struct {
	migrate func(context.Context, oversqlite.LocalMigrationTx, oversqlite.LocalMigrationStep) error
}

func (m upgradeMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m upgradeMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, step oversqlite.LocalMigrationStep) error {
	return m.migrate(ctx, tx, step)
}

func upgradeDefinitions(t *testing.T) (oversqlite.LocalGenerationContract, oversqlite.LocalGenerationContract, []oversync.RegisteredTable, []oversync.SchemaGeneration) {
	t.Helper()
	registered := []oversync.RegisteredTable{{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}}}
	oldGeneration := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{
		Schema: "business", Table: "users", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "name", WireType: oversync.GenerationWireTypeText},
			{Name: "email", WireType: oversync.GenerationWireTypeText},
		},
	}}}
	targetGeneration := oversync.SchemaGeneration{ID: 3, Tables: []oversync.GenerationTable{{
		Schema: "business", Table: "users", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "name", WireType: oversync.GenerationWireTypeText},
			{Name: "email", WireType: oversync.GenerationWireTypeText},
			{Name: "created_at", WireType: oversync.GenerationWireTypeText},
			{Name: "updated_at", WireType: oversync.GenerationWireTypeText},
		},
	}}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{oldGeneration, targetGeneration})
	require.NoError(t, err)
	local := func(generation oversync.SchemaGeneration) oversqlite.LocalGenerationContract {
		compiled, ok := registry.Generation(generation.ID)
		require.True(t, ok)
		columns := make([]oversqlite.LocalColumnContract, 0, len(generation.Tables[0].Columns))
		for _, column := range generation.Tables[0].Columns {
			codec := oversqlite.LocalCodecText
			if column.WireType == oversync.GenerationWireTypeUUID {
				codec = oversqlite.LocalCodecUUIDText
			}
			columns = append(columns, oversqlite.LocalColumnContract{Name: column.Name, Codec: codec})
		}
		return oversqlite.LocalGenerationContract{
			Proof:            oversync.SchemaGenerationProof{ID: generation.ID, Fingerprint: compiled.Fingerprint()},
			RegisteredTables: registered, Generation: generation,
			Tables:  []oversqlite.SyncTable{{TableName: "users", SyncKeyColumns: []string{"id"}}},
			Columns: []oversqlite.LocalTableColumns{{Schema: "business", Table: "users", Columns: columns}},
		}
	}
	return local(oldGeneration), local(targetGeneration), registered, []oversync.SchemaGeneration{oldGeneration, targetGeneration}
}

func upgradeFreshPlan(t *testing.T, old oversqlite.LocalGenerationContract) oversqlite.UpgradePlan {
	t.Helper()
	return oversqlite.UpgradePlan{
		PlanID: "upgrade-e2e-fresh-v1", Mode: oversqlite.UpgradeModeFresh,
		DatabaseID: "upgrade-e2e-device", ServiceID: "upgrade-e2e-service", TargetVersion: 1, TargetGenerationID: old.Proof.ID,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: upgradeMigrator{migrate: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE users (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel','preserved');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}
}

func upgradeUpgradePlan(t *testing.T, user string, old, target oversqlite.LocalGenerationContract) oversqlite.UpgradePlan {
	t.Helper()
	return oversqlite.UpgradePlan{
		PlanID: "upgrade-e2e-generation-v3", Mode: oversqlite.UpgradeModeGeneration,
		DatabaseID: "upgrade-e2e-device", ServiceID: "upgrade-e2e-service", UserID: user,
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: old.Proof.ID, TargetGenerationID: target.Proof.ID,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: upgradeMigrator{migrate: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				ALTER TABLE users ADD COLUMN created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z';
				ALTER TABLE users ADD COLUMN updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z';
				PRAGMA user_version=2;
			`)
			return err
		}},
	}
}

func upgradeReadyConfig(local oversqlite.LocalGenerationContract, version int64) *oversqlite.Config {
	config := oversqlite.DefaultConfig(local.Proof.ID, local.Tables)
	config.DatabaseID = "upgrade-e2e-device"
	config.ServiceID = "upgrade-e2e-service"
	config.LocalVersion = version
	config.LocalOnlyTables = []string{"local_notes"}
	return config
}

type upgradeExitAfterCommitTransport struct {
	kind string
}

func (transport upgradeExitAfterCommitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	isCommit := transport.kind == "push" && request.Method == http.MethodPost && request.URL.Path == "/sync/push"
	isReplacement := false
	if transport.kind == "replacement" && request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions" {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		var create oversync.SnapshotSessionCreateRequest
		if err := json.Unmarshal(body, &create); err != nil {
			return nil, err
		}
		isReplacement = create.SourceReplacement != nil
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err == nil && response.StatusCode == http.StatusOK && (isCommit || isReplacement) {
		if isCommit {
			os.Exit(91)
		}
		os.Exit(92)
	}
	return response, err
}

func TestUpgrade_ProductionProcessHelper(t *testing.T) {
	if os.Getenv(upgradeProcessHelperEnv) != "1" {
		return
	}
	old, target, _, _ := upgradeDefinitions(t)
	path := os.Getenv(upgradeProcessDBEnv)
	baseURL := os.Getenv(upgradeProcessURLEnv)
	token := os.Getenv(upgradeProcessTokenEnv)
	user := os.Getenv(upgradeProcessUserEnv)
	tokenProvider := func(context.Context) (string, error) { return token, nil }
	switch os.Getenv(upgradeProcessCaseEnv) {
	case "push":
		client := openReadySQLiteClient(t, path, baseURL, tokenProvider, upgradeReadyConfig(old, 1))
		client.HTTP = &http.Client{Transport: upgradeExitAfterCommitTransport{kind: "push"}}
		attach, err := client.Attach(context.Background(), user)
		require.NoError(t, err)
		require.Equal(t, oversqlite.AttachStatusConnected, attach.Status)
		_, err = client.Sync(context.Background())
		require.NoError(t, err)
	case "replacement":
		plan := upgradeUpgradePlan(t, user, old, target)
		opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path,
			Plan: &plan, BaseURL: baseURL, Token: tokenProvider,
			HTTP:               &http.Client{Transport: upgradeExitAfterCommitTransport{kind: "replacement"}},
			TargetClientConfig: upgradeReadyConfig(target, plan.TargetVersion),
		})
		require.NoError(t, err)
		_, err = opening.Advance(context.Background())
		require.NoError(t, err)
	default:
		t.Fatalf("unknown helper case")
	}
}

func TestUpgrade_ProductionRemoteCommitProcessDeath(t *testing.T) {
	old, target, registered, generations := upgradeDefinitions(t)
	server := newExampleServerWithConfig(t, "business", func(config *exampleserver.ServerConfig) {
		config.RegisteredTables = registered
		config.SchemaGenerations = generations
	})
	user := "upgrade-process-" + uuid.NewString()
	token, err := server.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "same-file.sqlite")
	provider := func(context.Context) (string, error) { return token, nil }
	freshPlan := upgradeFreshPlan(t, old)
	fresh, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path, Plan: &freshPlan, BaseURL: server.URL(), Token: provider, TargetClientConfig: upgradeReadyConfig(old, 1)})
	require.NoError(t, err)
	result, err := fresh.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversqlite.UpgradeOutcomeReady, result.Status.Outcome)
	client := result.Ready
	require.NotNil(t, client)
	db := client.Database()
	attach, err := client.Attach(context.Background(), user)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attach.Status)
	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES (?,?,?)`, rowID, "Ada", "ada@example.test")
	require.NoError(t, err)
	var oldSource string
	require.NoError(t, db.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&oldSource))
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	runChild := func(kind string, exitCode int) {
		command := exec.Command(os.Args[0], "-test.run=^TestUpgrade_ProductionProcessHelper$")
		command.Env = append(os.Environ(),
			upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"="+kind, upgradeProcessDBEnv+"="+path,
			upgradeProcessURLEnv+"="+server.URL(), upgradeProcessTokenEnv+"="+token, upgradeProcessUserEnv+"="+user,
		)
		output, err := command.CombinedOutput()
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, exitCode, exit.ExitCode(), string(output))
	}
	runChild("push", 91)

	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	var outboxState string
	var sourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT state,source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState, &sourceBundleID))
	require.Equal(t, "prepared", outboxState)
	require.Positive(t, sourceBundleID)
	require.NoError(t, db.Close())
	client = openReadySQLiteClient(t, path, server.URL(), provider, upgradeReadyConfig(old, 1))
	db = client.Database()
	attach, err = client.Attach(context.Background(), user)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attach.Status)
	newerRowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES (?,?,?)`, newerRowID, "Grace", "grace@example.test")
	require.NoError(t, err)
	pushReport, err := client.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, pushReport.Outcome)
	var newerIntentRows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name='users' AND key_json LIKE ?`, "%"+newerRowID+"%").Scan(&newerIntentRows))
	require.Equal(t, 1, newerIntentRows, "settling the lost reply must preserve newer local intent")
	pushReport, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, pushReport.Outcome)
	_, err = client.PullToStable(context.Background())
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	runChild("replacement", 92)
	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	var phase, newSource string
	require.NoError(t, db.QueryRow(`SELECT phase,new_source_id FROM _sync_upgrade_journal WHERE singleton_key=1`).Scan(&phase, &newSource))
	require.Equal(t, string(oversqlite.UpgradePhaseReattachPending), phase)
	require.NotEmpty(t, newSource)
	var oldState, replacedBy, newState string
	require.NoError(t, server.Pool.QueryRow(context.Background(), `
		SELECT old.state,old.replaced_by_source_id,new.state
		FROM sync.user_state u
		JOIN sync.source_state old ON old.user_pk=u.user_pk AND old.source_id=$2
		JOIN sync.source_state new ON new.user_pk=u.user_pk AND new.source_id=$3
		WHERE u.user_id=$1
	`, user, oldSource, newSource).Scan(&oldState, &replacedBy, &newState))
	require.Equal(t, "retired", oldState)
	require.Equal(t, newSource, replacedBy)
	require.Equal(t, "reserved", newState, "the committed replacement reserves the new source before client recovery")

	require.NoError(t, db.Close())
	upgradePlan := upgradeUpgradePlan(t, user, old, target)
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path,
		Plan: &upgradePlan, BaseURL: server.URL(), Token: provider,
		TargetClientConfig: upgradeReadyConfig(target, upgradePlan.TargetVersion),
	})
	require.NoError(t, err)
	result, err = opening.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversqlite.UpgradeOutcomeReady, result.Status.Outcome)
	require.NotNil(t, result.Ready)
	db = result.Ready.Database()
	var name, note string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&name))
	require.Equal(t, "Ada", name)
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
	require.Equal(t, "preserved", note)
	require.NoError(t, result.Ready.Close())

	var firstRequestCommits, committed int
	require.NoError(t, server.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM sync.bundle_log WHERE source_id=$1 AND source_bundle_id=1`, oldSource).Scan(&firstRequestCommits))
	require.Equal(t, 1, firstRequestCommits, fmt.Sprintf("old source %s first request must have exactly one durable commit", oldSource))
	require.NoError(t, server.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM sync.bundle_log WHERE source_id=$1`, oldSource).Scan(&committed))
	require.Equal(t, 2, committed, fmt.Sprintf("old source %s must include the preserved newer intent", oldSource))
	if observationPath := os.Getenv("OVERSYNC_UPGRADE_E2E_OBSERVATION"); observationPath != "" {
		observation := map[string]any{
			"push_outbox_after_death": outboxState,
			"push_locator_preserved":  sourceBundleID > 0,
			"first_request_commits":   firstRequestCommits,
			"newer_intent_preserved":  newerIntentRows == 1 && committed == 2,
			"replacement_phase":       phase,
			"replacement_exact":       oldState == "retired" && replacedBy == newSource && newState == "reserved",
			"final_outcome":           string(result.Status.Outcome),
			"old_source":              oldSource,
			"new_source":              newSource,
		}
		encoded, err := json.Marshal(observation)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(observationPath, encoded, 0o600))
	}
}
