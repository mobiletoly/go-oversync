package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func discoveryPeopleCapabilities(t *testing.T) oversync.GenerationDiscoveryResponse {
	t.Helper()
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	caps := upgradeCapabilities(t, contract)
	caps.SchemaGeneration.Tables[0].ForeignKeys = []oversync.SchemaGenerationForeignKeyCapabilities{}
	return caps
}

func newDiscoveryPeopleClient(t *testing.T, db *sql.DB, baseURL string) *Client {
	t.Helper()
	client, err := newClientFromDB(db, baseURL, func(context.Context) (string, error) { return "token", nil }, &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.open(context.Background()))
	return client
}

func TestContractDiscovery_OfflineAttachAndRetainedReopen(t *testing.T) {
	ctx := context.Background()
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
 INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','offline before open')`)
	require.NoError(t, err)
	client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
	status, err := client.SyncStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.ContractConfirmed)
	require.Equal(t, AuthorityStatusLocalOnly, status.Authority)
	require.EqualValues(t, 1, status.Pending.PendingRowCount)
	require.Empty(t, client.schemaGenerationProof.Fingerprint)
	caps := discoveryPeopleCapabilities(t)
	var paths []string
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		paths = append(paths, req.URL.Path)
		require.Equal(t, "Bearer token", req.Header.Get("Authorization"))
		require.Equal(t, client.sourceID, req.Header.Get(oversync.SourceIDHeader))
		if req.URL.Path == "/sync/generations/1" {
			require.Empty(t, req.Header.Values(oversync.SchemaGenerationIDHeader))
			require.Empty(t, req.Header.Values(oversync.SchemaGenerationFingerprintHeader))
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration}), nil
		}
		require.Equal(t, "1", req.Header.Get(oversync.SchemaGenerationIDHeader))
		require.Equal(t, caps.SchemaGeneration.Fingerprint, req.Header.Get(oversync.SchemaGenerationFingerprintHeader))
		switch req.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(caps), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			t.Fatalf("unexpected request %s", req.URL.Path)
			return nil, nil
		}
	})}
	result, err := client.Attach(ctx, "owner")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	require.True(t, result.SyncStatus.ContractConfirmed)
	require.Equal(t, []string{"/sync/generations/1", "/sync/connect"}, paths)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM people`).Scan(&name))
	require.Equal(t, "offline before open", name)
	source := client.sourceID
	require.NoError(t, client.Close())
	reopened := newDiscoveryPeopleClient(t, db, "http://relocated.invalid")
	require.Equal(t, source, reopened.sourceID)
	require.Equal(t, caps.SchemaGeneration.Fingerprint, reopened.schemaGenerationProof.Fingerprint)
	status, err = reopened.SyncStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.ContractConfirmed)
	require.EqualValues(t, 1, status.Pending.PendingRowCount)
	paths = nil
	reopened.HTTP = client.HTTP
	_, err = reopened.Attach(ctx, "owner")
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestContractDiscovery_InvalidStoragePreservesUnconfirmedIntent(t *testing.T) {
	ctx := context.Background()
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
	require.NoError(t, err)
	client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
	_, err = db.Exec(`INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111',x'616263')`)
	require.NoError(t, err)
	var before string
	require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&before))
	caps := discoveryPeopleCapabilities(t)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(caps), nil
		default:
			t.Fatalf("invalid data must fail before connect: %s", req.URL.Path)
			return nil, nil
		}
	})}
	_, err = client.Attach(ctx, "owner")
	require.ErrorContains(t, err, "incompatible SQLite storage blob")
	var after, storage, fingerprint string
	var admitted, outbox int
	require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&after))
	require.Equal(t, before, after)
	require.NoError(t, db.QueryRow(`SELECT typeof(name) FROM people`).Scan(&storage))
	require.Equal(t, "blob", storage)
	require.NoError(t, db.QueryRow(`SELECT fingerprint FROM _sync_contract_identity`).Scan(&fingerprint))
	require.Empty(t, fingerprint)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_admitted_contracts`).Scan(&admitted))
	require.Zero(t, admitted)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outbox))
	require.Zero(t, outbox)
}

func TestContractDiscovery_StrictErrorContext(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{"unknown", 409, `{"error":"unknown_schema_generation","message":"unknown","retryable":false,"remediation":"upgrade_or_reconfigure_client","schema_generation_id":"1"}`, true},
		{"retired", 426, `{"error":"retired_schema_generation","message":"retired","retryable":false,"remediation":"upgrade_application","schema_generation_id":"1"}`, true},
		{"wrong id", 409, `{"error":"unknown_schema_generation","message":"unknown","retryable":false,"remediation":"upgrade_or_reconfigure_client","schema_generation_id":"2"}`, false},
		{"missing retryable", 409, `{"error":"unknown_schema_generation","message":"unknown","remediation":"upgrade_or_reconfigure_client","schema_generation_id":"1"}`, false},
		{"null retryable", 409, `{"error":"unknown_schema_generation","message":"unknown","retryable":null,"remediation":"upgrade_or_reconfigure_client","schema_generation_id":"1"}`, false},
		{"duplicate id", 409, `{"error":"unknown_schema_generation","message":"unknown","retryable":false,"remediation":"upgrade_or_reconfigure_client","schema_generation_id":"1","schema_generation_id":"1"}`, false},
		{"wrong status", 409, `{"error":"retired_schema_generation","message":"retired","retryable":false,"remediation":"upgrade_application","schema_generation_id":"1"}`, false},
		{"wrong remediation", 426, `{"error":"retired_schema_generation","message":"retired","retryable":false,"remediation":"retry_later","schema_generation_id":"1"}`, false},
		{"invented fingerprint", 426, `{"error":"retired_schema_generation","message":"retired","retryable":false,"remediation":"upgrade_application","schema_generation_id":"1","schema_generation_fingerprint":"unknown"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeDiscoveryError(tc.status, []byte(tc.body), 1)
			require.Error(t, err)
			var generationErr *oversync.SchemaGenerationRequestError
			require.Equal(t, tc.valid, errors.As(err, &generationErr))
			if tc.valid {
				require.EqualValues(t, 1, generationErr.GenerationID)
			}
		})
	}
}

func TestContractDiscovery_CorruptRetainedStateBlocksReopen(t *testing.T) {
	for _, tc := range []struct{ name, mutation string }{
		{"missing identity", `DELETE FROM _sync_contract_identity`},
		{"different service", `UPDATE _sync_contract_identity SET service_id='other'`},
		{"different generation", `UPDATE _sync_contract_identity SET generation_id=2`},
		{"changed column", `ALTER TABLE people ADD COLUMN other TEXT`},
		{"attached without descriptor", `UPDATE _sync_attachment_state SET binding_state='attached',attached_user_id='owner'`},
		{"oversized unconfirmed cache", `INSERT INTO _sync_admitted_contracts VALUES(1,'bad',zeroblob(65537),'bad')`},
		{"unexpected other cache", `INSERT INTO _sync_admitted_contracts VALUES(2,'bad','{}','bad')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			_, err = db.Exec(`INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','pending')`)
			require.NoError(t, err)
			require.NoError(t, client.Close())
			_, err = db.Exec(tc.mutation)
			require.NoError(t, err)
			_, err = newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { t.Fatal("reopen cannot contact server"); return "", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.Error(t, err)
			var pending int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&pending))
			require.Equal(t, 1, pending)
		})
	}
}

func TestContractDiscovery_PartialRuntimeFootprintCannotBecomeFreshIdentity(t *testing.T) {
	objects := append([]string(nil), syncMetadataTableNames...)
	objects = append(objects, "managed trigger")
	for _, object := range objects {
		if object == "_sync_contract_identity" {
			continue
		}
		t.Run(object, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			if object == "managed trigger" {
				_, err = db.Exec(`CREATE TRIGGER trg_people_ai AFTER INSERT ON people BEGIN SELECT * FROM _sync_dirty_rows; END`)
			} else {
				_, err = db.Exec(`CREATE TABLE ` + quoteIdent(object) + `(sentinel TEXT)`)
			}
			require.NoError(t, err)

			_, err = newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.ErrorContains(t, err, "existing sync state has no retained contract identity")
			var identityExists int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_sync_contract_identity'`).Scan(&identityExists))
			require.Zero(t, identityExists)
		})
	}
}

func TestContractDiscovery_CaseVariantRuntimeFootprintRejectedBeforeSQLiteConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup string
	}{
		{name: "owned table", setup: `CREATE TABLE "_SYNC_DIRTY_ROWS" (sentinel TEXT)`},
		{name: "managed index", setup: `CREATE TABLE app(id TEXT); CREATE INDEX "IDX_SYNC_DIRTY_ROWS_DIRTY_ORDINAL" ON app(id)`},
		{name: "identity view", setup: `CREATE VIEW "_SYNC_CONTRACT_IDENTITY" AS SELECT 1 AS singleton_key`},
		{name: "trigger reference", setup: `CREATE TABLE app(id TEXT); CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM "_SYNC_DIRTY_ROWS"; END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec("CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY);" + tc.setup)
			require.NoError(t, err)
			var beforeMode, beforeVersion string
			require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&beforeMode))
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&beforeVersion))
			var beforeMetadataTables int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&beforeMetadataTables))

			_, err = newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.ErrorContains(t, err, "existing sync state has no retained contract identity")

			var afterMode, afterVersion string
			require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&afterMode))
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&afterVersion))
			require.Equal(t, beforeMode, afterMode)
			require.Equal(t, beforeVersion, afterVersion)
			var afterMetadataTables int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&afterMetadataTables))
			require.Equal(t, beforeMetadataTables, afterMetadataTables)
		})
	}
}

func TestContractDiscovery_UnrelatedSyncPrefixedApplicationIndexIsAccepted(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); CREATE INDEX _sync_people_lookup ON people(name)`)
	require.NoError(t, err)
	client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestContractDiscovery_ReservedConflictTriggerCollisionIsRejectedAsPartialRuntime(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`
		CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
		CREATE TRIGGER _sync_conflict_revision_insert AFTER INSERT ON people BEGIN SELECT 1; END;
	`)
	require.NoError(t, err)
	var beforeObjects int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&beforeObjects))

	_, err = newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	})
	require.ErrorContains(t, err, "existing sync state has no retained contract identity")
	var afterObjects, identities int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&afterObjects))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='_sync_contract_identity'`).Scan(&identities))
	require.Equal(t, beforeObjects, afterObjects)
	require.Zero(t, identities)
}

func TestContractDiscovery_IncompleteCurrentRuntimeRejectedBeforeSQLiteConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, mutation string }{
		{"missing table", `DROP TABLE _sync_outbox_bundle`},
		{"malformed table", `DROP TABLE _sync_apply_state; CREATE TABLE _sync_apply_state(singleton_key TEXT PRIMARY KEY,apply_mode TEXT)`},
		{"missing index", `DROP INDEX _sync_snapshot_stage_identity_idx`},
		{"missing conflict trigger", `DROP TRIGGER _sync_conflict_revision_update`},
		{"altered conflict trigger", `DROP TRIGGER _sync_conflict_revision_delete; CREATE TRIGGER _sync_conflict_revision_delete AFTER DELETE ON _sync_dirty_rows BEGIN SELECT 1; END`},
		{"trigger changed literal", `DROP TRIGGER _sync_conflict_revision_delete; CREATE TRIGGER _sync_conflict_revision_delete AFTER DELETE ON _sync_dirty_rows BEGIN UPDATE _sync_conflict_state SET revision=revision+0 WHERE singleton_key=1; END`},
		{"trigger added when", `DROP TRIGGER _sync_conflict_revision_delete; CREATE TRIGGER _sync_conflict_revision_delete AFTER DELETE ON _sync_dirty_rows WHEN 0 BEGIN UPDATE _sync_conflict_state SET revision=revision+1 WHERE singleton_key=1; END`},
		{"trigger extra statement", `DROP TRIGGER _sync_conflict_revision_delete; CREATE TRIGGER _sync_conflict_revision_delete AFTER DELETE ON _sync_dirty_rows BEGIN UPDATE _sync_conflict_state SET revision=revision+1 WHERE singleton_key=1; SELECT 1; END`},
		{"unique index", `DROP INDEX _sync_snapshot_stage_identity_idx; CREATE UNIQUE INDEX _sync_snapshot_stage_identity_idx ON _sync_snapshot_stage(snapshot_id,schema_name,table_name,key_json)`},
		{"partial index", `DROP INDEX _sync_snapshot_stage_identity_idx; CREATE INDEX _sync_snapshot_stage_identity_idx ON _sync_snapshot_stage(snapshot_id,schema_name,table_name,key_json) WHERE row_ordinal >= 0`},
		{"descending index", `DROP INDEX _sync_snapshot_stage_identity_idx; CREATE INDEX _sync_snapshot_stage_identity_idx ON _sync_snapshot_stage(snapshot_id DESC,schema_name,table_name,key_json)`},
		{"collated index", `DROP INDEX _sync_snapshot_stage_identity_idx; CREATE INDEX _sync_snapshot_stage_identity_idx ON _sync_snapshot_stage(snapshot_id COLLATE NOCASE,schema_name,table_name,key_json)`},
		{"retained prototype index", `CREATE INDEX idx_sync_dirty_rows_dirty_ordinal ON _sync_dirty_rows(dirty_ordinal)`},
		{"case variant retained prototype index", `CREATE INDEX IDX_SYNC_DIRTY_ROWS_DIRTY_ORDINAL ON _sync_dirty_rows(key_json)`},
		{"extra retained table trigger", `CREATE TRIGGER corrupt_dirty_rows AFTER INSERT ON _sync_dirty_rows BEGIN DELETE FROM _sync_outbox_rows; END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.NoError(t, err)
			require.NoError(t, client.Close())
			_, err = db.Exec(tc.mutation)
			require.NoError(t, err)
			_, err = db.Exec(`PRAGMA foreign_keys=OFF`)
			require.NoError(t, err)
			var beforeForeignKeys, beforeObjects int
			require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&beforeForeignKeys))
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&beforeObjects))

			_, err = newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.ErrorContains(t, err, "incompatible retained sync runtime")
			var afterForeignKeys, afterObjects int
			require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&afterForeignKeys))
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&afterObjects))
			require.Equal(t, beforeForeignKeys, afterForeignKeys)
			require.Equal(t, beforeObjects, afterObjects)
		})
	}
}

func TestContractDiscovery_CurrentRuntimeObjectNamesAreCaseInsensitive(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
	require.NoError(t, err)
	config := &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	}
	client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, config)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	var captureName, captureSQL string
	require.NoError(t, db.QueryRow(`SELECT name,sql FROM sqlite_schema WHERE name='_sync_conflict_revision_insert' COLLATE NOCASE`).Scan(&captureName, &captureSQL))
	_, err = db.Exec(`
		ALTER TABLE _sync_outbox_bundle RENAME TO retained_case_temp;
		ALTER TABLE retained_case_temp RENAME TO _SYNC_OUTBOX_BUNDLE;
		DROP INDEX _sync_snapshot_stage_identity_idx;
		CREATE INDEX _SYNC_SNAPSHOT_STAGE_IDENTITY_IDX ON _sync_snapshot_stage(snapshot_id,schema_name,table_name,key_json);
		DROP TRIGGER _sync_conflict_revision_insert;
	`)
	require.NoError(t, err)
	_, err = db.Exec(strings.Replace(captureSQL, captureName, "_SYNC_CONFLICT_REVISION_INSERT", 1))
	require.NoError(t, err)
	reopened, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, config)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestContractDiscovery_TransactionRaceRestoresOriginalSQLiteSettings(t *testing.T) {
	ctx := context.Background()
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
	require.NoError(t, err)
	config := &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	}
	client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, config)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	_, err = db.Exec(`PRAGMA foreign_keys=OFF`)
	require.NoError(t, err)
	var originalForeignKeys int
	var originalJournalMode string
	require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&originalForeignKeys))
	require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&originalJournalMode))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	err = prepareRetainedContractOnConnection(ctx, conn, config, func(ctx context.Context, tx *immediateTx) error {
		_, dropErr := tx.ExecContext(ctx, `DROP INDEX _sync_snapshot_stage_identity_idx`)
		return dropErr
	}, nil)
	require.ErrorContains(t, err, "required index")
	require.NoError(t, conn.Close())

	var restoredForeignKeys int
	var restoredJournalMode string
	require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&restoredForeignKeys))
	require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&restoredJournalMode))
	require.Equal(t, originalForeignKeys, restoredForeignKeys)
	require.True(t, strings.EqualFold(originalJournalMode, restoredJournalMode))
	var indexCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='index' AND lower(name)='_sync_snapshot_stage_identity_idx'`).Scan(&indexCount))
	require.Equal(t, 1, indexCount)
}

func TestContractDiscovery_TriggerCollisionRollsBackConstructionAndRestoresSQLiteSettings(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`
		CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
		CREATE TRIGGER trg_people_bu_guard BEFORE UPDATE ON people BEGIN SELECT 1; END;
		PRAGMA foreign_keys=OFF;
	`)
	require.NoError(t, err)
	config := &Config{
		GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
		Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
	}
	var originalForeignKeys int
	var originalJournalMode string
	require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&originalForeignKeys))
	require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&originalJournalMode))
	var beforeObjectCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&beforeObjectCount))

	client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, config)
	require.Nil(t, client)
	require.ErrorContains(t, err, "trg_people_bu_guard")

	var afterObjectCount, identityCount, createdTriggerCount, applicationTriggerCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&afterObjectCount))
	require.Equal(t, beforeObjectCount, afterObjectCount)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='_sync_contract_identity'`).Scan(&identityCount))
	require.Zero(t, identityCount)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='trg_people_bi_guard'`).Scan(&createdTriggerCount))
	require.Zero(t, createdTriggerCount)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='trg_people_bu_guard'`).Scan(&applicationTriggerCount))
	require.Equal(t, 1, applicationTriggerCount)
	var restoredForeignKeys int
	var restoredJournalMode string
	require.NoError(t, db.QueryRow(`PRAGMA foreign_keys`).Scan(&restoredForeignKeys))
	require.NoError(t, db.QueryRow(`PRAGMA journal_mode`).Scan(&restoredJournalMode))
	require.Equal(t, originalForeignKeys, restoredForeignKeys)
	require.True(t, strings.EqualFold(originalJournalMode, restoredJournalMode))
}

func TestContractDiscovery_TriggerReferenceDetectionIsExact(t *testing.T) {
	ownedTables := make(map[string]struct{}, len(syncMetadataTableNames))
	for _, name := range syncMetadataTableNames {
		ownedTables[name] = struct{}{}
	}
	for _, test := range []struct {
		name string
		sql  string
		want bool
	}{
		{name: "unquoted from", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM _sync_dirty_rows; END`, want: true},
		{name: "quoted qualified from", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM main."_sync_dirty_rows"; END`, want: true},
		{name: "single quoted from", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM '_sync_dirty_rows'; END`, want: true},
		{name: "comma list", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM app, [_sync_row_state]; END`, want: true},
		{name: "join", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT 1 FROM app JOIN "_sync_attachment_state" ON 1; END`, want: true},
		{name: "insert into", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN INSERT INTO _sync_dirty_rows DEFAULT VALUES; END`, want: true},
		{name: "update conflict", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN UPDATE OR ABORT _sync_dirty_rows SET table_name='app'; END`, want: true},
		{name: "table rhs in", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT NEW.id IN _sync_row_state; END`, want: true},
		{name: "trigger target", sql: `CREATE TRIGGER app_ai AFTER INSERT ON "_sync_dirty_rows" BEGIN SELECT 1; END`, want: true},
		{name: "trigger name", sql: `CREATE TRIGGER "_sync_dirty_rows" AFTER INSERT ON app BEGIN SELECT 1; END`},
		{name: "identifier substring", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app_sync_dirty_rows BEGIN SELECT 1; END`},
		{name: "string literal", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT '_sync_dirty_rows'; END`},
		{name: "comments", sql: "CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN -- FROM _sync_dirty_rows\nSELECT 1 /* JOIN _sync_row_state */; END"},
		{name: "table alias", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app BEGIN SELECT * FROM app AS _sync_dirty_rows; END`},
		{name: "header subquery alias", sql: `CREATE TRIGGER app_ai AFTER INSERT ON app WHEN EXISTS (SELECT 1 FROM other AS _sync_dirty_rows JOIN third ON _sync_dirty_rows.id=third.id) BEGIN SELECT 1; END`},
		{name: "quoted keywords", sql: `CREATE TRIGGER "from" AFTER INSERT ON app BEGIN SELECT "join", "into", "update", "in"; END`},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, triggerReferencesRetainedTable(test.sql, ownedTables))
		})
	}
}

func TestContractDiscovery_UnrelatedApplicationTriggersDoNotBlockFreshIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{name: "identifier substring", ddl: `CREATE TABLE app_sync_cache(id TEXT); CREATE TRIGGER app_sync_audit AFTER INSERT ON app_sync_cache BEGIN SELECT 1; END`},
		{name: "string literal", ddl: `CREATE TRIGGER app_literal AFTER INSERT ON people BEGIN SELECT '_sync_dirty_rows'; END`},
		{name: "comments", ddl: "CREATE TRIGGER app_comment AFTER INSERT ON people BEGIN -- FROM _sync_dirty_rows\nSELECT 1 /* JOIN _sync_row_state */; END"},
		{name: "table alias", ddl: `CREATE TRIGGER app_alias AFTER INSERT ON people BEGIN SELECT * FROM people AS _sync_dirty_rows; END`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			_, err = db.Exec(test.ddl)
			require.NoError(t, err)

			client, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.NoError(t, err)
			require.NoError(t, client.Close())
		})
	}
}

func TestContractDiscovery_DecodedLimitAndBodyCleanup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		extra      int
		compressed bool
	}{
		{"plain exact", 0, false}, {"plain plus one", 1, false}, {"gzip exact", 0, true}, {"gzip plus one", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			caps := discoveryPeopleCapabilities(t)
			descriptor, err := json.Marshal(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration, Limits: testWireLimits()})
			require.NoError(t, err)
			raw := string(descriptor) + strings.Repeat(" ", 65536+tc.extra-len(descriptor))
			response := rawJSONResponse(200, raw)
			if tc.compressed {
				response = gzipResponse(200, raw)
			}
			body := &closeTrackingBody{reader: response.Body}
			response.Body = body
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/sync/generations/1":
					return response, nil
				case "/sync/connect":
					return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
				default:
					t.Fatalf("unexpected request %s", req.URL.Path)
					return nil, nil
				}
			})}
			_, err = client.Attach(context.Background(), "owner")
			if tc.extra == 0 {
				require.NoError(t, err)
			} else if !tc.compressed {
				var tooLarge *EncodedResponseBodyTooLargeError
				require.ErrorAs(t, err, &tooLarge)
				require.EqualValues(t, 65536, tooLarge.Limit)
				require.Nil(t, client.localGeneration)
			} else {
				var tooLarge *SnapshotResponseBodyTooLargeError
				require.ErrorAs(t, err, &tooLarge)
				require.EqualValues(t, 65536, tooLarge.Limit)
				require.Nil(t, client.localGeneration)
			}
			require.EqualValues(t, 1, body.closed.Load())
		})
	}
}

func TestContractDiscovery_CorruptAdmittedCachePreservesPending(t *testing.T) {
	for _, scenario := range []string{"missing cache", "bad digest", "changed and rehashed descriptor", "oversized cache", "changed installed fingerprint"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			_, err = db.Exec(`INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','pending')`)
			require.NoError(t, err)
			caps := discoveryPeopleCapabilities(t)
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/sync/generations/1":
					return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration}), nil
				case "/sync/connect":
					return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
				default:
					t.Fatalf("unexpected request %s", req.URL.Path)
					return nil, nil
				}
			})}
			_, err = client.Attach(ctx, "owner")
			require.NoError(t, err)
			require.NoError(t, client.Close())
			var before string
			require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&before))
			switch scenario {
			case "missing cache":
				_, err = db.Exec(`DELETE FROM _sync_admitted_contracts`)
			case "bad digest":
				_, err = db.Exec(`UPDATE _sync_admitted_contracts SET descriptor_hash=?`, strings.Repeat("0", 64))
			case "changed and rehashed descriptor":
				caps.SchemaGeneration.Tables[0].Columns[1].Nullable = true
				raw, marshalErr := json.Marshal(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration, Limits: testWireLimits()})
				require.NoError(t, marshalErr)
				_, err = db.Exec(`UPDATE _sync_admitted_contracts SET descriptor=?,descriptor_hash=?`, string(raw), contractBytesHash(string(raw)))
			case "oversized cache":
				_, err = db.Exec(`UPDATE _sync_admitted_contracts SET descriptor=zeroblob(65537)`)
			case "changed installed fingerprint":
				_, err = db.Exec(`UPDATE _sync_contract_identity SET fingerprint=?`, strings.Repeat("0", 64))
			}
			require.NoError(t, err)
			reopened, err := newClientFromDB(db, "http://example.invalid", func(context.Context) (string, error) { t.Fatal("cache failure cannot contact server"); return "", nil }, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			})
			require.Nil(t, reopened)
			require.Error(t, err)
			var after, name string
			require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&after))
			require.Equal(t, before, after)
			require.NoError(t, db.QueryRow(`SELECT name FROM people`).Scan(&name))
			require.Equal(t, "pending", name)
		})
	}
}

func TestContractDiscovery_MalformedDescriptorNeverPublishes(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"wrong revision", `"protocol_version":"v1"`, `"protocol_version":"future"`},
		{"duplicate revision", `"protocol_version":"v1"`, `"protocol_version":"v1","protocol_version":"v1"`},
		{"missing revision", `"protocol_version":"v1",`, ``},
		{"wrong selected id", `"id":"1"`, `"id":"2"`},
		{"noncanonical selected id", `"id":"1"`, `"id":"01"`},
		{"unknown wire type", `"wire_type":"text"`, `"wire_type":"custom"`},
		{"null foreign keys", `"foreign_keys":[]`, `"foreign_keys":null`},
		{"wrong nullable type", `"nullable":false`, `"nullable":"false"`},
		{"duplicate column member", `"name":"name"`, `"name":"name","name":"name"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','pending')`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			caps := discoveryPeopleCapabilities(t)
			valid, err := json.Marshal(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration, Limits: testWireLimits()})
			require.NoError(t, err)
			raw := strings.Replace(string(valid), tc.old, tc.replacement, 1)
			require.NotEqual(t, string(valid), raw, "scenario must actually alter the descriptor")
			var before string
			require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&before))
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, "/sync/generations/1", req.URL.Path)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(raw))}, nil
			})}
			_, err = client.Attach(ctx, "owner")
			require.Error(t, err)
			var after, fingerprint, binding, outbox string
			require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&after))
			require.Equal(t, before, after)
			require.NoError(t, db.QueryRow(`SELECT fingerprint FROM _sync_contract_identity`).Scan(&fingerprint))
			require.Empty(t, fingerprint)
			require.NoError(t, db.QueryRow(`SELECT binding_state FROM _sync_attachment_state`).Scan(&binding))
			require.Equal(t, "anonymous", binding)
			require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle`).Scan(&outbox))
			require.Equal(t, "none", outbox)
		})
	}
}

func TestContractDiscovery_UsesDescriptorSchemasForDistinctLocalTables(t *testing.T) {
	for _, scenario := range []string{"distinct schemas", "partial generation", "colliding local names"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL)`)
			require.NoError(t, err)
			columns := []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}, {Name: "name", WireType: oversync.GenerationWireTypeText}}
			secondTable := "notes"
			if scenario == "colliding local names" {
				secondTable = "people"
			}
			registered := []oversync.RegisteredTable{{Schema: "alpha", Table: "people", SyncKeyColumns: []string{"id"}}, {Schema: "beta", Table: secondTable, SyncKeyColumns: []string{"id"}}}
			generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{Schema: "alpha", Table: "people", Columns: columns}, {Schema: "beta", Table: secondTable, Columns: columns}}}
			registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
			require.NoError(t, err)
			compiled, ok := registry.Generation(1)
			require.True(t, ok)
			caps := discoveryPeopleCapabilities(t)
			caps.SchemaGeneration.Fingerprint = compiled.Fingerprint()
			first := caps.SchemaGeneration.Tables[0]
			first.Schema = "alpha"
			second := first
			second.Schema, second.Table = "beta", secondTable
			caps.SchemaGeneration.Tables = []oversync.SchemaGenerationTableCapabilities{first, second}
			tables := []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}, {TableName: "notes", SyncKeyColumns: []string{"id"}}}
			if scenario == "partial generation" {
				tables = tables[:1]
			}
			config := DefaultConfig(1, tables)
			config.DatabaseID, config.ServiceID = "multi-db", "multi-service"
			client, err := newClientFromDB(db, "http://example.invalid", tokenProviderForTests, config)
			require.NoError(t, err)
			defer client.Close()
			require.NoError(t, client.open(ctx))
			connected := false
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/sync/generations/1":
					return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration}), nil
				case "/sync/connect":
					connected = true
					return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
				default:
					t.Fatalf("unexpected path %s", req.URL.Path)
					return nil, nil
				}
			})}
			_, err = client.Attach(ctx, "owner")
			if scenario != "distinct schemas" {
				require.Error(t, err)
				require.False(t, connected)
				require.Nil(t, client.localGeneration)
				return
			}
			require.NoError(t, err)
			require.True(t, connected)
			_, err = db.Exec(`INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','person'); INSERT INTO notes VALUES('22222222-2222-4222-8222-222222222222','note')`)
			require.NoError(t, err)
			for table, schema := range map[string]string{"people": "alpha", "notes": "beta"} {
				var actual string
				require.NoError(t, db.QueryRow(`SELECT schema_name FROM _sync_dirty_rows WHERE table_name=?`, table).Scan(&actual))
				require.Equal(t, schema, actual)
				require.Equal(t, schema, client.schemaForTable(table))
			}
		})
	}
}

func TestContractDiscovery_PublicReadyReopenPreservesDistinctSchemas(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "distinct-schemas.sqlite")
	columns := []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}, {Name: "name", WireType: oversync.GenerationWireTypeText}}
	registered := []oversync.RegisteredTable{{Schema: "alpha", Table: "people", SyncKeyColumns: []string{"id"}}, {Schema: "beta", Table: "notes", SyncKeyColumns: []string{"id"}}}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{Schema: "alpha", Table: "people", Columns: columns}, {Schema: "beta", Table: "notes", Columns: columns}}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, ok := registry.Generation(1)
	require.True(t, ok)
	caps := discoveryPeopleCapabilities(t)
	caps.SchemaGeneration.Fingerprint = compiled.Fingerprint()
	first := caps.SchemaGeneration.Tables[0]
	first.Schema = "alpha"
	second := first
	second.Schema, second.Table = "beta", "notes"
	caps.SchemaGeneration.Tables = []oversync.SchemaGenerationTableCapabilities{first, second}

	tables := []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}, {TableName: "notes", SyncKeyColumns: []string{"id"}}}
	target := DefaultConfig(1, tables)
	target.DatabaseID, target.ServiceID, target.LocalVersion = "multi-db", "multi-service", 1
	plan := UpgradePlan{
		PlanID: "distinct-schema-fresh", Mode: UpgradeModeFresh, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID,
		TargetGenerationID: target.GenerationID, TargetVersion: target.LocalVersion,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, migrateErr := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); PRAGMA user_version=1`)
			return migrateErr
		}},
	}
	opening, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: "http://example.invalid", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	opened, err := opening.Advance(ctx)
	require.NoError(t, err)
	client := opened.Ready
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: "v1", SchemaGeneration: caps.SchemaGeneration}), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", req.URL.Path)
		}
	})}
	_, err = client.Attach(ctx, "owner")
	require.NoError(t, err)
	require.NoError(t, client.Close())

	reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, BaseURL: "http://relocated.invalid", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	result, err := reopened.Advance(ctx)
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	_, err = result.Ready.Database().Exec(`INSERT INTO people VALUES('11111111-1111-4111-8111-111111111111','person'); INSERT INTO notes VALUES('22222222-2222-4222-8222-222222222222','note')`)
	require.NoError(t, err)
	for table, schema := range map[string]string{"people": "alpha", "notes": "beta"} {
		var actual string
		require.NoError(t, result.Ready.Database().QueryRow(`SELECT schema_name FROM _sync_dirty_rows WHERE table_name=?`, table).Scan(&actual))
		require.Equal(t, schema, actual)
		require.Equal(t, schema, result.Ready.schemaForTable(table))
	}
	require.NoError(t, result.Ready.Close())
}

func TestRecoveryProtocol_DiscoveryRequiresOutcomeCeiling(t *testing.T) {
	caps := discoveryPeopleCapabilities(t)
	caps.ProtocolVersion = requiredProtocolVersion
	caps.Limits = testWireLimits()
	raw, err := json.Marshal(caps)
	require.NoError(t, err)
	_, err = decodeDiscoveredContract(raw, 1)
	require.NoError(t, err)
	for _, mutation := range []func(map[string]any){
		func(root map[string]any) { delete(root["limits"].(map[string]any), "max_push_outcome_decoded_bytes") },
		func(root map[string]any) { root["limits"].(map[string]any)["max_push_outcome_decoded_bytes"] = 0 },
		func(root map[string]any) {
			root["limits"].(map[string]any)["max_push_outcome_decoded_bytes"] = 67117057
		},
		func(root map[string]any) { root["protocol_version"] = "v2" },
	} {
		var root map[string]any
		require.NoError(t, json.Unmarshal(raw, &root))
		mutation(root)
		candidate, err := json.Marshal(root)
		require.NoError(t, err)
		_, err = decodeDiscoveredContract(candidate, 1)
		require.Error(t, err)
	}
}
