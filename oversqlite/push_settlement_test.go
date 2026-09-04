// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPushPending_RebasesSameKeyLocalIntentDuringAuthoritativeReplay(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name              string
		seed              func(*testing.T, *Client, *sql.DB, string)
		firstMutation     func(*testing.T, *sql.DB, string)
		secondMutation    func(*testing.T, *sql.DB, string)
		expectedVisible   bool
		expectedName      string
		expectedDeleted   bool
		expectedDirtyOp   string
		expectedDirty     bool
		expectedDirtyName string
		expectedUploadOp  string
	}{
		{
			name: "update to later update",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Second",
			expectedDeleted:   false,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpUpdate,
			expectedDirtyName: "Second",
			expectedUploadOp:  oversync.OpUpdate,
		},
		{
			name: "update to later delete",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:  false,
			expectedDeleted:  false,
			expectedDirty:    true,
			expectedDirtyOp:  oversync.OpDelete,
			expectedUploadOp: oversync.OpUpdate,
		},
		{
			name: "delete to later recreate",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
		{
			name: "insert to later update before replay",
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "First", "first@example.com")
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Second",
			expectedDeleted:   false,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpUpdate,
			expectedDirtyName: "Second",
			expectedUploadOp:  oversync.OpInsert,
		},
		{
			name: "insert to later delete before replay",
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "First", "first@example.com")
				require.NoError(t, err)
			},
			secondMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:  false,
			expectedDeleted:  false,
			expectedDirty:    true,
			expectedDirtyOp:  oversync.OpDelete,
			expectedUploadOp: oversync.OpInsert,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			server := newMockPushServer(t)
			server.pushHook = newFixedVersionPushHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			snapshot, err := client.ensurePushOutboundSnapshot(ctx)
			require.NoError(t, err)
			require.Len(t, snapshot.Rows, 1)
			require.Equal(t, tc.expectedUploadOp, snapshot.Rows[0].Op)

			tc.secondMutation(t, db, rowID)

			committed, err := client.commitPushOutboundSnapshot(ctx, snapshot)
			require.NoError(t, err)
			require.NoError(t, client.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed))

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}

func TestPushPending_RebasesSameKeyLocalIntentMutatedDuringUpload(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name                 string
		seed                 func(*testing.T, *Client, *sql.DB, string)
		firstMutation        func(*testing.T, *sql.DB, string)
		duringUploadMutation func(*testing.T, *sql.DB, string)
		expectedVisible      bool
		expectedName         string
		expectedDeleted      bool
		expectedDirtyOp      string
		expectedDirty        bool
		expectedDirtyName    string
		expectedUploadOp     string
		expectedUploadName   string
	}{
		{
			name: "edit during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:    true,
			expectedName:       "Second",
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpUpdate,
			expectedDirtyName:  "Second",
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "delete during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:    false,
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpDelete,
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "reinsert during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			client.SetBeforePushCommitHook(func(context.Context) error {
				tc.duringUploadMutation(t, db, rowID)
				return nil
			})

			server := newMockPushServer(t)
			server.pushHook = newFixedVersionPushHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			mustPushPending(t, client, ctx)
			require.Len(t, server.pushRequests, 1)
			uploaded := expandCapturedPushRequest(t, client, server.pushRequests[0])
			require.Len(t, uploaded, 1)
			require.Equal(t, tc.expectedUploadOp, uploaded[0].Op)
			if tc.expectedUploadOp != oversync.OpDelete {
				require.Equal(t, tc.expectedUploadName, mustPushPayloadName(t, uploaded[0].Payload))
			}

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}

func TestPushPending_PreservesDirtyExtraEffectKeyDuringSettlement(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	extraID := uuid.NewString()
	seedSyncedUserRow(t, client, db, extraID, "Original Extra", 7)
	uploadedID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users VALUES(?,?,?)`, uploadedID, "Uploaded", "uploaded@example.com")
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		_, err := db.Exec(`UPDATE users SET name='Local Extra',email='local-extra@example.com' WHERE id=?`, extraID)
		return err
	})
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		requestRow := session.Rows[0]
		rows := []oversync.BundleRow{
			{Schema: requestRow.Schema, Table: requestRow.Table, Key: requestRow.Key, Op: requestRow.Op, RowVersion: committedRowVersion, Payload: requestRow.Payload},
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": extraID}, Op: oversync.OpUpdate, RowVersion: committedRowVersion,
				Payload: mustJSONPayload(t, map[string]any{"id": extraID, "name": "Server Extra", "email": "server-extra@example.com"})},
		}
		committed := &mockCommittedBundle{BundleSeq: committedRowVersion, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID, PlannedRowCount: session.RequestRowCount,
			Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}
	report := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	name, _, exists := loadUserForKey(t, db, extraID)
	require.True(t, exists)
	require.Equal(t, "Local Extra", name)
	op, baseVersion, payload, dirty := loadDirtyRowForKey(t, db, rowKeyJSON(extraID))
	require.True(t, dirty)
	require.Equal(t, oversync.OpUpdate, op)
	require.Equal(t, committedRowVersion, baseVersion)
	require.Equal(t, "Local Extra", decodeDirtyPayload(t, payload.String)["name"])
	_, _, _, uploadedDirty := loadDirtyRowForKey(t, db, rowKeyJSON(uploadedID))
	require.False(t, uploadedDirty)
}

func TestPushPending_SettlesBooleanExtraEffectAfterSQLiteRoundTrip(t *testing.T) {
	ctx := context.Background()
	uploadedID := uuid.NewString()
	extraID := uuid.NewString()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "typed_rows", SyncKeyColumnName: "id"},
	}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`, `
		CREATE TABLE typed_rows (id TEXT PRIMARY KEY NOT NULL, enabled_flag INTEGER NOT NULL)
	`)
	registered := []oversync.RegisteredTable{
		{Schema: "main", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "typed_rows", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: "main", Table: "users", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "name", WireType: oversync.GenerationWireTypeText},
			{Name: "email", WireType: oversync.GenerationWireTypeText},
		}},
		{Schema: "main", Table: "typed_rows", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "enabled_flag", WireType: oversync.GenerationWireTypeBoolean},
		}},
	}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	generationContract, ok := registry.Generation(1)
	require.True(t, ok)
	local := LocalGenerationContract{
		Proof: oversync.SchemaGenerationProof{ID: 1, Fingerprint: generationContract.Fingerprint()}, RegisteredTables: registered, Generation: generation,
		Tables: client.config.Tables,
		Columns: []LocalTableColumns{
			{Schema: "main", Table: "users", Columns: []LocalColumnContract{
				{Name: "id", Codec: LocalCodecUUIDText}, {Name: "name", Codec: LocalCodecText}, {Name: "email", Codec: LocalCodecText},
			}},
			{Schema: "main", Table: "typed_rows", Columns: []LocalColumnContract{
				{Name: "id", Codec: LocalCodecUUIDText}, {Name: "enabled_flag", Codec: LocalCodecBooleanInteger},
			}},
		},
	}
	client.localGeneration, err = compileLocalContract(local)
	require.NoError(t, err)
	client.schemaGenerationProof = local.Proof
	discovery := upgradeCapabilitiesValue(local)
	client.discovery = &discovery
	registerMockGeneration(discovery.SchemaGeneration)

	_, err = db.Exec(`INSERT INTO users VALUES (?, 'Uploaded', 'uploaded@example.com')`, uploadedID)
	require.NoError(t, err)

	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		requestRow := session.Rows[0]
		rows := []oversync.BundleRow{
			{Schema: requestRow.Schema, Table: requestRow.Table, Key: requestRow.Key, Op: requestRow.Op, RowVersion: 1, Payload: requestRow.Payload},
			{Schema: "main", Table: "typed_rows", Key: oversync.SyncKey{"id": extraID}, Op: oversync.OpInsert, RowVersion: 1,
				Payload: mustJSONPayload(t, map[string]any{"id": extraID, "enabled_flag": true})},
		}
		committed := &mockCommittedBundle{
			BundleSeq:            1,
			SourceID:             session.SourceID,
			SourceBundleID:       session.SourceBundleID,
			PlannedRowCount:      session.RequestRowCount,
			Rows:                 rows,
			BundleHash:           mustBundleHash(t, rows),
			ByteCount:            mustBundleByteCount(t, rows),
			CanonicalRequestHash: session.CanonicalRequestHash,
		}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}

	report := mustPushPending(t, client, ctx)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	var enabled int64
	require.NoError(t, db.QueryRow(`SELECT enabled_flag FROM typed_rows WHERE id = ?`, extraID).Scan(&enabled))
	require.Equal(t, int64(1), enabled)
	var outboxRows, stagedRows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.Zero(t, outboxRows)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedRows))
	require.Zero(t, stagedRows)
	requireLastBundleSeqSeen(t, client, ctx, 1)
}

func TestSettlementDecision_BooleanEffectMatchesSQLiteRoundTrip(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (id TEXT PRIMARY KEY NOT NULL, enabled_flag INTEGER NOT NULL)
	`)
	contract := cloneLocalGenerationContract(*client.config.localContract)
	for i := range contract.Generation.Tables[0].Columns {
		if contract.Generation.Tables[0].Columns[i].Name == "enabled_flag" {
			contract.Generation.Tables[0].Columns[i].WireType = oversync.GenerationWireTypeBoolean
		}
	}
	for i := range contract.Columns[0].Columns {
		if contract.Columns[0].Columns[i].Name == "enabled_flag" {
			contract.Columns[0].Columns[i].Codec = LocalCodecBooleanInteger
		}
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(contract.RegisteredTables, []oversync.SchemaGeneration{contract.Generation})
	require.NoError(t, err)
	generation, ok := registry.Generation(contract.Generation.ID)
	require.True(t, ok)
	contract.Proof.Fingerprint = generation.Fingerprint()
	client.localGeneration, err = compileLocalContract(contract)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		wireValue  bool
		localValue string
	}{
		{name: "true", wireValue: true, localValue: "1"},
		{name: "false", wireValue: false, localValue: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			effect := stagedPushBundleRow{
				SchemaName: "main",
				TableName:  "typed_rows",
				KeyJSON:    rowKeyJSON("extra"),
				Op:         oversync.OpUpdate,
				Payload: sql.NullString{
					String: string(mustJSONPayload(t, map[string]any{"id": "extra", "enabled_flag": tc.wireValue})),
					Valid:  true,
				},
			}
			livePayload := json.RawMessage(fmt.Sprintf(`{"id":{"storage":"text","value":"extra"},"enabled_flag":{"storage":"integer","value":"%s"}}`, tc.localValue))

			decision, err := client.settlementDecisionForNewerState(effect, livePayload, true)
			require.NoError(t, err)
			require.True(t, decision.skipApply)
			require.True(t, decision.needsRequeue, "a caller-confirmed newer intent survives wire normalization")
			require.Equal(t, oversync.OpUpdate, decision.requeueOp)
		})
	}
}

func TestPushPending_PreservesNewerIntentEqualToNormalizedAuthoritativeEffect(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	rowID := uuid.NewString()
	seedSyncedUserRow(t, client, db, rowID, "Original", 7)
	_, err := db.Exec(`UPDATE users SET name='First',email='first@example.com' WHERE id=?`, rowID)
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		_, err := db.Exec(`UPDATE users SET name='Second',email='second@example.com' WHERE id=?`, rowID)
		return err
	})
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := []oversync.BundleRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": rowID}, Op: oversync.OpUpdate, RowVersion: committedRowVersion,
			Payload: mustJSONPayload(t, map[string]any{"id": rowID, "name": "Second", "email": "second@example.com"})}}
		committed := &mockCommittedBundle{BundleSeq: committedRowVersion, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
			PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}
	mustPushPending(t, client, ctx)
	name, _, exists := loadUserForKey(t, db, rowID)
	require.True(t, exists)
	require.Equal(t, "Second", name)
	_, _, _, dirty := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
	require.True(t, dirty, "newer equal-valued local intent must survive the committed effect")
	rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
	require.True(t, metaExists)
	require.Equal(t, committedRowVersion, rowVersion)
	require.False(t, deleted)
}

func TestPushPending_RestoresPreservedRowsAfterCrossEffectTriggers(t *testing.T) {
	for _, tc := range []struct {
		name                string
		uploadSecond        bool
		newerSecondName     string
		authoritativeSecond string
		wantDirty           bool
	}{
		{name: "uploaded equal-valued newer intent", uploadSecond: true, newerSecondName: "BOB", authoritativeSecond: "BOB", wantDirty: true},
		{name: "dirty extra effect", newerSecondName: "Local Second", authoritativeSecond: "Server Second", wantDirty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			seedSyncedUserRow(t, client, db, "a", "Original A", 7)
			seedSyncedUserRow(t, client, db, "b", "Original B", 7)
			_, err := db.Exec(`
				CREATE TRIGGER mutate_b_after_a_update
				AFTER UPDATE ON users
				WHEN NEW.id = 'a'
				 AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 1
				BEGIN
					UPDATE users SET name = lower(name) WHERE id = 'b';
				END
			`)
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE users SET name='First A',email='first-a@example.com' WHERE id='a'`)
			require.NoError(t, err)
			if tc.uploadSecond {
				_, err = db.Exec(`UPDATE users SET name='First B',email='first-b@example.com' WHERE id='b'`)
				require.NoError(t, err)
			}
			client.SetBeforePushCommitHook(func(context.Context) error {
				_, hookErr := db.Exec(`UPDATE users SET name=?,email='newer-b@example.com' WHERE id='b'`, tc.newerSecondName)
				return hookErr
			})

			server := newMockPushServer(t)
			server.pushHook = func(session *mockDirectPush) *http.Response {
				rows := []oversync.BundleRow{
					{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "a"}, Op: oversync.OpUpdate, RowVersion: 11,
						Payload: mustJSONPayload(t, map[string]any{"id": "a", "name": "BOB", "email": "server-a@example.com"})},
					{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "b"}, Op: oversync.OpUpdate, RowVersion: 11,
						Payload: mustJSONPayload(t, map[string]any{"id": "b", "name": tc.authoritativeSecond, "email": "newer-b@example.com"})},
				}
				committed := &mockCommittedBundle{BundleSeq: 11, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
					PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
				server.committedBySource[session.SourceBundleID] = committed
				server.committedBySeq[committed.BundleSeq] = committed
				return server.compactResponseWithGeneration(session.Generation, committed)
			}
			client.HTTP = &http.Client{Transport: server}

			mustPushPending(t, client, ctx)
			require.Len(t, server.pushRequests, 1)
			uploaded := expandCapturedPushRequest(t, client, server.pushRequests[0])
			if tc.uploadSecond {
				require.Len(t, uploaded, 2)
			} else {
				require.Len(t, uploaded, 1, "the preserved second row must be an extra effect key")
			}
			name, _, exists := loadUserForKey(t, db, "b")
			require.True(t, exists)
			require.Equal(t, tc.newerSecondName, name)
			op, baseVersion, payload, dirty := loadDirtyRowForKey(t, db, rowKeyJSON("b"))
			require.Equal(t, tc.wantDirty, dirty)
			if tc.wantDirty {
				require.Equal(t, oversync.OpUpdate, op)
				require.Equal(t, int64(11), baseVersion)
				require.Equal(t, tc.newerSecondName, decodeDirtyPayload(t, payload.String)["name"])
			}
		})
	}
}

func TestPushPending_RestoresUnrelatedDirtyRowChangedByEffectTrigger(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	seedSyncedUserRow(t, client, db, "a", "Original A", 7)
	seedSyncedUserRow(t, client, db, "c", "Original C", 7)
	_, err := db.Exec(`CREATE TRIGGER mutate_c_after_a_update AFTER UPDATE ON users
		WHEN NEW.id = 'a' AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 1
		BEGIN UPDATE users SET name = 'Trigger C' WHERE id = 'c'; END`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE users SET name='Local A' WHERE id='a'`)
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		_, hookErr := db.Exec(`UPDATE users SET name='Desired C',email='desired-c@example.com' WHERE id='c'`)
		return hookErr
	})
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := []oversync.BundleRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "a"},
			Op: oversync.OpUpdate, RowVersion: 11,
			Payload: mustJSONPayload(t, map[string]any{"id": "a", "name": "Server A", "email": "server-a@example.com"})}}
		committed := &mockCommittedBundle{BundleSeq: 11, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
			PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows),
			ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}
	mustPushPending(t, client, ctx)
	require.Len(t, server.pushRequests, 1)
	require.Len(t, expandCapturedPushRequest(t, client, server.pushRequests[0]), 1)
	name, email, exists := loadUserForKey(t, db, "c")
	require.True(t, exists)
	require.Equal(t, "Desired C", name)
	require.Equal(t, "desired-c@example.com", email)
	op, baseVersion, payload, dirty := loadDirtyRowForKey(t, db, rowKeyJSON("c"))
	require.True(t, dirty)
	require.Equal(t, oversync.OpUpdate, op)
	require.Equal(t, int64(7), baseVersion)
	require.Equal(t, "Desired C", decodeDirtyPayload(t, payload.String)["name"])
}

func TestPushPending_RollsBackWhenCrossEffectTriggerInvalidatesRestoredState(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	seedSyncedUserRow(t, client, db, "a", "Original A", 7)
	seedSyncedUserRow(t, client, db, "b", "Original B", 7)
	_, err := db.Exec(`
		CREATE TRIGGER change_b_after_a_authoritative_update
		AFTER UPDATE ON users
		WHEN NEW.id = 'a'
		 AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 1
		BEGIN
			UPDATE users SET name = 'changed' WHERE id = 'b';
		END;
		CREATE TRIGGER corrupt_b_during_authoritative_apply
		AFTER UPDATE ON users
		WHEN NEW.id = 'b'
		 AND NEW.name = 'Desired B'
		 AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 1
		BEGIN
			UPDATE users SET name = 'corrupted' WHERE id = 'b';
		END
	`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE users SET name='First A',email='first-a@example.com' WHERE id='a'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE users SET name='First B',email='first-b@example.com' WHERE id='b'`)
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		_, hookErr := db.Exec(`UPDATE users SET name='Desired B',email='desired-b@example.com' WHERE id='b'`)
		return hookErr
	})

	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := []oversync.BundleRow{
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "a"}, Op: oversync.OpUpdate, RowVersion: 11,
				Payload: mustJSONPayload(t, map[string]any{"id": "a", "name": "Server A", "email": "server-a@example.com"})},
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "b"}, Op: oversync.OpUpdate, RowVersion: 11,
				Payload: mustJSONPayload(t, map[string]any{"id": "b", "name": "Server B", "email": "server-b@example.com"})},
		}
		committed := &mockCommittedBundle{BundleSeq: 11, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
			PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}

	_, err = client.PushPending(ctx)
	require.ErrorContains(t, err, "changed by cross-row effects")
	nameA, _, exists := loadUserForKey(t, db, "a")
	require.True(t, exists)
	require.Equal(t, "First A", nameA)
	nameB, _, exists := loadUserForKey(t, db, "b")
	require.True(t, exists)
	require.Equal(t, "Desired B", nameB)
	require.Equal(t, outboxStatePrepared, requireOutboxBundle(t, db).State)
	var outboxRows, stagedRows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.Equal(t, 2, outboxRows)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedRows))
	require.Equal(t, 2, stagedRows)
	requireLastBundleSeqSeen(t, client, ctx, 0)
}

func TestPushPending_VerifiesEveryAuthoritativeEffectAfterCrossRowTriggers(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	seedSyncedUserRow(t, client, db, "a", "Original A", 7)
	seedSyncedUserRow(t, client, db, "b", "Original B", 7)
	originalA := baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("a"))
	originalProvenance := baselineText(t, db, `SELECT server_provenance FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("a"))
	_, err := db.Exec(`
		CREATE TRIGGER corrupt_a_after_b_authoritative_update
		AFTER UPDATE ON users
		WHEN NEW.id = 'b'
		 AND NEW.name = 'Server B'
		BEGIN
			UPDATE users SET name = 'corrupted' WHERE id = 'a';
		END
	`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE users SET name='Local A',email='local-a@example.com' WHERE id='a'`)
	require.NoError(t, err)

	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := []oversync.BundleRow{
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "a"}, Op: oversync.OpUpdate, RowVersion: 11,
				Payload: mustJSONPayload(t, map[string]any{"id": "a", "name": "Server A", "email": "server-a@example.com"})},
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "b"}, Op: oversync.OpUpdate, RowVersion: 11,
				Payload: mustJSONPayload(t, map[string]any{"id": "b", "name": "Server B", "email": "server-b@example.com"})},
		}
		committed := &mockCommittedBundle{BundleSeq: 11, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
			PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[committed.BundleSeq] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}

	frozen, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, frozen.Rows, 1)
	require.Equal(t, "present", frozen.Rows[0].BaseKind)
	require.Equal(t, int64(7), frozen.Rows[0].BaseRowVersion)
	require.JSONEq(t, originalA, frozen.Rows[0].BasePayload.String)
	require.JSONEq(t, originalProvenance, frozen.Rows[0].BaseProvenance)
	require.JSONEq(t, originalA, baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("a")))
	localWire := `{"id":"a","name":"Local A","email":"local-a@example.com"}`
	localCapture, err := client.captureAuthoritativePayload(ctx, db, "users", []byte(localWire))
	require.NoError(t, err)
	require.JSONEq(t, localCapture, frozen.Rows[0].LocalPayload.String)
	requestRows, err := client.buildPushRequestRows(frozen.Rows)
	require.NoError(t, err)
	require.JSONEq(t, localWire, string(requestRows[0].Payload))
	require.Equal(t, string(requestRows[0].Payload), baselineText(t, db, `SELECT wire_payload FROM _sync_outbox_rows`))
	expectedHash, err := computeGenerationPinnedPushRequestHash(client.schemaGenerationProof, requestRows)
	require.NoError(t, err)
	originalSource := client.sourceID
	originalHash := baselineText(t, db, `SELECT canonical_request_hash FROM _sync_outbox_bundle`)
	require.Equal(t, expectedHash, originalHash)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	for key, payload := range map[string]string{
		"a": `{"id":"a","name":"Server A","email":"server-a@example.com"}`,
		"b": `{"id":"b","name":"Server B","email":"server-b@example.com"}`,
	} {
		var name, email string
		require.NoError(t, db.QueryRow(`SELECT name,email FROM users WHERE id=?`, key).Scan(&name, &email))
		require.JSONEq(t, payload, string(mustJSONPayload(t, map[string]any{"id": key, "name": name, "email": email})))
		var kind, captured string
		var version int64
		require.NoError(t, db.QueryRow(`SELECT server_kind,row_version,server_payload FROM _sync_row_state WHERE key_json=?`, rowKeyJSON(key)).Scan(&kind, &version, &captured))
		require.Equal(t, "live", kind)
		require.Equal(t, int64(11), version)
		expectedCapture, err := client.captureAuthoritativePayload(ctx, db, "users", []byte(payload))
		require.NoError(t, err)
		require.JSONEq(t, expectedCapture, captured)
	}
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_push_stage", "_sync_relationships", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan"} {
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM `+table).Scan(&count))
		require.Zero(t, count, table)
	}
	require.Equal(t, outboxStateNone, requireOutboxBundle(t, db).State)
	pending, err := client.PendingConflict(ctx)
	require.NoError(t, err)
	require.Nil(t, pending)
	// Settlement cannot skip unseen pull bundles by advancing the pull checkpoint.
	requireLastBundleSeqSeen(t, client, ctx, 0)
	require.Equal(t, originalSource, client.sourceID)
	var nextID int64
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, originalSource).Scan(&nextID))
	require.Equal(t, frozen.SourceBundleID+1, nextID)
	committed := server.committedBySource[frozen.SourceBundleID]
	require.Equal(t, originalHash, committed.CanonicalRequestHash)
	require.Equal(t, mustBundleHash(t, committed.Rows), committed.BundleHash)
	require.Len(t, committed.Rows, 2)
	before := vectorState(t, client)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, before, vectorState(t, client), "settled retry must not advance IDs or mutate A/B/L")
	require.Len(t, server.pushRequests, 1)
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
}

func TestPushPending_ClearsUncapturedNoOpAcrossWireNormalization(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "files", SyncKeyColumnName: "id"}}, `
		CREATE TABLE files (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL)
	`)
	rowID := uuid.NewString()
	wantData := []byte{0x00, 0x01, 0xfe, 0xff}
	_, err := db.Exec(`INSERT INTO files (id, name, data) VALUES (?, 'Binary', ?)`, rowID, wantData)
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		_, err := db.Exec(`UPDATE files SET data = data WHERE id = ?`, rowID)
		return err
	})
	client.HTTP = &http.Client{Transport: newMockPushServer(t)}

	mustPushPending(t, client, ctx)
	var gotData []byte
	require.NoError(t, db.QueryRow(`SELECT data FROM files WHERE id = ?`, rowID).Scan(&gotData))
	require.Equal(t, wantData, gotData)
	_, _, _, dirty := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
	require.False(t, dirty, "an update that produces no captured dirty intent is already satisfied")
}

func TestPushPending_PreservesNewerAbsentIntentAcrossAuthoritativeTombstone(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)
	`)
	rowID := uuid.NewString()
	seedSyncedUserRow(t, client, db, rowID, "Original", 7)
	_, err := db.Exec(`DELETE FROM users WHERE id = ?`, rowID)
	require.NoError(t, err)
	client.SetBeforePushCommitHook(func(context.Context) error {
		if _, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, 'Transient', 'transient@example.com')`, rowID); err != nil {
			return err
		}
		_, err := db.Exec(`DELETE FROM users WHERE id = ?`, rowID)
		return err
	})
	server := newMockPushServer(t)
	server.pushHook = newFixedVersionPushHook(t, server, committedRowVersion)
	client.HTTP = &http.Client{Transport: server}

	mustPushPending(t, client, ctx)
	_, _, exists := loadUserForKey(t, db, rowID)
	require.False(t, exists)
	_, _, _, dirty := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
	require.True(t, dirty, "a newer delete intent must survive an equal authoritative tombstone")
	rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
	require.True(t, metaExists)
	require.Equal(t, committedRowVersion, rowVersion)
	require.True(t, deleted)
}

func TestPushPending_RebasesSameKeyLocalIntentAfterRestartBeforeReplay(t *testing.T) {
	ctx := context.Background()
	const committedRowVersion int64 = 11

	cases := []struct {
		name                 string
		seed                 func(*testing.T, *Client, *sql.DB, string)
		firstMutation        func(*testing.T, *sql.DB, string)
		duringUploadMutation func(*testing.T, *sql.DB, string)
		expectedVisible      bool
		expectedName         string
		expectedDeleted      bool
		expectedDirtyOp      string
		expectedDirty        bool
		expectedDirtyName    string
		expectedUploadOp     string
		expectedUploadName   string
	}{
		{
			name: "edit during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "Second", "second@example.com", id)
				require.NoError(t, err)
			},
			expectedVisible:    true,
			expectedName:       "Second",
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpUpdate,
			expectedDirtyName:  "Second",
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "delete during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`UPDATE users SET name = ?, email = ? WHERE id = ?`, "First", "first@example.com", id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			expectedVisible:    false,
			expectedDeleted:    false,
			expectedDirty:      true,
			expectedDirtyOp:    oversync.OpDelete,
			expectedUploadOp:   oversync.OpUpdate,
			expectedUploadName: "First",
		},
		{
			name: "reinsert during upload",
			seed: func(t *testing.T, client *Client, db *sql.DB, id string) {
				seedSyncedUserRow(t, client, db, id, "Original", 7)
			},
			firstMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`DELETE FROM users WHERE id = ?`, id)
				require.NoError(t, err)
			},
			duringUploadMutation: func(t *testing.T, db *sql.DB, id string) {
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, "Recreated", "recreated@example.com")
				require.NoError(t, err)
			},
			expectedVisible:   true,
			expectedName:      "Recreated",
			expectedDeleted:   true,
			expectedDirty:     true,
			expectedDirtyOp:   oversync.OpInsert,
			expectedDirtyName: "Recreated",
			expectedUploadOp:  oversync.OpDelete,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
				CREATE TABLE users (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				)
			`)

			rowID := uuid.NewString()
			if tc.seed != nil {
				tc.seed(t, client, db, rowID)
			}
			tc.firstMutation(t, db, rowID)

			client.SetBeforePushCommitHook(func(context.Context) error {
				tc.duringUploadMutation(t, db, rowID)
				return nil
			})
			client.SetBeforePushReplayHook(func(context.Context) error {
				return fmt.Errorf("simulated crash before replay")
			})

			server := newMockPushServer(t)
			server.pushHook = newFixedVersionPushHook(t, server, committedRowVersion)
			client.HTTP = &http.Client{Transport: server}

			_, err := client.PushPending(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "simulated crash before replay")
			require.Len(t, server.pushRequests, 1)
			uploaded := expandCapturedPushRequest(t, client, server.pushRequests[0])
			require.Len(t, uploaded, 1)
			require.Equal(t, tc.expectedUploadOp, uploaded[0].Op)
			if tc.expectedUploadOp != oversync.OpDelete {
				require.Equal(t, tc.expectedUploadName, mustPushPayloadName(t, uploaded[0].Payload))
			}

			require.NoError(t, client.Close())

			restarted, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
				return "token", nil
			}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, restarted.Close()) })
			restarted.HTTP = &http.Client{Transport: server}
			mustOpen(t, restarted, ctx)
			result, err := restarted.Attach(ctx, "bundle-user")
			require.NoError(t, err)
			require.Equal(t, AttachStatusConnected, result.Status)
			mustPushPending(t, restarted, ctx)
			require.Len(t, server.pushRequests, 2, "restart replays the direct request before applying its authoritative response")

			name, _, exists := loadUserForKey(t, db, rowID)
			require.Equal(t, tc.expectedVisible, exists)
			if tc.expectedVisible {
				require.Equal(t, tc.expectedName, name)
			}

			rowVersion, deleted, metaExists := loadUserRowStateForKey(t, db, rowKeyJSON(rowID))
			require.True(t, metaExists)
			require.Equal(t, committedRowVersion, rowVersion)
			require.Equal(t, tc.expectedDeleted, deleted)

			op, baseRowVersion, payload, dirtyExists := loadDirtyRowForKey(t, db, rowKeyJSON(rowID))
			require.Equal(t, tc.expectedDirty, dirtyExists)
			if tc.expectedDirty {
				require.Equal(t, tc.expectedDirtyOp, op)
				require.Equal(t, committedRowVersion, baseRowVersion)
				if tc.expectedDirtyOp == oversync.OpDelete {
					require.False(t, payload.Valid)
				} else {
					require.True(t, payload.Valid)
					payloadObj := decodeDirtyPayload(t, payload.String)
					require.Equal(t, tc.expectedDirtyName, payloadObj["name"])
				}
			}
		})
	}
}
