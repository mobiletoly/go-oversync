// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const generationOneUsersDDL = `
	CREATE TABLE users (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		email TEXT NOT NULL
	)
`

func generationProjectionDeclarations(schema string) []oversync.SchemaGeneration {
	text := func(name string, nullable bool) oversync.GenerationColumn {
		return oversync.GenerationColumn{Name: name, WireType: oversync.GenerationWireTypeText, Nullable: nullable}
	}
	uuidColumn := func(name string, nullable bool) oversync.GenerationColumn {
		return oversync.GenerationColumn{Name: name, WireType: oversync.GenerationWireTypeUUID, Nullable: nullable}
	}
	oldUsers := oversync.GenerationTable{Schema: schema, Table: "users", Columns: []oversync.GenerationColumn{
		uuidColumn("id", false), text("name", false), text("email", false),
	}}
	newUsers := oversync.GenerationTable{Schema: schema, Table: "users", Columns: []oversync.GenerationColumn{
		text("created_at", false), text("email", false), uuidColumn("id", false), text("name", false), text("updated_at", false),
	}}
	newPosts := oversync.GenerationTable{Schema: schema, Table: "posts", Columns: []oversync.GenerationColumn{
		uuidColumn("author_id", true), text("content", false), text("created_at", false), uuidColumn("id", false), text("title", false), text("updated_at", false),
	}, ForeignKeys: []oversync.GenerationForeignKey{{
		Columns: []string{"author_id"}, ReferencedSchema: schema, ReferencedTable: "users", ReferencedColumns: []string{"id"},
		OnDelete: oversync.GenerationForeignKeyNoAction, OnUpdate: oversync.GenerationForeignKeyNoAction,
	}}}
	return []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{oldUsers}}, {ID: 2, Tables: []oversync.GenerationTable{newUsers, newPosts}}}
}

func generationProjectionProof(t *testing.T, schema string, generations []oversync.SchemaGeneration, id oversync.SchemaGenerationID) oversync.SchemaGenerationProof {
	t.Helper()
	registry, err := oversync.CompileSchemaGenerationRegistry(exampleserver.RegisteredTablesForBusinessSchema(schema), generations)
	require.NoError(t, err)
	contract, ok := registry.Generation(id)
	require.True(t, ok)
	return oversync.SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}
}

func generationProjectionConfig(t *testing.T, schema string, generations []oversync.SchemaGeneration, id oversync.SchemaGenerationID, tables []oversqlite.SyncTable) *oversqlite.Config {
	t.Helper()
	config := oversqlite.DefaultConfig(id, tables)
	config.DatabaseID, config.ServiceID = "projection-client", schema
	return config
}

func TestEndToEnd_TwoFixedGenerationsConvergeThroughProjectedHTTPDataPlane(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_projection_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			users := pgx.Identifier{schema, "users"}.Sanitize()
			posts := pgx.Identifier{schema, "posts"}.Sanitize()
			schemaID := pgx.Identifier{schema}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE FUNCTION %s.generation_project_user() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					NEW.name := upper(NEW.name);
					RETURN NEW;
				END $$;
				CREATE TRIGGER generation_project_user BEFORE INSERT OR UPDATE ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.generation_project_user();
				CREATE FUNCTION %s.generation_add_post() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					INSERT INTO %s(_sync_scope_id,id,title,content,author_id)
					VALUES (NEW._sync_scope_id,NEW.id,'generated','hidden from generation 1',NEW.id);
					RETURN NEW;
				END $$;
				CREATE TRIGGER generation_add_post AFTER INSERT ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.generation_add_post()
			`, schemaID, users, schemaID, schemaID, posts, users, schemaID))
			return err
		}
	})
	oldConfig := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	newConfig := generationProjectionConfig(t, schema, generations, 2, syncTables("users", "posts"))
	userID := "e2e-generation-user-" + uuid.NewString()
	oldClient, oldDB := newSQLiteClient(t, server, userID, "old-device", oldConfig, generationOneUsersDDL)
	newClient, newDB := newSQLiteClient(t, server, userID, "new-device", newConfig, usersDDL, postsDDL)

	rowID := uuid.NewString()
	_, err := oldDB.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "Bob", "bob@example.com")
	require.NoError(t, err)
	push := mustPushPendingE2E(t, oldClient, ctx)
	require.Equal(t, oversqlite.PushOutcomeCommitted, push.Outcome)

	var oldName string
	require.NoError(t, oldDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&oldName))
	require.Equal(t, "BOB", oldName, "the fixed old-generation client applies the normalized visible effect")
	var dirty int
	require.NoError(t, oldDB.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Zero(t, dirty)

	mustPullToStableE2E(t, newClient, ctx)
	var newName, generatedContent string
	require.NoError(t, newDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&newName))
	require.NoError(t, newDB.QueryRow(`SELECT content FROM posts WHERE id=?`, rowID).Scan(&generatedContent))
	require.Equal(t, "BOB", newName)
	require.Equal(t, "hidden from generation 1", generatedContent)

	var canonicalRows int64
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT row_count FROM sync.bundle_log
		WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1)
		ORDER BY bundle_seq DESC LIMIT 1
	`, userID).Scan(&canonicalRows))
	require.Equal(t, int64(2), canonicalRows, "one immutable canonical commit contains the normalized user and trigger effect")

	var oldPostTable sql.NullString
	err = oldDB.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='posts'`).Scan(&oldPostTable)
	require.ErrorIs(t, err, sql.ErrNoRows)
	oldCheckpoint, err := oldClient.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	newCheckpoint, err := newClient.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, oldCheckpoint, newCheckpoint)
}

func TestEndToEnd_EmptyProjectedPullAdvancesDurableCheckpointAcrossRestart(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_empty_pull_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
	})
	oldConfig := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	newConfig := generationProjectionConfig(t, schema, generations, 2, syncTables("users", "posts"))
	userID := "e2e-empty-pull-user-" + uuid.NewString()
	databasePath := filepath.Join(t.TempDir(), "empty-pull.sqlite")
	token := func(context.Context) (string, error) { return server.GenerateToken(userID, time.Hour) }

	openOldClient := func(createSchema bool) (*oversqlite.Client, *sql.DB) {
		var client *oversqlite.Client
		if createSchema {
			client = openFreshSQLiteClient(t, databasePath, server.URL(), token, oldConfig, generationOneUsersDDL)
		} else {
			client = openReadySQLiteClient(t, databasePath, server.URL(), token, oldConfig)
		}
		attached, err := client.Attach(ctx, userID)
		require.NoError(t, err)
		require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
		return client, client.Database()
	}

	oldClient, oldDB := openOldClient(true)
	rowID := uuid.NewString()
	_, err := oldDB.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "Visible", "visible@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, oldClient, ctx)
	checkpointBeforeEmpty, err := oldClient.LastBundleSeqSeen(ctx)
	require.NoError(t, err)

	newClient, newDB := newSQLiteClient(t, server, userID, "new-empty-device", newConfig, usersDDL, postsDDL)
	mustPullToStableE2E(t, newClient, ctx)
	postID := uuid.NewString()
	_, err = newDB.Exec(`INSERT INTO posts(id,title,content,author_id) VALUES(?,?,?,?)`, postID, "new-only", "hidden", rowID)
	require.NoError(t, err)
	mustPushPendingE2E(t, newClient, ctx)

	_, err = oldClient.PullToStable(ctx)
	require.NoError(t, err)
	checkpointAfterEmpty, err := oldClient.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Greater(t, checkpointAfterEmpty, checkpointBeforeEmpty)
	var oldUserCount int
	require.NoError(t, oldDB.QueryRow(`SELECT count(*) FROM users`).Scan(&oldUserCount))
	require.Equal(t, 1, oldUserCount)
	require.NoError(t, oldClient.Close())
	require.NoError(t, oldDB.Close())

	restarted, restartedDB := openOldClient(false)
	defer func() { require.NoError(t, restarted.Close()); require.NoError(t, restartedDB.Close()) }()
	durableCheckpoint, err := restarted.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, checkpointAfterEmpty, durableCheckpoint)
	_, err = restarted.PullToStable(ctx)
	require.NoError(t, err)
}

func TestEndToEnd_LostSuccessfulCommitReplyRestartsSameFileAndPreservesNewerEdit(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_lost_reply_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			users := pgx.Identifier{schema, "users"}.Sanitize()
			schemaID := pgx.Identifier{schema}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE FUNCTION %s.generation_normalize_user() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN NEW.name := upper(NEW.name); RETURN NEW; END $$;
				CREATE TRIGGER generation_normalize_user BEFORE INSERT OR UPDATE ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.generation_normalize_user()
			`, schemaID, users, schemaID))
			return err
		}
	})
	config := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	userID := "e2e-lost-success-user-" + uuid.NewString()
	databasePath := filepath.Join(t.TempDir(), "same-client.sqlite")

	openClient := func(createSchema bool) (*oversqlite.Client, *sql.DB) {
		token := func(context.Context) (string, error) { return server.GenerateToken(userID, time.Hour) }
		var client *oversqlite.Client
		if createSchema {
			client = openFreshSQLiteClient(t, databasePath, server.URL(), token, config, generationOneUsersDDL)
		} else {
			client = openReadySQLiteClient(t, databasePath, server.URL(), token, config)
		}
		return client, client.Database()
	}

	client, db := openClient(true)
	var err error
	attached, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "Bob", "bob@example.com")
	require.NoError(t, err)

	lostCommitReply := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		if lostCommitReply && request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			lostCommitReply = false
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, nil
	})}
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, lostCommitReply, "the server processed the commit before its reply was discarded")
	var serverName string
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID).Scan(&serverName))
	require.Equal(t, "BOB", serverName)

	// This edit occurs after the original outbox was frozen. Even though replay
	// later returns BOB, settlement must preserve Carol and rebase its dirty row.
	_, err = db.Exec(`UPDATE users SET name='Carol' WHERE id=?`, rowID)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	restarted, restartedDB := openClient(false)
	defer func() { require.NoError(t, restarted.Close()); require.NoError(t, restartedDB.Close()) }()
	attached, err = restarted.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	restarted.HTTP = &http.Client{Transport: http.DefaultTransport}
	recovered := mustPushPendingE2E(t, restarted, ctx)
	require.Equal(t, oversqlite.PushOutcomeCommitted, recovered.Outcome)

	var localName, dirtyOp string
	var baseVersion int64
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "Carol", localName)
	require.NoError(t, restartedDB.QueryRow(`
		SELECT op,base_row_version FROM _sync_dirty_rows
		WHERE table_name='users' AND key_json=?
	`, rowKeyJSON(rowID)).Scan(&dirtyOp, &baseVersion))
	require.Equal(t, "UPDATE", dirtyOp)
	require.Positive(t, baseVersion)
	var outboxState string
	require.NoError(t, restartedDB.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState))
	require.Equal(t, "none", outboxState)
}

func TestEndToEnd_LostNormalizedUpdateReplyReplaysImmutableEffectsAndSettlesClean(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_update_replay_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			schemaID := pgx.Identifier{schema}.Sanitize()
			users := pgx.Identifier{schema, "users"}.Sanitize()
			posts := pgx.Identifier{schema, "posts"}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE FUNCTION %s.normalize_updated_user() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN NEW.name := upper(NEW.name); RETURN NEW; END $$;
				CREATE TRIGGER normalize_updated_user BEFORE UPDATE ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.normalize_updated_user();
				CREATE FUNCTION %s.record_hidden_update() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					INSERT INTO %s(_sync_scope_id,id,title,content,author_id)
					VALUES(NEW._sync_scope_id,NEW.id,'generated',NEW.name,NEW.id)
					ON CONFLICT(_sync_scope_id,id) DO UPDATE SET content=EXCLUDED.content;
					RETURN NEW;
				END $$;
				CREATE TRIGGER record_hidden_update AFTER UPDATE ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.record_hidden_update();
			`, schemaID, users, schemaID, schemaID, posts, users, schemaID))
			return err
		}
	})
	config := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	userID := "e2e-update-replay-user-" + uuid.NewString()
	databasePath := filepath.Join(t.TempDir(), "normalized-update.sqlite")
	openClient := func(createSchema bool) (*oversqlite.Client, *sql.DB) {
		token := func(context.Context) (string, error) { return server.GenerateToken(userID, time.Hour) }
		var client *oversqlite.Client
		if createSchema {
			client = openFreshSQLiteClient(t, databasePath, server.URL(), token, config, generationOneUsersDDL)
		} else {
			client = openReadySQLiteClient(t, databasePath, server.URL(), token, config)
		}
		return client, client.Database()
	}

	client, db := openClient(true)
	attached, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users VALUES(?,?,?)`, rowID, "Alice", "alice@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, client, ctx)
	_, err = db.Exec(`UPDATE users SET name='Bob' WHERE id=?`, rowID)
	require.NoError(t, err)

	lostCommitReply := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		if lostCommitReply && request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			lostCommitReply = false
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, nil
	})}
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, lostCommitReply)
	var committedSeq, canonicalRows int64
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1`, userID).Scan(&committedSeq))
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT row_count FROM sync.bundle_log WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) AND bundle_seq=$2`, userID, committedSeq).Scan(&canonicalRows))
	require.Equal(t, int64(2), canonicalRows, "normalized UPDATE and hidden post effect share one canonical bundle")
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	serverActor := oversync.Actor{UserID: userID, SourceID: "later-server-write"}
	err = server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{SourceID: serverActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.users SET name='Zoe' WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID)
		return err
	})
	require.NoError(t, err)
	var laterSeq int64
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1`, userID).Scan(&laterSeq))
	require.Greater(t, laterSeq, committedSeq)

	restarted, restartedDB := openClient(false)
	defer func() { require.NoError(t, restarted.Close()); require.NoError(t, restartedDB.Close()) }()
	attached, err = restarted.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	recovered := mustPushPendingE2E(t, restarted, ctx)
	require.Equal(t, oversqlite.PushOutcomeCommitted, recovered.Outcome)
	var localName string
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "BOB", localName, "replay applies the immutable original committed effect, not current server state")
	var dirty, outbox int
	require.NoError(t, restartedDB.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.NoError(t, restartedDB.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outbox))
	require.Zero(t, dirty)
	require.Zero(t, outbox)
	mustPullToStableE2E(t, restarted, ctx)
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "ZOE", localName)
}

func TestEndToEnd_PrunedPreparedDirectPushFailsClosedBeforeSnapshotRebuild(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_pruned_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			users := pgx.Identifier{schema, "users"}.Sanitize()
			schemaID := pgx.Identifier{schema}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE FUNCTION %s.generation_normalize_pruned_user() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN NEW.name := upper(NEW.name); RETURN NEW; END $$;
				CREATE TRIGGER generation_normalize_pruned_user BEFORE INSERT OR UPDATE ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.generation_normalize_pruned_user()
			`, schemaID, users, schemaID))
			return err
		}
	})
	config := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	userID := "e2e-pruned-receipt-user-" + uuid.NewString()
	databasePath := filepath.Join(t.TempDir(), "pruned-receipt.sqlite")
	token := func(context.Context) (string, error) { return server.GenerateToken(userID, time.Hour) }
	openClient := func(createSchema bool) (*oversqlite.Client, *sql.DB) {
		var client *oversqlite.Client
		if createSchema {
			client = openFreshSQLiteClient(t, databasePath, server.URL(), token, config, generationOneUsersDDL)
		} else {
			client = openReadySQLiteClient(t, databasePath, server.URL(), token, config)
		}
		return client, client.Database()
	}

	client, db := openClient(true)
	attached, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "Bob", "bob@example.com")
	require.NoError(t, err)

	failEffectFetch := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		if failEffectFetch && request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			failEffectFetch = false
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, nil
	})}
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, failEffectFetch)
	var outboxState string
	var sourceBundleID int64
	require.NoError(t, db.QueryRow(`SELECT state,source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState, &sourceBundleID))
	require.Equal(t, "prepared", outboxState)
	require.Positive(t, sourceBundleID)
	receiptSeq := sourceBundleID
	frozenBeforeClose := durableRecoveryVectorE2E(t, db)
	requireFrozenRebuildRefusalE2E(t, client, db)
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	// Retained C is independent of pruned history. Resolve and acknowledge its
	// original effect first; only a subsequent clean snapshot may install Dana.
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	err = server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{SourceID: serverActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.users SET name='Dana' WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID)
		return err
	})
	require.NoError(t, err)
	var currentBundleSeq int64
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1`, userID).Scan(&currentBundleSeq))
	require.Greater(t, currentBundleSeq, receiptSeq)
	_, err = server.Pool.Exec(ctx, `
		WITH target AS (SELECT user_pk FROM sync.user_state WHERE user_id=$1)
		UPDATE sync.user_state SET retained_bundle_floor=$2 WHERE user_pk=(SELECT user_pk FROM target)
	`, userID, receiptSeq)
	require.NoError(t, err)
	_, err = server.Pool.Exec(ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) AND bundle_seq=$2
	`, userID, receiptSeq)
	require.NoError(t, err)

	restarted, restartedDB := openClient(false)
	defer func() { require.NoError(t, restarted.Close()); require.NoError(t, restartedDB.Close()) }()
	require.Equal(t, frozenBeforeClose, durableRecoveryVectorE2E(t, restartedDB))
	attached, err = restarted.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	requireFrozenRebuildRefusalE2E(t, restarted, restartedDB)
	settled, err := restarted.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, settled.Outcome)
	var localName string
	var checkpoint, journals int64
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "BOB", localName, "original C is verified and installed even after history is pruned")
	require.NoError(t, restartedDB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	require.Equal(t, receiptSeq, checkpoint, "newer Dana is not attributed to the original operation")
	require.NoError(t, restartedDB.QueryRow(`SELECT count(*) FROM _sync_upload_journal`).Scan(&journals))
	require.Zero(t, journals)
	require.NoError(t, restartedDB.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState))
	require.Equal(t, "none", outboxState)
	_, err = restarted.Rebuild(ctx)
	require.NoError(t, err)
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "DANA", localName)
	require.NoError(t, restartedDB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	require.Equal(t, currentBundleSeq, checkpoint)
}

func TestEndToEnd_LostCoverageRejectionReplyRestartsAndRejectsWithoutSideEffects(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_generation_coverage_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	generations := generationProjectionDeclarations(schema)
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = generations
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			users := pgx.Identifier{schema, "users"}.Sanitize()
			categories := pgx.Identifier{schema, "categories"}.Sanitize()
			schemaID := pgx.Identifier{schema}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE FUNCTION %s.suppress_user_with_hidden_effect() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					INSERT INTO %s(_sync_scope_id,id,name,parent_id)
					VALUES (NEW._sync_scope_id,NEW.id,'must rollback',NULL);
					RETURN NULL;
				END $$;
				CREATE TRIGGER suppress_user_with_hidden_effect BEFORE INSERT ON %s
				FOR EACH ROW EXECUTE FUNCTION %s.suppress_user_with_hidden_effect()
			`, schemaID, categories, users, schemaID))
			return err
		}
	})
	config := generationProjectionConfig(t, schema, generations, 1, syncTables("users"))
	config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	userID := "e2e-coverage-user-" + uuid.NewString()
	databasePath := filepath.Join(t.TempDir(), "coverage-rejection.sqlite")
	token := func(context.Context) (string, error) { return server.GenerateToken(userID, time.Hour) }
	openClient := func(createSchema bool) (*oversqlite.Client, *sql.DB) {
		var client *oversqlite.Client
		if createSchema {
			client = openFreshSQLiteClient(t, databasePath, server.URL(), token, config, generationOneUsersDDL)
		} else {
			client = openReadySQLiteClient(t, databasePath, server.URL(), token, config)
		}
		return client, client.Database()
	}

	client, db := openClient(true)
	attached, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "Bob", "bob@example.com")
	require.NoError(t, err)

	lostRejection := true
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		if lostRejection && request.Method == http.MethodPost && request.URL.Path == "/sync/push" && response.StatusCode == http.StatusUnprocessableEntity {
			lostRejection = false
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, nil
	})}
	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.False(t, lostRejection)
	var outboxState string
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState))
	require.Equal(t, "prepared", outboxState)
	frozenBeforeClose := durableRecoveryVectorE2E(t, db)
	requireFrozenRebuildRefusalE2E(t, client, db)
	var retained int
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT count(*) FROM sync.push_outcomes WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1)`, userID).Scan(&retained))
	require.Zero(t, retained, "effect-coverage admission failure is not a retained terminal rejection")
	require.NoError(t, client.Close())
	require.NoError(t, db.Close())

	restarted, restartedDB := openClient(false)
	defer func() { require.NoError(t, restarted.Close()); require.NoError(t, restartedDB.Close()) }()
	require.Equal(t, frozenBeforeClose, durableRecoveryVectorE2E(t, restartedDB))
	attached, err = restarted.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, attached.Status)
	cancelled, err := restarted.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeNoChange, cancelled.Outcome, "resolve cancels the original uncommitted tuple and restores its intent")
	var dirty, journals int
	require.NoError(t, restartedDB.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Equal(t, 1, dirty)
	require.NoError(t, restartedDB.QueryRow(`SELECT count(*) FROM _sync_upload_journal`).Scan(&journals))
	require.Zero(t, journals)
	var rejectionCode string
	var rejectionStatus int
	observedHTTP := *restarted.HTTP
	baseTransport := observedHTTP.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	observedHTTP.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := baseTransport.RoundTrip(request)
		if err == nil && request.URL.Path == "/sync/push" {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			var decoded oversync.ErrorResponse
			if err := json.Unmarshal(body, &decoded); err != nil {
				return nil, err
			}
			rejectionCode, rejectionStatus = decoded.Error, response.StatusCode
		}
		return response, err
	})
	restarted.HTTP = &observedHTTP
	_, err = restarted.PushPending(ctx)
	require.ErrorContains(t, err, "status 422")
	require.Equal(t, http.StatusUnprocessableEntity, rejectionStatus)
	require.Equal(t, "push_effect_coverage_incomplete", rejectionCode)
	// This admission failure still cannot masquerade as terminal C. Its new
	// frozen tuple remains intact and snapshot remains forbidden.
	requireFrozenRebuildRefusalE2E(t, restarted, restartedDB)

	var localName string
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&localName))
	require.Equal(t, "Bob", localName)
	require.NoError(t, restartedDB.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&outboxState))
	require.Equal(t, "prepared", outboxState)
	var users, categories, bundles int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID).Scan(&users))
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.categories WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID).Scan(&categories))
	require.NoError(t, server.Pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1)`, userID).Scan(&bundles))
	require.Zero(t, users)
	require.Zero(t, categories)
	require.Zero(t, bundles)
}

func TestEndToEnd_ProductionProjectionEncoderAndReaderCarryEscapingWorstCasePull(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_projection_large_pull_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-large-pull-user-" + uuid.NewString()
	client, db := newSQLiteClient(t, server, userID, "large-pull-client", defaultOversqliteConfig(schema, syncTables("users")), usersDDL)
	largeName := strings.Repeat("<", 512) + ">&\u2028\u2029\"\\" + string([]byte{1})
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-large-pull"}
	err := server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{SourceID: serverActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		for index := 0; index < 96; index++ {
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id,name,email) VALUES($1,$2,$3)`, pgx.Identifier{schema}.Sanitize()), uuid.NewString(), largeName, fmt.Sprintf("large-%03d@example.com", index))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	mustPullToStableE2E(t, client, ctx)
	var count, nameBytes int
	require.NoError(t, db.QueryRow(`SELECT count(*),length(CAST(max(name) AS BLOB)) FROM users`).Scan(&count, &nameBytes))
	require.Equal(t, 96, count)
	require.Equal(t, len([]byte(largeName)), nameBytes)
}

func TestEndToEnd_ProductionConflictReaderCarriesLargeStructuredRow(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_projection_large_conflict_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-large-conflict-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "large-conflict-a", defaultOversqliteConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "large-conflict-b", defaultOversqliteConfig(schema, syncTables("users")), usersDDL)
	rowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, "initial", "initial@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustPullToStableE2E(t, clientB, ctx)

	largeName := strings.Repeat("<", 512) + ">&\u2028\u2029\"\\"
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-large-conflict"}
	err = server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{SourceID: serverActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.users SET name=$2 WHERE id=$1`, pgx.Identifier{schema}.Sanitize()), rowID, largeName)
		return err
	})
	require.NoError(t, err)
	_, err = dbB.Exec(`UPDATE users SET email='local@example.com' WHERE id=?`, rowID)
	require.NoError(t, err)
	clientB.Resolver = &oversqlite.ServerWinsResolver{}
	mustPushPendingE2E(t, clientB, ctx)
	var resolvedName string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&resolvedName))
	require.Equal(t, largeName, resolvedName)
}

func TestEndToEnd_ProductionCommittedPageEncoderAndReaderCarryLargeEffect(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_projection_large_committed_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-large-committed-user-" + uuid.NewString()
	config := defaultOversqliteConfig(schema, syncTables("users"))
	// This deliberately expanded row produces about 13 MiB of escaped wire
	// data. The reader conservatively charges 64 copies of encoded/decoded
	// buffers alongside the frozen request. Supply a 4 GiB fixture allowance
	// for that bounded envelope so the codec and settlement assertions execute.
	// Workspace admission limits retain their dedicated owning coverage.
	config.MaxSyncWorkspaceBytes = 4 << 30
	client, db := newSQLiteClient(t, server, userID, "large-committed-client", config, usersDDL)
	rowID := uuid.NewString()
	largeName := strings.Repeat("<>&", 700<<10) + "\u2028\u2029\"\\" + string([]byte{1})
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, rowID, largeName, "large-committed@example.com")
	require.NoError(t, err)

	report := mustPushPendingE2E(t, client, ctx)
	require.Equal(t, oversqlite.PushOutcomeCommitted, report.Outcome)
	var storedName string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, rowID).Scan(&storedName))
	require.Equal(t, largeName, storedName)
	var dirtyRows, stagedRows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedRows))
	require.Zero(t, dirtyRows)
	require.Zero(t, stagedRows)
}
