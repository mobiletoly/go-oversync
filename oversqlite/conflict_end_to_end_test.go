package oversqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_ConflictServerWinsResolverRecoversAutomatically(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_serverwins_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-serverwins-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL)

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.service.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 201,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{server.schema}.Sanitize()), conflictUserID, "Ada Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", conflictUserID)
	require.NoError(t, err)

	require.NoError(t, clientB.PushPending(ctx))

	var localName string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, conflictUserID).Scan(&localName))
	require.Equal(t, "Ada Server", localName)

	require.NoError(t, clientB.PullToStable(ctx))
	var lastBundleSeq int64
	require.NoError(t, dbB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&lastBundleSeq))
	require.Equal(t, int64(2), lastBundleSeq)
}

func TestEndToEnd_ConflictClientWinsResolverStillConvergesWithUnseenPeerBundle(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_clientwins_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-clientwins-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL)
	clientB.Resolver = &ClientWinsResolver{}

	conflictUserID := uuid.NewString()
	peerUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Grace", "grace@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, peerUserID, "Peer", "peer@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.service.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 202,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{server.schema}.Sanitize()), conflictUserID, "Grace Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Grace Client", conflictUserID)
	require.NoError(t, err)

	require.NoError(t, clientB.PushPending(ctx))

	var lastBundleSeq int64
	require.NoError(t, dbB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&lastBundleSeq))
	require.Equal(t, int64(1), lastBundleSeq)

	require.NoError(t, clientB.PullToStable(ctx))
	require.NoError(t, dbB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&lastBundleSeq))
	require.Equal(t, int64(4), lastBundleSeq)

	var conflictName, peerName string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, conflictUserID).Scan(&conflictName))
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, peerUserID).Scan(&peerName))
	require.Equal(t, "Grace Client", conflictName)
	require.Equal(t, "Peer", peerName)
}

func TestEndToEnd_ConflictKeepMergedResolverRetriesMergedPayload(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_keepmerged_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-keepmerged-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL)

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.service.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 203,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{server.schema}.Sanitize()), conflictUserID, "Ada Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET email = ? WHERE id = ?`, "local@example.com", conflictUserID)
	require.NoError(t, err)
	clientB.Resolver = &staticResolver{result: KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":    conflictUserID,
		"name":  "Ada Server",
		"email": "local@example.com",
	})}}

	require.NoError(t, clientB.PushPending(ctx))
	require.NoError(t, clientB.PullToStable(ctx))

	var name, email string
	require.NoError(t, dbB.QueryRow(`SELECT name, email FROM users WHERE id = ?`, conflictUserID).Scan(&name, &email))
	require.Equal(t, "Ada Server", name)
	require.Equal(t, "local@example.com", email)
}
