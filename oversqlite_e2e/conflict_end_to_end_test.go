package oversqlite_e2e

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_ConflictServerWinsResolverRecoversAutomatically(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_serverwins_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-serverwins-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 1,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schema}.Sanitize()), conflictUserID, "Ada Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", conflictUserID)
	require.NoError(t, err)

	mustPushPendingE2E(t, clientB, ctx)

	var localName string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, conflictUserID).Scan(&localName))
	require.Equal(t, "Ada Server", localName)

	mustPullToStableE2E(t, clientB, ctx)
	lastBundleSeq := requireLastBundleSeqSeen(t, dbB)
	require.Equal(t, int64(2), lastBundleSeq)
}

func TestEndToEnd_ConflictClientWinsResolverStillConvergesWithUnseenPeerBundle(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_clientwins_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-clientwins-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB.Resolver = &oversqlite.ClientWinsResolver{}

	conflictUserID := uuid.NewString()
	peerUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Grace", "grace@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, peerUserID, "Peer", "peer@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 1,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schema}.Sanitize()), conflictUserID, "Grace Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Grace Client", conflictUserID)
	require.NoError(t, err)

	mustPushPendingE2E(t, clientB, ctx)

	lastBundleSeq := requireLastBundleSeqSeen(t, dbB)
	require.Equal(t, int64(1), lastBundleSeq)

	mustPullToStableE2E(t, clientB, ctx)
	lastBundleSeq = requireLastBundleSeqSeen(t, dbB)
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
	server := newExampleServer(t, schema)

	userID := "e2e-user-keepmerged-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 1,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schema}.Sanitize()), conflictUserID, "Ada Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET email = ? WHERE id = ?`, "local@example.com", conflictUserID)
	require.NoError(t, err)

	var createdAt, updatedAt string
	require.NoError(t, dbB.QueryRow(`SELECT created_at, updated_at FROM users WHERE id = ?`, conflictUserID).Scan(&createdAt, &updatedAt))
	clientB.Resolver = &staticResolver{result: oversqlite.KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":         conflictUserID,
		"name":       "Ada Server",
		"email":      "local@example.com",
		"created_at": createdAt,
		"updated_at": updatedAt,
	})}}

	mustPushPendingE2E(t, clientB, ctx)
	mustPullToStableE2E(t, clientB, ctx)

	var name, email string
	require.NoError(t, dbB.QueryRow(`SELECT name, email FROM users WHERE id = ?`, conflictUserID).Scan(&name, &email))
	require.Equal(t, "Ada Server", name)
	require.Equal(t, "local@example.com", email)
}

func TestEndToEnd_ConflictKeepLocalResolverAutoRetriesAndWins(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_keeplocal_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-keeplocal-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB.Resolver = &staticResolver{result: oversqlite.KeepLocal{}}

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Linus", "linus@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 1,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schema}.Sanitize()), conflictUserID, "Linus Server")
		return err
	}))

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Linus Local", conflictUserID)
	require.NoError(t, err)

	mustPushPendingE2E(t, clientB, ctx)
	mustPullToStableE2E(t, clientB, ctx)

	var name string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, conflictUserID).Scan(&name))
	require.Equal(t, "Linus Local", name)
}

func TestEndToEnd_ConflictRetryExhaustionLeavesReplayableDirtyState(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_conflict_exhaustion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-exhaustion-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB.Resolver = &oversqlite.ClientWinsResolver{}

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Ada Local", conflictUserID)
	require.NoError(t, err)

	commitAttempts := 0
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	baseTransport := http.DefaultTransport
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/") && strings.HasSuffix(r.URL.Path, "/commit") {
			commitAttempts++
			require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
				SourceID:       serverActor.SourceID,
				SourceBundleID: int64(commitAttempts),
			}, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					UPDATE %s.users
					SET name = $2
					WHERE id = $1
				`, pgx.Identifier{schema}.Sanitize()), conflictUserID, fmt.Sprintf("Ada Server %d", commitAttempts))
				return err
			}))
		}
		return baseTransport.RoundTrip(r)
	})}

	_, err = clientB.PushPending(ctx)
	require.Error(t, err)
	var exhaustedErr *oversqlite.PushConflictRetryExhaustedError
	require.ErrorAs(t, err, &exhaustedErr)
	require.Equal(t, 2, exhaustedErr.RetryCount)

	var dirtyCount, outboundCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 0, outboundCount)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, dbB, rowKeyJSON(conflictUserID))
	require.True(t, exists)
	require.Equal(t, oversync.OpUpdate, op)
	require.True(t, payload.Valid)
	require.GreaterOrEqual(t, baseRowVersion, int64(2))

	nextSourceBundleID := requireNextSourceBundleID(t, dbB)
	lastBundleSeqSeen := requireLastBundleSeqSeen(t, dbB)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(1), lastBundleSeqSeen)
	require.Equal(t, 3, commitAttempts)
}
