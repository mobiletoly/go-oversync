package oversqlite_e2e

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type userConflictFixture struct {
	ctx            context.Context
	schema         string
	server         *exampleserver.TestServer
	userID         string
	clientA        *oversqlite.Client
	dbA            *sql.DB
	clientB        *oversqlite.Client
	dbB            *sql.DB
	conflictUserID string
}

func newUserConflictFixture(t *testing.T, scenario string, initialName string) *userConflictFixture {
	t.Helper()

	ctx := context.Background()
	schema := "e2e_conflict_" + scenario + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-user-" + scenario + "-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	f := &userConflictFixture{
		ctx:            ctx,
		schema:         schema,
		server:         server,
		userID:         userID,
		clientA:        clientA,
		dbA:            dbA,
		clientB:        clientB,
		dbB:            dbB,
		conflictUserID: uuid.NewString(),
	}
	f.insertUserOnA(t, f.conflictUserID, initialName, strings.ToLower(initialName)+"@example.com")
	mustPushPendingE2E(t, f.clientA, f.ctx)
	mustRebuildE2E(t, f.clientB, f.ctx)
	return f
}

func (f *userConflictFixture) insertUserOnA(t *testing.T, id, name, email string) {
	t.Helper()

	_, err := f.dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, name, email)
	require.NoError(t, err)
}

func (f *userConflictFixture) serverUpdateName(t *testing.T, name string) {
	t.Helper()
	f.serverUpdateNameInBundle(t, 1, name)
}

func (f *userConflictFixture) serverUpdateNameInBundle(t *testing.T, sourceBundleID int64, name string) {
	t.Helper()

	serverActor := oversync.Actor{UserID: f.userID, SourceID: "server-writer"}
	require.NoError(t, f.server.SyncService.WithinSyncBundle(f.ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: sourceBundleID,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(f.ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{f.schema}.Sanitize()), f.conflictUserID, name)
		return err
	}))
}

func (f *userConflictFixture) updateClientBName(t *testing.T, name string) {
	t.Helper()

	_, err := f.dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, name, f.conflictUserID)
	require.NoError(t, err)
}

func TestEndToEnd_ConflictServerWinsResolverRecoversAutomatically(t *testing.T) {
	f := newUserConflictFixture(t, "serverwins", "Ada")
	f.serverUpdateName(t, "Ada Server")
	f.updateClientBName(t, "Ada Local")

	mustPushPendingE2E(t, f.clientB, f.ctx)

	var localName string
	require.NoError(t, f.dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, f.conflictUserID).Scan(&localName))
	require.Equal(t, "Ada Server", localName)

	mustPullToStableE2E(t, f.clientB, f.ctx)
	lastBundleSeq := requireLastBundleSeqSeen(t, f.dbB)
	require.Equal(t, int64(2), lastBundleSeq)
}

func TestEndToEnd_ConflictClientWinsResolverStillConvergesWithUnseenPeerBundle(t *testing.T) {
	f := newUserConflictFixture(t, "clientwins", "Grace")
	f.clientB.Resolver = &oversqlite.ClientWinsResolver{}

	peerUserID := uuid.NewString()
	f.insertUserOnA(t, peerUserID, "Peer", "peer@example.com")
	mustPushPendingE2E(t, f.clientA, f.ctx)

	f.serverUpdateName(t, "Grace Server")
	f.updateClientBName(t, "Grace Client")

	mustPushPendingE2E(t, f.clientB, f.ctx)

	lastBundleSeq := requireLastBundleSeqSeen(t, f.dbB)
	require.Equal(t, int64(1), lastBundleSeq)

	mustPullToStableE2E(t, f.clientB, f.ctx)
	lastBundleSeq = requireLastBundleSeqSeen(t, f.dbB)
	require.Equal(t, int64(4), lastBundleSeq)

	var conflictName, peerName string
	require.NoError(t, f.dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, f.conflictUserID).Scan(&conflictName))
	require.NoError(t, f.dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, peerUserID).Scan(&peerName))
	require.Equal(t, "Grace Client", conflictName)
	require.Equal(t, "Peer", peerName)
}

func TestEndToEnd_ConflictKeepMergedResolverRetriesMergedPayload(t *testing.T) {
	f := newUserConflictFixture(t, "keepmerged", "Ada")
	f.serverUpdateName(t, "Ada Server")

	_, err := f.dbB.Exec(`UPDATE users SET email = ? WHERE id = ?`, "local@example.com", f.conflictUserID)
	require.NoError(t, err)

	var createdAt, updatedAt string
	require.NoError(t, f.dbB.QueryRow(`SELECT created_at, updated_at FROM users WHERE id = ?`, f.conflictUserID).Scan(&createdAt, &updatedAt))
	f.clientB.Resolver = &staticResolver{result: oversqlite.KeepMerged{MergedPayload: mustJSONPayload(t, map[string]any{
		"id":         f.conflictUserID,
		"name":       "Ada Server",
		"email":      "local@example.com",
		"created_at": createdAt,
		"updated_at": updatedAt,
	})}}

	mustPushPendingE2E(t, f.clientB, f.ctx)
	mustPullToStableE2E(t, f.clientB, f.ctx)

	var name, email string
	require.NoError(t, f.dbB.QueryRow(`SELECT name, email FROM users WHERE id = ?`, f.conflictUserID).Scan(&name, &email))
	require.Equal(t, "Ada Server", name)
	require.Equal(t, "local@example.com", email)
}

func TestEndToEnd_ConflictKeepLocalResolverAutoRetriesAndWins(t *testing.T) {
	f := newUserConflictFixture(t, "keeplocal", "Linus")
	f.clientB.Resolver = &staticResolver{result: oversqlite.KeepLocal{}}
	f.serverUpdateName(t, "Linus Server")
	f.updateClientBName(t, "Linus Local")

	mustPushPendingE2E(t, f.clientB, f.ctx)
	mustPullToStableE2E(t, f.clientB, f.ctx)

	var name string
	require.NoError(t, f.dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, f.conflictUserID).Scan(&name))
	require.Equal(t, "Linus Local", name)
}

func TestEndToEnd_ConflictRetryExhaustionLeavesReplayableDirtyState(t *testing.T) {
	f := newUserConflictFixture(t, "exhaustion", "Ada")
	f.clientB.Resolver = &oversqlite.ClientWinsResolver{}
	f.updateClientBName(t, "Ada Local")

	commitAttempts := 0
	baseTransport := http.DefaultTransport
	f.clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sync/push-sessions/") && strings.HasSuffix(r.URL.Path, "/commit") {
			commitAttempts++
			f.serverUpdateNameInBundle(t, int64(commitAttempts), fmt.Sprintf("Ada Server %d", commitAttempts))
		}
		return baseTransport.RoundTrip(r)
	})}

	_, err := f.clientB.PushPending(f.ctx)
	require.Error(t, err)
	var exhaustedErr *oversqlite.PushConflictRetryExhaustedError
	require.ErrorAs(t, err, &exhaustedErr)
	require.Equal(t, 2, exhaustedErr.RetryCount)

	var dirtyCount, outboundCount int
	require.NoError(t, f.dbB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, f.dbB.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount))
	require.Equal(t, 1, dirtyCount)
	require.Equal(t, 0, outboundCount)

	op, baseRowVersion, payload, exists := loadDirtyRowForKey(t, f.dbB, rowKeyJSON(f.conflictUserID))
	require.True(t, exists)
	require.Equal(t, oversync.OpUpdate, op)
	require.True(t, payload.Valid)
	require.GreaterOrEqual(t, baseRowVersion, int64(2))

	nextSourceBundleID := requireNextSourceBundleID(t, f.dbB)
	lastBundleSeqSeen := requireLastBundleSeqSeen(t, f.dbB)
	require.Equal(t, int64(1), nextSourceBundleID)
	require.Equal(t, int64(1), lastBundleSeqSeen)
	require.Equal(t, 3, commitAttempts)
}
