package server

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"testing"
)

func reviewWrite(t *testing.T, ts *TestServer, scope string, fn oversync.RetryableDatabaseWrite) *oversync.ScopeWriteResult {
	t.Helper()
	result, err := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{}).ExecWrite(context.Background(), scope, oversync.ScopeWriteOptions{WriterID: "review-server", RetryableWriteOptions: retryableWriteOptionsForTest()}, fn)
	require.NoError(t, err)
	return result
}
func reviewSeed(t *testing.T, ts *TestServer, scope string, id uuid.UUID, name, email string) {
	reviewWrite(t, ts, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `INSERT INTO business.users(id,name,email)VALUES($1,$2,$3)`, id, name, email)
		return err
	})
}
func TestServerWriteReliability_TriggerFinalImage(t *testing.T) {
	for _, mode := range []string{"normalize", "delete"} {
		for _, trigger := range []string{"aaa_review", "zzz_review"} {
			t.Run(mode+"/"+trigger, func(t *testing.T) {
				statement := `UPDATE business.users SET name='final' WHERE _sync_scope_id=NEW._sync_scope_id AND id=NEW.id;`
				if mode == "delete" {
					statement = `DELETE FROM business.users WHERE _sync_scope_id=NEW._sync_scope_id AND id=NEW.id;`
				}
				ts, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-review", SchemaGenerations: scopeClientGeneration("users"), BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
					_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION business.review_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='raw' THEN %s END IF;RETURN NEW;END $$;
   CREATE TRIGGER %s AFTER UPDATE ON business.users FOR EACH ROW EXECUTE FUNCTION business.review_trigger();`, statement, trigger))
					return err
				}})
				require.NoError(t, err)
				defer ts.Close()
				ctx := context.Background()
				scope := uuid.NewString()
				id := uuid.New()
				db, client := newUsersClientForTest(t, ts, scope)
				reviewSeed(t, ts, scope, id, "seed", "seed@example.com")
				_, err = client.PullToStable(ctx)
				require.NoError(t, err)
				result := reviewWrite(t, ts, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
					_, err := tx.Exec(ctx, `UPDATE business.users SET name='raw' WHERE _sync_scope_id=$1 AND id=$2`, scope, id)
					return err
				})
				_, err = client.PullToStable(ctx)
				require.NoError(t, err)
				var serverCount, clientCount int
				require.NoError(t, ts.Pool.QueryRow(ctx, `SELECT count(*) FROM business.users WHERE _sync_scope_id=$1 AND id=$2`, scope, id).Scan(&serverCount))
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE id=?`, id.String()).Scan(&clientCount))
				checkpoint, err := client.LastBundleSeqSeen(ctx)
				require.NoError(t, err)
				t.Logf("serverCount=%d clientCount=%d checkpoint=%d committed=%d", serverCount, clientCount, checkpoint, result.Bundle.BundleSeq)
				require.Equal(t, result.Bundle.BundleSeq, checkpoint)
				require.Equal(t, serverCount, clientCount, "client existence must match committed server state")
				if serverCount > 0 {
					var sn, cn string
					require.NoError(t, ts.Pool.QueryRow(ctx, `SELECT name FROM business.users WHERE _sync_scope_id=$1 AND id=$2`, scope, id).Scan(&sn))
					require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, id.String()).Scan(&cn))
					t.Logf("server=%s client=%s", sn, cn)
					require.Equal(t, sn, cn)
				}
			})
		}
	}
}
func TestServerWriteReliability_RepeatedKeyUniqueOrdering(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-review", SchemaGenerations: scopeClientGeneration("users"), BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
		_, err := pool.Exec(ctx, `CREATE UNIQUE INDEX review_unique_email ON business.users(_sync_scope_id,email)`)
		return err
	}})
	require.NoError(t, err)
	defer ts.Close()
	ctx := context.Background()
	scope := uuid.NewString()
	a, b := uuid.New(), uuid.New()
	db, client := newUsersClientForTest(t, ts, scope)
	reviewSeed(t, ts, scope, a, "A", "a@example.com")
	reviewSeed(t, ts, scope, b, "B", "b@example.com")
	_, err = client.PullToStable(ctx)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE UNIQUE INDEX review_unique_email ON users(email)`)
	require.NoError(t, err)
	result := reviewWrite(t, ts, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, `UPDATE business.users SET name='A updated' WHERE _sync_scope_id=$1 AND id=$2`, scope, a); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE business.users SET email='c@example.com' WHERE _sync_scope_id=$1 AND id=$2`, scope, b); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE business.users SET email='b@example.com' WHERE _sync_scope_id=$1 AND id=$2`, scope, a)
		return err
	})
	_, pullErr := client.PullToStable(ctx)
	checkpoint, err := client.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	t.Logf("committed=%d checkpoint=%d pullError=%v", result.Bundle.BundleSeq, checkpoint, pullErr)
	require.NoError(t, pullErr, "a legal server transaction with the same unique constraint must remain replayable")
}
func TestServerWriteReliability_DeleteReinsertAndDirtyConflict(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{JWTSecret: "synthetic-review", SchemaGenerations: scopeClientGeneration("users")})
	require.NoError(t, err)
	defer ts.Close()
	ctx := context.Background()
	scope := uuid.NewString()
	id := uuid.New()
	db, client := newUsersClientForTest(t, ts, scope)
	reviewSeed(t, ts, scope, id, "seed", "seed@example.com")
	_, err = client.PullToStable(ctx)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		reviewWrite(t, ts, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, `DELETE FROM business.users WHERE _sync_scope_id=$1 AND id=$2`, scope, id)
			return err
		})
		_, err = client.PullToStable(ctx)
		require.NoError(t, err)
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE id=?`, id.String()).Scan(&n))
		require.Zero(t, n)
		reviewSeed(t, ts, scope, id, "reinserted", "seed@example.com")
		_, err = client.PullToStable(ctx)
		require.NoError(t, err)
	}
	_, err = db.Exec(`UPDATE users SET name='offline edit' WHERE id=?`, id.String())
	require.NoError(t, err)
	reviewWrite(t, ts, scope, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `DELETE FROM business.users WHERE _sync_scope_id=$1 AND id=$2`, scope, id)
		return err
	})
	_, err = client.Sync(ctx)
	var conflict *oversqlite.ConflictNeedsResolutionError
	require.ErrorAs(t, err, &conflict)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, id.String()).Scan(&name))
	require.Equal(t, "offline edit", name)
	pending, err := client.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.NoError(t, client.ResolveConflict(ctx, *pending, oversqlite.AcceptServer{}))
	_, err = client.Sync(ctx)
	require.NoError(t, err)
	err = db.QueryRow(`SELECT name FROM users WHERE id=?`, id.String()).Scan(&name)
	require.ErrorIs(t, err, sql.ErrNoRows)
	t.Log("three delete/reinsert cycles converged; offline edit survived server deletion until explicit AcceptServer")
}
