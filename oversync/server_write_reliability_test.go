package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCapturedFinalState_SameRowAfterTrigger(t *testing.T) {
	for _, triggerName := range []string{"aaa_normalize", "zzz_normalize"} {
		t.Run(triggerName, func(t *testing.T) {
			f := newRetryableReceiptFixture(t)
			id := uuid.New()
			_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("seed" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error { return f.insertUser(ctx, tx, id, "seed") })
			require.NoError(t, err)
			schema := pgx.Identifier{f.schemaName}.Sanitize()
			_, err = f.pool.Exec(f.ctx, fmt.Sprintf(`CREATE FUNCTION %s.normalize_review() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.name = 'raw' THEN UPDATE %s SET name='final' WHERE _sync_scope_id=NEW._sync_scope_id AND id=NEW.id; END IF;
    RETURN NEW; END $$;
    CREATE TRIGGER %s AFTER UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION %s.normalize_review();`, schema, f.tableIdent, triggerName, f.tableIdent, schema))
			require.NoError(t, err)
			result, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("update" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name='raw' WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id)
				return err
			})
			require.NoError(t, err)
			var actual string
			require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT name FROM %s WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id).Scan(&actual))
			mustInitializeEmptyScope(t, f.ctx, f.service, f.userID, "reader")
			response, err := f.service.ProcessPull(f.ctx, Actor{UserID: f.userID, SourceID: "reader"}, testSchemaGenerationProofForService(f.service), result.Bundle.BundleSeq-1, 10, 0)
			require.NoError(t, err)
			require.Len(t, response.Bundles, 1)
			require.Len(t, response.Bundles[0].Rows, 1)
			var payload map[string]any
			require.NoError(t, json.Unmarshal(response.Bundles[0].Rows[0].Payload, &payload))
			t.Logf("server=%s pulled=%s bundle=%d", actual, payload["name"], result.Bundle.BundleSeq)
			require.Equal(t, actual, payload["name"], "committed pull image must match the final database row")
		})
	}
}
func TestReservedWriterConcurrency_DifferentScopes(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("seed" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error { return f.insertUser(ctx, tx, uuid.New(), "seed") })
	require.NoError(t, err)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := f.manager.ExecWrite(f.ctx, f.userID+"-a", ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("a" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error {
			close(entered)
			<-release
			return f.insertUser(ctx, tx, uuid.New(), "a")
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first writer did not enter")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 400*time.Millisecond)
	called := false
	start := time.Now()
	_, secondErr := f.manager.ExecWrite(ctx, f.userID+"-b", ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("b" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error {
		called = true
		return f.insertUser(ctx, tx, uuid.New(), "b")
	})
	cancel()
	otherCtx, otherCancel := context.WithTimeout(f.ctx, 400*time.Millisecond)
	otherCalled := false
	_, otherErr := f.manager.ExecWrite(otherCtx, f.userID+"-c", ScopeWriteOptions{WriterID: "independent-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("c" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error {
		otherCalled = true
		return f.insertUser(ctx, tx, uuid.New(), "c")
	})
	otherCancel()
	close(release)
	require.NoError(t, <-done)
	t.Logf("different writer control while first still held: callbackCalled=%v error=%v", otherCalled, otherErr)
	require.NoError(t, otherErr)
	require.True(t, otherCalled)
	t.Logf("different scope: elapsed=%s callbackCalled=%v error=%v", time.Since(start), called, secondErr)
	require.NoError(t, secondErr, "a different scope should not wait for the entire first callback")
}

func TestCapturedFinalState_NormalizedTransitions(t *testing.T) {
	for _, kind := range []string{"update-delete", "delete-reinsert", "transient", "rekey", "deferred"} {
		t.Run(kind, func(t *testing.T) {
			f := newRetryableReceiptFixture(t)
			id, rekey, transient := uuid.New(), uuid.New(), uuid.New()
			_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("seed" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error { return f.insertUser(ctx, tx, id, "seed") })
			require.NoError(t, err)
			if kind == "deferred" {
				schema := pgx.Identifier{f.schemaName}.Sanitize()
				_, err = f.pool.Exec(f.ctx, fmt.Sprintf(`CREATE FUNCTION %s.normalize_final() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='raw' THEN UPDATE %s SET name='final' WHERE _sync_scope_id=NEW._sync_scope_id AND id=NEW.id; END IF; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER aaa_normalize AFTER UPDATE ON %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s.normalize_final()`, schema, f.tableIdent, f.tableIdent, schema))
				require.NoError(t, err)
			}
			result, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{WriterID: "server-writer", RetryableWriteOptions: retryableOptionsForLogicalWork("change" + f.userID)}, func(ctx context.Context, tx DatabaseWriteTx) error {
				if kind == "transient" {
					if e := f.insertUser(ctx, tx, transient, "transient"); e != nil {
						return e
					}
					if _, e := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, transient); e != nil {
						return e
					}
				}
				if kind == "delete-reinsert" {
					if _, e := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id); e != nil {
						return e
					}
					return f.insertUser(ctx, tx, id, "final")
				}
				if kind == "rekey" {
					_, e := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET id=$3 WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id, rekey)
					return e
				}
				if _, e := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name=$3 WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id, map[bool]string{true: "raw", false: "final"}[kind == "deferred"]); e != nil {
					return e
				}
				if kind == "update-delete" {
					_, e := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE _sync_scope_id=$1 AND id=$2`, f.tableIdent), f.userID, id)
					return e
				}
				return nil
			})
			require.NoError(t, err)
			mustInitializeEmptyScope(t, f.ctx, f.service, f.userID, "reader")
			pull, err := f.service.ProcessPull(f.ctx, Actor{UserID: f.userID, SourceID: "reader"}, testSchemaGenerationProofForService(f.service), result.Bundle.BundleSeq-1, 10, 0)
			require.NoError(t, err)
			require.Len(t, pull.Bundles, 1)
			want := 1
			if kind == "rekey" {
				want = 2
			}
			require.Len(t, pull.Bundles[0].Rows, want)
			for _, row := range pull.Bundles[0].Rows {
				key := row.Key["id"].(string)
				var exists bool
				require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE _sync_scope_id=$1 AND id=$2)`, f.tableIdent), f.userID, key).Scan(&exists))
				require.Equal(t, row.Op != OpDelete, exists)
				var deleted bool
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT deleted FROM sync.row_state WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) AND key_bytes=decode(replace($2,'-',''),'hex')`, f.userID, key).Scan(&deleted))
				require.Equal(t, !exists, deleted)
				if exists {
					var payload map[string]any
					require.NoError(t, json.Unmarshal(row.Payload, &payload))
					if kind != "rekey" {
						require.Equal(t, "final", payload["name"])
					}
				}
			}
		})
	}
}
