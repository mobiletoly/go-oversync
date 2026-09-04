package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func pendingConflictFixture(t *testing.T) (*Client, string) {
	t.Helper()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	id := uuid.NewString()
	seedSyncedUserRow(t, c, db, id, "Original", 1)
	_, err := db.Exec(`UPDATE users SET name='Offline' WHERE id=?`, id)
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(s *mockDirectPush) *http.Response {
		return structuredConflictResponse(s.Rows[0], 2, false, map[string]any{"id": id, "name": "Original", "email": "ada@example.com"})
	}
	c.HTTP = &http.Client{Transport: server}
	_, err = c.PushPending(context.Background())
	var pending *ConflictNeedsResolutionError
	require.ErrorAs(t, err, &pending)
	return c, id
}

func TestPendingConflict_DefaultPreservesAndExplicitlyResolves(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "UPDATE", p.Conflict.LocalOp)
	name, _, _ := loadUserForKey(t, c.db, id)
	require.Equal(t, "Offline", name)
	var raw string
	require.NoError(t, c.db.QueryRow(`SELECT pending FROM _sync_conflict_state`).Scan(&raw))
	require.Contains(t, raw, p.ID)
	// Retrying ordinary synchronization cannot silently resolve the recorded conflict.
	_, err = c.PushPending(ctx)
	var needs *ConflictNeedsResolutionError
	require.ErrorAs(t, err, &needs)
	require.Equal(t, p.ID, needs.Pending.ID)
	require.NoError(t, c.ResolveConflict(ctx, *p, KeepLocal{}))
	p, err = c.PendingConflict(ctx)
	require.NoError(t, err)
	require.Nil(t, p)
	var base int64
	require.NoError(t, c.db.QueryRow(`SELECT base_row_version FROM _sync_dirty_rows`).Scan(&base))
	require.Equal(t, int64(2), base)
	name, _, _ = loadUserForKey(t, c.db, id)
	require.Equal(t, "Offline", name)
}

func TestPendingConflict_RejectsRetainedContextThatDisagreesWithFrozenRequest(t *testing.T) {
	ctx := context.Background()
	c, _ := pendingConflictFixture(t)
	var raw string
	require.NoError(t, c.db.QueryRow(`SELECT pending FROM _sync_conflict_state`).Scan(&raw))
	var pending PendingConflict
	require.NoError(t, json.Unmarshal([]byte(raw), &pending))
	pending.Conflict.LocalOp = "DELETE"
	corrupt, err := json.Marshal(pending)
	require.NoError(t, err)
	_, err = c.db.Exec(`UPDATE _sync_conflict_state SET pending=?`, string(corrupt))
	require.NoError(t, err)

	_, err = c.PendingConflict(ctx)
	require.ErrorContains(t, err, "retained conflict envelope disagrees")
	var outboxRows int
	require.NoError(t, c.db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.Equal(t, 1, outboxRows)
}

func TestPendingConflict_StaleDecisionAndNewerEditSurvive(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	selected, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	_, err = c.db.Exec(`UPDATE users SET name='Newer' WHERE id=?`, id)
	require.NoError(t, err)
	err = c.ResolveConflict(ctx, *selected, AcceptServer{})
	var stale *StaleConflictResolutionError
	require.ErrorAs(t, err, &stale)
	latest, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.Greater(t, latest.Revision, selected.Revision)
	require.Contains(t, string(latest.NewerLocalPayload), "Newer")
	require.ErrorAs(t, c.ResolveConflict(ctx, *latest, KeepLocal{}), &stale)
	require.NoError(t, c.ResolveConflict(ctx, *latest, AcceptServer{}))
	name, _, _ := loadUserForKey(t, c.db, id)
	require.Equal(t, "Newer", name)
	var payload string
	require.NoError(t, c.db.QueryRow(`SELECT payload FROM _sync_dirty_rows`).Scan(&payload))
	require.Contains(t, payload, "Newer")
}

func TestPendingConflict_DecisionRollbackAndRetry(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	_, err = c.db.Exec(`CREATE TRIGGER reject_conflict_decision BEFORE UPDATE ON _sync_conflict_state WHEN OLD.pending<>'' AND NEW.pending='' BEGIN SELECT RAISE(ABORT,'injected decision failure'); END`)
	require.NoError(t, err)
	require.Error(t, c.ResolveConflict(ctx, *p, AcceptServer{}))
	name, _, _ := loadUserForKey(t, c.db, id)
	require.Equal(t, "Offline", name)
	current, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.Equal(t, p.ID, current.ID)
	_, err = c.db.Exec(`DROP TRIGGER reject_conflict_decision`)
	require.NoError(t, err)
	require.NoError(t, c.RetryConflict(ctx, *current))
	_, err = c.PushPending(ctx)
	var needs *ConflictNeedsResolutionError
	require.ErrorAs(t, err, &needs)
	require.NotEqual(t, p.ID, needs.Pending.ID)
}

type panicConflictResolver struct{}

func (panicConflictResolver) Resolve(ConflictContext) MergeResult { panic("application resolver") }
func TestPendingConflict_ResolverFailuresPreserve(t *testing.T) {
	for _, resolver := range []Resolver{nil, panicConflictResolver{}, nilConflictResolver{}} {
		c, id := pendingConflictFixture(t)
		p, err := c.PendingConflict(context.Background())
		require.NoError(t, err)
		require.NoError(t, c.RetryConflict(context.Background(), *p))
		c.Resolver = resolver
		_, err = c.PushPending(context.Background())
		var needs *ConflictNeedsResolutionError
		require.ErrorAs(t, err, &needs)
		name, _, _ := loadUserForKey(t, c.db, id)
		require.Equal(t, "Offline", name)
		var stored PendingConflict
		var raw string
		require.NoError(t, c.db.QueryRow(`SELECT pending FROM _sync_conflict_state`).Scan(&raw))
		require.NoError(t, json.Unmarshal([]byte(raw), &stored))
	}
}

type nilConflictResolver struct{}

func (nilConflictResolver) Resolve(ConflictContext) MergeResult { return nil }

func TestPendingConflict_CrossRowTriggerCannotEraseOtherIntent(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	other := uuid.NewString()
	seedSyncedUserRow(t, c, c.db, other, "Other", 1)
	_, err := c.db.Exec(`UPDATE users SET name='Newer unrelated work' WHERE id=?`, other)
	require.NoError(t, err)
	// A normal application trigger executes even while SDK dirty tracking is off.
	_, err = c.db.Exec(`CREATE TRIGGER conflict_cross_row AFTER UPDATE ON users WHEN NEW.id='` + id + `' BEGIN UPDATE users SET name='Overwritten' WHERE id='` + other + `'; END`)
	require.NoError(t, err)
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NoError(t, c.ResolveConflict(ctx, *p, AcceptServer{}))
	name, _, _ := loadUserForKey(t, c.db, other)
	require.Equal(t, "Newer unrelated work", name)
	name, _, _ = loadUserForKey(t, c.db, id)
	require.Equal(t, "Original", name)
	after, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.Nil(t, after)
}

func TestPendingConflict_RuntimeReopenAndDestructiveGuards(t *testing.T) {
	ctx := context.Background()
	c, id := pendingConflictFixture(t)
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	snapshot := requireOutboxBundle(t, c.db)
	_, err = c.Rebuild(ctx)
	var needs *ConflictNeedsResolutionError
	require.ErrorAs(t, err, &needs)
	_, err = c.PullToStable(ctx)
	require.ErrorAs(t, err, &needs)
	require.ErrorAs(t, c.UninstallSync(ctx), &needs)
	detach, err := c.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detach.Outcome)
	require.Equal(t, snapshot, requireOutboxBundle(t, c.db))
	path := filepath.Join(t.TempDir(), "runtime-update.sqlite")
	_, err = c.db.Exec(`VACUUM INTO ?`, path)
	require.NoError(t, err)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	reopened, err := newTestClient(t, db, "http://example.invalid", tokenProviderForTests, c.config)
	require.NoError(t, err)
	defer reopened.Close()
	current, err := reopened.PendingConflict(ctx)
	require.NoError(t, err)
	require.Equal(t, p.ID, current.ID)
	require.Equal(t, snapshot, requireOutboxBundle(t, db))
	name, _, _ := loadUserForKey(t, db, id)
	require.Equal(t, "Offline", name)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, reopened.ResolveConflict(canceled, *current, AcceptServer{}))
	again, err := reopened.PendingConflict(ctx)
	require.NoError(t, err)
	require.Equal(t, current.ID, again.ID)
	require.NoError(t, reopened.ResolveConflict(ctx, *current, KeepMerged{MergedPayload: json.RawMessage(`{"id":"` + id + `","name":"Merged","email":"ada@example.com"}`)}))
	name, _, _ = loadUserForKey(t, db, id)
	require.Equal(t, "Merged", name)
}

func TestPendingConflict_MergeCannotMoveKey(t *testing.T) {
	c, id := pendingConflictFixture(t)
	ctx := context.Background()
	p, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	err = c.ResolveConflict(ctx, *p, KeepMerged{MergedPayload: json.RawMessage(`{"id":"` + uuid.NewString() + `","name":"Other","email":"ada@example.com"}`)})
	require.ErrorContains(t, err, "cannot change")
	name, _, _ := loadUserForKey(t, c.db, id)
	require.Equal(t, "Offline", name)
	p, err = c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestPendingConflict_DeletedUpdateRejectsExplicitResolutionAfterDurableRead(t *testing.T) {
	for _, decision := range []string{"local", "merged"} {
		for _, deleted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_deleted_%t", decision, deleted), func(t *testing.T) {
				ctx := context.Background()
				c, db := newUsersBundleClient(t)
				id := seedSyncedConflictUser(t, c, db)
				updateConflictUserName(t, db, id, "Offline")
				server := newMockPushServer(t)
				server.pushHook = func(s *mockDirectPush) *http.Response {
					version := int64(0)
					if deleted {
						version = 2
					}
					return structuredConflictResponse(s.Rows[0], version, deleted, nil)
				}
				c.HTTP = &http.Client{Transport: server}
				_, err := c.PushPending(ctx)
				var needs *ConflictNeedsResolutionError
				require.ErrorAs(t, err, &needs)
				p, err := c.PendingConflict(ctx)
				require.NoError(t, err)
				require.NotNil(t, p)
				require.Empty(t, p.Conflict.ServerRowPayload)
				var frozen, pending string
				require.NoError(t, db.QueryRow(`SELECT wire_payload FROM _sync_outbox_rows`).Scan(&frozen))
				require.NoError(t, db.QueryRow(`SELECT pending FROM _sync_conflict_state`).Scan(&pending))
				result := MergeResult(KeepLocal{})
				if decision == "merged" {
					result = KeepMerged{MergedPayload: mergedConflictUserPayload(t, id)}
				}
				var invalid *InvalidConflictResolutionError
				require.ErrorAs(t, c.ResolveConflict(ctx, *p, result), &invalid)
				var afterFrozen, afterPending string
				require.NoError(t, db.QueryRow(`SELECT wire_payload FROM _sync_outbox_rows`).Scan(&afterFrozen))
				require.NoError(t, db.QueryRow(`SELECT pending FROM _sync_conflict_state`).Scan(&afterPending))
				require.Equal(t, frozen, afterFrozen)
				require.Equal(t, pending, afterPending)
				name, _, _ := loadUserForKey(t, db, id)
				require.Equal(t, "Offline", name)
			})
		}
	}
}
