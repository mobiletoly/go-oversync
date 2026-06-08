package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type bundleChangeWatchFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	suffix     string
}

func newBundleChangeWatchFixture(t *testing.T, watchEnabled bool) *bundleChangeWatchFixture {
	t.Helper()

	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "watch_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	cfg := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "watch-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}
	if watchEnabled {
		cfg.BundleChangeWatch = BundleChangeWatchConfig{
			Enabled:           true,
			HeartbeatInterval: time.Second,
		}
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, cfg, logger)

	return &bundleChangeWatchFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		suffix:     suffix,
	}
}

func (f *bundleChangeWatchFixture) actor(userLabel, sourceID string) Actor {
	return Actor{UserID: userLabel + "-" + f.suffix, SourceID: sourceID}
}

func (f *bundleChangeWatchFixture) userInsert(name string) PushRequestRow {
	rowID := uuid.NewString()
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"%s","email":"%s@example.com"}`, rowID, name, name)),
	}
}

func (f *bundleChangeWatchFixture) pushOne(t *testing.T, actor Actor, sourceBundleID int64, name string) *Bundle {
	t.Helper()
	bundle, err := pushRowsViaSession(t, f.ctx, f.svc, actor, sourceBundleID, []PushRequestRow{f.userInsert(name)})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func (f *bundleChangeWatchFixture) userPK(t *testing.T, actor Actor) int64 {
	t.Helper()
	userPK, err := lookupUserPK(f.ctx, f.pool, actor.UserID)
	require.NoError(t, err)
	return userPK
}

func receiveBundleChangeEvent(t *testing.T, ch <-chan BundleChangeEvent) BundleChangeEvent {
	t.Helper()
	select {
	case event, ok := <-ch:
		require.True(t, ok, "watch channel closed before event")
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bundle change event")
		return BundleChangeEvent{}
	}
}

func assertNoWatchEvent(t *testing.T, ch <-chan BundleChangeEvent) {
	t.Helper()
	select {
	case event, ok := <-ch:
		t.Fatalf("unexpected watch event ok=%t event=%#v", ok, event)
	case <-time.After(100 * time.Millisecond):
	}
}

func startBundleChangeListenerForTest(t *testing.T, svc *SyncService) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.RunBundleChangeListener(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected listener error: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("bundle change listener did not stop")
		}
	})
	return cancel
}

func acquireRawBundleChangeListener(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg BundleChangeWatchConfig) *pgxpool.Conn {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(conn.Release)
	quotedChannel, err := quotePostgresNotificationChannel(cfg.NotifyChannel)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "LISTEN "+quotedChannel)
	require.NoError(t, err)
	return conn
}

func waitForRawBundleChangeNotification(t *testing.T, conn *pgxpool.Conn, timeout time.Duration) *bundleChangeNotification {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	notification, err := conn.Conn().WaitForNotification(waitCtx)
	require.NoError(t, err)
	var payload bundleChangeNotification
	require.NoError(t, json.Unmarshal([]byte(notification.Payload), &payload))
	return &payload
}

func assertNoRawBundleChangeNotification(t *testing.T, conn *pgxpool.Conn) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	notification, err := conn.Conn().WaitForNotification(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "unexpected notification: %#v", notification)
}

func TestSubscribeBundleChanges_RejectsDisabledFeature(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{AppName: "watch-disabled-test"}, slog.Default())
	require.NoError(t, err)

	ch, err := svc.SubscribeBundleChanges(context.Background(), Actor{UserID: "user"}, 0)

	require.Nil(t, ch)
	require.ErrorIs(t, err, errBundleChangeWatchDisabled)
}

func TestSubscribeBundleChanges_RejectsInvalidAfterBundleSeq(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-invalid-after-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
	}, slog.Default())
	require.NoError(t, err)

	ch, err := svc.SubscribeBundleChanges(context.Background(), Actor{UserID: "user"}, -1)

	require.Nil(t, ch)
	require.EqualError(t, err, "after_bundle_seq must be >= 0")
}

func TestSubscribeBundleChanges_RejectsUninitializedScope(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)

	ch, err := f.svc.SubscribeBundleChanges(f.ctx, f.actor("uninitialized", "device-a"), 0)

	require.Nil(t, ch)
	var uninitializedErr *ScopeUninitializedError
	require.ErrorAs(t, err, &uninitializedErr)
}

func TestSubscribeBundleChanges_ImmediateEventWhenBehind(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("behind", "device-a")
	bundle := f.pushOne(t, actor, 1, "behind")

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
}

func TestSubscribeBundleChanges_WakesOnCommittedBundle(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("committed", "device-a")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)
	startBundleChangeListenerForTest(t, f.svc)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	bundle := f.pushOne(t, actor, 1, "committed")
	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
	require.Equal(t, actor.SourceID, event.SourceID)
	require.Equal(t, int64(1), event.SourceBundleID)
}

func TestSubscribeBundleChanges_DoesNotWakeOnRolledBackBundle(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("rollback", "server-app")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)
	startBundleChangeListenerForTest(t, f.svc)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	err = f.svc.WithinSyncBundle(f.ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, func(tx pgx.Tx) error {
		tableIdent := pgx.Identifier{f.schemaName, "users"}.Sanitize()
		_, execErr := tx.Exec(f.ctx, fmt.Sprintf(`INSERT INTO %s (_sync_scope_id, id, name, email) VALUES ($1, $2, $3, $4)`, tableIdent), actor.UserID, uuid.New(), "rollback", "rollback@example.com")
		require.NoError(t, execErr)
		return errors.New("force rollback")
	})
	require.EqualError(t, err, "force rollback")
	assertNoWatchEvent(t, ch)

	bundle := f.pushOne(t, actor, 1, "rollback-commit")
	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
}

func TestSubscribeBundleChanges_IsolatesDifferentUsers(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actorA := f.actor("isolated-a", "device-a")
	actorB := f.actor("isolated-b", "device-b")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actorA.UserID, actorA.SourceID)
	mustInitializeEmptyScope(t, f.ctx, f.svc, actorB.UserID, actorB.SourceID)
	startBundleChangeListenerForTest(t, f.svc)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actorA, 0)
	require.NoError(t, err)

	f.pushOne(t, actorB, 1, "isolated-b")
	assertNoWatchEvent(t, ch)

	bundle := f.pushOne(t, actorA, 1, "isolated-a")
	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
}

func TestSubscribeBundleChanges_UnregistersOnContextCancel(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("cleanup", "device-a")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)
	userPK := f.userPK(t, actor)

	ctx, cancel := context.WithCancel(f.ctx)
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)
	require.Equal(t, 1, f.svc.bundleChangeSubscriberCount(userPK))

	cancel()
	waitForBundleChangeSubscriberCount(t, f.svc.ensureBundleChangeHub(), userPK, 0)
	_, ok := <-ch
	require.False(t, ok)
}

func TestBundleChangeNotify_EmitsCommittedPayload(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("raw", "device-a")
	rawListener := acquireRawBundleChangeListener(t, f.ctx, f.pool, f.svc.effectiveBundleChangeWatchConfig())

	bundle := f.pushOne(t, actor, 1, "raw")
	payload := waitForRawBundleChangeNotification(t, rawListener, 2*time.Second)

	require.Equal(t, f.userPK(t, actor), payload.UserPK)
	require.Equal(t, bundle.BundleSeq, payload.BundleSeq)
	require.Equal(t, actor.SourceID, payload.SourceID)
	require.Equal(t, int64(1), payload.SourceBundleID)
}

func TestBundleChangeNotify_DisabledEmitsNoPostgresNotify(t *testing.T) {
	f := newBundleChangeWatchFixture(t, false)
	actor := f.actor("disabled-raw", "device-a")
	rawListener := acquireRawBundleChangeListener(t, f.ctx, f.pool, f.svc.effectiveBundleChangeWatchConfig())

	f.pushOne(t, actor, 1, "disabled-raw")

	assertNoRawBundleChangeNotification(t, rawListener)
}

func TestBundleChangeNotify_NoCapturedChangesEmitsNoPostgresNotify(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("nochange", "server-app")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)
	rawListener := acquireRawBundleChangeListener(t, f.ctx, f.pool, f.svc.effectiveBundleChangeWatchConfig())

	err := f.svc.WithinSyncBundle(f.ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, func(tx pgx.Tx) error {
		return nil
	})
	require.NoError(t, err)

	assertNoRawBundleChangeNotification(t, rawListener)
}

func TestSubscribeBundleChanges_AfterMissedNotifyImmediateCheckCatchesUp(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("missed-immediate", "device-a")
	bundle := f.pushOne(t, actor, 1, "missed-immediate")

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
}

func TestRunBundleChangeListener_ReconnectCatchUpWakesActiveSubscribers(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("reconnect-catchup", "device-a")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	ch, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	bundle := f.pushOne(t, actor, 1, "reconnect-catchup")
	require.NoError(t, f.svc.catchUpActiveBundleChangeSubscribers(f.ctx))

	event := receiveBundleChangeEvent(t, ch)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
}

func TestBundleChangeWatch_MultipleServicesReceiveSameCommittedNotify(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("multi-service", "device-a")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)

	cfg := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "watch-test-secondary",
		RegisteredTables: []RegisteredTable{
			{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled:           true,
			HeartbeatInterval: time.Second,
		},
	}
	second := newBootstrappedIntegrationService(t, f.ctx, f.pool, cfg, integrationTestLogger(slog.LevelWarn))

	startBundleChangeListenerForTest(t, f.svc)
	startBundleChangeListenerForTest(t, second)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	chA, err := f.svc.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)
	chB, err := second.SubscribeBundleChanges(ctx, actor, 0)
	require.NoError(t, err)

	bundle := f.pushOne(t, actor, 1, "multi-service")

	require.Equal(t, bundle.BundleSeq, receiveBundleChangeEvent(t, chA).BundleSeq)
	require.Equal(t, bundle.BundleSeq, receiveBundleChangeEvent(t, chB).BundleSeq)
}
