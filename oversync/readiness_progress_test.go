package oversync

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type progressAdmission struct{ ReadinessAdapter }

type blockedProgressAdmission struct {
	ReadinessAdapter
	entered chan struct{}
	release chan struct{}
}

func newTestReadinessPoolPair(t *testing.T, config *pgxpool.Config, admissionMaxConns int32) *ReadinessPoolPair {
	t.Helper()
	pair, err := NewReadinessPoolPair(context.Background(), config, admissionMaxConns)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	return pair
}

func (a blockedProgressAdmission) Admit(ctx context.Context, r DatabaseReadinessReader) error {
	close(a.entered)
	select {
	case <-a.release:
		return progressAdmission{a.ReadinessAdapter}.Admit(ctx, r)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (progressAdmission) Admit(ctx context.Context, r DatabaseReadinessReader) error {
	var n int
	return r.QueryRow(ctx, `SELECT 1`).Scan(&n)
}

func TestReadiness_AdmissionPoolRequiresEquivalentDestination(t *testing.T) {
	newPair := func(t *testing.T, connString string, mutate func(*pgxpool.Config)) *ReadinessPoolPair {
		t.Helper()
		config, err := pgxpool.ParseConfig(connString)
		require.NoError(t, err)
		if mutate != nil {
			mutate(config)
		}
		pair, err := NewReadinessPoolPair(context.Background(), config, 2)
		require.NoError(t, err)
		t.Cleanup(pair.Close)
		return pair
	}

	t.Run("default transport and diagnostic callbacks remain permitted", func(t *testing.T) {
		pair := newPair(t, "postgres://user:password@127.0.0.1:5432/database?sslmode=disable", func(c *pgxpool.Config) {
			c.ConnConfig.OnNotice = func(*pgconn.PgConn, *pgconn.Notice) {}
			c.ConnConfig.OnPgError = func(*pgconn.PgConn, *pgconn.PgError) bool { return true }
		})
		cfg := &ReadinessConfig{Adapter: progressAdmission{}, PoolPair: pair}
		require.NoError(t, validateAdmissionPool(pair.ExecutionPool(), cfg))
	})

	t.Run("fallback route is copied into both pools", func(t *testing.T) {
		pair := newPair(t, "postgres://user:password@127.0.0.1:5432/database?sslmode=disable", func(c *pgxpool.Config) {
			c.ConnConfig.Fallbacks = []*pgconn.FallbackConfig{{Host: "127.0.0.2", Port: 5432}}
		})
		require.Equal(t, pair.ExecutionPool().Config().ConnConfig.Fallbacks, pair.AdmissionPool().Config().ConnConfig.Fallbacks)
	})

	t.Run("independent pools have no provenance", func(t *testing.T) {
		config, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:5432/database?sslmode=disable")
		require.NoError(t, err)
		execution, err := pgxpool.NewWithConfig(context.Background(), config)
		require.NoError(t, err)
		admission, err := pgxpool.NewWithConfig(context.Background(), config.Copy())
		require.NoError(t, err)
		defer execution.Close()
		defer admission.Close()
		cfg := &ReadinessConfig{Adapter: progressAdmission{}, AdmissionPool: admission}
		require.ErrorContains(t, validateAdmissionPool(execution, cfg), "proven")
	})

	t.Run("routing callback captured state is shared by construction", func(t *testing.T) {
		pair := newPair(t, "postgres://user:password@127.0.0.1:5432/database?sslmode=disable", func(c *pgxpool.Config) {
			c.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) { return nil, context.Canceled }
		})
		cfg := &ReadinessConfig{Adapter: progressAdmission{}, PoolPair: pair}
		require.NoError(t, validateAdmissionPool(pair.ExecutionPool(), cfg))
		require.NotZero(t, reflect.ValueOf(pair.ExecutionPool().Config().ConnConfig.DialFunc).Pointer())
		require.Equal(t, reflect.ValueOf(pair.ExecutionPool().Config().ConnConfig.DialFunc).Pointer(), reflect.ValueOf(pair.AdmissionPool().Config().ConnConfig.DialFunc).Pointer())
	})

	t.Run("session hook is rejected before pool creation", func(t *testing.T) {
		config, err := pgxpool.ParseConfig("postgres://user:password@127.0.0.1:5432/database?sslmode=disable")
		require.NoError(t, err)
		config.BeforeAcquire = func(context.Context, *pgx.Conn) bool { return true }
		_, err = NewReadinessPoolPair(context.Background(), config, 1)
		require.ErrorContains(t, err, "hooks are prohibited")
	})
}

func TestReadiness_SeparateAdmissionPoolProgress(t *testing.T) {
	for _, n := range []int32{1, 4} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			f := newBundleChangeWatchFixture(t, true)
			require.NoError(t, f.svc.Close(context.Background()))
			cfg := f.pool.Config().Copy()
			cfg.MaxConns = n
			pair := newTestReadinessPoolPair(t, cfg, n)
			execution := pair.ExecutionPool()
			admission := pair.AdmissionPool()
			sc := *f.svc.config
			rc := *sc.Readiness
			sc.Readiness = &rc
			rc.Adapter = progressAdmission{rc.Adapter}
			_, err := NewRuntimeService(execution, &sc, nil)
			require.Error(t, err)
			rc.AdmissionPool = execution
			_, err = NewRuntimeService(execution, &sc, nil)
			require.Error(t, err)
			rc.AdmissionPool = nil
			rc.PoolPair = pair
			svc, err := NewRuntimeService(execution, &sc, nil)
			require.NoError(t, err)
			require.NoError(t, svc.Bootstrap(context.Background()))
			defer svc.Close(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var admitted sync.WaitGroup
			admitted.Add(int(n))
			results := make(chan error, n)
			for i := int32(0); i < n; i++ {
				go func() {
					done, err := svc.beginOperation(ctx)
					admitted.Done()
					if err != nil {
						results <- err
						return
					}
					defer done()
					admitted.Wait()
					var one int
					results <- execution.QueryRow(ctx, `SELECT 1`).Scan(&one)
				}()
			}
			for i := int32(0); i < n; i++ {
				require.NoError(t, <-results)
			}
			require.NoError(t, svc.Close(ctx))
			require.Zero(t, admission.Stat().AcquiredConns())
		})
	}
}

func TestReadiness_GuardedWatchDrainsWithoutClientCancellation(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("guarded-drain", "device")
	f.pushOne(t, actor, 1, "before")
	pair := newTestReadinessPoolPair(t, f.pool.Config().Copy(), 2)
	admission := pair.AdmissionPool()
	f.svc.pool = pair.ExecutionPool()
	f.svc.config.Readiness.Adapter = progressAdmission{f.svc.config.Readiness.Adapter}
	f.svc.config.Readiness.PoolPair = pair
	h := NewHTTPSyncHandlers(f.svc, nil)
	server := httptest.NewServer(f.svc.ReadinessGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setTestSchemaGenerationHeaders(r, f.svc)
		h.HandleWatch(w, r.WithContext(ContextWithActor(r.Context(), actor)))
	})))
	defer server.Close()
	response, err := http.Get(server.URL + "/sync/watch?after_bundle_seq=0")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Eventually(t, func() bool { return admission.Stat().AcquiredConns() == 0 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, f.svc.Close(ctx))
	require.NoError(t, f.svc.Close(ctx))
}

func TestReadiness_WatchRegistrationRacesDrain(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("registration-drain", "device")
	f.pushOne(t, actor, 1, "before")
	pair := newTestReadinessPoolPair(t, f.pool.Config().Copy(), 2)
	f.svc.pool = pair.ExecutionPool()
	gate := blockedProgressAdmission{f.svc.config.Readiness.Adapter, make(chan struct{}), make(chan struct{})}
	f.svc.config.Readiness.Adapter = gate
	f.svc.config.Readiness.PoolPair = pair
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registered := make(chan (<-chan BundleChangeEvent), 1)
	registerErr := make(chan error, 1)
	go func() {
		ch, err := f.svc.SubscribeBundleChanges(ctx, actor, testSchemaGenerationProofForService(f.svc), 1)
		registered <- ch
		registerErr <- err
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("registration did not reach admission")
	}
	draining := f.svc.drainStartedChannel()
	closed := make(chan error, 1)
	go func() { closed <- f.svc.Close(ctx) }()
	select {
	case <-draining:
	case <-ctx.Done():
		t.Fatal("drain did not begin")
	}
	close(gate.release)
	require.NoError(t, <-registerErr)
	ch := <-registered
	select {
	case _, open := <-ch:
		require.False(t, open)
	case <-ctx.Done():
		t.Fatal("late subscription was not canceled by drain")
	}
	require.NoError(t, <-closed)
	require.Zero(t, pair.AdmissionPool().Stat().AcquiredConns())
	require.NoError(t, f.svc.Close(ctx))
}

func TestReadiness_ListenerReconnectWithOneExecutionConnection(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	require.NoError(t, f.svc.Close(context.Background()))
	pc := f.pool.Config().Copy()
	pc.MaxConns = 1
	pc.MinConns = 0
	const listenerApplicationName = "oversync-readiness-listener-reconnect-test"
	pc.ConnConfig.RuntimeParams["application_name"] = listenerApplicationName
	pair := newTestReadinessPoolPair(t, pc, 1)
	execution := pair.ExecutionPool()
	admission := pair.AdmissionPool()
	cfg := *f.svc.config
	rc := *cfg.Readiness
	cfg.Readiness = &rc
	rc.Adapter = progressAdmission{rc.Adapter}
	rc.PoolPair = pair
	svc, err := NewRuntimeService(execution, &cfg, nil)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(context.Background()))
	defer svc.Close(context.Background())
	f.svc = svc
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	f.ctx = ctx
	actor := f.actor("one-connection-listener", "device")
	f.pushOne(t, actor, 1, "before")
	ch, err := svc.SubscribeBundleChanges(ctx, actor, testSchemaGenerationProofForService(svc), 1)
	require.NoError(t, err)
	stopped := make(chan error, 1)
	go func() { stopped <- svc.RunBundleChangeListener(ctx) }()
	listenerPID := func() int {
		var pid int
		_ = f.pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query=$1 AND state='idle' AND application_name=$2`, "LISTEN \""+svc.effectiveBundleChangeWatchConfig().NotifyChannel+"\"", listenerApplicationName).Scan(&pid)
		return pid
	}
	var first int
	require.Eventually(t, func() bool { first = listenerPID(); return first > 0 }, 2*time.Second, 10*time.Millisecond)
	require.ErrorContains(t, svc.RunBundleChangeListener(ctx), "already running")
	f.pushOne(t, actor, 2, "live")
	require.EqualValues(t, 2, receiveBundleChangeEvent(t, ch).BundleSeq)
	var killed bool
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, first).Scan(&killed))
	require.True(t, killed)
	f.pushOne(t, actor, 3, "during-reconnect")
	require.EqualValues(t, 3, receiveBundleChangeEvent(t, ch).BundleSeq)
	require.Eventually(t, func() bool { pid := listenerPID(); return pid > 0 && pid != first }, 2*time.Second, 10*time.Millisecond)
	f.pushOne(t, actor, 4, "after-reconnect")
	require.EqualValues(t, 4, receiveBundleChangeEvent(t, ch).BundleSeq)
	require.NoError(t, svc.Close(ctx))
	<-stopped
	require.Zero(t, admission.Stat().AcquiredConns())
	require.Zero(t, execution.Stat().AcquiredConns())
	require.Zero(t, listenerPID())
}
