package schemaevolution

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

var fixtureServer struct {
	once           sync.Once
	dsn, container string
	err            error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if fixtureServer.container != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := exec.CommandContext(ctx, "docker", "rm", "-f", fixtureServer.container).Run(); err != nil {
			code = 1
		}
	}
	os.Exit(code)
}

func fixtureDSN(t *testing.T) string {
	t.Helper()
	fixtureServer.once.Do(func() {
		fixtureServer.dsn = os.Getenv("TRANSITION_TEST_DATABASE_URL")
		if fixtureServer.dsn != "" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--publish", "127.0.0.1::5432", "--env", "POSTGRES_PASSWORD=postgres", "postgres:"+fixturePostgresMajor(t)).CombinedOutput()
		if err != nil {
			fixtureServer.err = fmt.Errorf("postgres16: %w: %s", err, out)
			return
		}
		fixtureServer.container = strings.TrimSpace(string(out))
		out, err = exec.CommandContext(ctx, "docker", "port", fixtureServer.container, "5432/tcp").Output()
		if err != nil {
			fixtureServer.err = err
			return
		}
		address := strings.TrimSpace(strings.Split(string(out), "\n")[0])
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			fixtureServer.err = err
			return
		}
		fixtureServer.dsn = "postgres://postgres:postgres@127.0.0.1:" + port + "/postgres?sslmode=disable"
		for {
			conn, err := pgx.Connect(ctx, fixtureServer.dsn)
			if err == nil {
				_ = conn.Close(ctx)
				return
			}
			if ctx.Err() != nil {
				fixtureServer.err = ctx.Err()
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	})
	require.NoError(t, fixtureServer.err)
	return fixtureServer.dsn
}

func fixturePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	base := fixtureDSN(t)
	admin, err := pgx.Connect(ctx, base)
	require.NoError(t, err)
	var version int
	require.NoError(t, admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version))
	require.Equal(t, fixturePostgresMajor(t), fmt.Sprint(version/10000), "explicit qualification PostgreSQL major")
	database := "transition_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, admin.Close(ctx))
	parsed, err := url.Parse(base)
	require.NoError(t, err)
	parsed.Path = "/" + database
	pool, err := pgxpool.New(ctx, parsed.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(cleanup, base)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close(cleanup)
		_, err = conn.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		if err != nil {
			t.Error(err)
		}
	})
	return pool
}

func bootstrapFixture(t *testing.T, pool *pgxpool.Pool, cfg *oversync.ServiceConfig) *oversync.SyncService {
	t.Helper()
	for _, expected := range cfg.Readiness.Expected.Mechanisms {
		actual, err := readMechanism(context.Background(), pool, expected)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	pair, err := oversync.NewReadinessPoolPair(context.Background(), pool.Config().Copy(), 2)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	cfg.Readiness.PoolPair = pair
	service, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(context.Background()))
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func fixtureWrite(t *testing.T, service *oversync.SyncService, scope, sql string, args ...any) error {
	t.Helper()
	id := uuid.New()
	hash := [32]byte{1}
	_, err := oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}).ExecWrite(context.Background(), scope, oversync.ScopeWriteOptions{WriterID: "schema-evolution-writer", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: id, OperationHash: hash, OperationValidUntil: time.Now().Add(time.Hour)}}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
	return err
}

func fixtureMaintaining(t *testing.T) (*pgxpool.Pool, *oversync.SyncService, uuid.UUID) {
	pool, service, ids := fixtureMaintainingRows(t, []string{"A|B"})
	return pool, service, ids[0]
}

func fixtureMaintainingRows(t *testing.T, values []string) (*pgxpool.Pool, *oversync.SyncService, []uuid.UUID) {
	return fixtureMaintainingRowsWithKeys(t, values, nil)
}
func fixtureMaintainingRowsWithKeys(t *testing.T, values []string, keys []uuid.UUID) (*pgxpool.Pool, *oversync.SyncService, []uuid.UUID) {
	return fixtureMaintainingBounds(t, values, keys, fixtureBounds())
}
func fixtureMaintainingBounds(t *testing.T, values []string, keys []uuid.UUID, bounds Bounds) (*pgxpool.Pool, *oversync.SyncService, []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	pool := fixturePool(t)
	authority, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, authority.Apply(ctx, 1))
	initial, err := Configuration(1, 1, false)
	require.NoError(t, err)
	require.NoError(t, authority.Publish(ctx, initial))
	authority.Close()
	service := bootstrapFixture(t, pool, initial)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	_, err = CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	var ids []uuid.UUID
	for i, value := range values {
		id := uuid.New()
		if keys != nil {
			id = keys[i]
		}
		ids = append(ids, id)
		require.NoError(t, fixtureWrite(t, service, "scope", `INSERT INTO main.upgrade_people(_sync_scope_id,id,legacy_name) VALUES('scope',$1,$2)`, id, value))
	}
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	authority, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, authority.Apply(ctx, 3))
	maintaining, err := Configuration(3, 2, false)
	require.NoError(t, err)
	require.NoError(t, authority.Publish(ctx, maintaining))
	require.NoError(t, authority.RequirePreservingClients(ctx, "authenticated-preserving-runtime-v1"))
	require.NoError(t, authority.Activate(ctx, bounds, fixtureDiskCapacity()))
	authority.Close()
	service = bootstrapFixture(t, pool, maintaining)
	require.Error(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	return pool, service, ids
}

func fixtureCapacity(ctx context.Context, pool *pgxpool.Pool) (Capacity, error) {
	// These fixtures have no attached lagging consumer; the empty binding query
	// proves that precondition instead of inventing an observed client checkpoint.
	var clients int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM sync.source_generation_bindings`).Scan(&clients)
	return Capacity{FreeDiskBytes: 1 << 30, Available: clients == 0, RetentionRemaining: 7 * 24 * time.Hour}, err
}

func fixtureWorker(pool *pgxpool.Pool, service *oversync.SyncService) *Worker {
	var bounds Bounds
	if err := pool.QueryRow(context.Background(), `SELECT bounds FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&bounds); err != nil {
		panic(err)
	}
	return &Worker{Pool: pool, Manager: oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}), Bounds: bounds, Capacity: func(ctx context.Context) (Capacity, error) { return fixtureCapacity(ctx, pool) }}
}

func TestEvolution_BackfillAndAuthoritySwitch(t *testing.T) {
	ctx := context.Background()
	pool, service, id := fixtureMaintaining(t)
	worker := fixtureWorker(pool, service)
	require.NoError(t, worker.Run(ctx))
	var legacy, name, label string
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name,name,new_label FROM main.upgrade_people WHERE id=$1`, id).Scan(&legacy, &name, &label))
	require.Equal(t, []string{"A|B", "A", "B"}, []string{legacy, name, label})
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Certify(ctx, 3))
	require.NoError(t, a.Apply(ctx, 4))
	require.NoError(t, a.Certify(ctx, 4))
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	service = bootstrapFixture(t, pool, cfg)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	require.NoError(t, fixtureWrite(t, service, "scope", `UPDATE main.upgrade_people SET name='C' WHERE id=$1`, id))
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name,name,new_label FROM main.upgrade_people WHERE id=$1`, id).Scan(&legacy, &name, &label))
	require.Equal(t, []string{"C|B", "C", "B"}, []string{legacy, name, label})
}

func fixtureWorkerContext(t *testing.T, pool *pgxpool.Pool) context.Context {
	t.Helper()
	ctx := context.Background()
	conn, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	capability, err := claimWorker(ctx, conn)
	require.NoError(t, err)
	t.Cleanup(func() { releaseSessionLock(conn, workerLock) })
	return context.WithValue(ctx, migrationContextKey{}, capability)
}

// Test-only migration writes exercise transition policy under the private
// capability. Ordinary fixtureWrite always remains subject to closed admission.
func fixtureAuthorizedWrite(t *testing.T, pool *pgxpool.Pool, service *oversync.SyncService, scope, statement string, args ...any) error {
	t.Helper()
	ctx := context.Background()
	conn, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(conn, workerLock)
	capability, err := claimWorker(ctx, conn)
	require.NoError(t, err)
	ctx = context.WithValue(ctx, migrationContextKey{}, capability)
	_, err = oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}).ExecWrite(ctx, scope, oversync.ScopeWriteOptions{WriterID: "schema-evolution-backfill", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if err := permitWorkerTransaction(ctx, tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, statement, args...)
		return err
	})
	return err
}

func fixtureBounds() Bounds {
	b := DefaultBounds()
	b.Deadline = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	b.MaxEvidenceBytes = 64 << 20
	b.MaxHistoryBytes = 64 << 20
	b.MaxBundles = 1024
	b.MinFreeDiskBytes = 64 << 20
	return b
}
func fixtureDiskCapacity() Capacity {
	return Capacity{Available: true, FreeDiskBytes: 1 << 30, RetentionRemaining: 7 * 24 * time.Hour}
}

// fixturePrepareNext creates historical restore inputs under a real worker claim,
// then releases it before tests restart Worker.Run on the same durable job.
func fixturePrepareNext(t *testing.T, w *Worker) *BatchDescriptor {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lock, err := acquireSessionLock(ctx, w.Pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(lock, workerLock)
	capability, err := claimWorker(ctx, lock)
	require.NoError(t, err)
	ctx = context.WithValue(ctx, migrationContextKey{}, capability)
	require.NoError(t, w.resumeJob(ctx))
	d, err := w.prepareNext(ctx)
	require.NoError(t, err)
	require.NotNil(t, d)
	return d
}

func fixturePostgresMajor(t *testing.T) string {
	t.Helper()
	major := os.Getenv("ROLLOUT_PG_MAJOR")
	if major == "" {
		return "16"
	}
	require.Contains(t, []string{"16", "17"}, major)
	return major
}
