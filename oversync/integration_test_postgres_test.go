package oversync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	managedIntegrationPostgresImage        = "postgres:16"
	managedIntegrationPostgresPassword     = "oversync_test_password"
	managedIntegrationPostgresTestLabel    = "com.mobiletoly.go-oversync.test=true"
	managedIntegrationPostgresTestRunLabel = "com.mobiletoly.go-oversync.test-run"
)

type managedIntegrationPostgresServer struct {
	containerID string
	dataDir     string
	host        string
	port        string
}

var (
	managedIntegrationPostgresOnce     sync.Once
	managedIntegrationPostgres         *managedIntegrationPostgresServer
	managedIntegrationPostgresErr      error
	managedIntegrationDatabaseSequence atomic.Uint64
	callerManagedIntegrationDatabaseMu sync.Mutex
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	if err := stopManagedIntegrationPostgres(); err != nil {
		fmt.Fprintf(os.Stderr, "stop managed integration PostgreSQL: %v\n", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func provisionIntegrationTestDatabase(t *testing.T, ctx context.Context) (string, bool) {
	t.Helper()

	if databaseURL := os.Getenv("TEST_DATABASE_URL"); databaseURL != "" {
		if os.Getenv("OVERSYNC_TEST_ISOLATE_DATABASES") == "1" {
			return provisionIsolatedCallerDatabase(t, ctx, databaseURL), true
		}
		callerManagedIntegrationDatabaseMu.Lock()
		t.Cleanup(callerManagedIntegrationDatabaseMu.Unlock)
		return databaseURL, false
	}

	server, err := getManagedIntegrationPostgres()
	if err != nil {
		t.Fatalf("start managed integration PostgreSQL (set TEST_DATABASE_URL to use an existing database): %v", err)
	}
	return provisionIntegrationTestDatabaseOnServer(t, ctx, server)
}

// Qualification fixtures grant CREATE DATABASE. Give each pool its own database
// in that lane: a test may legitimately own multiple pools simultaneously, which
// cannot acquire the shared-database mutex twice before its cleanup runs.
func provisionIsolatedCallerDatabase(t *testing.T, parent context.Context, databaseURL string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal("parse caller-managed fixture URL")
	}
	name := fmt.Sprintf("oversync_test_%d_%d", os.Getpid(), managedIntegrationDatabaseSequence.Add(1))
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to caller-managed fixture for database creation")
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create run-owned integration database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		admin, err := pgx.Connect(cleanupCtx, databaseURL)
		if err != nil {
			t.Errorf("connect to drop run-owned database %s", name)
			return
		}
		defer admin.Close(cleanupCtx)
		if _, err := admin.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop run-owned database %s: %v", name, err)
		}
	})
	parsed.Path = "/" + name
	return parsed.String()
}

func provisionIntegrationTestDatabaseOnServer(
	t *testing.T,
	ctx context.Context,
	server *managedIntegrationPostgresServer,
) (string, bool) {
	t.Helper()

	databaseName := fmt.Sprintf(
		"oversync_test_%d_%d",
		os.Getpid(),
		managedIntegrationDatabaseSequence.Add(1),
	)
	if err := server.createDatabase(ctx, databaseName); err != nil {
		t.Fatalf("create isolated integration test database %s: %v", databaseName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.dropDatabase(cleanupCtx, databaseName); err != nil {
			t.Errorf("drop isolated integration test database %s: %v", databaseName, err)
		}
	})

	return server.databaseURL(databaseName), true
}

func getManagedIntegrationPostgres() (*managedIntegrationPostgresServer, error) {
	managedIntegrationPostgresOnce.Do(func() {
		managedIntegrationPostgres, managedIntegrationPostgresErr = startManagedIntegrationPostgres(managedIntegrationPostgresImage)
	})
	return managedIntegrationPostgres, managedIntegrationPostgresErr
}

func startManagedIntegrationPostgres(image string) (*managedIntegrationPostgresServer, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("find docker CLI: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	dataDir, err := os.MkdirTemp("", "go-oversync-postgres-")
	if err != nil {
		return nil, fmt.Errorf("create managed PostgreSQL data directory: %w", err)
	}
	if err := os.Chmod(dataDir, 0o777); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("prepare managed PostgreSQL data directory: %w", err)
	}

	containerName := fmt.Sprintf("go-oversync-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	dockerArgs := []string{
		"run",
		"--detach",
		"--rm",
		"--name", containerName,
		"--label", managedIntegrationPostgresTestLabel,
	}
	if testRunID := os.Getenv("OVERSYNC_TEST_RUN_ID"); testRunID != "" {
		dockerArgs = append(dockerArgs, "--label", managedIntegrationPostgresTestRunLabel+"="+testRunID)
	}
	// Docker volumes avoid macOS host-filesystem round trips by default.
	// PostgreSQL's image-provided anonymous volume remains durable across process
	// restarts; fsync and all database settings are unchanged. Container cleanup explicitly removes anonymous volumes.
	storageOverride := os.Getenv("OVERSYNC_TEST_DOCKER_VOLUME")
	useDockerVolume := storageOverride == "1" || storageOverride == "" && runtime.GOOS == "darwin"
	if !useDockerVolume {
		dockerArgs = append(dockerArgs, "--mount", "type=bind,source="+dataDir+",target=/var/lib/postgresql/data")
	}
	dockerArgs = append(dockerArgs,
		"--publish", "127.0.0.1::5432",
		"--env", "POSTGRES_USER=postgres",
		"--env", "POSTGRES_PASSWORD="+managedIntegrationPostgresPassword,
		"--env", "POSTGRES_DB=postgres",
		"--health-cmd", "pg_isready --username postgres --dbname postgres",
		"--health-interval", "250ms",
		"--health-timeout", "5s",
		"--health-retries", "120",
		image,
	)
	containerID, err := dockerOutput(ctx, dockerArgs...)
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, err
	}
	containerID = strings.TrimSpace(containerID)

	started := false
	defer func() {
		if !started {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = dockerOutput(cleanupCtx, "rm", "--force", "--volumes", containerID)
			_ = os.RemoveAll(dataDir)
		}
	}()

	portOutput, err := dockerOutput(ctx, "port", containerID, "5432/tcp")
	if err != nil {
		return nil, err
	}
	host, port, err := parseDockerPublishedPort(portOutput)
	if err != nil {
		return nil, err
	}
	server := &managedIntegrationPostgresServer{
		containerID: containerID,
		dataDir:     dataDir,
		host:        host,
		port:        port,
	}
	if err := server.waitUntilReady(ctx); err != nil {
		logs, logsErr := dockerOutput(context.Background(), "logs", "--tail", "50", containerID)
		if logsErr == nil && logs != "" {
			return nil, fmt.Errorf("wait for %s: %w; container logs: %s", image, err, logs)
		}
		return nil, fmt.Errorf("wait for %s: %w", image, err)
	}

	started = true
	return server, nil
}

func stopManagedIntegrationPostgres() error {
	if managedIntegrationPostgres == nil {
		return nil
	}
	return managedIntegrationPostgres.close()
}

func (s *managedIntegrationPostgresServer) close() error {
	if s.containerID == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, containerErr := dockerOutput(ctx, "rm", "--force", "--volumes", s.containerID)
	if containerErr != nil && strings.Contains(containerErr.Error(), "No such container") {
		containerErr = nil
	}
	return errors.Join(containerErr, os.RemoveAll(s.dataDir))
}

func parseDockerPublishedPort(output string) (string, string, error) {
	line := strings.TrimSpace(strings.SplitN(output, "\n", 2)[0])
	host, port, err := net.SplitHostPort(line)
	if err != nil {
		return "", "", fmt.Errorf("parse docker published port %q: %w", line, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}

func dockerOutput(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	output, err := command.Output()
	if err == nil {
		return strings.TrimSpace(string(output)), nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		stderr := strings.TrimSpace(string(exitErr.Stderr))
		if stderr != "" {
			return "", fmt.Errorf("docker %s: %w: %s", args[0], err, stderr)
		}
	}
	return "", fmt.Errorf("docker %s: %w", args[0], err)
}

func (s *managedIntegrationPostgresServer) databaseURL(databaseName string) string {
	databaseURL := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", managedIntegrationPostgresPassword),
		Host:     net.JoinHostPort(s.host, s.port),
		Path:     "/" + databaseName,
		RawQuery: "sslmode=disable",
	}
	return databaseURL.String()
}

func (s *managedIntegrationPostgresServer) waitUntilReady(ctx context.Context) error {
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		conn, err := pgx.Connect(attemptCtx, s.databaseURL("postgres"))
		if err == nil {
			err = conn.Ping(attemptCtx)
			closeErr := conn.Close(attemptCtx)
			if err == nil {
				err = closeErr
			}
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("PostgreSQL did not become ready: %w (last connection error: %v)", ctx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *managedIntegrationPostgresServer) createDatabase(ctx context.Context, databaseName string) error {
	conn, err := pgx.Connect(ctx, s.databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize())
	return err
}

func (s *managedIntegrationPostgresServer) dropDatabase(ctx context.Context, databaseName string) error {
	conn, err := pgx.Connect(ctx, s.databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)")
	return err
}
