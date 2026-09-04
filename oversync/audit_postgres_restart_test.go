//go:build oversync_audit

package oversync

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditRestartPostgres struct {
	containerID, containerName, dataDir string
	server                              *managedIntegrationPostgresServer
}

func newAuditRestartPostgres(t *testing.T) *auditRestartPostgres {
	t.Helper()
	require.NoError(t, exec.Command("docker", "version").Run())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	dir, err := os.MkdirTemp("", "go-oversync-audit-restart-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o777))
	p := &auditRestartPostgres{containerName: "go-oversync-audit-restart-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12], dataDir: dir, server: &managedIntegrationPostgresServer{host: host, port: port}}
	p.start(t)
	t.Cleanup(func() {
		if p.containerID != "" {
			_, _ = dockerOutput(context.Background(), "rm", "--force", "--volumes", p.containerID)
		}
		require.NoError(t, os.RemoveAll(dir))
	})
	return p
}

func (p *auditRestartPostgres) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := dockerOutput(ctx, "run", "--detach", "--name", p.containerName, "--publish", net.JoinHostPort(p.server.host, p.server.port)+":5432", "--mount", "type=bind,source="+p.dataDir+",target=/var/lib/postgresql/data", "--env", "POSTGRES_USER=postgres", "--env", "POSTGRES_PASSWORD="+managedIntegrationPostgresPassword, "--env", "POSTGRES_DB=postgres", managedIntegrationPostgresImage)
	require.NoError(t, err)
	p.containerID = strings.TrimSpace(id)
	p.server.containerID = p.containerID
	require.NoError(t, p.server.waitUntilReady(ctx))
}

func (p *auditRestartPostgres) databaseURL() string { return p.server.databaseURL("postgres") }

func (p *auditRestartPostgres) crashAndRestart(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := dockerOutput(ctx, "kill", "--signal", "KILL", p.containerID)
	require.NoError(t, err)
	_, err = dockerOutput(ctx, "rm", "--force", p.containerID)
	require.NoError(t, err)
	p.containerID = ""
	p.start(t)
}

func newAuditRestartPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	require.NoError(t, err)
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))
	return pool
}

func requireAuditRestartPoolReady(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return pool.Ping(ctx) == nil
	}, 30*time.Second, 50*time.Millisecond)
}

func TestAuditPostgresRestart_DirectPushReplayAfterInterruptedResponse(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())
	schema := "audit_restart_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	tables := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(tables), RegisteredTables: tables, RegisteredTableCatalogRevision: 1, AppName: "audit-restart-direct"}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: "audit-restart-user", SourceID: "writer"}
	id := uuid.NewString()
	row := PushRequestRow{Schema: schema, Table: "users", Key: SyncKey{"id": id}, Op: OpInsert, Payload: []byte(`{"id":"` + id + `","name":"Restart","email":"restart@example.com"}`)}
	// Build one immutable request, then prove it remains replayable after a physical restart.
	init := resolveConnectForPush(t, ctx, svc, actor, true)
	hash := mustCanonicalPushRequestHash(t, svc, []PushRequestRow{row})
	request := &OneShotPushRequest{SourceBundleID: 1, CanonicalRequestHash: hash, InitializationID: init, Rows: []PushRequestRow{row}}
	response, err := svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(svc), request)
	require.NoError(t, err)
	postgres.crashAndRestart(t)
	requireAuditRestartPoolReady(t, pool)
	replay, err := svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(svc), request)
	require.NoError(t, err)
	require.Equal(t, response.Bundle.BundleSeq, replay.Bundle.BundleSeq)
	require.Equal(t, response.Bundle.BundleHash, replay.Bundle.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, pool)
}
