//go:build oversync_audit

package oversync

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	auditHelperProcessEnv        = "OVERSYNC_AUDIT_HELPER_PROCESS"
	auditHelperDatabaseURLEnv    = "OVERSYNC_AUDIT_HELPER_DATABASE_URL"
	auditHelperSchemaEnv         = "OVERSYNC_AUDIT_HELPER_SCHEMA"
	auditHelperUserIDHeader      = "Oversync-Audit-User-ID"
	auditHelperReadyLinePrefix   = "OVERSYNC_AUDIT_HELPER_ADDR="
	auditCommitGateArmPath       = "/__audit/commit-response-gate/arm"
	auditCommitGateStatusPath    = "/__audit/commit-response-gate/status"
	auditCommitGateReleasePath   = "/__audit/commit-response-gate/release"
	auditResponseGateArmPath     = "/__audit/response-gate/arm"
	auditResponseGateStatusPath  = "/__audit/response-gate/status"
	auditResponseGateReleasePath = "/__audit/response-gate/release"

	auditResponseGatePushCreate     = "push_create"
	auditResponseGatePushChunk      = "push_chunk"
	auditResponseGatePushCommit     = "push_commit"
	auditResponseGatePushDelete     = "push_delete"
	auditResponseGatePull           = "pull"
	auditResponseGateSnapshotCreate = "snapshot_create"
	auditResponseGateSnapshotGet    = "snapshot_get"
	auditResponseGateSnapshotDelete = "snapshot_delete"
)

type auditHelperUserIDContextKey struct{}

type auditCommitResponseGateStatus struct {
	Armed     bool   `json:"armed"`
	Waiting   bool   `json:"waiting"`
	Operation string `json:"operation,omitempty"`
}

type auditCommitResponseGate struct {
	mu               sync.Mutex
	armedOperation   string
	waitingOperation string
	release          chan struct{}
}

func auditResponseGateOperationValid(operation string) bool {
	switch operation {
	case auditResponseGatePushCreate,
		auditResponseGatePushChunk,
		auditResponseGatePushCommit,
		auditResponseGatePushDelete,
		auditResponseGatePull,
		auditResponseGateSnapshotCreate,
		auditResponseGateSnapshotGet,
		auditResponseGateSnapshotDelete:
		return true
	default:
		return false
	}
}

func (g *auditCommitResponseGate) arm(operation string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !auditResponseGateOperationValid(operation) || g.armedOperation != "" || g.waitingOperation != "" || g.release != nil {
		return false
	}
	g.armedOperation = operation
	g.release = make(chan struct{})
	return true
}

func (g *auditCommitResponseGate) waitAfterHandler(operation string) {
	g.mu.Lock()
	if g.armedOperation != operation || g.release == nil {
		g.mu.Unlock()
		return
	}
	g.armedOperation = ""
	g.waitingOperation = operation
	release := g.release
	g.mu.Unlock()

	<-release

	g.mu.Lock()
	g.waitingOperation = ""
	g.mu.Unlock()
}

func (g *auditCommitResponseGate) releaseResponse() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release == nil {
		return false
	}
	close(g.release)
	g.release = nil
	g.armedOperation = ""
	return true
}

func (g *auditCommitResponseGate) status() auditCommitResponseGateStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	operation := g.armedOperation
	if g.waitingOperation != "" {
		operation = g.waitingOperation
	}
	return auditCommitResponseGateStatus{
		Armed:     g.armedOperation != "",
		Waiting:   g.waitingOperation != "",
		Operation: operation,
	}
}

func (g *auditCommitResponseGate) wrap(operation string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)
		g.waitAfterHandler(operation)

		for name, values := range recorder.Header() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(recorder.Code)
		_, _ = io.Copy(w, recorder.Body)
	})
}

func newAuditFaultHTTPHandler(service *SyncService, logger *slog.Logger) http.Handler {
	handlers := NewHTTPSyncHandlers(service, logger)
	responseGate := &auditCommitResponseGate{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
	mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)
	armGate := func(w http.ResponseWriter, operation string) {
		if !responseGate.arm(operation) {
			http.Error(w, "response gate operation is invalid or already active", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseGate.status())
	}
	gateStatus := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseGate.status())
	}
	releaseGate := func(w http.ResponseWriter, _ *http.Request) {
		if !responseGate.releaseResponse() {
			http.Error(w, "response gate is not active", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseGate.status())
	}
	mux.HandleFunc("POST "+auditCommitGateArmPath, func(w http.ResponseWriter, _ *http.Request) {
		armGate(w, auditResponseGatePushCommit)
	})
	mux.HandleFunc("GET "+auditCommitGateStatusPath, gateStatus)
	mux.HandleFunc("POST "+auditCommitGateReleasePath, releaseGate)
	mux.HandleFunc("POST "+auditResponseGateArmPath, func(w http.ResponseWriter, r *http.Request) {
		operation := r.URL.Query().Get("operation")
		if !auditResponseGateOperationValid(operation) {
			http.Error(w, "unknown response gate operation", http.StatusBadRequest)
			return
		}
		armGate(w, operation)
	})
	mux.HandleFunc("GET "+auditResponseGateStatusPath, gateStatus)
	mux.HandleFunc("POST "+auditResponseGateReleasePath, releaseGate)

	actorMiddleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			userID, _ := ctx.Value(auditHelperUserIDContextKey{}).(string)
			if userID == "" {
				return "", errors.New("audit user id not found")
			}
			return userID, nil
		},
	})
	withActor := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(
				r.Context(),
				auditHelperUserIDContextKey{},
				r.Header.Get(auditHelperUserIDHeader),
			)
			actorMiddleware(next).ServeHTTP(w, r.WithContext(ctx))
		})
	}

	mux.Handle("POST /sync/connect", withActor(http.HandlerFunc(handlers.HandleConnect)))
	mux.Handle("POST /sync/push", withActor(responseGate.wrap(auditResponseGatePushCommit, http.HandlerFunc(handlers.HandlePush))))
	mux.Handle("GET /sync/pull", withActor(responseGate.wrap(auditResponseGatePull, http.HandlerFunc(handlers.HandlePull))))
	mux.Handle("GET /sync/watch", withActor(http.HandlerFunc(handlers.HandleWatch)))
	mux.Handle("POST /sync/snapshot-sessions", withActor(responseGate.wrap(auditResponseGateSnapshotCreate, http.HandlerFunc(handlers.HandleCreateSnapshotSession))))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", withActor(responseGate.wrap(auditResponseGateSnapshotGet, http.HandlerFunc(handlers.HandleGetSnapshotChunk))))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", withActor(responseGate.wrap(auditResponseGateSnapshotDelete, http.HandlerFunc(handlers.HandleDeleteSnapshotSession))))
	mux.Handle("GET /sync/generations/{generation_id}", withActor(http.HandlerFunc(handlers.HandleGeneration)))
	mux.Handle("GET /sync/generations/", withActor(http.HandlerFunc(handlers.HandleGeneration)))
	return mux
}

func TestAuditHarnessHelperProcess(t *testing.T) {
	if os.Getenv(auditHelperProcessEnv) != "1" {
		return
	}
	require.NoError(t, runAuditHarnessHelperProcess())
}

func runAuditHarnessHelperProcess() error {
	databaseURL := os.Getenv(auditHelperDatabaseURLEnv)
	if databaseURL == "" {
		return errors.New("audit helper database URL is required")
	}
	schemaName := os.Getenv(auditHelperSchemaEnv)
	if schemaName == "" {
		return errors.New("audit helper schema is required")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create audit helper pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping audit helper database: %w", err)
	}

	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{
		Schema: schemaName,
		Table:  "users",
		Columns: []GenerationColumn{
			{Name: "id", WireType: GenerationWireTypeUUID},
			{Name: "name", WireType: GenerationWireTypeText},
			{Name: "email", WireType: GenerationWireTypeText},
		},
	}}}}, AppName: "oversync-audit-helper",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	if err != nil {
		return fmt.Errorf("create audit helper service: %w", err)
	}
	if err := service.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap audit helper service: %w", err)
	}

	watchCtx, cancelWatch := context.WithCancel(ctx)
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- service.RunBundleChangeListener(watchCtx)
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancelWatch()
		_ = service.Close(context.Background())
		return fmt.Errorf("listen for audit helper HTTP: %w", err)
	}
	server := &http.Server{
		Handler:           newAuditFaultHTTPHandler(service, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdownSignal)

	select {
	case serveErr := <-serveDone:
		cancelWatch()
		_ = service.Close(context.Background())
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve audit helper HTTP: %w", serveErr)
	default:
	}
	proof := testSchemaGenerationProofForService(service)
	fmt.Printf("%s%s|%d|%s\n", auditHelperReadyLinePrefix, listener.Addr().String(), proof.ID, proof.Fingerprint)

	select {
	case serveErr := <-serveDone:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			cancelWatch()
			_ = service.Close(context.Background())
			return fmt.Errorf("serve audit helper HTTP: %w", serveErr)
		}
	case <-shutdownSignal:
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		shutdownErr := server.Shutdown(shutdownCtx)
		cancelShutdown()
		if shutdownErr != nil {
			cancelWatch()
			_ = service.Close(context.Background())
			return fmt.Errorf("shutdown audit helper HTTP: %w", shutdownErr)
		}
	}

	cancelWatch()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := service.Close(closeCtx)
	cancelClose()
	if closeErr != nil {
		return fmt.Errorf("close audit helper service: %w", closeErr)
	}

	select {
	case watchErr := <-watchDone:
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) && !errors.Is(watchErr, errServiceShuttingDown) {
			return fmt.Errorf("stop audit helper watch listener: %w", watchErr)
		}
	case <-time.After(5 * time.Second):
		return errors.New("audit helper watch listener did not stop")
	}
	return nil
}

type auditHelperProcessOutput struct {
	mu    sync.Mutex
	lines []string
}

func (o *auditHelperProcessOutput) add(stream, line string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lines = append(o.lines, stream+": "+line)
}

func (o *auditHelperProcessOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.Join(o.lines, "\n")
}

type auditHTTPHelperProcess struct {
	command  *exec.Cmd
	address  string
	proof    SchemaGenerationProof
	done     chan struct{}
	waitErr  error
	output   *auditHelperProcessOutput
	stopOnce sync.Once
	stopErr  error
}

var auditSchemaGenerationProofByAddress sync.Map

func startAuditHTTPHelperProcess(t *testing.T, databaseURL, schemaName string) *auditHTTPHelperProcess {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.Command(
		executable,
		"-test.run=^TestAuditHarnessHelperProcess$",
		"-test.count=1",
		"-test.timeout=30s",
	)
	command.Env = append(os.Environ(),
		auditHelperProcessEnv+"=1",
		auditHelperDatabaseURLEnv+"="+databaseURL,
		auditHelperSchemaEnv+"="+schemaName,
	)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	stderr, err := command.StderrPipe()
	require.NoError(t, err)

	process := &auditHTTPHelperProcess{
		command: command,
		done:    make(chan struct{}),
		output:  &auditHelperProcessOutput{},
	}
	ready := make(chan string, 1)
	scan := func(stream string, reader io.Reader, inspectReady bool) {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			process.output.add(stream, line)
			if inspectReady && strings.HasPrefix(line, auditHelperReadyLinePrefix) {
				select {
				case ready <- strings.TrimPrefix(line, auditHelperReadyLinePrefix):
				default:
				}
			}
		}
		if err := scanner.Err(); err != nil {
			process.output.add(stream, "scanner error: "+err.Error())
		}
	}

	require.NoError(t, command.Start())
	go scan("stdout", stdout, true)
	go scan("stderr", stderr, false)
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()

	select {
	case readyValue := <-ready:
		parts := strings.SplitN(readyValue, "|", 3)
		if len(parts) != 3 {
			process.abort()
			t.Fatalf("audit helper reported malformed readiness %q\n%s", readyValue, process.output.String())
		}
		process.address = parts[0]
		if _, err := net.ResolveTCPAddr("tcp", process.address); err != nil {
			process.abort()
			t.Fatalf("audit helper reported invalid address %q: %v\n%s", process.address, err, process.output.String())
		}
		parsedGenerationID, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			process.abort()
			t.Fatalf("audit helper reported invalid generation id %q: %v", parts[1], err)
		}
		process.proof = SchemaGenerationProof{ID: SchemaGenerationID(parsedGenerationID), Fingerprint: parts[2]}
		auditSchemaGenerationProofByAddress.Store(process.address, process.proof)
	case <-process.done:
		t.Fatalf("audit helper exited before readiness: %v\n%s", process.waitErr, process.output.String())
	case <-time.After(10 * time.Second):
		process.abort()
		t.Fatalf("timed out waiting for audit helper readiness\n%s", process.output.String())
	}

	t.Cleanup(func() {
		auditSchemaGenerationProofByAddress.Delete(process.address)
		if err := process.Stop(); err != nil {
			t.Errorf("stop audit helper: %v\n%s", err, process.output.String())
		}
	})
	return process
}

func (p *auditHTTPHelperProcess) URL() string {
	return "http://" + p.address
}

func (p *auditHTTPHelperProcess) abort() {
	if p.command.Process == nil {
		return
	}
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

func (p *auditHTTPHelperProcess) Stop() error {
	p.stopOnce.Do(func() {
		if p.command.Process != nil {
			if err := p.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
				p.stopErr = fmt.Errorf("signal audit helper: %w", err)
			}
		}

		select {
		case <-p.done:
			if p.waitErr != nil && p.stopErr == nil {
				p.stopErr = fmt.Errorf("wait for audit helper: %w", p.waitErr)
			}
		case <-time.After(10 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			p.stopErr = errors.New("audit helper did not stop after interrupt")
		}
	})
	return p.stopErr
}

func (p *auditHTTPHelperProcess) Kill() error {
	p.stopOnce.Do(func() {
		if p.command.Process != nil {
			if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				p.stopErr = fmt.Errorf("kill audit helper: %w", err)
			}
		}

		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			p.stopErr = errors.New("audit helper did not exit after kill")
		}
	})
	return p.stopErr
}

func newAuditHelperDatabase(t *testing.T) (string, string) {
	t.Helper()
	ctx := context.Background()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))

	if !managed {
		require.NoError(t, resetTestSyncSchema(ctx, pool))
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed audit helper database: %v", err)
			}
		})
	}

	schemaName := fmt.Sprintf("audit_harness_%d", managedIntegrationDatabaseSequence.Add(1))
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
			t.Errorf("drop audit helper schema: %v", err)
		}
	})
	return databaseURL, schemaName
}

func newAuditHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   5 * time.Second,
	}
}

func TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops(t *testing.T) {
	databaseURL, schemaName := newAuditHelperDatabase(t)
	process := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	client := newAuditHTTPClient()
	t.Cleanup(client.CloseIdleConnections)

	response, err := client.Get(process.URL() + "/syncx/health")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())

	request, err := http.NewRequest(http.MethodGet, process.URL()+"/sync/generations/1", nil)
	require.NoError(t, err)
	request.Header.Set(auditHelperUserIDHeader, "audit-user")
	request.Header.Set(SourceIDHeader, "audit-source")
	response, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())

	require.NoError(t, process.Stop())
	_, err = client.Get(process.URL() + "/syncx/health")
	require.Error(t, err, "stopped audit helper must no longer accept HTTP connections")
}
