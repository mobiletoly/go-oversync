//go:build perf

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/integration/mobile_flow/simulator"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

func TestPerfComplexMultiBatch_RepeatedPull503RestartsConverge(t *testing.T) {
	const injectedFailures = 6
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	databaseURL := strings.TrimSpace(os.Getenv("OVERSYNC_PERF_DATABASE_URL"))
	if databaseURL == "" {
		databaseURL = defaultPerfDatabaseURL
	}
	testDatabaseURL, cleanupDatabase := createIsolatedPerfDatabase(t, ctx, databaseURL)
	defer cleanupDatabase()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		DatabaseURL: testDatabaseURL, JWTSecret: "pull-503-recovery-test", Logger: logger, AppName: "pull-503-recovery-test",
	})
	if err != nil {
		t.Fatalf("start isolated server: %v", err)
	}
	defer server.Close()
	target, err := url.Parse(server.URL())
	if err != nil {
		t.Fatalf("parse isolated server URL: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var rejected atomic.Int64
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync/pull" {
			for {
				attempt := rejected.Load()
				if attempt >= injectedFailures {
					break
				}
				if rejected.CompareAndSwap(attempt, attempt+1) {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(oversync.GenerationErrorResponse{
						Error: string(oversync.HistoryProjectionCapacityExceeded), Message: "history projection capacity exceeded", Retryable: true, Remediation: "retry",
					})
					return
				}
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	defer front.Close()

	verifier, err := simulator.NewWorkerDatabaseVerifier(ctx, testDatabaseURL, logger)
	if err != nil {
		t.Fatalf("open isolated verifier: %v", err)
	}
	defer verifier.Close()
	const userID = "pull-503-recovery-user"
	result := runSimulationUser(ctx, &config.Config{
		ServerURL: front.URL, DatabaseURL: testDatabaseURL, JWTSecret: "pull-503-recovery-test",
		PreserveDB: true, Logger: logger,
	}, "complex-multi-batch", userID, "phone", verifier)
	if result.sqliteDBPath != "" {
		t.Cleanup(func() {
			for _, suffix := range []string{"", "-wal", "-shm"} {
				if err := os.Remove(result.sqliteDBPath + suffix); err != nil && !os.IsNotExist(err) {
					t.Errorf("remove test-owned SQLite file: %v", err)
				}
			}
		})
	}
	if result.err != nil {
		t.Fatalf("scenario did not recover after %d pull 503s: %v", injectedFailures, result.err)
	}
	if got := rejected.Load(); got != injectedFailures {
		t.Fatalf("injected pull 503s = %d, want %d", got, injectedFailures)
	}

	expected := map[string]int{
		"business.users": 23, "business.posts": 447, "business.files": 50, "business.file_reviews": 50,
	}
	report, err := simulator.NewReportGenerator(verifier).GenerateUserReport(ctx, userID, "complex-multi-batch", result.sqliteDBPath, expected, result.duration)
	if err != nil {
		t.Fatalf("read converged phone and server: %v", err)
	}
	verifyComplexMultiBatchCounts(report, expected)
	if !report.VerificationPassed {
		t.Fatalf("phone and server did not converge: %v", report.Errors)
	}
}
