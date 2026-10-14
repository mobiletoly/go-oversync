package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
)

// The helper runs only inside an owned child test executable.
func TestProcessUserRunner_Helper(t *testing.T) {
	if os.Getenv(processWorkerEnvironment) != "1" {
		return
	}
	var job processUserJob
	if err := json.NewDecoder(os.Stdin).Decode(&job); err != nil {
		t.Fatal(err)
	}
	mode := os.Getenv("MOBILE_FLOW_TEST_CHILD_MODE")
	result := processUserResult{Version: 1, UserID: job.UserID, Completed: true, Duration: time.Millisecond, SQLitePath: "fixture.sqlite"}
	if job.JWTSecret != "private-input-secret" {
		t.Fatal("private input lost its token")
	}
	if strings.Contains(strings.Join(os.Args, " "), job.JWTSecret) {
		t.Fatal("secret exposed in command arguments")
	}
	switch mode {
	case "wrong-user":
		result.UserID = "wrong-user"
	case "reported-failure":
		result.Completed, result.Error = false, "controlled child failure"
	case "missing":
		return
	case "malformed":
		_ = os.WriteFile(job.ResultPath, []byte("{broken"), 0600)
		return
	case "cancel", "stubborn":
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		if mode == "stubborn" {
			signal.Ignore(os.Interrupt, syscall.SIGTERM)
		}
		if err := os.WriteFile(job.ResultPath+".ready", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			t.Fatal(err)
		}
		if mode == "stubborn" {
			time.Sleep(time.Hour)
		} else {
			<-ctx.Done()
		}
		result.Completed, result.Error = false, "caller cancelled"
	case "barrier":
		if err := os.WriteFile(job.ResultPath+".ready", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(filepath.Dir(job.ResultPath), "release")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("parent did not release child barrier")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "trailing" {
		body = append(body, []byte(" {}")...)
	}
	if err := os.WriteFile(job.ResultPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	if mode == "nonzero" {
		os.Exit(7)
	}
}

func newTestProcessUserRunner(t *testing.T) (*processUserRunner, *config.Config) {
	t.Helper()
	p, err := newProcessUserRunner()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.cleanup(); err != nil {
			t.Error(err)
		}
	})
	script := filepath.Join(t.TempDir(), "child")
	quoted := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec "+quoted+" -test.run='^TestProcessUserRunner_Helper$'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p.executable = script
	p.output = &childLogWriter{writer: io.Discard}
	return p, &config.Config{JWTSecret: "private-input-secret", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestProcessUserRunner_ResultAndExitPrecedence(t *testing.T) {
	for _, mode := range []string{"success", "wrong-user", "reported-failure", "missing", "malformed", "trailing", "nonzero"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MOBILE_FLOW_TEST_CHILD_MODE", mode)
			p, cfg := newTestProcessUserRunner(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := p.run(ctx, cfg, "complex-multi-batch", "exact-user", "exact-device", 1)
			if (result.err == nil) != (mode == "success") {
				t.Fatalf("mode=%s result=%+v", mode, result)
			}
			if result.userID != "exact-user" {
				t.Fatal("parent identity changed")
			}
		})
	}
}

func waitOwnedChildren(t *testing.T, directory string, count int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		files, err := filepath.Glob(filepath.Join(directory, "*.ready"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) >= count {
			return files
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("owned children did not reach their barriers")
	return nil
}

func TestProcessUserRunner_CancellationReapsOwnedChildAndStopsQueuedLaunch(t *testing.T) {
	for _, mode := range []string{"cancel", "stubborn"} {
		t.Run(mode, func(t *testing.T) { cancelledProcessCase(t, mode) })
	}
}

func cancelledProcessCase(t *testing.T, mode string) {
	t.Helper()
	t.Setenv("MOBILE_FLOW_TEST_CHILD_MODE", mode)
	p, cfg := newTestProcessUserRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan simulationUserResult, 1)
	go func() { done <- p.run(ctx, cfg, "complex-multi-batch", "cancel-user", "device", 1) }()
	ready := waitOwnedChildren(t, p.directory, 1)
	body, err := os.ReadFile(ready[0])
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(body))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("cancelled result=%v", result.err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("owned child was not joined")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("owned child still exists: %v", err)
	}
	result := p.run(ctx, cfg, "complex-multi-batch", "queued-user", "device", 2)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatal(result.err)
	}
	if _, err := os.Stat(filepath.Join(p.directory, "user-002.json.ready")); !os.IsNotExist(err) {
		t.Fatal("queued child started after cancellation")
	}
}

func TestProcessUserRunner_ActiveProcessesStayWithinConcurrency(t *testing.T) {
	t.Setenv("MOBILE_FLOW_TEST_CHILD_MODE", "barrier")
	p, cfg := newTestProcessUserRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var mu sync.Mutex
	var failures []error
	done := make(chan struct{})
	go func() {
		runBoundedUserWorkers(8, 3, func(index int) {
			result := p.run(ctx, cfg, "complex-multi-batch", fmt.Sprintf("user-%d", index), "device", index)
			if result.err != nil {
				mu.Lock()
				failures = append(failures, result.err)
				mu.Unlock()
			}
		})
		close(done)
	}()
	ready := waitOwnedChildren(t, p.directory, 3)
	if len(ready) != 3 {
		t.Fatalf("active children=%d, want3", len(ready))
	}
	if err := os.WriteFile(filepath.Join(p.directory, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(failures) != 0 {
		t.Fatal(failures)
	}
	if got := len(waitOwnedChildren(t, p.directory, 8)); got != 8 {
		t.Fatalf("completed child processes=%d", got)
	}
}
