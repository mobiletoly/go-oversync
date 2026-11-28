// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// A local, isolated live authority for the Swift caller-cancellation regression.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (result error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == "--cleanup" {
		cleanupCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		return serverpkg.CleanupSmokeDatabase(cleanupCtx)
	}
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range serverpkg.SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "users" {
			generation.Tables = append(generation.Tables, table)
		}
	}
	inner, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		AppName:           "nethttp-server-example",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		SchemaGenerations: []oversync.SchemaGeneration{generation},
	})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, inner.CloseWithError()) }()
	target, err := url.Parse(inner.URL())
	if err != nil {
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	stopRequests := make(chan struct{})
	outer := httptest.NewServer(cancellationFixtureHandler(proxy, stopRequests, os.Getenv("OVERSYNC_SWIFT_A3_FIXTURE") == "1"))
	defer outer.Close()
	if err := qualificationReadiness(ctx, "swift-cancellation", outer.URL, inner, nil); err != nil {
		return err
	}
	fmt.Printf("OVERSQLITE_SWIFT_CONVERGENT_BASE_URL=%s\n", outer.URL)
	<-ctx.Done()
	close(stopRequests)
	return nil
}

// A3 controls exist only on the isolated local authority. Each barrier has
// request-cancellation and authority-shutdown exits; no runtime owner changes.
func cancellationFixtureHandler(proxy http.Handler, stop <-chan struct{}, a3 bool) http.Handler {
	fixture := &cancellationBarrier{proxy: proxy, stop: stop, controlled: a3}
	fixture.mode.Store("none")
	return fixture
}

type cancellationBarrier struct {
	proxy      http.Handler
	stop       <-chan struct{}
	controlled bool
	pushes     atomic.Int64
	blocked    atomic.Int64
	mode       atomic.Value // string, initialized before serving
}

func (f *cancellationBarrier) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/__test/pushes" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"pushes": f.pushes.Load(), "blocked": f.blocked.Load()})
		return
	}
	if f.controlled && r.URL.Path == "/__test/block" {
		mode := r.URL.Query().Get("path")
		switch mode {
		case "none", "push", "push-response", "resolve", "snapshot", "ack":
			f.mode.Store(mode)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unknown cancellation barrier", http.StatusBadRequest)
		}
		return
	}
	mode := f.mode.Load().(string)
	if strings.HasPrefix(r.URL.Path, "/sync/") {
		fmt.Printf("RECOVERY_SWIFT_HTTP method=%s path=%s barrier=%s source=%q generation=%q fingerprint=%q version=%q\n",
			r.Method, r.URL.Path, mode, r.Header.Get(oversync.SourceIDHeader),
			r.Header.Get(oversync.SchemaGenerationIDHeader), r.Header.Get(oversync.SchemaGenerationFingerprintHeader),
			r.Header.Get(oversync.ProtocolVersionHeader))
	}
	if r.URL.Path == "/sync/push" {
		count := f.pushes.Add(1)
		if mode == "push" || (!f.controlled && count == 2) {
			f.awaitCancellation(r)
			return
		}
		if mode == "push-response" {
			f.holdCommittedReply(w, r)
			return
		}
	}
	if (mode == "resolve" && r.URL.Path == "/sync/push-outcomes/resolve") ||
		(mode == "ack" && r.URL.Path == "/sync/push-outcomes/ack") ||
		(mode == "snapshot" && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodGet) {
		f.awaitCancellation(r)
		return
	}
	f.proxy.ServeHTTP(w, r)
}

// Wait only after the real authority has produced the terminal response. This
// creates a lost committed reply, distinct from a push held before dispatch.
func (f *cancellationBarrier) holdCommittedReply(w http.ResponseWriter, r *http.Request) {
	reply := httptest.NewRecorder()
	f.proxy.ServeHTTP(reply, r)
	if reply.Code >= 200 && reply.Code < 300 {
		f.awaitCancellation(r)
		return
	}
	for name, values := range reply.Header() {
		w.Header()[name] = append([]string(nil), values...)
	}
	w.WriteHeader(reply.Code)
	_, _ = w.Write(reply.Body.Bytes())
}

func (f *cancellationBarrier) awaitCancellation(r *http.Request) {
	f.blocked.Add(1)
	select {
	case <-r.Context().Done():
	case <-f.stop:
	}
}

// qualificationReadiness is opt-in. Ordinary fixture output and lifecycle stay
// unchanged; the coordinator consumes this private receipt before any client.
func qualificationReadiness(ctx context.Context, profile, base string, authority *serverpkg.TestServer, details map[string]any) error {
	if os.Getenv("OVERSYNC_QUALIFICATION_READY_FILE") == "" {
		return nil
	}
	generation, err := authority.SyncService.GetGeneration(ctx, oversync.Actor{UserID: "qualification-readiness", SourceID: "qualification-readiness"}, 1)
	if err != nil {
		return fmt.Errorf("qualification generation readiness: %w", err)
	}
	return publishQualificationReadiness(profile, base, authority.Pool.Config().ConnConfig.Database, generation, details)
}

func publishQualificationReadiness(profile, base, database string, generation any, details map[string]any) error {
	path := os.Getenv("OVERSYNC_QUALIFICATION_READY_FILE")
	if path == "" {
		return nil
	}
	nonce, owner := os.Getenv("OVERSYNC_QUALIFICATION_READY_NONCE"), os.Getenv("OVERSQLITE_PROCESS_OWNER")
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(nonce) || !regexp.MustCompile(`^oversqlite-[a-f0-9]{32}$`).MatchString(owner) {
		return errors.New("private readiness requires exact owner and nonce")
	}
	if profile == "" || base == "" || database == "" || generation == nil {
		return errors.New("incomplete private readiness identity")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.Open(executable)
	if err != nil {
		return err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, binary)
	if err := errors.Join(copyErr, binary.Close()); err != nil {
		return err
	}
	record := map[string]any{
		"schema_version": 1, "nonce": nonce, "owner": owner,
		"pid": os.Getpid(), "pgid": syscall.Getpgrp(), "profile": profile,
		"authority_url": base, "database": database, "generation": generation,
		"fixture_sha256": fmt.Sprintf("%x", digest.Sum(nil)),
		"reset_domain":   nonce, "created_unix_ns": time.Now().UnixNano(), "details": details,
	}
	return writeQualificationReadiness(path, record)
}

func writeQualificationReadiness(path string, record map[string]any) error {
	if !filepath.IsAbs(path) {
		return errors.New("private readiness needs an absolute path")
	}
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || canonical != parent {
		return errors.New("private readiness parent must be a canonical private directory")
	}
	file, err := os.CreateTemp(parent, ".readiness-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encodeErr := json.NewEncoder(file).Encode(record)
	syncErr := file.Sync()
	if err := errors.Join(encodeErr, syncErr, file.Close()); err != nil {
		return err
	}
	// A hard link atomically publishes a complete file and refuses an existing
	// destination, including a stale file or symlink; rename would overwrite it.
	if err := os.Link(file.Name(), path); err != nil {
		return fmt.Errorf("publish private readiness: %w", err)
	}
	return nil
}
