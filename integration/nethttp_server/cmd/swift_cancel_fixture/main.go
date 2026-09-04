// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// A local, isolated live authority for the Swift caller-cancellation regression.
package main

import (
	"context"
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
	fmt.Printf("OVERSQLITE_SWIFT_CONVERGENT_BASE_URL=%s\n", outer.URL)
	<-ctx.Done()
	close(stopRequests)
	return nil
}

// A3 controls exist only on the isolated local authority. Each barrier has
// request-cancellation and authority-shutdown exits; no runtime owner changes.
func cancellationFixtureHandler(proxy http.Handler, stop <-chan struct{}, a3 bool) http.Handler {
	var pushes, blocked atomic.Int64
	var blockPush, blockSnapshot atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__test/pushes" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"pushes": pushes.Load(), "blocked": blocked.Load()})
			return
		}
		if a3 && r.URL.Path == "/__test/block" {
			blockPush.Store(r.URL.Query().Get("path") == "push")
			blockSnapshot.Store(r.URL.Query().Get("path") == "snapshot")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		hold := false
		if r.URL.Path == "/sync/push" {
			count := pushes.Add(1)
			hold = blockPush.Load() || (!a3 && count == 2)
		}
		if strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") && r.Method == http.MethodGet {
			hold = blockSnapshot.Load()
		}
		if hold {
			blocked.Add(1)
			select {
			case <-r.Context().Done():
			case <-stop:
			}
			return
		}
		proxy.ServeHTTP(w, r)
	})
}
