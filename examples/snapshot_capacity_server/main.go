// Snapshot capacity authority for the owned SQLiteNow qualification lane.
// Running this example creates 50,000 synthetic rows in an isolated database.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	defaultCapacityRows = 50_000
	childFirstOrdinal   = 13
	bundleRows          = 200
	businessSchema      = "business"
	capacityUser        = "snapshot-capacity-user"
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
		return serverpkg.CleanupSmokeDatabase(ctx)
	}
	if len(os.Args) != 1 {
		return errors.New("usage: snapshotcapacity [--cleanup]")
	}
	capacityRows, err := configuredCapacityRows(os.Getenv("OVERSQLITE_CAPACITY_ROWS"))
	if err != nil {
		return err
	}
	authority, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		AppName:           "nethttp-server-example",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		RegisteredTables:  registeredTables(),
		SchemaGenerations: schemaGenerations(),
		BeforeBootstrap:   createCapacityTables,
	})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, authority.CloseWithError()) }()
	actor := oversync.Actor{UserID: capacityUser, SourceID: "capacity-seed-source"}
	generation, err := authority.SyncService.GetGeneration(ctx, actor, 1)
	if err != nil {
		return err
	}
	proof := oversync.SchemaGenerationProof{ID: 1, Fingerprint: generation.SchemaGeneration.Fingerprint}
	if _, err := authority.SyncService.Connect(ctx, actor, proof, &oversync.ConnectRequest{}); err != nil {
		return err
	}
	lastBundle, err := seedRows(ctx, authority, actor, capacityRows)
	if err != nil {
		return err
	}
	if err := verifyRows(ctx, authority.Pool, capacityRows); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	control := replacementControl(authority, actor, lastBundle, capacityRows)
	controlServer := &http.Server{Handler: control}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, controlServer.Shutdown(shutdownCtx))
	}()
	go func() { _ = controlServer.Serve(listener) }()
	if err := qualificationReadiness(ctx, "capacity", authority.URL(), authority, map[string]any{"control_url": "http://" + listener.Addr().String(), "rows": capacityRows, "tables": 14, "initial_bundle": lastBundle}); err != nil {
		return err
	}
	fmt.Printf("READY url=%s control=http://%s rows=%d tables=14 fingerprint=%s initial_bundle=%d\n",
		authority.URL(), listener.Addr(), capacityRows, proof.Fingerprint, lastBundle)
	<-ctx.Done()
	return nil
}

func registeredTables() []oversync.RegisteredTable {
	tables := make([]oversync.RegisteredTable, 14)
	for index := range tables {
		tables[index] = oversync.RegisteredTable{
			Schema: businessSchema, Table: tableName(index), SyncKeyColumns: []string{"id"},
		}
	}
	return tables
}

func schemaGenerations() []oversync.SchemaGeneration {
	tables := make([]oversync.GenerationTable, 14)
	for index := range tables {
		columns := []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeText},
			{Name: "name", WireType: oversync.GenerationWireTypeText},
		}
		var foreignKeys []oversync.GenerationForeignKey
		if index == 13 {
			for parent := 0; parent < 3; parent++ {
				column := fmt.Sprintf("parent_%d", parent)
				columns = append(columns, oversync.GenerationColumn{Name: column, WireType: oversync.GenerationWireTypeText})
				foreignKeys = append(foreignKeys, oversync.GenerationForeignKey{
					Columns: []string{column}, ReferencedSchema: businessSchema,
					ReferencedTable: tableName(parent), ReferencedColumns: []string{"id"},
					OnDelete: oversync.GenerationForeignKeyNoAction,
					OnUpdate: oversync.GenerationForeignKeyNoAction,
				})
			}
		}
		tables[index] = oversync.GenerationTable{
			Schema: businessSchema, Table: tableName(index), Columns: columns, ForeignKeys: foreignKeys,
		}
	}
	return []oversync.SchemaGeneration{{ID: 1, Tables: tables}}
}

func createCapacityTables(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if schema != businessSchema {
		return fmt.Errorf("unexpected business schema %q", schema)
	}
	for index := 0; index < 14; index++ {
		columns := "_sync_scope_id text NOT NULL, id text NOT NULL, name text NOT NULL, PRIMARY KEY (_sync_scope_id,id)"
		if index == 13 {
			for parent := 0; parent < 3; parent++ {
				columns += fmt.Sprintf(", parent_%d text NOT NULL, FOREIGN KEY (_sync_scope_id,parent_%d) REFERENCES business.%s(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED",
					parent, parent, tableName(parent))
			}
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE business.%s (%s)", tableName(index), columns)); err != nil {
			return fmt.Errorf("create %s: %w", tableName(index), err)
		}
	}
	return nil
}

func tableName(index int) string { return fmt.Sprintf("records_%02d", index) }

func seedRows(ctx context.Context, authority *serverpkg.TestServer, actor oversync.Actor, capacityRows int) (int64, error) {
	var bundleID int64 = 1
	if err := writeBundle(ctx, authority, actor, bundleID, "initial-parents", func(writeCtx context.Context, tx oversync.DatabaseWriteTx) error {
		for index := 0; index < childFirstOrdinal; index++ {
			if _, err := tx.Exec(writeCtx, fmt.Sprintf("INSERT INTO business.%s(_sync_scope_id,id,name) VALUES($1,'parent',$2)", tableName(index)),
				actor.UserID, fmt.Sprintf("synthetic-%d", index)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for first := childFirstOrdinal; first < capacityRows; first += bundleRows {
		bundleID++
		last := min(first+bundleRows-1, capacityRows-1)
		if err := writeBundle(ctx, authority, actor, bundleID, "initial-children", func(writeCtx context.Context, tx oversync.DatabaseWriteTx) error {
			_, err := tx.Exec(writeCtx, `INSERT INTO business.records_13(_sync_scope_id,id,name,parent_0,parent_1,parent_2)
				SELECT $1,'row-'||lpad(i::text,5,'0'),'synthetic-'||i,'parent','parent','parent'
				FROM generate_series($2::int,$3::int) AS i`, actor.UserID, first, last)
			return err
		}); err != nil {
			return 0, err
		}
	}
	return bundleID, nil
}

func replacementControl(authority *serverpkg.TestServer, actor oversync.Actor, initialBundle int64, capacityRows int) http.Handler {
	mux := http.NewServeMux()
	var started atomic.Bool
	mux.HandleFunc("POST /replace", func(w http.ResponseWriter, r *http.Request) {
		if !started.CompareAndSwap(false, true) {
			http.Error(w, "replacement already requested", http.StatusConflict)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		last, err := replaceRows(ctx, authority, actor, initialBundle, capacityRows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := verifyRows(ctx, authority.Pool, capacityRows); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"rows\":%d,\"tables\":14,\"bundle_seq\":%d}\n", capacityRows, last)
	})
	return mux
}

func replaceRows(ctx context.Context, authority *serverpkg.TestServer, actor oversync.Actor, initialBundle int64, capacityRows int) (int64, error) {
	bundleID := initialBundle + 1
	if err := writeBundle(ctx, authority, actor, bundleID, "replacement-parents", func(writeCtx context.Context, tx oversync.DatabaseWriteTx) error {
		for index := 0; index < childFirstOrdinal; index++ {
			if _, err := tx.Exec(writeCtx, fmt.Sprintf("UPDATE business.%s SET name=$1 WHERE id='parent'", tableName(index)),
				fmt.Sprintf("replacement-%d", index)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for first := childFirstOrdinal; first < capacityRows; first += bundleRows {
		bundleID++
		last := min(first+bundleRows-1, capacityRows-1)
		if err := writeBundle(ctx, authority, actor, bundleID, "replacement-children", func(writeCtx context.Context, tx oversync.DatabaseWriteTx) error {
			_, err := tx.Exec(writeCtx, `UPDATE business.records_13 SET name='replacement-'||i::text
				FROM generate_series($1::int,$2::int) AS i
				WHERE id='row-'||lpad(i::text,5,'0')`, first, last)
			return err
		}); err != nil {
			return 0, err
		}
	}
	return bundleID, nil
}

func writeBundle(ctx context.Context, authority *serverpkg.TestServer, actor oversync.Actor,
	bundleID int64, label string, write func(context.Context, oversync.DatabaseWriteTx) error,
) error {
	opts := oversync.RetryableBundleWriteOptions{
		OperationHash:       sha256.Sum256([]byte(fmt.Sprintf("%s-%d", label, bundleID))),
		OperationValidUntil: time.Now().UTC().Add(time.Hour),
	}
	return authority.SyncService.WithinSyncBundle(ctx, actor,
		oversync.BundleSource{SourceID: actor.SourceID, SourceBundleID: bundleID}, opts,
		write)
}

func verifyRows(ctx context.Context, pool *pgxpool.Pool, capacityRows int) error {
	var total int64
	for index := 0; index < 14; index++ {
		var count int64
		if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM business.%s", tableName(index))).Scan(&count); err != nil {
			return err
		}
		total += count
	}
	if total != int64(capacityRows) {
		return fmt.Errorf("wrong 14-table row count: %d", total)
	}
	return nil
}

func configuredCapacityRows(raw string) (int, error) {
	if raw == "" {
		return defaultCapacityRows, nil
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows < 50 || rows > defaultCapacityRows {
		return 0, errors.New("OVERSQLITE_CAPACITY_ROWS must be an integer in 50..50000")
	}
	return rows, nil
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
