package oversqlite_e2e

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_GettingStartedDDLAttachPushPull(t *testing.T) {
	ctx := context.Background()
	guide, err := os.ReadFile(filepath.Join("..", "docs", "getting-started.md"))
	require.NoError(t, err)
	serverDDL := gettingStartedFencedBlock(t, string(guide), "## Step 2: Create Your Business Tables", "sql")
	clientDDL := gettingStartedClientDDL(t, string(guide))

	databaseURL := createIsolatedExampleDatabase(t, integrationTestDatabaseURL())
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	_, err = pool.Exec(ctx, serverDDL)
	require.NoError(t, err)

	registeredTables := []oversync.RegisteredTable{
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
	}
	schemaGenerations := []oversync.SchemaGeneration{{
		ID: 1,
		Tables: []oversync.GenerationTable{
			{
				Schema: "business",
				Table:  "users",
				Columns: []oversync.GenerationColumn{
					{Name: "created_at", WireType: oversync.GenerationWireTypeText},
					{Name: "email", WireType: oversync.GenerationWireTypeText},
					{Name: "id", WireType: oversync.GenerationWireTypeUUID},
					{Name: "name", WireType: oversync.GenerationWireTypeText},
				},
			},
			{
				Schema: "business",
				Table:  "posts",
				Columns: []oversync.GenerationColumn{
					{Name: "author_id", WireType: oversync.GenerationWireTypeUUID},
					{Name: "content", WireType: oversync.GenerationWireTypeText, Nullable: true},
					{Name: "created_at", WireType: oversync.GenerationWireTypeText},
					{Name: "id", WireType: oversync.GenerationWireTypeUUID},
					{Name: "title", WireType: oversync.GenerationWireTypeText},
				},
				ForeignKeys: []oversync.GenerationForeignKey{{
					Columns:           []string{"author_id"},
					ReferencedSchema:  "business",
					ReferencedTable:   "users",
					ReferencedColumns: []string{"id"},
					OnDelete:          oversync.GenerationForeignKeyCascade,
					OnUpdate:          oversync.GenerationForeignKeyNoAction,
				}},
			},
		},
	}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := &oversync.ServiceConfig{
		SchemaGenerations:              schemaGenerations,
		AppName:                        "getting-started-executable-test",
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}
	config.Readiness = gettingStartedReadiness(t, ctx, pool, config, serverDDL)
	service, err := oversync.NewRuntimeService(pool, config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close(context.Background())) })
	require.NoError(t, service.Bootstrap(ctx))

	clientConfig := &oversqlite.Config{
		GenerationID: 1, DatabaseID: "app-db", ServiceID: "my-service", LocalVersion: 1,
		Tables:      []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}, {TableName: "posts", SyncKeyColumnName: "id"}},
		UploadLimit: 1000, DownloadLimit: 1000,
	}

	const userID = "getting-started-user"
	handlers := oversync.NewHTTPSyncHandlers(service, logger)
	withActor := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: func(context.Context) (string, error) { return userID, nil },
	})
	mux := http.NewServeMux()
	mux.Handle("POST /sync/connect", withActor(http.HandlerFunc(handlers.HandleConnect)))
	mux.Handle("POST /sync/push", withActor(http.HandlerFunc(handlers.HandlePush)))
	mux.Handle("POST /sync/push-outcomes/resolve", withActor(http.HandlerFunc(handlers.HandlePushOutcomeResolve)))
	mux.Handle("POST /sync/push-outcomes/ack", withActor(http.HandlerFunc(handlers.HandlePushOutcomeAck)))
	mux.Handle("GET /sync/pull", withActor(http.HandlerFunc(handlers.HandlePull)))
	mux.Handle("POST /sync/snapshot-sessions", withActor(http.HandlerFunc(handlers.HandleCreateSnapshotSession)))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", withActor(http.HandlerFunc(handlers.HandleGetSnapshotChunk)))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", withActor(http.HandlerFunc(handlers.HandleDeleteSnapshotSession)))
	generationHandler := withActor(http.HandlerFunc(handlers.HandleGeneration))
	mux.Handle("GET /sync/generations/{generation_id}", generationHandler)
	mux.Handle("GET /sync/generations/", generationHandler)
	mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
	mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)
	operational := service.ReadinessGuard(mux)
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (r.URL.Path == "/sync/generations" || strings.HasPrefix(r.URL.Path, "/sync/generations/")) {
			generationHandler.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/sync/") {
			operational.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
	server := httptest.NewServer(rootHandler)
	t.Cleanup(server.Close)

	newDocumentedClient := func() (*oversqlite.Client, *sql.DB) {
		client := openFreshSQLiteClient(t, filepath.Join(t.TempDir(), "client.sqlite"), server.URL, func(context.Context) (string, error) {
			return "test-token", nil
		}, clientConfig, clientDDL)
		attach, attachErr := client.Attach(ctx, userID)
		require.NoError(t, attachErr)
		require.Equal(t, oversqlite.AttachStatusConnected, attach.Status)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client, client.Database()
	}

	writer, writerDB := newDocumentedClient()
	reader, readerDB := newDocumentedClient()
	userRowID := uuid.NewString()
	postRowID := uuid.NewString()
	createdAt := time.Date(2026, time.September, 5, 12, 34, 56, 0, time.UTC).Format(time.RFC3339)
	_, err = writerDB.Exec(`INSERT INTO users(id, name, email, created_at) VALUES (?, ?, ?, ?)`, userRowID, "Ada", "ada@example.com", createdAt)
	require.NoError(t, err)
	_, err = writerDB.Exec(`INSERT INTO posts(id, author_id, title, content, created_at) VALUES (?, ?, ?, ?, ?)`, postRowID, userRowID, "Hello", nil, createdAt)
	require.NoError(t, err)

	mustPushPendingE2E(t, writer, ctx)
	mustPullToStableE2E(t, reader, ctx)

	var name, email, pulledCreatedAt string
	require.NoError(t, readerDB.QueryRow(`SELECT name, email, created_at FROM users WHERE id=?`, userRowID).Scan(&name, &email, &pulledCreatedAt))
	require.Equal(t, "Ada", name)
	require.Equal(t, "ada@example.com", email)
	require.Equal(t, createdAt, pulledCreatedAt)
	var authorID, title string
	var content sql.NullString
	require.NoError(t, readerDB.QueryRow(`SELECT author_id, title, content FROM posts WHERE id=?`, postRowID).Scan(&authorID, &title, &content))
	require.Equal(t, userRowID, authorID)
	require.Equal(t, "Hello", title)
	require.False(t, content.Valid)
}

func gettingStartedFencedBlock(t *testing.T, guide, heading, language string) string {
	t.Helper()
	headingIndex := strings.Index(guide, heading)
	require.NotEqual(t, -1, headingIndex, "missing getting-started heading %q", heading)
	marker := "```" + language + "\n"
	start := strings.Index(guide[headingIndex:], marker)
	require.NotEqual(t, -1, start, "missing %s fence after %q", language, heading)
	start += headingIndex + len(marker)
	end := strings.Index(guide[start:], "\n```")
	require.NotEqual(t, -1, end, "unterminated %s fence after %q", language, heading)
	return guide[start : start+end]
}

func gettingStartedClientDDL(t *testing.T, guide string) string {
	t.Helper()
	heading := "## Step 4: Create The SQLite Client"
	headingIndex := strings.Index(guide, heading)
	require.NotEqual(t, -1, headingIndex)
	const marker = "_, err := tx.ExecContext(ctx, `\n"
	start := strings.Index(guide[headingIndex:], marker)
	require.NotEqual(t, -1, start, "missing documented SQLite migration block")
	start += headingIndex + len(marker)
	end := strings.Index(guide[start:], "\n    `)")
	require.NotEqual(t, -1, end, "unterminated documented SQLite db.Exec block")
	return guide[start : start+end]
}

// This executable DDL guide has one static application deployment. Its explicit
// durable evidence is installed by the test controller before Bootstrap.
type gettingStartedReadinessAdapter struct{}

func (gettingStartedReadinessAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var evidence oversync.ReadinessEvidence
	err := r.QueryRow(ctx, `SELECT evidence FROM public.guide_readiness WHERE singleton`).Scan(&evidence)
	return evidence, err
}
func gettingStartedReadiness(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg *oversync.ServiceConfig, ddl string) *oversync.ReadinessConfig {
	t.Helper()
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	require.NoError(t, err)
	hash := sha256.Sum256([]byte(ddl))
	identity := hex.EncodeToString(hash[:])
	deployment := oversync.DeploymentContract{AuthorityID: "getting-started-static", Revision: 1, RegistryFingerprint: registry.Fingerprint(), TransitionRevision: "baseline", TransitionPhase: "baseline", ApplicationCodeSHA256: identity, QualificationSHA256: identity}
	config := &oversync.ReadinessConfig{Adapter: gettingStartedReadinessAdapter{}, AuthorityLock: [2]int32{1869833075, 6}, Expected: deployment}
	evidence := oversync.ReadinessEvidence{Deployment: deployment}
	for _, id := range registry.GenerationIDs() {
		contract, _ := registry.Generation(id)
		generation := oversync.GenerationReadiness{Proof: oversync.SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}}
		config.Generations = append(config.Generations, generation)
		evidence.Generations = append(evidence.Generations, oversync.GenerationEnablement{Requirement: generation, DeploymentRevision: 1, Enabled: true})
	}
	_, err = pool.Exec(ctx, `CREATE TABLE public.guide_readiness(singleton boolean PRIMARY KEY CHECK(singleton),evidence jsonb NOT NULL)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO public.guide_readiness VALUES(true,$1)`, evidence)
	require.NoError(t, err)
	return config
}
