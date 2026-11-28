package simulator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	evolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func startTableHTTP(t *testing.T, dsn string, phase evolution.TablePhase) *serverpkg.TestServer {
	t.Helper()
	cfg, err := evolution.TableEvolutionConfiguration(phase)
	require.NoError(t, err)
	s, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{DatabaseURL: dsn, JWTSecret: "table_evolution-local-secret", Logger: slog.New(slog.DiscardHandler), AppName: "table_evolution", BusinessSchema: "main", ApplicationSchemaPrepared: true, Readiness: cfg.Readiness, RegisteredTables: cfg.RegisteredTables, RegisteredTableCatalogRevision: cfg.RegisteredTableCatalogRevision, SchemaGenerations: cfg.SchemaGenerations, ReservedServerSourceIDs: cfg.ReservedServerSourceIDs, SyncAdmissionMiddleware: func(next http.Handler) http.Handler {
		return evolution.ClientRuntimeGuard(func(r *http.Request) bool { return r.Header.Get("Authorization") != "" }, next)
	}})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}
func tableMobilePlan(old, target versionedApplicationDefinition) oversqlite.UpgradePlan {
	return oversqlite.UpgradePlan{PlanID: "table_evolution-upgrade", Mode: oversqlite.UpgradeModeGeneration, DatabaseID: "first", ServiceID: "table_evolution", InstalledVersion: 1, TargetVersion: 2, UserID: "scope", InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID, LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator}
}

func TestMobileFlow_TableEvolutionProcessHelper(t *testing.T) {
	path := os.Getenv("TABLE_EVOLUTION_SQLITE")
	if path == "" {
		t.Skip("process entry")
	}
	old, target, err := tableUpgradeDefinitions()
	require.NoError(t, err)
	plan := tableMobilePlan(old, target)
	if os.Getenv("TABLE_EVOLUTION_BOUNDARY") == "migration" {
		base := target.Migrator
		plan.Migrator = mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, step oversqlite.LocalMigrationStep) error {
			require.NoError(t, base.Migrate(ctx, tx, step))
			os.Exit(97)
			return nil
		}}
	}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: os.Getenv("TABLE_EVOLUTION_URL"), Token: func(context.Context) (string, error) { return os.Getenv("TABLE_EVOLUTION_TOKEN"), nil }, TargetClientConfig: versionedTargetConfig(plan, target), Progress: func(s oversqlite.OpeningStatus) {
		if os.Getenv("TABLE_EVOLUTION_BOUNDARY") == "snapshot" && s.Phase == oversqlite.UpgradePhaseRebuildRunning && s.SnapshotRowCount > 0 && s.SnapshotRowsStaged == s.SnapshotRowCount {
			os.Exit(97)
		}
	}})
	require.NoError(t, err)
	_, err = opening.Advance(context.Background())
	require.NoError(t, err)
	t.Fatal("boundary not reached")
}

func TestMobileFlow_TableEvolutionAddRemoveFKSameFile(t *testing.T) {
	runTableEvolutionMobileJourney(t, nil)
}

func runTableEvolutionMobileJourney(t *testing.T, extension func(mobileRolloutState)) {

	data, err := os.ReadFile("../../../conformance/table-evolution/v1/cases.json")
	require.NoError(t, err)
	var vector struct {
		Person string `json:"person"`
		Cases  []struct {
			ID                string   `json:"id"`
			Pending           []string `json:"pending"`
			Boundaries        []string `json:"boundaries"`
			MaintenanceStatus int      `json:"maintenance_status"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(data, &vector))
	var boundaries []string
	for _, c := range vector.Cases {
		switch c.ID {
		case "omitted-table-upgrade":
			require.ElementsMatch(t, []string{"insert", "update", "delete", "committed_reply_lost", "newer_intent"}, c.Pending)
			boundaries = c.Boundaries
		case "interrupted-append-sessions":
			require.Equal(t, http.StatusServiceUnavailable, c.MaintenanceStatus)
		}
	}
	require.Equal(t, []string{"migration", "snapshot"}, boundaries)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	dsn := createMobileUpgradeDatabase(t)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	var version int
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version))
	require.Equal(t, mobileQualificationMajor(t), version/10000)
	publish := func(a *evolution.Authority, phase evolution.TablePhase) {
		cfg, err := evolution.TableEvolutionConfiguration(phase)
		require.NoError(t, err)
		require.NoError(t, a.Publish(ctx, cfg))
		a.Close()
	}
	a, err := evolution.OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, evolution.TablesBaseline))
	publish(a, evolution.TablesBaseline)
	s := startTableHTTP(t, dsn, evolution.TablesBaseline)
	address, retarget := newStableMobileAuthority(t, s.URL())
	require.NoError(t, evolution.ReopenTableServing(ctx, pool, []*oversync.SyncService{s.SyncService}))
	_, err = evolution.CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	old, target, err := tableUpgradeDefinitions()
	require.NoError(t, err)
	token, err := s.GenerateToken("scope", time.Hour)
	require.NoError(t, err)
	provider := func(context.Context) (string, error) { return token, nil }
	fresh := func(id string) oversqlite.UpgradePlan {
		return oversqlite.UpgradePlan{PlanID: "table_evolution-fresh-" + id, Mode: oversqlite.UpgradeModeFresh, DatabaseID: id, ServiceID: "table_evolution", TargetVersion: 1, TargetGenerationID: old.Contract.Proof.ID, LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator}
	}
	path := filepath.Join(t.TempDir(), "same.sqlite")
	first, _, err := openVersionedApplication(ctx, path, address, provider, http.DefaultClient, fresh("first"), old)
	require.NoError(t, err)
	attachVersionedTestApp(t, first, "scope")
	second, _, err := openVersionedApplication(ctx, filepath.Join(t.TempDir(), "second.sqlite"), address, provider, http.DefaultClient, fresh("second"), old)
	require.NoError(t, err)
	defer second.Close()
	attachVersionedTestApp(t, second, "scope")
	person := vector.Person
	note := uuid.NewString()
	_, err = first.DB.Exec(`INSERT INTO people VALUES(?, 'Person');INSERT INTO legacy_notes VALUES(?,'original');INSERT INTO table_local_notes VALUES('sentinel','preserve');INSERT INTO legacy_notes VALUES('77777777-7777-4777-8777-777777777777','delete me')`, person, note)
	require.NoError(t, err)
	_, err = first.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	// One uncommitted staged note and one committed note with an unseen reply.
	_, err = second.DB.Exec(`INSERT INTO legacy_notes VALUES(?,'disconnected')`, uuid.NewString())
	require.NoError(t, err)
	second.Client.HTTP = &http.Client{Transport: &evolutionFaultTransport{stage: true}}
	_, err = second.Client.PushPending(ctx)
	require.Error(t, err)
	_, err = first.DB.Exec(`UPDATE legacy_notes SET body='committed reply lost' WHERE id=?`, note)
	require.NoError(t, err)
	first.Client.HTTP = &http.Client{Transport: &evolutionFaultTransport{lose: true}}
	_, err = first.Client.PushPending(ctx)
	require.Error(t, err)
	var frozen, source string
	require.NoError(t, second.DB.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&frozen))
	require.NoError(t, first.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&source))
	// A newer edit on the soon-omitted table must settle after lost-reply replay.
	_, err = first.DB.Exec(`UPDATE legacy_notes SET body='newer offline intent' WHERE id=?`, note)
	require.NoError(t, err)
	cfg, _ := evolution.TableEvolutionConfiguration(evolution.TablesBaseline)
	actor := oversync.Actor{UserID: "scope", SourceID: "snapshot-device"}
	_, err = s.SyncService.Connect(ctx, actor, cfg.Readiness.Generations[0].Proof, &oversync.ConnectRequest{})
	require.NoError(t, err)
	_, err = s.SyncService.CreateSnapshotSession(ctx, actor, cfg.Readiness.Generations[0].Proof)
	require.NoError(t, err)
	drain, err := evolution.Drain(ctx, pool, []*oversync.SyncService{s.SyncService}, func(context.Context) error { return nil })
	require.NoError(t, err)
	s.Close()
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	require.NoError(t, a.RequirePreservingClients(ctx, "table_evolution-test-authenticated-runtime"))
	b := evolution.DefaultBounds()
	b.Deadline = time.Now().UTC().Add(time.Minute)
	b.MaxEvidenceBytes = 8 << 20
	b.MaxHistoryBytes = 8 << 20
	b.MaxBundles = 100
	b.MinFreeDiskBytes = 1
	b.Rows = 1
	capacity := func(context.Context) (evolution.Capacity, error) {
		return evolution.Capacity{Available: true, FreeDiskBytes: 1 << 30, RetentionRemaining: 7 * 24 * time.Hour}, nil
	}
	cap, _ := capacity(ctx)
	require.NoError(t, a.ActivateTableEvolution(ctx, uuid.New(), evolution.TableBounds{Bounds: b, CleanupRows: 1, CleanupSessions: 1, CleanupBytes: 8192, MaxCleanupBatches: 100}, cap))
	publish(a, evolution.TablesCleanup)
	s = startTableHTTP(t, dsn, evolution.TablesCleanup)
	retarget(s.URL())
	second.Client.HTTP = http.DefaultClient
	_, err = second.Client.PushPending(ctx)
	require.Error(t, err)
	var same string
	require.NoError(t, second.DB.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&same))
	require.Equal(t, frozen, same)
	// Deliberately shorten fixture TTLs without replacing the durable sessions.
	_, err = pool.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()+interval '50 milliseconds'`)
	require.NoError(t, err)
	w := &evolution.TableWorker{Pool: pool, Service: s.SyncService, Capacity: capacity}
	require.NoError(t, w.Cleanup(ctx))
	s.Close()
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, evolution.TablesAppend))
	publish(a, evolution.TablesAppend)
	s = startTableHTTP(t, dsn, evolution.TablesAppend)
	retarget(s.URL())
	s.Close()
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, evolution.TablesBackfill))
	publish(a, evolution.TablesBackfill)
	s = startTableHTTP(t, dsn, evolution.TablesBackfill)
	retarget(s.URL())
	w.Service = s.SyncService
	require.NoError(t, w.Populate(ctx))
	s.Close()
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	publish(a, evolution.TablesServing)
	s = startTableHTTP(t, dsn, evolution.TablesServing)
	retarget(s.URL())
	require.NoError(t, evolution.ReopenTableServing(ctx, pool, []*oversync.SyncService{s.SyncService}))
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	first.Client.HTTP = http.DefaultClient
	// Dirty DELETE on an old-only table must also reach PostgreSQL before DROP.
	_, err = first.DB.Exec(`DELETE FROM legacy_notes WHERE body='delete me'`)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	// A concurrent old-only edit forces an explicit decision before table DROP.
	_, err = second.DB.Exec(`UPDATE legacy_notes SET body='competing old device' WHERE id=?`, note)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	pausedPlan := tableMobilePlan(old, target)
	pausedPlan.Migrator = mobileUpgradeMigrator{MigrateFunc: func(context.Context, oversqlite.LocalMigrationTx, oversqlite.LocalMigrationStep) error {
		return errors.New("pause before application DDL")
	}}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path, Plan: &pausedPlan, BaseURL: address, Token: provider, TargetClientConfig: versionedTargetConfig(pausedPlan, target)})
	require.NoError(t, err)
	blocked, err := opening.Advance(ctx)
	require.Error(t, err)
	require.Equal(t, oversqlite.UpgradeOutcomeNeedsResolution, blocked.Status.Outcome)
	resolved, err := opening.ResolvePendingIntent(ctx, oversqlite.PendingIntentResolution{Action: oversqlite.PendingIntentKeepLocal, PlanID: blocked.Status.PlanID, Revision: blocked.Status.Revision, RequestHash: blocked.Status.PendingRequestHash})
	require.Error(t, err)
	require.Equal(t, oversqlite.UpgradeOutcomeLocalMigrationFailed, resolved.Status.Outcome)
	require.NoError(t, opening.Close())
	diagnostic, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	var retained string
	require.NoError(t, diagnostic.QueryRow(`SELECT body FROM legacy_notes WHERE id=?`, note).Scan(&retained))
	require.Equal(t, "newer offline intent", retained)
	require.NoError(t, diagnostic.Close())
	// Actual process death rolls back local DDL, then resumes a staged snapshot.
	for _, boundary := range boundaries {
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMobileFlow_TableEvolutionProcessHelper$")
		child.Env = append(os.Environ(), "TABLE_EVOLUTION_SQLITE="+path, "TABLE_EVOLUTION_URL="+address, "TABLE_EVOLUTION_TOKEN="+token, "TABLE_EVOLUTION_BOUNDARY="+boundary)
		out, err := child.CombinedOutput()
		require.Error(t, err, string(out))
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 97, exit.ExitCode(), string(out))
	}
	upgraded, result, err := openVersionedApplication(ctx, path, address, provider, http.DefaultClient, tableMobilePlan(old, target), target)
	require.NoError(t, err)
	defer upgraded.Close()
	attachVersionedTestApp(t, upgraded, "scope")
	require.Equal(t, oversqlite.UpgradePhaseReady, result.Status.Phase)
	var n int
	var newSource, project, body, local string
	require.NoError(t, upgraded.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='legacy_notes'`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, upgraded.DB.QueryRow(`SELECT project_id FROM people WHERE id=?`, person).Scan(&project))
	require.Equal(t, evolution.TableProjectID, project)
	require.NoError(t, upgraded.DB.QueryRow(`SELECT note FROM table_local_notes WHERE id='sentinel'`).Scan(&local))
	require.Equal(t, "preserve", local)
	require.NoError(t, upgraded.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&newSource))
	require.NotEqual(t, source, newSource)
	require.NoError(t, pool.QueryRow(ctx, `SELECT body FROM main.legacy_notes WHERE id=$1::uuid`, note).Scan(&body))
	require.Equal(t, "newer offline intent", body)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.legacy_notes WHERE body='delete me'`).Scan(&n))
	require.Zero(t, n)
	fkRows, err := upgraded.DB.Query(`PRAGMA foreign_key_check`)
	require.NoError(t, err)
	require.False(t, fkRows.Next())
	require.NoError(t, fkRows.Close())
	// Old and target remain operational with distinct views. New explicit NULL
	// clears the relationship; old INSERT stays NULL and UPDATE preserves it.
	_, err = upgraded.DB.Exec(`UPDATE people SET project_id=NULL WHERE id=?`, person)
	require.NoError(t, err)
	_, err = upgraded.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	late := uuid.NewString()
	_, err = second.DB.Exec(`INSERT INTO people VALUES(?,'Late')`, late)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = upgraded.Client.Sync(ctx)
	require.NoError(t, err)
	var nullable *string
	require.NoError(t, upgraded.DB.QueryRow(`SELECT project_id FROM people WHERE id=?`, late).Scan(&nullable))
	require.Nil(t, nullable)
	if extension != nil {
		extension(mobileRolloutState{ctx: ctx, pool: pool, server: s, address: address, retarget: retarget, old: second, target: upgraded, token: token, secret: "table_evolution-local-secret", scope: "scope", table: true})
	}
	t.Logf("table_evolution same-file journey old=%s target=%s removed=legacy_notes added=projects,attachments restart=migration,snapshot source_rotated=%t", old.Contract.Proof.Fingerprint, target.Contract.Proof.Fingerprint, source != newSource)
}
