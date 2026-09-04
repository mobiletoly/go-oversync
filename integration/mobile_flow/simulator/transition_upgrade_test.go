package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	evolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func startEvolutionHTTP(t *testing.T, dsn string, cfg *oversync.ServiceConfig, allowed *atomic.Value) *serverpkg.TestServer {
	t.Helper()
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{SyncAdmissionMiddleware: func(next http.Handler) http.Handler {
		return evolution.ClientRuntimeGuard(func(r *http.Request) bool { return r.Header.Get("Authorization") == allowed.Load().(string) }, next)
	}, DatabaseURL: dsn, JWTSecret: "transition-test-secret", Logger: slog.New(slog.DiscardHandler), AppName: "transition-http", BusinessSchema: "main", ApplicationSchemaPrepared: true, Readiness: cfg.Readiness, RegisteredTables: cfg.RegisteredTables, SchemaGenerations: cfg.SchemaGenerations, ReservedServerSourceIDs: cfg.ReservedServerSourceIDs})
	require.NoError(t, err)
	t.Cleanup(server.Close)
	return server
}

func TestMobileFlow_TransitionPostgreSQLTransitionAndUpgrade(t *testing.T) {
	runTransitionMobileJourney(t, nil)
}

func runTransitionMobileJourney(t *testing.T, extension func(mobileRolloutState)) {
	var vector struct {
		Keys                    []string `json:"keys"`
		OldGeneration           int64    `json:"old_generation"`
		TargetGeneration        int64    `json:"target_generation"`
		QueuedLegacy            string   `json:"queued_legacy"`
		AfterTargetWrite        string   `json:"after_target_write"`
		AfterOldWrite           []string `json:"after_old_write"`
		RuntimeRejectionStatus  int      `json:"runtime_rejection_status"`
		RuntimeRejectionError   string   `json:"runtime_rejection_error"`
		RuntimeRejectionMessage string   `json:"runtime_rejection_message"`
		PreserveFields          []string `json:"preserve_fields"`
	}
	data, err := os.ReadFile("../../../conformance/maintenance-conflicts/v2/runtime-upgrade.json")
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&vector))
	require.ElementsMatch(t, []string{"source_id", "next_source_bundle_id", "canonical_request_hash", "frozen_rows", "business_intent"}, vector.PreserveFields)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dsn := createMobileUpgradeDatabase(t)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	var version int
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version))
	require.Equal(t, mobileQualificationMajor(t), version/10000)
	a, err := evolution.OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 1))
	cfg, err := evolution.Configuration(1, 1, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	var allowed atomic.Value
	allowed.Store("")
	server := startEvolutionHTTP(t, dsn, cfg, &allowed)
	require.NoError(t, evolution.ReopenServing(ctx, pool, []*oversync.SyncService{server.SyncService}))
	scope := "mobile-transition"
	_, err = evolution.CreateScope(ctx, pool, scope)
	require.NoError(t, err)
	old, target, err := mobileUpgradeDefinitions()
	require.NoError(t, err)
	require.Equal(t, vector.OldGeneration, int64(old.Contract.Proof.ID))
	require.Equal(t, vector.TargetGeneration, int64(target.Contract.Proof.ID))
	token, err := server.GenerateToken(scope, time.Hour)
	require.NoError(t, err)
	allowed.Store("Bearer " + token)
	provider := func(context.Context) (string, error) { return token, nil }
	fresh := func(id string) oversqlite.UpgradePlan {
		return oversqlite.UpgradePlan{PlanID: "transition-fresh-" + id, Mode: oversqlite.UpgradeModeFresh, DatabaseID: id, ServiceID: "transition", TargetVersion: 1, TargetGenerationID: old.Contract.Proof.ID, LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator}
	}
	mainPath := filepath.Join(t.TempDir(), "same-file.sqlite")
	first, _, err := openVersionedApplication(ctx, mainPath, server.URL(), provider, http.DefaultClient, fresh("first"), old)
	require.NoError(t, err)
	attachVersionedTestApp(t, first, scope)
	secondPath := filepath.Join(t.TempDir(), "second.sqlite")
	second, _, err := openVersionedApplication(ctx, secondPath, server.URL(), provider, http.DefaultClient, fresh("second"), old)
	require.NoError(t, err)
	defer second.Close()
	attachVersionedTestApp(t, second, scope)
	_, err = first.DB.Exec(`INSERT INTO upgrade_local_notes VALUES('sentinel','preserve')`)
	require.NoError(t, err)
	keys := vector.Keys
	for _, key := range keys {
		_, err = first.Statements["insert"].Exec(key, "A|B")
		require.NoError(t, err)
	}
	_, err = first.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	var oldSource string
	require.NoError(t, first.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&oldSource))
	// Freeze one direct request before dispatch and another after a real committed
	// write whose response never reached its client. Neither outcome is discarded.
	_, err = second.DB.Exec(`UPDATE upgrade_people SET legacy_name=? WHERE id=?`, vector.QueuedLegacy, keys[0])
	require.NoError(t, err)
	staged := &evolutionFaultTransport{stage: true}
	second.Client.HTTP = &http.Client{Transport: staged}
	_, err = second.Client.PushPending(ctx)
	require.Error(t, err)
	var stagedBundle int64
	var stagedHash string
	require.NoError(t, second.DB.QueryRow(`SELECT source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&stagedBundle, &stagedHash))
	require.Positive(t, stagedBundle)
	_, err = first.DB.Exec(`UPDATE upgrade_people SET legacy_name='Before|Migration' WHERE id=?`, keys[1])
	require.NoError(t, err)
	lost := &evolutionFaultTransport{lose: true}
	first.Client.HTTP = &http.Client{Transport: lost}
	_, err = first.Client.PushPending(ctx)
	require.Error(t, err)
	require.True(t, lost.lost.Load())
	var lostBundle int64
	require.NoError(t, first.DB.QueryRow(`SELECT source_bundle_id FROM _sync_outbox_bundle`).Scan(&lostBundle))
	drain, err := evolution.Drain(ctx, pool, []*oversync.SyncService{server.SyncService}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 3))
	cfg, err = evolution.Configuration(3, 2, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	bounds := evolution.DefaultBounds()
	bounds.Deadline = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	bounds.MaxEvidenceBytes, bounds.MaxHistoryBytes, bounds.MaxBundles, bounds.MinFreeDiskBytes = 64<<20, 64<<20, 1024, 64<<20
	bounds.Rows = 1
	require.NoError(t, a.RequirePreservingClients(ctx, "signed-token-preserving-runtime-allowlist-v1"))
	require.NoError(t, a.Activate(ctx, bounds, evolution.Capacity{Available: true, FreeDiskBytes: 1 << 30}))
	a.Close()
	server.Close()
	server = startEvolutionHTTP(t, dsn, cfg, &allowed)
	require.Error(t, evolution.ReopenServing(ctx, pool, []*oversync.SyncService{server.SyncService}))
	first.Client.HTTP = http.DefaultClient
	second.Client.HTTP = http.DefaultClient
	first.Client.BaseURL = server.URL()
	second.Client.BaseURL = server.URL()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL()+"/sync/generations/"+fmt.Sprint(target.Contract.Proof.ID), nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set(oversync.SourceIDHeader, "target-probe")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode)
	require.NoError(t, response.Body.Close())
	var capacityCalls atomic.Int32
	capacity := func(context.Context) (evolution.Capacity, error) {
		return evolution.Capacity{FreeDiskBytes: 1 << 30, Available: true, RetentionRemaining: 7 * 24 * time.Hour}, nil
	}
	worker := &evolution.Worker{Pool: pool, Manager: oversync.NewScopeManager(server.SyncService, oversync.ScopeManagerConfig{}), Bounds: bounds, Capacity: func(ctx context.Context) (evolution.Capacity, error) {
		if capacityCalls.Add(1) > 1 {
			return evolution.Capacity{}, errors.New("controlled application pause")
		}
		return capacity(ctx)
	}}
	_, err = second.Client.PushPending(ctx)
	require.Error(t, err)
	var sameStagedBundle int64
	var sameStagedHash string
	require.NoError(t, second.DB.QueryRow(`SELECT source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle`).Scan(&sameStagedBundle, &sameStagedHash))
	require.Equal(t, stagedBundle, sameStagedBundle)
	require.Equal(t, stagedHash, sameStagedHash)
	require.Error(t, worker.Run(ctx))
	var pending int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people WHERE name IS NULL`).Scan(&pending))
	require.Equal(t, 1, pending, "pause must leave work for restart")
	worker = &evolution.Worker{Pool: pool, Manager: oversync.NewScopeManager(server.SyncService, oversync.ScopeManagerConfig{}), Bounds: bounds, Capacity: capacity}
	require.NoError(t, worker.Run(ctx))
	drain, err = evolution.Drain(ctx, pool, []*oversync.SyncService{server.SyncService}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = evolution.OpenAuthority(ctx, drain)
	require.NoError(t, err)
	require.NoError(t, a.Certify(ctx, 3))
	require.NoError(t, a.Apply(ctx, 4))
	cfg, err = evolution.Configuration(4, 3, true)
	require.NoError(t, err)
	require.Error(t, a.Publish(ctx, cfg), "maintaining certificate cannot enable post-switch target")
	require.NoError(t, a.Certify(ctx, 4))
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	server.Close()
	server = startEvolutionHTTP(t, dsn, cfg, &allowed)
	require.NoError(t, evolution.ReopenServing(ctx, pool, []*oversync.SyncService{server.SyncService}))
	second.Client.BaseURL = server.URL()
	first.Client.BaseURL = server.URL()
	// Exact committed receipt replay settles first, then catches up backfill.
	_, err = first.Client.Sync(ctx)
	require.NoError(t, err)
	var commits int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log b JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1 AND b.source_id=$2 AND b.source_bundle_id=$3`, scope, oldSource, lostBundle).Scan(&commits))
	require.Equal(t, 1, commits)
	// Unsupported runtime is denied despite an unchanged valid schema proof.
	allowed.Store("revoked-runtime")
	unsupported, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, server.URL()+"/sync/generations/"+fmt.Sprint(target.Contract.Proof.ID), nil)
	require.NoError(t, requestErr)
	unsupported.Header.Set("Authorization", "Bearer "+token)
	rejected, requestErr := http.DefaultClient.Do(unsupported)
	require.NoError(t, requestErr)
	require.Equal(t, vector.RuntimeRejectionStatus, rejected.StatusCode)
	var rejectionBody struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(rejected.Body).Decode(&rejectionBody))
	require.NoError(t, rejected.Body.Close())
	require.Equal(t, vector.RuntimeRejectionError, rejectionBody.Error)
	require.Equal(t, vector.RuntimeRejectionMessage, rejectionBody.Message)
	_, err = second.Client.PushPending(ctx)
	require.Error(t, err)
	require.NoError(t, second.DB.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&sameStagedHash))
	require.Equal(t, stagedHash, sameStagedHash)
	// Reopen the complete admitted runtime on the same file. A partial retained
	// inventory is corruption, not a valid older client shape, and must never be
	// repaired while frozen intent is present.
	var sourceBefore, frozenBefore, businessBefore string
	const frozenQuery = `SELECT json_group_array(json_object('bundle',source_bundle_id,'ordinal',row_ordinal,'op',op,'base',base_row_version,'key',wire_key_json,'payload',wire_payload)) FROM (SELECT * FROM _sync_outbox_rows ORDER BY source_bundle_id,row_ordinal)`
	require.NoError(t, second.DB.QueryRow(frozenQuery).Scan(&frozenBefore))
	require.NoError(t, second.DB.QueryRow(`SELECT legacy_name FROM upgrade_people WHERE id=?`, keys[0]).Scan(&businessBefore))
	require.Equal(t, vector.QueuedLegacy, businessBefore)
	var sequenceBefore int64
	require.NoError(t, second.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&sourceBefore))
	require.NoError(t, second.DB.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, sourceBefore).Scan(&sequenceBefore))
	require.NoError(t, second.Close())
	allowed.Store("Bearer " + token)
	second, _, err = openVersionedApplication(ctx, secondPath, server.URL(), provider, http.DefaultClient, fresh("second"), old)
	require.NoError(t, err)
	defer second.Close()
	var sourceAfter string
	var sequenceAfter int64
	require.NoError(t, second.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&sourceAfter))
	require.NoError(t, second.DB.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, sourceAfter).Scan(&sequenceAfter))
	require.Equal(t, sourceBefore, sourceAfter)
	require.Equal(t, sequenceBefore, sequenceAfter)
	var frozenAfter, businessAfter string
	require.NoError(t, second.DB.QueryRow(frozenQuery).Scan(&frozenAfter))
	require.Equal(t, frozenBefore, frozenAfter)
	require.NoError(t, second.DB.QueryRow(`SELECT legacy_name FROM upgrade_people WHERE id=?`, keys[0]).Scan(&businessAfter))
	require.Equal(t, businessBefore, businessAfter)
	require.NoError(t, second.DB.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&sameStagedHash))
	require.Equal(t, stagedHash, sameStagedHash)
	attachVersionedTestApp(t, second, scope)
	_, err = second.Client.Sync(ctx)
	var conflict *oversqlite.ConflictNeedsResolutionError
	require.ErrorAs(t, err, &conflict)
	decision, err := second.Client.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, decision)
	require.NoError(t, second.Client.ResolveConflict(ctx, *decision, oversqlite.KeepLocal{}))
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = first.Client.Sync(ctx)
	require.NoError(t, err)
	require.NoError(t, first.Close())
	plan := oversqlite.UpgradePlan{PlanID: "transition-upgrade", Mode: oversqlite.UpgradeModeGeneration, DatabaseID: "first", ServiceID: "transition", InstalledVersion: 1, UserID: scope, TargetVersion: 2, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID, LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator}
	upgraded, result, err := openVersionedApplication(ctx, mainPath, server.URL(), provider, http.DefaultClient, plan, target)
	require.NoError(t, err)
	defer upgraded.Close()
	attachVersionedTestApp(t, upgraded, scope)
	require.Equal(t, oversqlite.UpgradePhaseReady, result.Status.Phase)
	var name, label, note, newSource string
	require.NoError(t, upgraded.DB.QueryRow(`SELECT name,new_label FROM upgrade_people WHERE id=?`, keys[0]).Scan(&name, &label))
	require.Equal(t, []string{"Newer", "Value"}, []string{name, label})
	require.NoError(t, upgraded.DB.QueryRow(`SELECT note FROM upgrade_local_notes WHERE id='sentinel'`).Scan(&note))
	require.Equal(t, "preserve", note)
	require.NoError(t, upgraded.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&newSource))
	require.NotEqual(t, oldSource, newSource)
	_, err = upgraded.DB.Exec(`UPDATE upgrade_people SET name='Target' WHERE id=?`, keys[0])
	require.NoError(t, err)
	_, err = upgraded.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	var legacy string
	require.NoError(t, second.DB.QueryRow(`SELECT legacy_name FROM upgrade_people WHERE id=?`, keys[0]).Scan(&legacy))
	require.Equal(t, vector.AfterTargetWrite, legacy)
	_, err = second.DB.Exec(`UPDATE upgrade_people SET legacy_name='Old|Again' WHERE id=?`, keys[1])
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	require.NoError(t, err)
	_, err = upgraded.Client.Sync(ctx)
	require.NoError(t, err)
	require.NoError(t, upgraded.DB.QueryRow(`SELECT name,new_label FROM upgrade_people WHERE id=?`, keys[1]).Scan(&name, &label))
	require.Equal(t, vector.AfterOldWrite, []string{name, label})
	var replaced string
	require.NoError(t, pool.QueryRow(ctx, `SELECT s.replaced_by_source_id FROM sync.source_state s JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1 AND s.source_id=$2`, scope, oldSource).Scan(&replaced))
	require.Equal(t, newSource, replaced)
	if extension != nil {
		extension(mobileRolloutState{ctx: ctx, pool: pool, server: server, old: second, target: upgraded, token: token, secret: "transition-test-secret", scope: scope})
		return
	}
	// A permanent application rejection returns through real HTTP and preserves
	// the exact frozen client intent on repeated attempts; it never settles it.
	_, err = second.DB.Exec(`UPDATE upgrade_people SET legacy_name='invalid' WHERE id=?`, keys[1])
	require.NoError(t, err)
	_, err = second.Client.Sync(ctx)
	var rejection *oversqlite.PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, http.StatusUnprocessableEntity, rejection.Pending.Status)
	var pendingID int64
	var pendingHash, pendingState string
	require.NoError(t, second.DB.QueryRow(`SELECT source_bundle_id,canonical_request_hash,state FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&pendingID, &pendingHash, &pendingState))
	require.Equal(t, "prepared", pendingState)
	require.NotEmpty(t, pendingHash)
	_, err = second.Client.Sync(ctx)
	rejection = nil
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, http.StatusUnprocessableEntity, rejection.Pending.Status)
	var sameID int64
	var sameHash string
	require.NoError(t, second.DB.QueryRow(`SELECT source_bundle_id,canonical_request_hash FROM _sync_outbox_bundle WHERE singleton_key=1`).Scan(&sameID, &sameHash))
	require.Equal(t, pendingID, sameID)
	require.Equal(t, pendingHash, sameHash)
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name FROM main.upgrade_people WHERE id=$1`, keys[1]).Scan(&legacy))
	require.Equal(t, "Old|Again", legacy)

}

// All successful responses here come from real HTTP/PostgreSQL. Faults only
// withhold delivery or prevent a request from reaching its next server boundary.
type evolutionFaultTransport struct {
	stage, lose bool
	lost        atomic.Bool
}

func (f *evolutionFaultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.stage && r.URL.Path == "/sync/push" {
		f.lost.Store(true)
	}
	if f.lost.Load() {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"service_not_ready"}`)), Request: r}, nil
	}
	response, err := http.DefaultTransport.RoundTrip(r)
	if err == nil && f.lose && r.URL.Path == "/sync/push" && response.StatusCode == http.StatusOK {
		f.lost.Store(true)
		_ = response.Body.Close()
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}
