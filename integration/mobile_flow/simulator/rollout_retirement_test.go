package simulator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	evolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type mobileRolloutState struct {
	ctx                  context.Context
	pool                 *pgxpool.Pool
	server               *serverpkg.TestServer
	address              string
	retarget             func(string)
	old, target          *versionedApplication
	token, secret, scope string
	table                bool
}

func TestMobileFlow_RolloutTableRetirementSameFiles(t *testing.T) {
	runTableEvolutionMobileJourney(t, func(s mobileRolloutState) { qualifyMobileRetirement(t, s) })
}
func TestMobileFlow_RolloutColumnRetirementSameFiles(t *testing.T) {
	runTransitionMobileJourney(t, func(s mobileRolloutState) { qualifyMobileRetirement(t, s) })
}

func mobileRolloutPlan(t *testing.T, s mobileRolloutState, cfg *oversync.ServiceConfig, action, role string) evolution.RolloutPlan {
	t.Helper()
	hash, err := evolution.RolloutTargetDigest(cfg)
	require.NoError(t, err)
	now := time.Now().UTC()
	p := evolution.RolloutPlan{Version: 1, ID: uuid.NewString(), Action: action, TargetSHA256: hash, Deadline: now.Add(60 * time.Second), MaxRows: 4096, MaxBatches: 100, MaxAttempts: 100, QueryTimeout: 10 * time.Second, MaxEvidenceBytes: 1 << 20,
		Members: []evolution.RolloutMember{{Name: "closed-mobile-fixture", Role: role, ProcessStartID: uuid.NewString(), ApplicationCodeSHA256: cfg.Readiness.Expected.ApplicationCodeSHA256}},
		Policy:  evolution.RetirementPolicy{Revision: "rollout-mobile-v1", OwnerApproval: "qualification-owner", ReplacementAvailableAt: now.Add(-time.Hour), NoticeAt: now.Add(-time.Hour), SupportDuration: time.Minute, NoticeDuration: time.Minute, ChannelEvidence: []string{"two-preserving-Go-applications"}, OfflineIntentDisposition: "preserve_unresolved_no_retired_upload", BackupEvidence: strings.Repeat("b", 64)}}
	if action == "retire_tables" {
		p.TableWritePolicy = "deny_and_revoke_atomically"
	}
	require.NoError(t, s.pool.QueryRow(s.ctx, `SELECT current_database(),m.epoch,m.client_policy,d.contract->>'AuthorityID',d.revision,d.contract->>'RegistryFingerprint',l.revision,c.catalog_revision,c.catalog_fingerprint FROM app_evolution.maintenance m CROSS JOIN app_evolution.deployment d CROSS JOIN app_evolution.generation_ledger_meta l CROSS JOIN sync.meta c`).Scan(&p.Database, &p.Epoch, &p.Policy.ClientAdmissionPolicy, &p.AuthorityID, &p.DeploymentRevision, &p.RegistryFingerprint, &p.LedgerRevision, &p.CatalogRevision, &p.CatalogFingerprint))
	return p
}

func mobileApplyRollout(t *testing.T, s mobileRolloutState, p evolution.RolloutPlan, cfg *oversync.ServiceConfig) {
	t.Helper()
	report, err := evolution.RolloutReportReadOnly(s.ctx, s.pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	a, err := evolution.OpenClosedRolloutAuthority(s.ctx, s.pool)
	require.NoError(t, err)
	defer a.Close()
	result, err := a.ApplyRollout(s.ctx, p, report, cfg, nil)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
}

func qualifyMobileRetirement(t *testing.T, s mobileRolloutState) {
	t.Helper()
	vector := mobileRolloutVector(t, s.table)
	require.EqualValues(t, vector.OldGeneration, s.old.Definition.Contract.Proof.ID)
	require.EqualValues(t, vector.SupportedGeneration, s.target.Definition.Contract.Proof.ID)
	empty, err := mobileEmptyRetirementDevice(t, s)
	require.NoError(t, err)
	defer empty.Close()
	// This old application goes offline with valid intent before retirement.
	statement := `UPDATE upgrade_people SET legacy_name='Offline|Preserved'`
	if s.table {
		statement = `UPDATE legacy_notes SET body='offline after support window'`
	}
	_, err = s.old.DB.Exec(statement)
	require.NoError(t, err)
	proof, err := evolution.Drain(s.ctx, s.pool, []*oversync.SyncService{s.server.SyncService}, func(context.Context) error { return nil })
	require.NoError(t, err)
	s.server.Close()
	a, err := evolution.OpenAuthority(s.ctx, proof)
	require.NoError(t, err)
	var previous *oversync.ServiceConfig
	if s.table {
		previous, err = evolution.TableEvolutionConfiguration(evolution.TablesServing)
	} else {
		previous, err = evolution.Configuration(4, 3, true)
	}
	require.NoError(t, err)
	ledger, err := a.PrepareGenerationLedger(s.ctx, previous)
	require.NoError(t, err)
	a.Close()
	role := "m8_mobile_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = s.pool.Exec(s.ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` NOLOGIN`)
	require.NoError(t, err)
	defer s.pool.Exec(context.Background(), `DROP ROLE `+pgx.Identifier{role}.Sanitize())
	h := sha256.Sum256([]byte(t.Name()))
	qualification := hex.EncodeToString(h[:])
	factory := func(stage int, l *oversync.GenerationLedger) *oversync.ServiceConfig {
		var cfg *oversync.ServiceConfig
		if s.table {
			cfg, err = evolution.RolloutTableConfiguration(evolution.RolloutTableStage(stage), l, qualification)
		} else {
			cfg, err = evolution.RolloutColumnConfiguration(evolution.RolloutColumnStage(stage), l, qualification)
		}
		require.NoError(t, err)
		return cfg
	}
	stage := 5
	if s.table {
		stage = 6
	}
	cfg := factory(stage, ledger)
	mobileApplyRollout(t, s, mobileRolloutPlan(t, s, cfg, "prepare", role), cfg)
	retired := *ledger
	retired.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	retired.Revision++
	retired.Entries[0].State = "retired"
	retired.Entries[0].RetiredRevision = retired.Revision
	cfg = factory(stage+1, &retired)
	p := mobileRolloutPlan(t, s, cfg, "retire_generation", role)
	p.Retiring = ledger.Entries[0].Proof
	mobileApplyRollout(t, s, p, cfg)
	if s.table {
		cfg = factory(8, &retired)
		for _, action := range []string{"retire_tables", "expire_history", "prune_table_state"} {
			p = mobileRolloutPlan(t, s, cfg, action, role)
			p.TableIDs = []int32{1}
			if action == "expire_history" {
				p.RetentionWindow = 1
			}
			report, err := evolution.RolloutReportReadOnly(s.ctx, s.pool, p)
			require.NoError(t, err)
			require.Empty(t, report.Blockers)
			input := evolution.RolloutExecution{Plan: p, Report: report, TargetStage: evolution.RolloutTableIdentityRetired, TargetLedger: &retired, QualificationSHA256: qualification}
			complete := false
			for i := 0; i < p.MaxBatches; i++ {
				result, err := evolution.ExecuteTableRollout(s.ctx, s.pool, input, i > 0)
				require.NoError(t, err)
				if result.State == "completed" {
					complete = true
					break
				}
			}
			require.True(t, complete)
		}
	}
	finalStage := 7
	if s.table {
		finalStage = 9
	}
	cfg = factory(finalStage, &retired)
	p = mobileRolloutPlan(t, s, cfg, "contract", role)
	for _, migration := range cfg.Readiness.Expected.Migrations {
		if migration.Class == "contract" {
			m := migration
			p.Contract = &m
		}
	}
	if s.table {
		p.TableIDs = []int32{1}
	} else {
		p.Columns = []string{"main.upgrade_people.legacy_name"}
	}
	mobileApplyRollout(t, s, p, cfg)
	var successor *evolution.LocalMemberSuccessor
	if s.table {
		cfg, successor = mobileEnableSuccessor(t, s, cfg, role)
	}
	backend := mobileReopenFleet(t, s, cfg, finalStage, role, successor)
	s.retarget(backend)
	address := s.address
	mobileRetiredHTTPMatrix(t, s, address, ledger.Entries[0].Proof)
	checkpoint, err := s.old.Client.LastBundleSeqSeen(s.ctx)
	require.NoError(t, err)
	_, err = s.old.Client.Sync(s.ctx)
	var retiredError *oversync.SchemaGenerationRequestError
	require.ErrorAs(t, err, &retiredError)
	require.Equal(t, vector.RetiredError, string(retiredError.Code))
	var hash, source string
	var sequence int64
	require.NoError(t, s.old.DB.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&hash))
	require.NoError(t, s.old.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&source))
	require.NoError(t, s.old.DB.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, source).Scan(&sequence))
	_, err = s.old.Client.Sync(s.ctx)
	require.ErrorAs(t, err, &retiredError)
	after, err := s.old.Client.LastBundleSeqSeen(s.ctx)
	require.NoError(t, err)
	require.Equal(t, checkpoint, after)
	require.NoError(t, s.old.Close())
	child := exec.CommandContext(s.ctx, os.Args[0], "-test.run=^TestMobileFlow_RolloutRetiredRestartHelper$")
	child.Env = append(os.Environ(), "ROLLOUT_SQLITE="+s.old.Path, "ROLLOUT_URL="+address, "ROLLOUT_TOKEN="+s.token, "ROLLOUT_SCOPE="+s.scope, "ROLLOUT_TABLE="+strconv.FormatBool(s.table))
	output, err := child.CombinedOutput()
	require.NoError(t, err, string(output))
	serviceID := "transition"
	if s.table {
		serviceID = "table_evolution"
	}
	upgradePlan := oversqlite.UpgradePlan{PlanID: "rollout-pending-retired", Mode: oversqlite.UpgradeModeGeneration, DatabaseID: "second", ServiceID: serviceID, UserID: s.scope, InstalledVersion: s.old.Definition.LocalVersion, TargetVersion: s.target.Definition.LocalVersion, InstalledGenerationID: s.old.Definition.Contract.Proof.ID, TargetGenerationID: s.target.Definition.Contract.Proof.ID, LocalOnlyTables: s.target.Definition.LocalOnlyTables, Migrator: s.target.Definition.Migrator}
	blocked, blockedResult, err := openVersionedApplication(s.ctx, s.old.Path, address, func(context.Context) (string, error) { return s.token, nil }, http.DefaultClient, upgradePlan, s.target.Definition)
	require.Nil(t, blocked)
	require.Error(t, err)
	require.Equal(t, vector.PendingOutcome, string(blockedResult.Status.Outcome))
	// Reopen the original physical file and verify the frozen outbox survives.
	db, err := sql.Open("sqlite3", s.old.Path)
	require.NoError(t, err)
	defer db.Close()
	var localVersion int
	require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&localVersion))
	require.EqualValues(t, s.old.Definition.LocalVersion, localVersion)
	var sameHash, sameSource string
	var sameSequence int64
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&sameHash))
	require.NoError(t, db.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&sameSource))
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, source).Scan(&sameSequence))
	require.Equal(t, []string{hash, source}, []string{sameHash, sameSource})
	require.Equal(t, sequence, sameSequence)
	update := `UPDATE upgrade_people SET name='AfterContraction'`
	if s.table {
		update = `UPDATE people SET name='AfterContraction'`
	}
	_, err = s.target.DB.Exec(update)
	require.NoError(t, err)
	_, err = s.target.Client.Sync(s.ctx)
	require.NoError(t, err)
	mobileUpgradeEmptyRetiredDevice(t, s, empty, address)
	if successor != nil {
		mobileUpgradeSuccessor(t, s, cfg, successor.Generation, address)
	}
	t.Logf("rollout real Go SDK table=%t retired=%d preserved_file=%t frozen_hash=%s source=%s checkpoint=%d target_operational=true", s.table, retiredError.GenerationID, true, hash, source, checkpoint)
}

func mobileReopenFleet(t *testing.T, s mobileRolloutState, cfg *oversync.ServiceConfig, stage int, closedRole string, successor *evolution.LocalMemberSuccessor) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "rollout-member")
	build := exec.CommandContext(s.ctx, "go", "build", "-o", binary, "github.com/mobiletoly/go-oversync/integration/schema_evolution/cmd/rollout-member")
	build.Env = append(os.Environ(), "GOTOOLCHAIN=go1.25.12")
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))
	data, err := os.ReadFile(binary)
	require.NoError(t, err)
	hash := sha256.Sum256(data)
	p := mobileRolloutPlan(t, s, cfg, "reopen", closedRole)
	p.Members = nil
	var members []*evolution.LocalFleetProcess
	for _, name := range []string{"server-a", "server-b", "writer"} {
		role := "m8_mobile_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		password := strings.ReplaceAll(uuid.NewString(), "-", "")
		_, err = s.pool.Exec(s.ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD '`+password+`'`)
		require.NoError(t, err)
		a, err := evolution.OpenClosedRolloutAuthority(s.ctx, s.pool)
		require.NoError(t, err)
		require.NoError(t, a.GrantTableRuntime(s.ctx, role))
		a.Close()
		parsed, err := url.Parse(s.pool.Config().ConnString())
		require.NoError(t, err)
		parsed.User = url.UserPassword(role, password)
		input := evolution.LocalMemberConfig{Successor: successor, Name: name, Ledger: cfg.Readiness.Expected.GenerationLedger, QualificationSHA256: cfg.Readiness.Expected.QualificationSHA256, JWTSecret: s.secret, ClientPolicy: p.Policy.ClientAdmissionPolicy, ClientTokens: []string{"Bearer " + s.token}}
		if s.table {
			input.TableStage = evolution.RolloutTableStage(stage)
		} else {
			input.ColumnStage = evolution.RolloutColumnStage(stage)
		}
		file := filepath.Join(t.TempDir(), "member.json")
		data, err = json.Marshal(input)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(file, data, 0600))
		member, err := evolution.StartLocalFleetProcess(s.ctx, binary, hex.EncodeToString(hash[:]), []string{"-config", file}, []string{"DATABASE_URL=" + parsed.String()})
		require.NoError(t, err)
		members = append(members, member)
		p.Members = append(p.Members, member.Member())
	}
	// Cleanup runs while this journey's database pool is still open.
	t.Cleanup(func() {
		for _, member := range members {
			member.Stop()
		}
	})
	report, err := evolution.RolloutReportReadOnly(s.ctx, s.pool, p)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	_, err = evolution.ReopenLocalFleet(s.ctx, s.pool, p, report, cfg, members)
	require.NoError(t, err)
	response, err := http.Get(members[1].Address() + "/sync/pull")
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	response.Body.Close()
	return members[0].Address()
}

func mobileEnableSuccessor(t *testing.T, s mobileRolloutState, cfg *oversync.ServiceConfig, role string) (*oversync.ServiceConfig, *evolution.LocalMemberSuccessor) {
	t.Helper()
	next := cfg.SchemaGenerations[0]
	next.ID = 3
	a, err := evolution.OpenClosedRolloutAuthority(s.ctx, s.pool)
	require.NoError(t, err)
	ledger, err := a.AllocateGeneration(s.ctx, cfg.Readiness.Expected.GenerationLedger.Revision, cfg.RegisteredTables, next)
	require.NoError(t, err)
	a.Close()
	ledger.Revision++
	ledger.Entries[len(ledger.Entries)-1].State = "supported"
	successor, err := evolution.RolloutSuccessorConfiguration(cfg, next, ledger, 10, cfg.Readiness.Expected.QualificationSHA256)
	require.NoError(t, err)
	p := mobileRolloutPlan(t, s, successor, "enable_generation", role)
	p.Activating = ledger.Entries[len(ledger.Entries)-1].Proof
	mobileApplyRollout(t, s, p, successor)
	return successor, &evolution.LocalMemberSuccessor{PreviousLedger: cfg.Readiness.Expected.GenerationLedger, Generation: next, Revision: 10}
}
func mobileUpgradeSuccessor(t *testing.T, s mobileRolloutState, cfg *oversync.ServiceConfig, generation oversync.SchemaGeneration, address string) {
	t.Helper()
	previous := s.target.Definition
	next := previous
	next.Name = "tables-successor"
	next.LocalVersion = 3
	next.Contract.RegisteredTables = cfg.RegisteredTables
	next.Contract.Generation = generation
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, _ := registry.Generation(generation.ID)
	next.Contract.Proof = oversync.SchemaGenerationProof{ID: generation.ID, Fingerprint: compiled.Fingerprint()}
	next.Migrator = mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `PRAGMA user_version=3`)
		return err
	}}
	var source string
	require.NoError(t, s.target.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&source))
	path := s.target.Path
	require.NoError(t, s.target.Close())
	plan := oversqlite.UpgradePlan{PlanID: "rollout-successor", Mode: oversqlite.UpgradeModeGeneration, DatabaseID: "first", ServiceID: "table_evolution", InstalledVersion: 2, TargetVersion: 3, UserID: s.scope, InstalledGenerationID: previous.Contract.Proof.ID, TargetGenerationID: next.Contract.Proof.ID, LocalOnlyTables: next.LocalOnlyTables, Migrator: next.Migrator}
	app, result, err := openVersionedApplication(s.ctx, path, address, func(context.Context) (string, error) { return s.token, nil }, http.DefaultClient, plan, next)
	require.NoError(t, err)
	defer app.Close()
	require.Equal(t, oversqlite.UpgradePhaseReady, result.Status.Phase)
	attachVersionedTestApp(t, app, s.scope)
	var nextSource string
	require.NoError(t, app.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&nextSource))
	require.NotEqual(t, source, nextSource)
	_, err = app.DB.Exec(`UPDATE people SET name='LaterGeneration'`)
	require.NoError(t, err)
	_, err = app.Client.Sync(s.ctx)
	require.NoError(t, err)
	t.Logf("rollout E3 same database and SQLite file generation=3 source_rotated=true schema_contracted=true")
}

func mobileRetiredHTTPMatrix(t *testing.T, s mobileRolloutState, address string, proof oversync.SchemaGenerationProof) {
	t.Helper()
	raw, err := os.ReadFile("../../../conformance/rollout-retirement/v1/http.json")
	require.NoError(t, err)
	var corpus struct {
		Version int
		Cases   []struct {
			ID, Method, Path, Body, Error, Remediation string
			Status                                     int
			Retryable                                  bool
			UnauthenticatedStatus                      int `json:"unauthenticated_status"`
		}
		SourceID string `json:"source_id"`
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.Len(t, corpus.Cases, 12)
	for _, vector := range corpus.Cases {
		for _, authenticated := range []bool{false, true} {
			request, err := http.NewRequestWithContext(s.ctx, vector.Method, address+vector.Path, strings.NewReader(vector.Body))
			require.NoError(t, err)
			request.Header.Set(oversync.SourceIDHeader, corpus.SourceID)
			request.Header.Set(oversync.ProtocolVersionHeader, oversync.SyncProtocolVersion)
			// Generation discovery is the sole proof-less endpoint: attaching a
			// generation proof would exercise request validation (400), not the
			// retired-generation behavior this corpus qualifies.
			if !strings.HasPrefix(vector.Path, "/sync/generations/") {
				request.Header.Set(oversync.SchemaGenerationIDHeader, strconv.Itoa(int(proof.ID)))
				request.Header.Set(oversync.SchemaGenerationFingerprintHeader, proof.Fingerprint)
			}
			request.Header.Set("Content-Type", "application/json")
			if authenticated {
				request.Header.Set("Authorization", "Bearer "+s.token)
			}
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			body, err := io.ReadAll(io.LimitReader(response.Body, 65536))
			response.Body.Close()
			require.NoError(t, err)
			if !authenticated {
				require.Equal(t, vector.UnauthenticatedStatus, response.StatusCode, vector.ID, string(body))
				continue
			}
			require.Equal(t, vector.Status, response.StatusCode, vector.ID, string(body))
			var result struct {
				Error, Remediation string
				Retryable          bool
			}
			require.NoError(t, json.Unmarshal(body, &result))
			require.Equal(t, vector.Error, result.Error, vector.ID)
			require.Equal(t, vector.Remediation, result.Remediation, vector.ID)
			require.Equal(t, vector.Retryable, result.Retryable, vector.ID)
		}
	}
	var bindings int
	require.NoError(t, s.pool.QueryRow(s.ctx, `SELECT count(*) FROM sync.source_generation_bindings WHERE source_id=$1`, corpus.SourceID).Scan(&bindings))
	require.Zero(t, bindings, "retired connect must not bind a previously unseen source")
	for _, variant := range []struct {
		id, header, value, code string
		status                  int
	}{
		{"999999", "", "", "unknown_schema_generation", 409},
		{"01", "", "", "invalid_generation_discovery_request", 400},
		{strconv.Itoa(int(proof.ID)), oversync.SchemaGenerationIDHeader, strconv.Itoa(int(proof.ID)), "invalid_generation_discovery_request", 400},
		{strconv.Itoa(int(proof.ID)), oversync.SchemaGenerationFingerprintHeader, proof.Fingerprint, "invalid_generation_discovery_request", 400},
	} {
		request, err := http.NewRequestWithContext(s.ctx, "GET", address+"/sync/generations/"+variant.id, nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+s.token)
		request.Header.Set(oversync.SourceIDHeader, corpus.SourceID)
		request.Header.Set(oversync.ProtocolVersionHeader, oversync.SyncProtocolVersion)
		if variant.header != "" {
			request.Header.Set(variant.header, variant.value)
		}
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		raw, err := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, variant.status, response.StatusCode, string(raw))
		var result struct{ Error string }
		require.NoError(t, json.Unmarshal(raw, &result))
		require.Equal(t, variant.code, result.Error)
	}

}

func TestMobileFlow_RolloutRetiredRestartHelper(t *testing.T) {
	path := os.Getenv("ROLLOUT_SQLITE")
	if path == "" {
		t.Skip("separate process entry")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old, _, err := mobileUpgradeDefinitions()
	serviceID := "transition"
	if os.Getenv("ROLLOUT_TABLE") == "true" {
		old, _, err = tableUpgradeDefinitions()
		serviceID = "table_evolution"
	}
	require.NoError(t, err)
	cfg := oversqlite.DefaultConfig(old.Contract.Proof.ID, old.Contract.Tables)
	cfg.DatabaseID = "second"
	cfg.ServiceID = serviceID
	cfg.LocalVersion = old.LocalVersion
	cfg.LocalOnlyTables = old.LocalOnlyTables
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path, BaseURL: os.Getenv("ROLLOUT_URL"), Token: func(context.Context) (string, error) { return os.Getenv("ROLLOUT_TOKEN"), nil }, TargetClientConfig: cfg})
	require.NoError(t, err)
	result, err := opening.Advance(ctx)
	require.NoError(t, err)
	client := result.Ready
	require.NotNil(t, client)
	defer client.Close()
	// Re-establish the persisted attachment, then replay its pending direct
	// push; the replay is the restart boundary that must observe retirement.
	_, err = client.Attach(ctx, os.Getenv("ROLLOUT_SCOPE"))
	require.NoError(t, err)
	_, err = client.Sync(ctx)
	var retired *oversync.SchemaGenerationRequestError
	require.ErrorAs(t, err, &retired)
	require.Equal(t, oversync.SchemaGenerationRetired, retired.Code)
}

func mobileQualificationMajor(t *testing.T) int {
	t.Helper()
	if os.Getenv("ROLLOUT_PG_MAJOR") == "17" {
		return 17
	}
	require.True(t, os.Getenv("ROLLOUT_PG_MAJOR") == "" || os.Getenv("ROLLOUT_PG_MAJOR") == "16")
	return 16
}

func mobileEmptyRetirementDevice(t *testing.T, s mobileRolloutState) (*versionedApplication, error) {
	t.Helper()
	id := "transition"
	if s.table {
		id = "table_evolution"
	}
	old := s.old.Definition
	plan := oversqlite.UpgradePlan{PlanID: "rollout-empty-old", Mode: oversqlite.UpgradeModeFresh, DatabaseID: "empty", ServiceID: id, TargetVersion: old.LocalVersion, TargetGenerationID: old.Contract.Proof.ID, LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator}
	app, _, err := openVersionedApplication(s.ctx, filepath.Join(t.TempDir(), "empty-old.sqlite"), s.address, func(context.Context) (string, error) { return s.token, nil }, http.DefaultClient, plan, old)
	if err != nil {
		return nil, err
	}
	attachVersionedTestApp(t, app, s.scope)
	_, err = app.Client.Sync(s.ctx)
	return app, err
}
func mobileUpgradeEmptyRetiredDevice(t *testing.T, s mobileRolloutState, app *versionedApplication, address string) {
	t.Helper()
	id := "transition"
	if s.table {
		id = "table_evolution"
	}
	old, target := app.Definition, s.target.Definition
	var source string
	require.NoError(t, app.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&source))
	require.NoError(t, app.Close())
	plan := oversqlite.UpgradePlan{PlanID: "rollout-empty-retired-upgrade", Mode: oversqlite.UpgradeModeGeneration, DatabaseID: "empty", ServiceID: id, UserID: s.scope, InstalledVersion: old.LocalVersion, TargetVersion: target.LocalVersion, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID, LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator}
	upgraded, result, err := openVersionedApplication(s.ctx, app.Path, address, func(context.Context) (string, error) { return s.token, nil }, http.DefaultClient, plan, target)
	require.NoError(t, err)
	defer upgraded.Close()
	require.Equal(t, oversqlite.UpgradePhaseReady, result.Status.Phase)
	attachVersionedTestApp(t, upgraded, s.scope)
	var next string
	require.NoError(t, upgraded.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&next))
	require.NotEqual(t, source, next)
	_, err = upgraded.Client.Sync(s.ctx)
	require.NoError(t, err)
	t.Logf("rollout empty retired generation upgraded on same file with supported target source replacement")
}

type mobileRetirementVector struct {
	Table               bool
	OldGeneration       int    `json:"old_generation"`
	SupportedGeneration int    `json:"supported_generation"`
	PendingOutcome      string `json:"pending_outcome"`
	RetiredError        string `json:"retired_error"`
}

func mobileRolloutVector(t *testing.T, table bool) mobileRetirementVector {
	t.Helper()
	raw, err := os.ReadFile("../../../conformance/rollout-retirement/v1/journeys.json")
	require.NoError(t, err)
	var corpus struct {
		Version int
		Cases   []mobileRetirementVector
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.Len(t, corpus.Cases, 2)
	for _, entry := range corpus.Cases {
		if entry.Table == table {
			return entry
		}
	}
	t.Fatal("missing mobile retirement vector")
	return mobileRetirementVector{}
}
