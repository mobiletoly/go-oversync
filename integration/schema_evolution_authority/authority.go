// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

type authorityMode string

const (
	modeAdditive authorityMode = "additive"
	modeSparse   authorityMode = "sparse"
	modeRetired  authorityMode = "retired"
	schemaName                 = "main"
	authorityID                = "sqlitenow-schema-evolution-authority"
)

type fixtureConfig struct {
	mode              authorityMode
	registeredTables  []oversync.RegisteredTable
	generations       []oversync.SchemaGeneration
	allGenerations    []oversync.SchemaGeneration
	targetGeneration  oversync.SchemaGenerationID
	readiness         *oversync.ReadinessConfig
	readinessEvidence *oversync.ReadinessEvidence
}

func parseMode(raw string) (authorityMode, error) {
	mode := authorityMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		mode = modeAdditive
	}
	switch mode {
	case modeAdditive, modeSparse, modeRetired:
		return mode, nil
	default:
		return "", fmt.Errorf("SCHEMA_EVOLUTION_AUTHORITY_MODE must be additive, sparse, or retired")
	}
}

func registeredTables() []oversync.RegisteredTable {
	return []oversync.RegisteredTable{
		{Schema: schemaName, Table: "people", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "notes", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "projects", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "attachments", SyncKeyColumns: []string{"id"}},
	}
}

func generationGraph(targetID oversync.SchemaGenerationID) []oversync.SchemaGeneration {
	c := func(name string, wire oversync.GenerationWireType, nullable bool) oversync.GenerationColumn {
		return oversync.GenerationColumn{Name: name, WireType: wire, Nullable: nullable}
	}
	fk := func(column, table string, action oversync.GenerationForeignKeyAction) oversync.GenerationForeignKey {
		return oversync.GenerationForeignKey{
			Columns: []string{column}, ReferencedSchema: schemaName, ReferencedTable: table,
			ReferencedColumns: []string{"id"}, OnDelete: action, OnUpdate: oversync.GenerationForeignKeyNoAction,
		}
	}
	peopleOne := oversync.GenerationTable{Schema: schemaName, Table: "people", Columns: []oversync.GenerationColumn{
		c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false),
	}}
	notes := oversync.GenerationTable{Schema: schemaName, Table: "notes", Columns: []oversync.GenerationColumn{
		c("id", oversync.GenerationWireTypeUUID, false), c("body", oversync.GenerationWireTypeText, false),
	}}
	peopleTarget := oversync.GenerationTable{Schema: schemaName, Table: "people", Columns: []oversync.GenerationColumn{
		c("details", oversync.GenerationWireTypeText, true), c("id", oversync.GenerationWireTypeUUID, false),
		c("name", oversync.GenerationWireTypeText, false), c("project_id", oversync.GenerationWireTypeUUID, true),
	}, ForeignKeys: []oversync.GenerationForeignKey{fk("project_id", "projects", oversync.GenerationForeignKeySetNull)}}
	projects := oversync.GenerationTable{Schema: schemaName, Table: "projects", Columns: []oversync.GenerationColumn{
		c("id", oversync.GenerationWireTypeUUID, false), c("title", oversync.GenerationWireTypeText, false),
	}}
	attachments := oversync.GenerationTable{Schema: schemaName, Table: "attachments", Columns: []oversync.GenerationColumn{
		c("filename", oversync.GenerationWireTypeText, false), c("id", oversync.GenerationWireTypeUUID, false),
		c("person_id", oversync.GenerationWireTypeUUID, true),
	}, ForeignKeys: []oversync.GenerationForeignKey{fk("person_id", "people", oversync.GenerationForeignKeyCascade)}}
	return []oversync.SchemaGeneration{
		{ID: 1, Tables: []oversync.GenerationTable{notes, peopleOne}},
		{ID: targetID, Tables: []oversync.GenerationTable{attachments, notes, peopleTarget, projects}},
	}
}

func newFixtureConfig(mode authorityMode) (fixtureConfig, error) {
	targetID := oversync.SchemaGenerationID(2)
	if mode == modeSparse {
		targetID = 3
	}
	all := generationGraph(targetID)
	active := all
	if mode == modeRetired {
		active = []oversync.SchemaGeneration{all[1]}
	}
	config := fixtureConfig{
		mode: mode, registeredTables: registeredTables(), generations: active,
		allGenerations: all, targetGeneration: targetID,
	}
	if mode == modeRetired {
		readiness, evidence, err := retiredReadiness(config)
		if err != nil {
			return fixtureConfig{}, err
		}
		config.readiness, config.readinessEvidence = readiness, &evidence
	}
	return config, nil
}

type durableReadinessAdapter struct{}

func (durableReadinessAdapter) Inspect(ctx context.Context, reader oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var evidence oversync.ReadinessEvidence
	err := reader.QueryRow(ctx, `SELECT evidence FROM schema_evolution_authority.readiness WHERE singleton`).Scan(&evidence)
	return evidence, err
}

func retiredReadiness(config fixtureConfig) (*oversync.ReadinessConfig, oversync.ReadinessEvidence, error) {
	activeRegistry, err := oversync.CompileSchemaGenerationRegistry(config.registeredTables, config.generations)
	if err != nil {
		return nil, oversync.ReadinessEvidence{}, err
	}
	allRegistry, err := oversync.CompileSchemaGenerationRegistry(config.registeredTables, config.allGenerations)
	if err != nil {
		return nil, oversync.ReadinessEvidence{}, err
	}
	old, ok := allRegistry.Generation(1)
	if !ok {
		return nil, oversync.ReadinessEvidence{}, fmt.Errorf("generation 1 proof unavailable")
	}
	target, ok := activeRegistry.Generation(config.targetGeneration)
	if !ok {
		return nil, oversync.ReadinessEvidence{}, fmt.Errorf("target generation proof unavailable")
	}
	ledger := &oversync.GenerationLedger{Revision: 3, Entries: []oversync.GenerationLifecycle{
		{Proof: oversync.SchemaGenerationProof{ID: 1, Fingerprint: old.Fingerprint()}, State: "retired", AllocatedRevision: 1, RetiredRevision: 3},
		{Proof: oversync.SchemaGenerationProof{ID: config.targetGeneration, Fingerprint: target.Fingerprint()}, State: "supported", AllocatedRevision: 2},
	}}
	deployment := oversync.DeploymentContract{
		GenerationLedger: ledger, AuthorityID: authorityID + "/retired", Revision: 3,
		RegistryFingerprint: activeRegistry.Fingerprint(), TransitionRevision: "generation-1-retired",
		TransitionPhase: "retired", ApplicationCodeSHA256: strings.Repeat("a", 64),
		QualificationSHA256: strings.Repeat("b", 64),
	}
	requirement := oversync.GenerationReadiness{Proof: oversync.SchemaGenerationProof{ID: config.targetGeneration, Fingerprint: target.Fingerprint()}}
	evidence := oversync.ReadinessEvidence{
		Deployment:  deployment,
		Generations: []oversync.GenerationEnablement{{Requirement: requirement, DeploymentRevision: deployment.Revision, Enabled: true}},
	}
	return &oversync.ReadinessConfig{
		Adapter: durableReadinessAdapter{}, AuthorityLock: [2]int32{1869833075, 9009},
		Expected: deployment, Generations: []oversync.GenerationReadiness{requirement},
	}, evidence, nil
}

func baselineReadinessManifest(mode authorityMode) []byte {
	return []byte(fmt.Sprintf(`{
  "format":"go-oversync.static-baseline.v1",
  "authority_id":"%s/%s",
  "deployment_revision":1,
  "transition_revision":"schema-evolution-%s-v1",
  "transition_phase":"coexistence",
  "application":{"id":"schema-evolution-authority-%s","revision":1},
  "qualification":{"id":"schema-evolution-authority-qualification","revision":1}
}`, authorityID, mode, mode, mode))
}

func prepareApplication(ctx context.Context, pool *pgxpool.Pool, config fixtureConfig) error {
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS main`,
		`CREATE TABLE IF NOT EXISTS main.projects (
			_sync_scope_id text NOT NULL,
			id uuid NOT NULL,
			title text NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`,
		`CREATE TABLE IF NOT EXISTS main.people (
			_sync_scope_id text NOT NULL,
			id uuid NOT NULL,
			name text NOT NULL,
			details text NULL,
			project_id uuid NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT people_project FOREIGN KEY (_sync_scope_id, project_id)
				REFERENCES main.projects(_sync_scope_id, id)
				ON DELETE SET NULL (project_id) DEFERRABLE INITIALLY DEFERRED
		)`,
		`CREATE TABLE IF NOT EXISTS main.notes (
			_sync_scope_id text NOT NULL,
			id uuid NOT NULL,
			body text NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`,
		`CREATE TABLE IF NOT EXISTS main.attachments (
			_sync_scope_id text NOT NULL,
			id uuid NOT NULL,
			person_id uuid NULL,
			filename text NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT attachment_person FOREIGN KEY (_sync_scope_id, person_id)
				REFERENCES main.people(_sync_scope_id, id)
				ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if config.readinessEvidence == nil {
		return nil
	}
	encoded, err := json.Marshal(config.readinessEvidence)
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS schema_evolution_authority`); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_evolution_authority.readiness(
			singleton boolean PRIMARY KEY CHECK(singleton), evidence jsonb NOT NULL
		)`); err != nil {
		return err
	}
	result, err := pool.Exec(ctx, `INSERT INTO schema_evolution_authority.readiness(singleton,evidence) VALUES(true,$1)
		ON CONFLICT(singleton) DO UPDATE SET evidence=EXCLUDED.evidence
		WHERE schema_evolution_authority.readiness.evidence=EXCLUDED.evidence
	`, encoded)
	if err != nil {
		return err
	}
	return requireImmutableReadinessWrite(result.RowsAffected())
}

func requireImmutableReadinessWrite(rowsAffected int64) error {
	if rowsAffected != 1 {
		return fmt.Errorf("schema-evolution readiness evidence is immutable and does not match the stored ledger")
	}
	return nil
}

type requestCounters struct {
	mu     sync.Mutex
	paths  map[string]int64
	proofs map[string]int64
}

func newRequestCounters() *requestCounters {
	return &requestCounters{paths: map[string]int64{}, proofs: map[string]int64{}}
}

func (c *requestCounters) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths[r.Method+" "+r.URL.Path]++
	proof := r.Header.Get(oversync.SchemaGenerationIDHeader) + "/" + r.Header.Get(oversync.SchemaGenerationFingerprintHeader)
	if proof != "/" {
		c.proofs[proof]++
	}
}

func (c *requestCounters) snapshot() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	paths, proofs := map[string]int64{}, map[string]int64{}
	for key, value := range c.paths {
		paths[key] = value
	}
	for key, value := range c.proofs {
		proofs[key] = value
	}
	return map[string]any{"paths": paths, "proofs": proofs}
}

type targetGate struct {
	target oversync.SchemaGenerationID
	paused atomic.Bool
}

func (g *targetGate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested := r.Header.Get(oversync.SchemaGenerationIDHeader)
		isTarget := requested == strconv.FormatInt(int64(g.target), 10) ||
			strings.HasSuffix(r.URL.Path, "/generations/"+strconv.FormatInt(int64(g.target), 10))
		if g.paused.Load() && isTarget {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "schema_generation_not_ready", "message": "target generation admission is paused",
				"retryable": true, "generation_id": g.target,
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

type faultController struct {
	mu                    sync.Mutex
	lostReply             int
	commitRejectionStatus int
	commitRejectionCount  int
}

func (f *faultController) setCommitRejection(status, count int) error {
	if count < 0 || count > 8 || status < 400 || status > 599 {
		return fmt.Errorf("commit-rejection requires count 0..8 and status 400..599")
	}
	f.mu.Lock()
	f.commitRejectionStatus = status
	f.commitRejectionCount = count
	f.mu.Unlock()
	return nil
}

func (f *faultController) consumeCommitRejection() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitRejectionCount == 0 {
		return 0, false
	}
	f.commitRejectionCount--
	return f.commitRejectionStatus, true
}

func (f *faultController) setLostReply(count int) error {
	if count < 0 || count > 8 {
		return fmt.Errorf("lost-reply count must be in 0..8")
	}
	f.mu.Lock()
	f.lostReply = count
	f.mu.Unlock()
	return nil
}

func (f *faultController) consumeLostReply() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lostReply == 0 {
		return false
	}
	f.lostReply--
	return true
}

type capturedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *capturedResponse) Header() http.Header    { return w.header }
func (w *capturedResponse) WriteHeader(status int) { w.status = status }
func (w *capturedResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (f *faultController) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/commit") {
			next.ServeHTTP(w, r)
			return
		}
		if status, reject := f.consumeCommitRejection(); reject {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if status == http.StatusUnprocessableEntity {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": "push_effect_coverage_incomplete", "message": "schema-evolution deterministic rejection",
					"remediation": "correct_server_write_logic", "retryable": false,
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": "schema_evolution_commit_rejected", "message": "schema-evolution deterministic rejection",
				})
			}
			return
		}
		captured := &capturedResponse{header: make(http.Header)}
		next.ServeHTTP(captured, r)
		if captured.status >= 200 && captured.status < 300 && f.consumeLostReply() {
			panic(http.ErrAbortHandler)
		}
		for key, values := range captured.header {
			w.Header()[key] = append([]string(nil), values...)
		}
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write(captured.body.Bytes())
	})
}

type controlRequest struct {
	Action   string  `json:"action"`
	UserID   string  `json:"user_id,omitempty"`
	Table    string  `json:"table,omitempty"`
	ID       string  `json:"id,omitempty"`
	Name     string  `json:"name,omitempty"`
	Body     string  `json:"body,omitempty"`
	Details  *string `json:"details,omitempty"`
	ParentID *string `json:"parent_id,omitempty"`
}

func authenticateControl(r *http.Request, token string) bool {
	supplied := r.Header.Get("X-Schema-Evolution-Control-Token")
	expectedDigest := sha256.Sum256([]byte(token))
	suppliedDigest := sha256.Sum256([]byte(supplied))
	return token != "" && subtle.ConstantTimeCompare(suppliedDigest[:], expectedDigest[:]) == 1
}

func requireControlToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authenticateControl(r, token) {
			http.Error(w, "control token required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func seedScope(ctx context.Context, components *server.ServerComponents, request controlRequest) (any, error) {
	if request.UserID == "" || request.Table == "" || request.ID == "" {
		return nil, fmt.Errorf("seed requires user_id, table, and id")
	}
	id, err := uuid.Parse(request.ID)
	if err != nil {
		return nil, fmt.Errorf("seed id must be a UUID: %w", err)
	}
	body, _ := json.Marshal(request)
	manager := oversync.NewScopeManager(components.SyncService, oversync.ScopeManagerConfig{})
	result, err := manager.ExecWrite(ctx, request.UserID, oversync.ScopeWriteOptions{
		WriterID: "schema-evolution-control",
		RetryableWriteOptions: oversync.RetryableWriteOptions{
			OperationID: uuid.New(), OperationHash: sha256.Sum256(body),
			OperationValidUntil: time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond),
		},
		RequiredEffectTables: []oversync.RegisteredEffectTable{{Schema: schemaName, Table: request.Table}},
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		switch request.Table {
		case "people":
			var projectID *uuid.UUID
			if request.ParentID != nil {
				parsed, parseErr := uuid.Parse(*request.ParentID)
				if parseErr != nil {
					return parseErr
				}
				projectID = &parsed
			}
			_, err = tx.Exec(ctx, `INSERT INTO main.people(id,name,details,project_id) VALUES($1,$2,$3,$4)`, id, request.Name, request.Details, projectID)
		case "notes":
			_, err = tx.Exec(ctx, `INSERT INTO main.notes(id,body) VALUES($1,$2)`, id, request.Body)
		case "projects":
			_, err = tx.Exec(ctx, `INSERT INTO main.projects(id,title) VALUES($1,$2)`, id, request.Name)
		case "attachments":
			var personID *uuid.UUID
			if request.ParentID != nil {
				parsed, parseErr := uuid.Parse(*request.ParentID)
				if parseErr != nil {
					return parseErr
				}
				personID = &parsed
			}
			_, err = tx.Exec(ctx, `INSERT INTO main.attachments(id,person_id,filename) VALUES($1,$2,$3)`, id, personID, request.Name)
		default:
			return fmt.Errorf("unsupported seed table %q", request.Table)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"action": "seed", "user_id": request.UserID, "table": request.Table, "bundle_seq": result.Bundle.BundleSeq}, nil
}

func inspectScope(ctx context.Context, pool *pgxpool.Pool, userID string) (map[string]any, error) {
	if userID == "" {
		return nil, fmt.Errorf("inspect requires user_id")
	}
	result := map[string]any{"action": "inspect", "user_id": userID}
	for _, table := range []string{"people", "notes", "projects", "attachments"} {
		var count int64
		query := fmt.Sprintf("SELECT count(*) FROM main.%s WHERE _sync_scope_id=$1", table)
		if err := pool.QueryRow(ctx, query, userID).Scan(&count); err != nil {
			return nil, err
		}
		result[table] = count
	}
	people := make([]map[string]any, 0)
	rows, err := pool.Query(ctx, `
		SELECT id::text,name,details,project_id::text
		FROM main.people WHERE _sync_scope_id=$1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, name string
		var details, projectID *string
		if err := rows.Scan(&id, &name, &details, &projectID); err != nil {
			rows.Close()
			return nil, err
		}
		people = append(people, map[string]any{
			"id": id, "name": name, "details": details, "project_id": projectID,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	projects := make([]map[string]any, 0)
	rows, err = pool.Query(ctx, `
		SELECT id::text,title FROM main.projects WHERE _sync_scope_id=$1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, title string
		if err := rows.Scan(&id, &title); err != nil {
			rows.Close()
			return nil, err
		}
		projects = append(projects, map[string]any{"id": id, "title": title})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	attachments := make([]map[string]any, 0)
	rows, err = pool.Query(ctx, `
		SELECT id::text,person_id::text,filename
		FROM main.attachments WHERE _sync_scope_id=$1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, filename string
		var personID *string
		if err := rows.Scan(&id, &personID, &filename); err != nil {
			rows.Close()
			return nil, err
		}
		attachments = append(attachments, map[string]any{
			"id": id, "person_id": personID, "filename": filename,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	notes := make([]map[string]any, 0)
	rows, err = pool.Query(ctx, `
		SELECT id::text,body FROM main.notes WHERE _sync_scope_id=$1 ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, body string
		if err := rows.Scan(&id, &body); err != nil {
			rows.Close()
			return nil, err
		}
		notes = append(notes, map[string]any{"id": id, "body": body})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var peopleProjects, attachmentPeople int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM main.people p
		LEFT JOIN main.projects project
		  ON project._sync_scope_id=p._sync_scope_id AND project.id=p.project_id
		WHERE p._sync_scope_id=$1 AND p.project_id IS NOT NULL AND project.id IS NULL`, userID).Scan(&peopleProjects); err != nil {
		return nil, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM main.attachments attachment
		LEFT JOIN main.people person
		  ON person._sync_scope_id=attachment._sync_scope_id AND person.id=attachment.person_id
		WHERE attachment._sync_scope_id=$1 AND attachment.person_id IS NOT NULL AND person.id IS NULL`, userID).Scan(&attachmentPeople); err != nil {
		return nil, err
	}
	result["rows"] = map[string]any{
		"people": people, "notes": notes, "projects": projects, "attachments": attachments,
	}
	result["orphan_counts"] = map[string]int64{
		"people_projects": peopleProjects, "attachments_people": attachmentPeople,
	}
	return result, nil
}

func generationProofs(config fixtureConfig) ([]map[string]any, error) {
	registry, err := oversync.CompileSchemaGenerationRegistry(config.registeredTables, config.allGenerations)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(config.allGenerations))
	for _, generation := range config.allGenerations {
		contract, ok := registry.Generation(generation.ID)
		if !ok {
			return nil, fmt.Errorf("generation %d missing", generation.ID)
		}
		state := "supported"
		if config.mode == modeRetired && generation.ID == 1 {
			state = "retired"
		}
		result = append(result, map[string]any{"id": generation.ID, "fingerprint": contract.Fingerprint(), "state": state})
	}
	return result, nil
}

func authorityHandler(components *server.ServerComponents, config fixtureConfig, controlToken string, gate *targetGate, faults *faultController, counters *requestCounters) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /schema-evolution/status", requireControlToken(controlToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proofs, err := generationProofs(config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var postgres string
		if err := components.Pool.QueryRow(r.Context(), "SHOW server_version").Scan(&postgres); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"authority_id": authorityID, "mode": config.mode, "postgres_identity": postgres,
			"generations": proofs, "target_generation": config.targetGeneration,
			"target_paused": gate.paused.Load(), "request_counters": counters.snapshot(),
			"graph": map[string]any{
				"generation_1": []string{"people(id,name)", "notes(id,body)"},
				"target":       []string{"people(+details,+project_id SET NULL)", "notes", "projects", "attachments(person_id CASCADE)"},
				"local_only":   []string{"drafts", "drafts_normalized", "drafts_touch"},
			},
		})
	})))
	mux.Handle("POST /schema-evolution/control", requireControlToken(controlToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var request controlRequest
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		var response any
		var err error
		switch request.Action {
		case "pause_target":
			gate.paused.Store(true)
			response = map[string]any{"action": request.Action, "target_paused": true}
		case "resume_target":
			gate.paused.Store(false)
			response = map[string]any{"action": request.Action, "target_paused": false}
		case "seed":
			response, err = seedScope(r.Context(), components, request)
		case "inspect":
			response, err = inspectScope(r.Context(), components.Pool, request.UserID)
		default:
			err = fmt.Errorf("action must be seed, inspect, pause_target, or resume_target")
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})))
	mux.Handle("POST /schema-evolution/fault", requireControlToken(controlToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Kind   string `json:"kind"`
			Status int    `json:"status"`
			Count  int    `json:"count"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		switch request.Kind {
		case "commit_rejection":
			if err := faults.setCommitRejection(request.Status, request.Count); err != nil {
				http.Error(w, "bounded count and 4xx/5xx status required", http.StatusBadRequest)
				return
			}
		case "lost_reply":
			if err := faults.setLostReply(request.Count); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "kind must be commit_rejection or lost_reply", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(request)
	})))
	mux.Handle("/", faults.middleware(components.Handler))
	counted := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { counters.record(r); mux.ServeHTTP(w, r) })
	return server.BrowserTestCORSMiddleware(counted)
}

func validateFixtureOnly(config fixtureConfig) error {
	registry, err := oversync.CompileSchemaGenerationRegistry(config.registeredTables, config.generations)
	if err != nil {
		return err
	}
	if _, ok := registry.Generation(config.targetGeneration); !ok {
		return errors.New("target generation absent")
	}
	return nil
}
