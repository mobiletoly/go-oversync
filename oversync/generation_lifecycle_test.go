package oversync

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerationLifecycle_ImmutableReadinessAndRetiredProof(t *testing.T) {
	tables := []RegisteredTable{{Schema: "main", Table: "items", SyncKeyColumns: []string{"id"}}}
	declarations := testSchemaGenerationsForRegisteredTables(tables)
	declarations[0].ID = 3
	registry, err := CompileSchemaGenerationRegistry(tables, declarations)
	require.NoError(t, err)
	current, _ := registry.Generation(3)
	old := SchemaGenerationProof{ID: 1, Fingerprint: strings.Repeat("a", 64)}
	ledger := &GenerationLedger{Revision: 4, Entries: []GenerationLifecycle{
		{Proof: old, State: "retired", AllocatedRevision: 1, RetiredRevision: 4},
		{Proof: SchemaGenerationProof{ID: 3, Fingerprint: current.Fingerprint()}, State: "supported", AllocatedRevision: 2},
	}}
	require.NoError(t, ValidateGenerationLedger(ledger, registry))
	s := &SyncService{schemaGenerationRegistry: registry, config: &ServiceConfig{Readiness: &ReadinessConfig{Expected: DeploymentContract{GenerationLedger: ledger}}}}
	// A nil pool proves exact retired rejection happens before source/history access.
	_, err = s.resolveRequestGeneration(context.Background(), Actor{}, old)
	var typed *SchemaGenerationRequestError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, SchemaGenerationRetired, typed.Code)
	w := httptest.NewRecorder()
	require.True(t, (&HTTPSyncHandlers{}).writeGenerationError(w, nil, err))
	require.Equal(t, 426, w.Code)
	require.Contains(t, w.Body.String(), `"upgrade_application"`)
	require.NotContains(t, w.Body.String(), old.Fingerprint)
	old.Fingerprint = strings.Repeat("b", 64)
	_, err = s.resolveRequestGeneration(context.Background(), Actor{}, old)
	require.ErrorAs(t, err, &typed)
	require.Equal(t, SchemaGenerationFingerprintMismatch, typed.Code)
	copy := cloneReadinessConfig(s.config.Readiness)
	ledger.Entries[0].Proof.Fingerprint = strings.Repeat("c", 64)
	require.Equal(t, strings.Repeat("a", 64), copy.Expected.GenerationLedger.Entries[0].Proof.Fingerprint)
}

func TestGenerationLifecycle_RejectInvalidHistory(t *testing.T) {
	tables := []RegisteredTable{{Schema: "main", Table: "items", SyncKeyColumns: []string{"id"}}}
	registry, err := CompileSchemaGenerationRegistry(tables, testSchemaGenerationsForRegisteredTables(tables))
	require.NoError(t, err)
	contract, _ := registry.Generation(1)
	for _, tc := range []struct {
		name string
		edit func(*GenerationLedger)
	}{
		{"missing", func(l *GenerationLedger) { l.Entries = nil }},
		{"capacity", func(l *GenerationLedger) { l.Entries = make([]GenerationLifecycle, MaxGenerationLedgerEntries+1) }},
		{"revision", func(l *GenerationLedger) { l.Revision = 0 }},
		{"proof", func(l *GenerationLedger) { l.Entries[0].Proof.Fingerprint = "bad" }},
		{"changed", func(l *GenerationLedger) { l.Entries[0].Proof.Fingerprint = strings.Repeat("a", 64) }},
		{"omission", func(l *GenerationLedger) { l.Entries[0].State = "allocated" }},
		{"future", func(l *GenerationLedger) { l.Entries[0].AllocatedRevision = 2 }},
		{"unknown_state", func(l *GenerationLedger) { l.Entries[0].State = "enabled" }},
		{"duplicate", func(l *GenerationLedger) { l.Entries = append(l.Entries, l.Entries[0]) }},
		{"retired_active", func(l *GenerationLedger) {
			l.Revision = 2
			l.Entries[0].State = "retired"
			l.Entries[0].RetiredRevision = 2
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &GenerationLedger{Revision: 1, Entries: []GenerationLifecycle{{Proof: SchemaGenerationProof{ID: 1, Fingerprint: contract.Fingerprint()}, State: "supported", AllocatedRevision: 1}}}
			tc.edit(l)
			require.Error(t, ValidateGenerationLedger(l, registry))
		})
	}
}

func TestGenerationLifecycle_RetiredProofEveryDirectOperation(t *testing.T) {
	registered := []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}
	declarations := testSchemaGenerationsForRegisteredTables(registered)
	declarations[0].ID = 3
	service, err := newTestRuntimeService(nil, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: declarations,
	}, nil)
	require.NoError(t, err)
	actor := Actor{UserID: "proof-user", SourceID: "proof-source"}
	missing := SchemaGenerationProof{ID: 1, Fingerprint: strings.Repeat("a", 64)}
	active, _ := service.schemaGenerationRegistry.Generation(3)
	service.config.Readiness.Expected.GenerationLedger = &GenerationLedger{Revision: 3, Entries: []GenerationLifecycle{
		{Proof: missing, State: "retired", AllocatedRevision: 1, RetiredRevision: 3},
		{Proof: SchemaGenerationProof{ID: 3, Fingerprint: active.Fingerprint()}, State: "supported", AllocatedRevision: 2}}}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "generation discovery", call: func() error { _, err := service.GetGeneration(context.Background(), actor, missing.ID); return err }},
		{name: "connect", call: func() error {
			_, err := service.Connect(context.Background(), actor, missing, &ConnectRequest{})
			return err
		}},
		{name: "push", call: func() error {
			// Current push admission first owns the durable source/outcome.
			// A nil-pool service cannot stand in for that direct runtime boundary.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			f := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
			old := testSchemaGenerationProofForService(f.svc)
			declarations := append([]SchemaGeneration(nil), f.svc.config.SchemaGenerations...)
			declarations[0].ID = 3
			registry, compileErr := CompileSchemaGenerationRegistry(f.svc.config.RegisteredTables, declarations)
			require.NoError(t, compileErr)
			current, _ := registry.Generation(3)
			f.svc.schemaGenerationRegistry = registry
			f.svc.config.Readiness.Expected.GenerationLedger = &GenerationLedger{Revision: 3, Entries: []GenerationLifecycle{
				{Proof: old, State: "retired", AllocatedRevision: 1, RetiredRevision: 3},
				{Proof: SchemaGenerationProof{ID: 3, Fingerprint: current.Fingerprint()}, State: "supported", AllocatedRevision: 2},
			}}
			rows := []PushRequestRow{f.userRow(uuid.New(), "retired")}
			hash, hashErr := computeGenerationPinnedPushRequestHash(old, rows)
			require.NoError(t, hashErr)
			_, pushErr := f.svc.PushOneShot(ctx, f.writer, old, &OneShotPushRequest{SourceBundleID: 1, CanonicalRequestHash: hash, Rows: rows})
			var count int
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{f.schemaName, "users"}.Sanitize()).Scan(&count))
			require.Zero(t, count, "retired fresh push cannot publish business DML")
			return pushErr
		}},
		{name: "pull", call: func() error { _, err := service.ProcessPull(context.Background(), actor, missing, 0, 1, 0); return err }},
		{name: "watch", call: func() error {
			_, err := service.SubscribeBundleChanges(context.Background(), actor, missing, 0)
			return err
		}},
		{name: "snapshot create", call: func() error {
			_, err := service.CreateSnapshotSession(context.Background(), actor, missing)
			return err
		}},
		{name: "snapshot create request", call: func() error {
			_, err := service.CreateSnapshotSessionWithRequest(context.Background(), actor, missing, nil)
			return err
		}},
		{name: "snapshot get", call: func() error {
			_, err := service.GetSnapshotChunk(context.Background(), actor, missing, uuid.NewString(), 0, 1, 1)
			return err
		}},
		{name: "snapshot delete", call: func() error {
			return service.DeleteSnapshotSession(context.Background(), actor, missing, uuid.NewString())
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var typed *SchemaGenerationRequestError
			require.ErrorAs(t, test.call(), &typed)
			require.Equal(t, SchemaGenerationRetired, typed.Code)
		})
	}
}
