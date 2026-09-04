// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSchemaGenerationProofHeaderMatrix(t *testing.T) {
	fingerprint := strings.Repeat("a", 64)
	tests := []struct {
		name   string
		header http.Header
		code   SchemaGenerationRequestErrorCode
		field  string
		id     SchemaGenerationID
	}{
		{name: "missing id", header: http.Header{}, code: SchemaGenerationMissing, field: "schema_generation_id"},
		{name: "duplicate id", header: http.Header{SchemaGenerationIDHeader: {"1", "1"}}, code: SchemaGenerationIDInvalid, field: "schema_generation_id"},
		{name: "case colliding id", header: http.Header{SchemaGenerationIDHeader: {"1"}, strings.ToLower(SchemaGenerationIDHeader): {"1"}}, code: SchemaGenerationIDInvalid, field: "schema_generation_id"},
		{name: "leading zero", header: http.Header{SchemaGenerationIDHeader: {"01"}}, code: SchemaGenerationIDInvalid, field: "schema_generation_id"},
		{name: "signed id", header: http.Header{SchemaGenerationIDHeader: {"+1"}}, code: SchemaGenerationIDInvalid, field: "schema_generation_id"},
		{name: "oversized id", header: http.Header{SchemaGenerationIDHeader: {"2147483648"}}, code: SchemaGenerationIDInvalid, field: "schema_generation_id"},
		{name: "missing fingerprint", header: http.Header{SchemaGenerationIDHeader: {"1"}}, code: SchemaGenerationFingerprintMissing, field: "schema_generation_fingerprint", id: 1},
		{name: "duplicate fingerprint", header: http.Header{SchemaGenerationIDHeader: {"1"}, SchemaGenerationFingerprintHeader: {fingerprint, fingerprint}}, code: SchemaGenerationFingerprintMalformed, field: "schema_generation_fingerprint", id: 1},
		{name: "uppercase fingerprint", header: http.Header{SchemaGenerationIDHeader: {"1"}, SchemaGenerationFingerprintHeader: {strings.ToUpper(fingerprint)}}, code: SchemaGenerationFingerprintMalformed, field: "schema_generation_fingerprint", id: 1},
		{name: "comma fingerprint", header: http.Header{SchemaGenerationIDHeader: {"1"}, SchemaGenerationFingerprintHeader: {fingerprint + "," + fingerprint}}, code: SchemaGenerationFingerprintMalformed, field: "schema_generation_fingerprint", id: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSchemaGenerationProofHeaders(test.header)
			var typed *SchemaGenerationRequestError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, test.code, typed.Code)
			require.Equal(t, test.field, typed.Field)
			require.Equal(t, test.id, typed.GenerationID)
		})
	}
	proof, err := parseSchemaGenerationProofHeaders(http.Header{
		SchemaGenerationIDHeader:          {"2147483647"},
		SchemaGenerationFingerprintHeader: {fingerprint},
	})
	require.NoError(t, err)
	require.Equal(t, SchemaGenerationID(2147483647), proof.ID)
	require.Equal(t, fingerprint, proof.Fingerprint)
}

func TestSchemaGenerationProofIsRequiredByEveryBoundDirectOperation(t *testing.T) {
	registered := []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}
	service, err := newTestRuntimeService(nil, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered),
	}, nil)
	require.NoError(t, err)
	actor := Actor{UserID: "proof-user", SourceID: "proof-source"}
	missing := SchemaGenerationProof{}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "connect", call: func() error {
			_, err := service.Connect(context.Background(), actor, missing, &ConnectRequest{})
			return err
		}},
		{name: "push", call: func() error {
			_, err := service.PushOneShot(context.Background(), actor, missing, &OneShotPushRequest{SourceBundleID: 1, Rows: []PushRequestRow{{}}})
			return err
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
			require.Equal(t, SchemaGenerationMissing, typed.Code)
		})
	}
}

func TestSchemaGenerationProofIsRequiredByEveryBoundSyncHTTPHandler(t *testing.T) {
	handlers := NewHTTPSyncHandlers(nil, nil)
	actor := Actor{UserID: "proof-user", SourceID: "proof-source"}
	tests := []struct {
		name   string
		method string
		path   string
		serve  func(http.ResponseWriter, *http.Request)
	}{
		{name: "connect", method: http.MethodPost, path: "/sync/connect", serve: handlers.HandleConnect},
		{name: "push", method: http.MethodPost, path: "/sync/push", serve: handlers.HandlePush},
		{name: "pull", method: http.MethodGet, path: "/sync/pull", serve: handlers.HandlePull},
		{name: "watch", method: http.MethodGet, path: "/sync/watch", serve: handlers.HandleWatch},
		{name: "snapshot create", method: http.MethodPost, path: "/sync/snapshot-sessions", serve: handlers.HandleCreateSnapshotSession},
		{name: "snapshot get", method: http.MethodGet, path: "/sync/snapshot-sessions/id", serve: handlers.HandleGetSnapshotChunk},
		{name: "snapshot delete", method: http.MethodDelete, path: "/sync/snapshot-sessions/id", serve: handlers.HandleDeleteSnapshotSession},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request = request.WithContext(ContextWithActor(request.Context(), actor))
			recorder := httptest.NewRecorder()
			test.serve(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			var response GenerationErrorResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, string(SchemaGenerationMissing), response.Error)
			require.False(t, response.Retryable)
		})
	}
}

func TestSchemaGenerationErrorHTTPMappingsAreSafe(t *testing.T) {
	handler := NewHTTPSyncHandlers(nil, nil)
	tests := []struct {
		err         error
		status      int
		code        string
		remediation string
	}{
		{&SchemaGenerationRequestError{Code: SchemaGenerationMissing, Message: "missing"}, 400, "missing_schema_generation", "provide_schema_generation"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Message: "invalid"}, 400, "invalid_schema_generation_id", "correct_schema_generation_id"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMissing, Message: "missing fingerprint"}, 400, "missing_schema_generation_fingerprint", "provide_schema_generation_fingerprint"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMalformed, Message: "malformed fingerprint"}, 400, "malformed_schema_generation_fingerprint", "correct_schema_generation_fingerprint"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationUnknown, GenerationID: 9, Message: "unknown"}, 409, "unknown_schema_generation", "upgrade_or_reconfigure_client"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMismatch, GenerationID: 1, Message: "mismatch"}, 409, "schema_generation_fingerprint_mismatch", "upgrade_or_reconfigure_client"},
		{&SchemaGenerationRequestError{Code: SchemaGenerationUnsupported, GenerationID: 1, Message: "unsupported"}, 410, "unsupported_schema_generation", "upgrade_or_reconfigure_client"},
		{&SourceGenerationError{Code: SourceGenerationUnbound, RequestedGenerationID: 1, Message: "unbound"}, 409, "source_generation_unbound", "connect_source"},
		{&SourceGenerationError{Code: SourceGenerationConflict, RequestedGenerationID: 1, Message: "conflict"}, 409, "source_generation_conflict", "rotate_source"},
		{&SourceGenerationError{Code: SchemaGenerationDowngradeForbidden, RequestedGenerationID: 1, Message: "downgrade"}, 409, "schema_generation_downgrade_forbidden", "keep_or_upgrade_generation"},
		{&SourceGenerationError{Code: SourceGenerationStateCorrupt, RequestedGenerationID: 1, Message: "corrupt"}, 500, "corrupt_source_generation_state", "contact_operator"},
		{&SourceGenerationError{Code: SourceGenerationBindingLimitExceeded, RequestedGenerationID: 1, Message: "limit"}, 409, "source_binding_limit_exceeded", "contact_operator"},
		{&SessionGenerationError{Code: SessionGenerationMismatch, SessionKind: "snapshot", RequestedGenerationID: 2, StoredGenerationID: 1, Message: "mismatch"}, 409, "session_generation_mismatch", "restart_session_with_matching_generation"},
		{&SessionGenerationError{Code: SessionGenerationStateCorrupt, SessionKind: "snapshot", RequestedGenerationID: 1, Message: "corrupt"}, 500, "corrupt_session_generation_state", "contact_operator"},
		{&CommittedPushStateError{Code: CommittedPushStateCorrupt, Field: "planned_row_count", Message: "corrupt"}, 500, "corrupt_committed_push_state", "contact_operator"},
	}
	for _, test := range tests {
		recorder := httptest.NewRecorder()
		require.True(t, handler.writeGenerationError(recorder, nil, fmt.Errorf("wrapped: %w", test.err)))
		require.Equal(t, test.status, recorder.Code)
		var response GenerationErrorResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Equal(t, test.code, response.Error)
		require.Equal(t, test.remediation, response.Remediation)
		require.False(t, response.Retryable)
		for _, secret := range []string{strings.Repeat("a", 64), "source-id", "session-id", "user-id"} {
			require.NotContains(t, recorder.Body.String(), secret)
		}
	}
}

func TestGenerationPinnedPushRequestHashGoldenVector(t *testing.T) {
	proof := SchemaGenerationProof{ID: 1, Fingerprint: strings.Repeat("0", 64)}
	hash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{{
		Schema: "business", Table: "items", Key: SyncKey{"id": "a"}, Op: OpDelete, BaseRowVersion: 0,
	}})
	require.NoError(t, err)
	require.Equal(t, "591628de43090d836024a07353ea219a25fffd7f80f0158dc0a8494a0278dc18", hash)
}

func TestServiceSchemaGenerationConfigurationIsImmutableAndBounded(t *testing.T) {
	registered := []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}
	generations := []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: "business", Table: "items", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}}}}}
	config := &ServiceConfig{RegisteredTables: registered, RegisteredTableCatalogRevision: 1, SchemaGenerations: generations}
	service, err := newTestRuntimeService(nil, config, nil)
	require.NoError(t, err)
	require.Equal(t, defaultMaxClientSourcesPerUser, service.config.MaxClientSourcesPerUser)
	require.Empty(t, service.generationViews)

	registered[0].Schema = "mutated"
	generations[0].ID = 2
	generations[0].Tables[0].Columns[0].Name = "mutated"
	contract, ok := service.schemaGenerationRegistry.Generation(1)
	require.True(t, ok)
	require.Equal(t, "business", contract.Tables()[0].Schema)
	require.Equal(t, "id", contract.Tables()[0].Columns[0].Name)

	for _, limit := range []int{-1, maxMaxClientSourcesPerUser + 1} {
		bad := &ServiceConfig{RegisteredTables: []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}, RegisteredTableCatalogRevision: 1, SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: "business", Table: "items", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}}}}}, MaxClientSourcesPerUser: limit}
		_, err := newTestRuntimeService(nil, bad, nil)
		require.ErrorContains(t, err, "MaxClientSourcesPerUser")
	}
	_, err = newTestRuntimeService(nil, &ServiceConfig{RegisteredTables: []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}, RegisteredTableCatalogRevision: 1}, nil)
	var generationErr *SchemaGenerationConfigError
	require.True(t, errors.As(err, &generationErr))
}

func TestSchemaGenerationDiscoveryAndBoundSourceSemantics(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_capabilities_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	table := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{table}}, {ID: 2, Tables: []GenerationTable{table}}},
	}, nil)
	proof1 := proofForGeneration(t, service, 1)
	proof2 := proofForGeneration(t, service, 2)
	actor := Actor{UserID: "generation-capabilities-user", SourceID: "device-a"}

	preview, err := service.GetGeneration(ctx, actor, proof2.ID)
	require.NoError(t, err)
	require.Equal(t, "2", preview.SchemaGeneration.ID)
	require.Len(t, preview.SchemaGeneration.Tables, 1)
	_, err = service.Connect(ctx, actor, proof1, &ConnectRequest{})
	require.NoError(t, err)
	preview, err = service.GetGeneration(ctx, actor, proof2.ID)
	require.NoError(t, err)
	require.Equal(t, "2", preview.SchemaGeneration.ID)
	_, err = service.ProcessPull(ctx, actor, proof2, 0, 1, 0)
	var conflict *SourceGenerationError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, SourceGenerationConflict, conflict.Code)

	actor2 := Actor{UserID: actor.UserID, SourceID: "device-b"}
	_, err = service.Connect(ctx, actor2, proof2, &ConnectRequest{})
	require.NoError(t, err)
	_, err = service.GetGeneration(ctx, actor2, proof1.ID)
	require.NoError(t, err)
	_, err = service.ProcessPull(ctx, actor2, proof1, 0, 1, 0)
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, SchemaGenerationDowngradeForbidden, conflict.Code)
	_, err = service.GetGeneration(ctx, Actor{UserID: actor.UserID, SourceID: "device-c"}, 3)
	var requestErr *SchemaGenerationRequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, SchemaGenerationUnknown, requestErr.Code)
}

func TestGenerationDataPlaneServesNonCanonicalVisibleSurfaceThroughProjection(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_surface_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	oldSurface := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}}
	currentSurface := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{
		{Name: "email", WireType: GenerationWireTypeText},
		{Name: "id", WireType: GenerationWireTypeUUID},
		{Name: "name", WireType: GenerationWireTypeText},
	}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{oldSurface}}, {ID: 2, Tables: []GenerationTable{currentSurface}}},
	}, nil)
	proof1 := proofForGeneration(t, service, 1)
	proof2 := proofForGeneration(t, service, 2)
	oldActor := Actor{UserID: "generation-surface-user", SourceID: "device-old"}
	_, err := service.Connect(ctx, oldActor, proof1, &ConnectRequest{})
	require.NoError(t, err)
	oldDiscovery, err := service.GetGeneration(ctx, oldActor, proof1.ID)
	require.NoError(t, err)
	require.Equal(t, "1", oldDiscovery.SchemaGeneration.ID)
	require.Len(t, oldDiscovery.SchemaGeneration.Tables, 1)
	_, err = service.ProcessPull(ctx, oldActor, proof1, 0, 1, 0)
	require.NoError(t, err)

	currentActor := Actor{UserID: "generation-surface-current-user", SourceID: "device-current"}
	_, err = service.Connect(ctx, currentActor, proof2, &ConnectRequest{})
	require.NoError(t, err)
	currentDiscovery, err := service.GetGeneration(ctx, currentActor, proof2.ID)
	require.NoError(t, err)
	require.Equal(t, "2", currentDiscovery.SchemaGeneration.ID)
	require.Len(t, currentDiscovery.SchemaGeneration.Tables[0].Columns, 3)
	_, err = service.ProcessPull(ctx, currentActor, proof2, 0, 1, 0)
	require.NoError(t, err)
}

func TestGenerationDataPlaneSupportsGenerationAllowlistSurfaces(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		registered  []string
		generations func(string) []SchemaGeneration
	}{
		{
			name:       "single generation omits physical column",
			registered: []string{"users"},
			generations: func(schema string) []SchemaGeneration {
				return []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}}}}}}
			},
		},
		{
			name:       "highest generation omits physical column",
			registered: []string{"users"},
			generations: func(schema string) []SchemaGeneration {
				table := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}}}
				return []SchemaGeneration{{ID: 1, Tables: []GenerationTable{table}}, {ID: 2, Tables: []GenerationTable{table}}}
			},
		},
		{
			name:       "generation omits registered physical table",
			registered: []string{"users", "files"},
			generations: func(schema string) []SchemaGeneration {
				return []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "email", WireType: GenerationWireTypeText}, {Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}}}}}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := newIntegrationTestPool(t, ctx)
			schema := "generation_incomplete_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
			t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
			registered := make([]RegisteredTable, 0, len(test.registered))
			for _, table := range test.registered {
				registered = append(registered, RegisteredTable{Schema: schema, Table: table, SyncKeyColumns: []string{"id"}})
			}
			service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
				RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
				SchemaGenerations: test.generations(schema),
			}, nil)
			ids := service.schemaGenerationRegistry.GenerationIDs()
			proof := proofForGeneration(t, service, ids[len(ids)-1])
			actor := Actor{UserID: "incomplete-user", SourceID: "device"}
			_, err := service.Connect(ctx, actor, proof, &ConnectRequest{})
			require.NoError(t, err)
			discovery, err := service.GetGeneration(ctx, actor, proof.ID)
			require.NoError(t, err)
			require.Equal(t, canonicalSchemaGenerationID(proof.ID), discovery.SchemaGeneration.ID)
			_, err = service.ProcessPull(ctx, actor, proof, 0, 1, 0)
			require.NoError(t, err)
		})
	}
}

func TestGenerationForeignKeyValidationUsesCanonicalDefectOrder(t *testing.T) {
	contract := SchemaGenerationContract{tables: []GenerationTableContract{{
		Schema: "business", Table: "children",
		Columns: []GenerationColumn{
			{Name: "a", WireType: GenerationWireTypeUUID},
			{Name: "z", WireType: GenerationWireTypeUUID},
		},
	}}}
	physical := []ForeignKeyConstraint{
		{ConstraintName: "z_fk", TableSchema: "business", TableName: "children", ColumnName: "z", Ordinal: 1, RefSchema: "business", RefTable: "hidden", RefColumn: "id"},
		{ConstraintName: "a_fk", TableSchema: "business", TableName: "children", ColumnName: "a", Ordinal: 1, RefSchema: "business", RefTable: "hidden", RefColumn: "id"},
	}
	for i := 0; i < 1000; i++ {
		err := validateGenerationForeignKeys(1, contract, physical)
		var typed *UnsupportedSchemaError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, "a", typed.Column)
	}
}

func TestGenerationForeignKeyValidationOrdersAcrossDefectClasses(t *testing.T) {
	contract := SchemaGenerationContract{tables: []GenerationTableContract{
		{Schema: "business", Table: "children", Columns: []GenerationColumn{{Name: "a", WireType: GenerationWireTypeUUID}, {Name: "z", WireType: GenerationWireTypeUUID}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"a"}, ReferencedSchema: "business", ReferencedTable: "parents", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction}}},
		{Schema: "business", Table: "parents", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}},
	}}
	physical := []ForeignKeyConstraint{
		{constraintOID: 10, ConstraintName: "z_fk", TableSchema: "business", TableName: "children", ColumnName: "z", Ordinal: 1, RefSchema: "business", RefTable: "hidden", RefColumn: "id", OnDeleteAction: "NO ACTION", OnUpdateAction: "NO ACTION"},
		{constraintOID: 11, ConstraintName: "a_fk", TableSchema: "business", TableName: "children", ColumnName: "a", Ordinal: 1, RefSchema: "business", RefTable: "parents", RefColumn: "id", OnDeleteAction: "CASCADE", OnUpdateAction: "NO ACTION"},
	}
	for i := 0; i < 1000; i++ {
		err := validateGenerationForeignKeys(1, contract, physical)
		var typed *UnsupportedSchemaError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, "a", typed.Column)
	}
}

func TestGenerationForeignKeyValidationRequiresScopePreservingDeleteSetNull(t *testing.T) {
	contract := SchemaGenerationContract{tables: []GenerationTableContract{
		{Schema: "business", Table: "children", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}, {Name: "parent_id", WireType: GenerationWireTypeUUID, Nullable: true}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"parent_id"}, ReferencedSchema: "business", ReferencedTable: "parents", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeySetNull, OnUpdate: GenerationForeignKeyNoAction}}},
		{Schema: "business", Table: "parents", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}},
	}}
	physical := []ForeignKeyConstraint{
		{constraintOID: 12, ConstraintName: "parent_fk", TableSchema: "business", TableName: "children", ColumnName: syncScopeColumnName, Ordinal: 1, RefSchema: "business", RefTable: "parents", RefColumn: syncScopeColumnName, OnDeleteAction: "SET NULL", OnUpdateAction: "NO ACTION"},
		{constraintOID: 12, ConstraintName: "parent_fk", TableSchema: "business", TableName: "children", ColumnName: "parent_id", Ordinal: 2, RefSchema: "business", RefTable: "parents", RefColumn: "id", OnDeleteAction: "SET NULL", OnUpdateAction: "NO ACTION"},
	}
	err := validateGenerationForeignKeys(1, contract, physical)
	var typed *UnsupportedSchemaError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "on_delete", typed.Field)

	physical[0].deleteSetColumnsSpecified = true
	physical[1].deleteSetColumnsSpecified = true
	physical[1].deleteSetsThisColumn = true
	require.NoError(t, validateGenerationForeignKeys(1, contract, physical))
}

func TestGenerationForeignKeyValidationUsesConstraintObjectIdentity(t *testing.T) {
	contract := SchemaGenerationContract{tables: []GenerationTableContract{
		{Schema: "business", Table: "a_children", Columns: []GenerationColumn{{Name: "parent_id", WireType: GenerationWireTypeUUID}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"parent_id"}, ReferencedSchema: "business", ReferencedTable: "parents", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction}}},
		{Schema: "business", Table: "z_children", Columns: []GenerationColumn{{Name: "parent_id", WireType: GenerationWireTypeUUID}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"parent_id"}, ReferencedSchema: "business", ReferencedTable: "parents", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction}}},
		{Schema: "business", Table: "parents", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}}},
	}}
	physical := []ForeignKeyConstraint{
		{constraintOID: 20, ConstraintName: "same_name", TableSchema: "business", TableName: "a_children", ColumnName: "parent_id", Ordinal: 1, RefSchema: "business", RefTable: "parents", RefColumn: "id", OnDeleteAction: "NO ACTION", OnUpdateAction: "NO ACTION"},
		{constraintOID: 21, ConstraintName: "same_name", TableSchema: "business", TableName: "z_children", ColumnName: "parent_id", Ordinal: 1, RefSchema: "business", RefTable: "parents", RefColumn: "id", OnDeleteAction: "NO ACTION", OnUpdateAction: "NO ACTION"},
	}
	require.NoError(t, validateGenerationForeignKeys(1, contract, physical))
}

func TestSourceGenerationBindingAdmissionIsAtomicAndBounded(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_admission_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations:       testSchemaGenerationsForRegisteredTables(registered),
		MaxClientSourcesPerUser: 1,
	}, nil)
	proof := testSchemaGenerationProofForService(service)
	userID := "generation-admission-user"
	actors := []Actor{{UserID: userID, SourceID: "device-a"}, {UserID: userID, SourceID: "device-b"}}
	type result struct{ err error }
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, actor := range actors {
		actor := actor
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := service.Connect(ctx, actor, proof, &ConnectRequest{})
			results <- result{err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes, limited := 0, 0
	for result := range results {
		if result.err == nil {
			successes++
			continue
		}
		var sourceErr *SourceGenerationError
		require.ErrorAs(t, result.err, &sourceErr)
		require.Equal(t, SourceGenerationBindingLimitExceeded, sourceErr.Code)
		require.Zero(t, sourceErr.BoundGenerationID)
		require.Empty(t, sourceErr.Field)
		limited++
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, limited)
	var bindingCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.source_generation_bindings b JOIN sync.user_state u ON u.user_pk=b.user_pk WHERE u.user_id=$1`, userID).Scan(&bindingCount))
	require.Equal(t, 1, bindingCount)
}

func TestSourceGenerationOneSidedCorruptionValidatesStoredPairBeforePresence(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_corrupt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered),
	}, nil)
	userID := "corrupt-binding-user"
	mustInitializeEmptyScope(t, ctx, service, userID, "corrupt-binding-owner")
	var userPK int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, userID).Scan(&userPK))
	_, err := pool.Exec(ctx, `ALTER TABLE sync.source_generation_bindings DROP CONSTRAINT source_generation_bindings_source_fk`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ALTER TABLE sync.source_generation_bindings DROP CONSTRAINT source_generation_bindings_id_chk`)
	require.NoError(t, err)
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, `INSERT INTO sync.source_generation_bindings (user_pk,source_id,schema_generation_id,schema_generation_fingerprint) VALUES ($1,$2,-1,$3)`, userPK, "orphan", strings.Repeat("0", 64))
		return err
	}())

	proof := testSchemaGenerationProofForService(service)
	_, err = service.ProcessPull(ctx, Actor{UserID: userID, SourceID: "orphan"}, proof, 0, 1, 0)
	var corrupt *SourceGenerationError
	require.ErrorAs(t, err, &corrupt)
	require.Equal(t, SourceGenerationStateCorrupt, corrupt.Code)
	require.Equal(t, "schema_generation_id", corrupt.Field)
	require.Zero(t, corrupt.BoundGenerationID)

	_, err = service.ProcessPull(ctx, Actor{UserID: userID, SourceID: "orphan"}, proof, 0, 1, 0)
	corrupt = nil
	require.ErrorAs(t, err, &corrupt)
	require.Equal(t, SourceGenerationStateCorrupt, corrupt.Code)
	require.Equal(t, "schema_generation_id", corrupt.Field)
	require.Zero(t, corrupt.BoundGenerationID)
}

func proofForGeneration(t *testing.T, service *SyncService, id SchemaGenerationID) SchemaGenerationProof {
	t.Helper()
	contract, ok := service.schemaGenerationRegistry.Generation(id)
	require.True(t, ok)
	return SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}
}

func TestSnapshotSessionBindingCascadeUsesExpectedIndexPrefix(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_explain_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	registered := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered),
	}, nil)
	proof := testSchemaGenerationProofForService(service)
	_, err := pool.Exec(ctx, `INSERT INTO sync.user_state (user_id) SELECT 'plan-user-'||lpad(n::text,3,'0') FROM generate_series(0,199) AS n`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.source_state (user_pk,source_id,state,max_committed_source_bundle_id,replaced_by_source_id,retirement_reason)
		SELECT u.user_pk,'source-'||lpad(n::text,3,'0'),'active',0,'',''
		FROM sync.user_state AS u CROSS JOIN generate_series(0,19) AS n
		WHERE u.user_id LIKE 'plan-user-%'
	`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.source_generation_bindings (user_pk,source_id,schema_generation_id,schema_generation_fingerprint)
		SELECT s.user_pk,s.source_id,$1,$2 FROM sync.source_state AS s
		JOIN sync.user_state AS u ON u.user_pk=s.user_pk WHERE u.user_id LIKE 'plan-user-%'
	`, proof.ID, proof.Fingerprint)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.snapshot_sessions (snapshot_id,user_pk,source_id,schema_generation_id,schema_generation_fingerprint,snapshot_bundle_seq,row_count,byte_count,expires_at)
		SELECT md5(u.user_id||':'||s.source_id)::uuid,s.user_pk,s.source_id,$1,$2,0,0,0,transaction_timestamp()+interval '1 hour'
		FROM sync.source_state AS s JOIN sync.user_state AS u ON u.user_pk=s.user_pk WHERE u.user_id LIKE 'plan-user-%'
	`, proof.ID, proof.Fingerprint)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE sync.snapshot_sessions`)
	require.NoError(t, err)
	var targetUserPK int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id='plan-user-000'`).Scan(&targetUserPK))
	require.NoError(t, func() error {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		for _, query := range []string{
			fmt.Sprintf(`EXPLAIN (FORMAT TEXT) SELECT 1 FROM sync.snapshot_sessions WHERE user_pk=%d`, targetUserPK),
			fmt.Sprintf(`EXPLAIN (FORMAT TEXT) SELECT 1 FROM sync.snapshot_sessions WHERE user_pk=%d AND source_id='source-000' AND schema_generation_id=%d AND schema_generation_fingerprint='%s'`, targetUserPK, proof.ID, proof.Fingerprint),
		} {
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return err
			}
			var plan strings.Builder
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					rows.Close()
					return err
				}
				plan.WriteString(line)
			}
			rows.Close()
			if !strings.Contains(plan.String(), "ss_source_generation_idx") {
				return fmt.Errorf("snapshot binding/user cascade plan does not use ss_source_generation_idx: %s", plan.String())
			}
		}
		return nil
	}())
	_, err = pool.Exec(ctx, `DELETE FROM sync.source_generation_bindings WHERE user_pk=$1 AND source_id='source-000'`, targetUserPK)
	require.NoError(t, err)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.snapshot_sessions WHERE user_pk=$1 AND source_id='source-000'`, targetUserPK).Scan(&count))
	require.Zero(t, count)
	_, err = pool.Exec(ctx, `DELETE FROM sync.user_state WHERE user_pk=$1`, targetUserPK)
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.snapshot_sessions WHERE user_pk=$1`, targetUserPK).Scan(&count))
	require.Zero(t, count)
}
