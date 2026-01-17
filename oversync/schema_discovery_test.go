package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type mockHandler struct {
	convertValue any
	err          error
}

func (m *mockHandler) ApplyUpsert(_ context.Context, _ pgx.Tx, _ string, _ string, _ uuid.UUID, _ []byte) error {
	return nil
}

func (m *mockHandler) ApplyDelete(_ context.Context, _ pgx.Tx, _ string, _ string, _ uuid.UUID) error {
	return nil
}

func (m *mockHandler) ConvertReferenceKey(_ string, _ any) (any, error) {
	return m.convertValue, m.err
}

// Test helper to ensure FK conversion does not panic on non-string payloads.
func TestConvertFKValueForHandler_AllowsNonStringPayload(t *testing.T) {
	handler := &mockHandler{convertValue: 42}
	val, bad := convertFKValueForHandler(handler, "user_id", 42)
	if bad {
		t.Fatalf("expected non-bad payload")
	}
	if val != 42 {
		t.Fatalf("expected converted value to be preserved, got %v", val)
	}
}

func TestConvertFKValueForHandler_ReportsBadPayloadOnError(t *testing.T) {
	handler := &mockHandler{err: fmt.Errorf("parse error")}
	_, bad := convertFKValueForHandler(handler, "user_id", "abc")
	if !bad {
		t.Fatalf("expected bad payload flag on handler error")
	}
}

func TestDependencyOverrides_AffectOrderingAndHasDependencies(t *testing.T) {
	registered := map[string]bool{
		"public.parents":  true,
		"public.children": true,
	}
	dependencies := map[string][]string{
		"public.parents":  {},
		"public.children": {},
	}

	applyDependencyOverrides(dependencies, registered, map[string][]string{
		"public.children": {"public.parents"},
	})

	sd := &SchemaDiscovery{logger: slog.Default()}
	order, err := sd.topologicalSort(dependencies, registered)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	idx := func(key string) int {
		for i, v := range order {
			if v == key {
				return i
			}
		}
		return -1
	}
	if idx("public.parents") == -1 || idx("public.children") == -1 {
		t.Fatalf("expected both tables in order, got %v", order)
	}
	if idx("public.parents") >= idx("public.children") {
		t.Fatalf("expected parents before children, got %v", order)
	}

	d := &DiscoveredSchema{Dependencies: dependencies}
	if !d.HasDependencies([]ChangeUpload{
		{Schema: "public", Table: "children"},
		{Schema: "public", Table: "parents"},
	}) {
		t.Fatalf("expected HasDependencies to detect override dependency")
	}
}

func TestBuildInBatchIndex_RefColumnAwareIndexesParentRefColumns(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{FKPrecheckMode: FKPrecheckRefColumnAware},
		discoveredSchema: &DiscoveredSchema{
			FKMap: map[string][]FK{
				Key("public", "children"): {
					{Col: "parent_code", RefSchema: "public", RefTable: "parents", RefCol: "code"},
				},
			},
		},
		tableHandlers: make(map[string]MaterializationHandler),
	}

	parentPK := "11111111-1111-1111-1111-111111111111"
	idx := svc.buildInBatchIndex(
		[]ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "parents",
				Op:             OpInsert,
				PK:             parentPK,
				ServerVersion:  0,
				Payload:        []byte(`{"id":"` + parentPK + `","code":"abc"}`),
			},
		},
		nil,
	)

	set := idx.ref[refKey{table: Key("public", "parents"), col: "code"}]
	if _, ok := set["abc"]; !ok {
		t.Fatalf("expected ref index to contain parents.code=abc, got %v", set)
	}

	// If the parent row is deleted in the same batch, it should not be considered "will exist" for ref indexing.
	idx2 := svc.buildInBatchIndex(
		[]ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "parents",
				Op:             OpInsert,
				PK:             parentPK,
				ServerVersion:  0,
				Payload:        []byte(`{"id":"` + parentPK + `","code":"abc"}`),
			},
		},
		[]ChangeUpload{
			{SourceChangeID: 2, Schema: "public", Table: "parents", Op: OpDelete, PK: parentPK},
		},
	)
	if set2, ok := idx2.ref[refKey{table: Key("public", "parents"), col: "code"}]; ok && len(set2) > 0 {
		t.Fatalf("expected ref index to exclude deleted row values, got %v", set2)
	}

	// Default mode should not build ref index.
	svc.config.FKPrecheckMode = FKPrecheckEnabled
	idx3 := svc.buildInBatchIndex(
		[]ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "parents",
				Op:             OpInsert,
				PK:             parentPK,
				ServerVersion:  0,
				Payload:        []byte(`{"id":"` + parentPK + `","code":"abc"}`),
			},
		},
		nil,
	)
	if idx3.ref != nil {
		t.Fatalf("expected ref index to be nil in default mode")
	}
}
