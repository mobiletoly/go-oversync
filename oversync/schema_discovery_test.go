package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestDependencyOverrides_AffectOrdering(t *testing.T) {
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
	order, cycleGroup, cycles, err := sd.topologicalSort(dependencies, registered)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cycleGroup) != 0 || len(cycles) != 0 {
		t.Fatalf("expected no cycles for override test, got cycleGroup=%v cycles=%v", cycleGroup, cycles)
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
}

func TestSchemaDiscovery_ForeignKeysUseObjectIdentityAndExposeDeleteSetColumns(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "fk_identity_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ident := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s.parents (_sync_scope_id text NOT NULL, id uuid NOT NULL, PRIMARY KEY (_sync_scope_id,id));
		CREATE TABLE %s.a_children (_sync_scope_id text NOT NULL, id uuid NOT NULL, parent_id uuid, PRIMARY KEY (_sync_scope_id,id), CONSTRAINT same_name FOREIGN KEY (_sync_scope_id,parent_id) REFERENCES %s.parents (_sync_scope_id,id) DEFERRABLE);
		CREATE TABLE %s.z_children (_sync_scope_id text NOT NULL, id uuid NOT NULL, parent_id uuid, PRIMARY KEY (_sync_scope_id,id), CONSTRAINT same_name FOREIGN KEY (_sync_scope_id,parent_id) REFERENCES %s.parents (_sync_scope_id,id) ON DELETE SET NULL (parent_id) DEFERRABLE);
	`, ident, ident, ident, ident, ident, ident))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	parentID := uuid.New()
	childID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.parents (_sync_scope_id,id) VALUES ('scope-a',$1)`, ident), parentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.z_children (_sync_scope_id,id,parent_id) VALUES ('scope-a',$1,$2)`, ident), childID, parentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.parents WHERE _sync_scope_id = 'scope-a' AND id = $1`, ident), parentID)
	require.NoError(t, err)
	var retainedScope string
	var nulledParent *uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT _sync_scope_id,parent_id FROM %s.z_children WHERE id = $1
	`, ident), childID).Scan(&retainedScope, &nulledParent))
	require.Equal(t, "scope-a", retainedScope)
	require.Nil(t, nulledParent)

	discovery := newSchemaDiscoveryWithQuerier(pool, integrationTestLogger(slog.LevelWarn))
	constraints, err := discovery.getForeignKeyConstraints(ctx, map[string]bool{
		Key(schema, "parents"): true, Key(schema, "a_children"): true, Key(schema, "z_children"): true,
	})
	require.NoError(t, err)
	require.Len(t, constraints, 4)
	oids := make(map[int64]bool)
	for _, constraint := range constraints {
		oids[constraint.constraintOID] = true
		if constraint.TableName == "z_children" {
			require.True(t, constraint.deleteSetColumnsSpecified)
			require.Equal(t, constraint.ColumnName == "parent_id", constraint.deleteSetsThisColumn)
		}
	}
	require.Len(t, oids, 2)
}

func TestTopologicalSort_GroupsStronglyConnectedComponentsDeterministically(t *testing.T) {
	registered := map[string]bool{
		"public.alpha": true,
		"public.beta":  true,
		"public.gamma": true,
	}
	dependencies := map[string][]string{
		"public.alpha": {"public.beta"},
		"public.beta":  {"public.alpha"},
		"public.gamma": {"public.beta"},
	}

	sd := &SchemaDiscovery{logger: slog.Default()}
	order, cycleGroup, cycles, err := sd.topologicalSort(dependencies, registered)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != 3 {
		t.Fatalf("expected 3 tables in order, got %v", order)
	}
	if order[0] != "public.alpha" || order[1] != "public.beta" {
		t.Fatalf("expected cycle members to be ordered deterministically first, got %v", order)
	}
	if order[2] != "public.gamma" {
		t.Fatalf("expected dependent table after cycle, got %v", order)
	}
	if cycleGroup["public.alpha"] == 0 || cycleGroup["public.alpha"] != cycleGroup["public.beta"] {
		t.Fatalf("expected alpha and beta in same cycle group, got %v", cycleGroup)
	}
	if _, ok := cycleGroup["public.gamma"]; ok {
		t.Fatalf("did not expect gamma to be marked as cyclic, got %v", cycleGroup)
	}
	members := cycles[cycleGroup["public.alpha"]]
	if len(members) != 2 || members[0] != "public.alpha" || members[1] != "public.beta" {
		t.Fatalf("unexpected cycle members: %v", members)
	}
}
