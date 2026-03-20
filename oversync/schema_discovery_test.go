package oversync

import (
	"log/slog"
	"testing"
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
