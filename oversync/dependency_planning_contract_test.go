package oversync

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pure contract vectors only: no authoritative database baseline or graph planner exists here.
func TestContractCorpus_DependencyPlanning(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var f struct {
		FormatVersion          int
		Status                 string
		HTTPChanged            bool
		ExecutedContractChecks []string
		StateKinds             []struct {
			Kind, RelationshipKnowledge string
			HasPayload, HasVersion      bool
		}
		HardBoundaryCases []struct {
			ID                                                      string
			ComponentRows, ComponentDecodedBytes, MaxRows, MaxBytes int64
			Expected                                                string
		}
		ResumeCases []struct {
			ID, Saved, Current string
			Resume             bool
		}
		RuntimeScenarios []struct {
			ID, Owner, Given, Expected string
			Executed                   bool
		}
	}
	readContractFixture(t, root, "local-planning/dependency-planning.json", &f)
	var witnesses struct {
		BaseWitnesses []struct {
			Event                                           string
			BeforeA, BeforeB, Local, Server, AfterA, AfterB map[string]json.RawMessage
		}
	}
	readContractFixture(t, root, "local-planning/dependency-planning.json", &witnesses)
	require.Len(t, witnesses.BaseWitnesses, 4)
	for _, w := range witnesses.BaseWitnesses {
		wantA, wantB := w.BeforeA, w.BeforeB
		switch w.Event {
		case "local_edit", "requeue":
		case "snapshot_preserve_outbox":
			wantA = w.Server
		case "settlement_rebase":
			wantA, wantB = w.Server, w.Server
		default:
			t.Fatalf("unowned base witness %q", w.Event)
		}
		require.Equal(t, wantA, w.AfterA, w.Event)
		require.Equal(t, wantB, w.AfterB, w.Event)
		require.NotEqual(t, w.Local, w.BeforeB)
		require.Len(t, w.BeforeB, 7)
	}

	require.Equal(t, 1, f.FormatVersion)
	require.Equal(t, "contract-candidate-runtime-pending", f.Status)
	require.False(t, f.HTTPChanged)
	require.Equal(t, []string{"state-discriminators", "full-base-copy-witnesses", "hard-boundary-arithmetic", "resume-guard-equality", "scenario-ownership", "rekey-relationship-ordering"}, f.ExecutedContractChecks)
	var kinds []string
	for _, state := range f.StateKinds {
		kinds = append(kinds, state.Kind)
		knowledge := "known"
		if state.Kind == "unknown" {
			knowledge = "unknown"
		}
		require.Equal(t, knowledge, state.RelationshipKnowledge)
		require.Equal(t, state.Kind == "live", state.HasPayload)
		require.Equal(t, state.Kind == "live" || state.Kind == "deleted", state.HasVersion)
	}
	require.Equal(t, []string{"live", "deleted", "absent", "unpublished", "unknown"}, kinds)
	require.Len(t, f.HardBoundaryCases, 6)
	for _, c := range f.HardBoundaryCases {
		t.Run(c.ID, func(t *testing.T) {
			require.Positive(t, c.MaxRows)
			require.Positive(t, c.MaxBytes)
			require.Greater(t, c.ComponentRows, int64(1))
			require.Positive(t, c.ComponentDecodedBytes)
			result := "fits"
			if c.ComponentRows > c.MaxRows {
				result = "rows"
			} else if c.ComponentDecodedBytes > c.MaxBytes {
				result = "decoded_bytes"
			}
			require.Equal(t, c.Expected, result)
		})
	}
	require.Len(t, f.ResumeCases, 4)
	for _, c := range f.ResumeCases {
		require.Equal(t, c.Saved == c.Current, c.Resume, c.ID)
	}
	ids := map[string]bool{}
	for _, c := range f.RuntimeScenarios {
		require.False(t, ids[c.ID], "duplicate scenario: %s", c.ID)
		ids[c.ID] = true
		require.Contains(t, []string{"baseline-lifecycle", "dependency-planning", "public-sync-live"}, c.Owner)
		require.NotEmpty(t, c.Given)
		require.NotEmpty(t, c.Expected)
		require.False(t, c.Executed, "contract ownership is not runtime proof: %s", c.ID)
	}
	require.Len(t, ids, 33)
	for _, id := range []string{"typed-base", "snapshot-outbox", "conflict-rebase", "format-rejection", "beyond-hard-lookahead", "independent-pairs", "oversized-plus-independent", "diagnostics", "one-call-live", "rekey-without-dependents", "rekey-unrelated-insert", "rekey-chain-coalescing", "rekey-restart", "rekey-partial-failure"} {
		require.True(t, ids[id], id)
	}
	// Bounded acyclic contract model only; capture, SQLite restart and SCCs remain runtime tests.
	var rekeys struct {
		RekeyWitnesses []struct {
			ID         string
			ServerLive []string
			Rows       []struct {
				Key, Op, ServerParent, DesiredParent string
				Ordinal                              int
			}
			ExpectedEdges   []string
			ExpectedBatches [][]string
		}
	}
	readContractFixture(t, root, "local-planning/dependency-planning.json", &rekeys)
	require.Len(t, rekeys.RekeyWitnesses, 6)
	for _, w := range rekeys.RekeyWitnesses {
		t.Run(w.ID, func(t *testing.T) {
			// Consume only serialized persisted facts, never the human-readable edit history.
			encoded, err := json.Marshal(w)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(encoded, &w))
			require.LessOrEqual(t, len(w.Rows), 4)
			pending := map[string]bool{}
			for _, row := range w.Rows {
				require.Len(t, row.Key, 1) // Fixture labels only, not production key encoding.
				require.False(t, pending[row.Key])
				pending[row.Key] = true
				require.Contains(t, []string{"upsert", "delete"}, row.Op)
				if row.ServerParent != "" {
					require.Contains(t, w.ServerLive, row.Key, "server-parent owner must be live: %s", row.Key)
					require.Contains(t, w.ServerLive, row.ServerParent, "server-parent target must be live: %s", row.ServerParent)
				}
			}
			var edges []string
			for _, row := range w.Rows {
				parentLive := false
				for _, key := range w.ServerLive {
					parentLive = parentLive || key == row.DesiredParent
				}
				if row.Op == "upsert" && row.DesiredParent != "" && !parentLive {
					require.True(t, pending[row.DesiredParent])
					edges = append(edges, row.Key+">"+row.DesiredParent)
				}
				if row.Op == "delete" {
					for _, child := range w.Rows {
						if child.ServerParent == row.Key && (child.Op == "delete" || child.DesiredParent != row.Key) {
							edges = append(edges, row.Key+">"+child.Key)
						}
					}
				}
			}
			sort.Strings(edges)
			require.Equal(t, w.ExpectedEdges, append([]string{}, edges...))
			sort.Slice(w.Rows, func(i, j int) bool { return w.Rows[i].Ordinal < w.Rows[j].Ordinal })
			var batches [][]string
			for range w.Rows {
				selected := ""
				for _, row := range w.Rows {
					if !pending[row.Key] {
						continue
					}
					ready := true
					for _, edge := range edges {
						if edge[:1] == row.Key && pending[edge[2:]] {
							ready = false
						}
					}
					if ready {
						selected = row.Key
						break
					}
				}
				require.NotEmpty(t, selected, "witness must be acyclic")
				delete(pending, selected)
				batches = append(batches, []string{selected}) // Hard row limit is one.
			}
			require.Equal(t, w.ExpectedBatches, batches)
		})
	}
	var classification struct {
		LocalContractCandidates []struct {
			Path, Contract, GoConsumer, JVMConsumer string
			RuntimeOwners                           []string
		}
	}
	readContractFixture(t, root, "CLASSIFICATIONS.json", &classification)
	require.Len(t, classification.LocalContractCandidates, 3)
	var index = -1
	for i, entry := range classification.LocalContractCandidates {
		if entry.Contract == "dependency-planning" {
			require.Equal(t, -1, index)
			index = i
		}
	}
	require.NotEqual(t, -1, index)
	c := classification.LocalContractCandidates[index]
	require.Equal(t, "local-planning/dependency-planning.json", c.Path)
	require.Equal(t, "dependency-planning", c.Contract)
	require.Equal(t, "oversync/dependency_planning_contract_test.go:TestContractCorpus_DependencyPlanning", c.GoConsumer)
	require.Equal(t, "SharedDependencyPlanningContractTest.dependencyPlanningContract", c.JVMConsumer)
	require.Equal(t, []string{"baseline-lifecycle", "dependency-planning", "public-sync-live"}, c.RuntimeOwners)
}
