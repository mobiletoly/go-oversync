package oversync

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContractCorpus_LocalApply(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var fixture struct {
		FormatVersion int
		Defaults      map[string]int64
		Results       []struct {
			Reason, Dimension             string
			Resumable, AdvancesCheckpoint bool
		}
		RevisionCases []struct {
			Saved, Current int64
			Resume         bool
		}
		SurvivorWitness struct {
			Before, After map[string]*string
			Wire          []struct {
				Key, Op string
				Parent  *string
			}
		}
		PersistedFormat struct {
			Tables, Phases                              []string
			GuardBindsRegistrationOrder, AllRootsExpand bool
			RelationshipBuildPassesPerUnit              int
		}
		AllocationCases []struct {
			Payload      json.RawMessage
			EncodedBytes int
		}
	}
	readContractFixture(t, root, "local-apply/a1.json", &fixture)
	require.Equal(t, 1, fixture.FormatVersion)
	require.Equal(t, map[string]int64{"entries": 100000, "bytes": 256 << 20, "work": 1000000}, fixture.Defaults)
	require.Len(t, fixture.Results, 4)
	for _, result := range fixture.Results {
		require.False(t, result.AdvancesCheckpoint)
		require.Equal(t, result.Reason == "sync_work_limit", result.Resumable)
		if result.Resumable {
			require.Equal(t, "work", result.Dimension)
		} else {
			require.Equal(t, "sync_workspace_limit", result.Reason)
			require.Contains(t, []string{"entries", "bytes", "atomic_apply_work"}, result.Dimension)
		}
	}
	for _, revision := range fixture.RevisionCases {
		require.Equal(t, revision.Saved == revision.Current, revision.Resume)
	}
	rows := fixture.SurvivorWitness.Before
	for _, effect := range fixture.SurvivorWitness.Wire {
		switch effect.Op {
		case "DELETE":
			delete(rows, effect.Key)
		case "UPDATE":
			rows[effect.Key] = effect.Parent
		default:
			t.Fatalf("unsupported local-apply operation %q", effect.Op)
		}
	}
	require.Equal(t, fixture.SurvivorWitness.After, rows)
	require.Equal(t, []string{"_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"}, fixture.PersistedFormat.Tables)
	require.Equal(t, []string{"clear", "roots", "index", "walk", "ready"}, fixture.PersistedFormat.Phases)
	require.True(t, fixture.PersistedFormat.GuardBindsRegistrationOrder)
	require.True(t, fixture.PersistedFormat.AllRootsExpand)
	require.Equal(t, 1, fixture.PersistedFormat.RelationshipBuildPassesPerUnit)
	for _, allocation := range fixture.AllocationCases {
		var compact bytes.Buffer
		require.NoError(t, json.Compact(&compact, allocation.Payload))
		require.Equal(t, allocation.EncodedBytes, compact.Len())
	}
}
