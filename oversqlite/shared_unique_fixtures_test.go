package oversqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func uniqueFixtureRoot(t *testing.T) string {
	t.Helper()
	root := os.Getenv("OVERSQLITE_CONTRACTS_ROOT")
	require.NotEmpty(t, root, "OVERSQLITE_CONTRACTS_ROOT is required")
	raw, err := os.ReadFile(filepath.Join(root, "MANIFEST.sha256"))
	require.NoError(t, err)
	expected := map[string]string{}
	previous := ""
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		parts := strings.SplitN(line, "  ", 2)
		require.Len(t, parts, 2)
		require.Len(t, parts[0], 64)
		require.Greater(t, parts[1], previous)
		require.False(t, filepath.IsAbs(parts[1]))
		require.Equal(t, filepath.ToSlash(filepath.Clean(parts[1])), parts[1])
		require.False(t, strings.HasPrefix(parts[1], "../"))
		previous = parts[1]
		expected[parts[1]] = parts[0]
	}
	count := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "MANIFEST.sha256" {
			return nil
		}
		count++
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(bytes)
		require.Equal(t, expected[filepath.ToSlash(rel)], hex.EncodeToString(hash[:]), rel)
		return nil
	}))
	require.Equal(t, len(expected), count)
	return root
}
func TestSharedFinalStateUniqueFixtures(t *testing.T) {
	root := uniqueFixtureRoot(t)
	var fixture struct {
		Authority struct{ GoSpecSHA256, OpenAPISHA256 string }
		Initial   []map[string]any
		Cases     []struct {
			ID, IndexSQL, ApplicationTriggerSQL, ExpectBlockedReason string
			Effects                                                  []map[string]any
			AdditionalIndexes                                        []string
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "local-apply/unique-final-state.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	for path, want := range map[string]string{"../specs/p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md": fixture.Authority.GoSpecSHA256, "../swagger/two_way_sync.yaml": fixture.Authority.OpenAPISHA256} {
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		hash := sha256.Sum256(body)
		require.Equal(t, want, hex.EncodeToString(hash[:]))
	}
	require.NotEmpty(t, fixture.Cases)
	for _, tc := range fixture.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec(tc.IndexSQL)
			require.NoError(t, err)
			for _, index := range tc.AdditionalIndexes {
				_, err = db.Exec(index)
				require.NoError(t, err)
			}
			if tc.ApplicationTriggerSQL != "" {
				_, err = db.Exec(tc.ApplicationTriggerSQL)
				require.NoError(t, err)
			}
			effects := make([]oversync.BundleRow, 0, len(tc.Effects))
			for _, value := range tc.Effects {
				effects = append(effects, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": value["id"]}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, value)})
			}
			setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}})
			_, err = c.PullToStable(context.Background())
			expected := tc.Effects
			if tc.ExpectBlockedReason != "" {
				var blocked *FinalStateApplyBlockedError
				require.ErrorAs(t, err, &blocked)
				require.Equal(t, tc.ExpectBlockedReason, blocked.Reason)
				require.NotEmpty(t, blocked.Table)
				expected = fixture.Initial
				requireLastBundleSeqSeen(t, c, context.Background(), 1)
			} else {
				require.NoError(t, err)
				requireLastBundleSeqSeen(t, c, context.Background(), 2)
			}
			for _, value := range expected {
				var name string
				var parent any
				require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id=?`, value["id"]).Scan(&name, &parent))
				require.Equal(t, value["name"], name)
				require.Equal(t, value["parent_id"], parent)
			}
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes`).Scan(&count))
			require.Equal(t, 4, count)
		})
	}
}
