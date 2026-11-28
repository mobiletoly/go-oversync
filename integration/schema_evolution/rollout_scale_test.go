package schemaevolution

import (
	"os"
	"testing"
)

func TestRolloutScale_100000Rows100Scopes(t *testing.T) {
	if os.Getenv("ROLLOUT_SCALE") != "1" {
		t.Skip("separate required rollout scale lane; set ROLLOUT_SCALE=1")
	}
	runRolloutTableJourney(t, 100, 1000, 4096, true)
}

// New destructive/fleet/archive scenarios own a separate accepted lane so the
// established root aggregate retains its fixed deadline. Selected lanes must
// set this explicitly and require PASS for every declared scenario.
func requireRolloutQualification(t *testing.T) {
	t.Helper()
	if os.Getenv("ROLLOUT_QUALIFY") != "1" {
		t.Skip("required separate rollout lane: ROLLOUT_QUALIFY=1")
	}
}
