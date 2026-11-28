package oversqlite

import (
	"context"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestA3Pending_PublicResults(t *testing.T) {
	for _, detail := range []string{"dependency_unavailable", "relationship_unknown", "historical_repair_required"} {
		t.Run(detail, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			parent := "missing"
			if detail == "relationship_unknown" {
				parent = "Q"
				_, err := db.Exec(`UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{"id":"Q"}'`)
				require.NoError(t, err)
			}
			if detail == "historical_repair_required" {
				parent = "Q"
				_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; DELETE FROM nodes WHERE id='Q'; UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{"id":"Q"}'; UPDATE _sync_apply_state SET apply_mode=0`)
				require.NoError(t, err)
			}
			before := qualificationState(t, db, "nodes")
			response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("R"), Op: oversync.OpInsert, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": "R", "name": "pending", "parent_id": parent})})}}
			setPullResponses(t, c, response)
			_, err := c.PullToStable(context.Background())
			var pending *FKReconciliationRequiredError
			require.ErrorAs(t, err, &pending)
			require.Equal(t, detail, pending.Detail)
			require.Len(t, pending.Unit, 64)
			require.Len(t, pending.Sample, 1)
			require.Equal(t, mustBundleKey("R"), pending.Sample[0].Key)
			require.Equal(t, SyncProgress{}, pending.Progress)
			require.Equal(t, before, qualificationState(t, db, "nodes"))
			unit := pending.Unit
			setPullResponses(t, c, response)
			_, err = c.Sync(context.Background())
			require.ErrorAs(t, err, &pending)
			setPullResponses(t, c, response)
			_, err = c.SyncThenDetach(context.Background())
			require.ErrorAs(t, err, &pending)
			require.Equal(t, before, qualificationState(t, db, "nodes"))
			setPullResponses(t, c, response)
			_, err = c.PullToStable(context.Background())
			require.ErrorAs(t, err, &pending)
			require.Equal(t, unit, pending.Unit)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = c.PullToStable(ctx)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
