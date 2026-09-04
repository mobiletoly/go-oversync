package oversqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestA3Recovery_FirstDiscoveredInPull(t *testing.T) {
	c, db, path := qualificationDurableClient(t)
	_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; DELETE FROM nodes WHERE id='Q'; UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE table_name='nodes' AND key_json='{"id":"Q"}'; UPDATE _sync_apply_state SET apply_mode=0`)
	require.NoError(t, err)
	before := qualificationState(t, db, "nodes")
	response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1,
		oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("R"), Op: oversync.OpInsert, RowVersion: 2,
			Payload: mustJSONPayload(t, map[string]any{"id": "R", "name": "historical child", "parent_id": "Q"})})}}
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	var pending *FKReconciliationRequiredError
	require.ErrorAs(t, err, &pending)
	require.Equal(t, "historical_repair_required", pending.Detail)
	require.Equal(t, before, qualificationState(t, db, "nodes"))
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	unit := pending.Unit
	config := cloneCoordinatorClientConfig(c.config)
	config.GenerationID = c.schemaGenerationProof.ID
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	reopened, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	reopened.SetMaxOpenConns(1)
	owned, err := newClientFromDBWithOwnership(reopened, c.BaseURL, c.Token, config, true)
	require.NoError(t, err)
	defer owned.Close()
	defer reopened.Close()
	attachTestClient(t, owned, c.UserID, "bundle-device")
	setPullResponses(t, owned, response)
	_, err = owned.PullToStable(context.Background())
	require.ErrorAs(t, err, &pending)
	require.Equal(t, unit, pending.Unit)
	require.Equal(t, before, qualificationState(t, reopened, "nodes"))
}

func TestA3Recovery_PreparedOutbox(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='local parent' WHERE id='P'`)
	require.NoError(t, err)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	frozen := baselineText(t, db, `SELECT canonical_request_hash FROM _sync_outbox_bundle`)
	base := baselineText(t, db, `SELECT json_array(base_kind,base_payload,base_provenance,wire_payload,local_payload) FROM _sync_outbox_rows`)
	a3MarkSnapshot(t, c)
	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindSourceRecovery, Reason: string(SourceRecoveryHistoryPruned), ReplacementSourceID: "a3-replacement"})
	s := stageUserSnapshotRowsForGate(t, c, "a3-prepared", 2, a3SnapshotRows(t, [][3]string{{"Q", "new clean Q", ""}, {"P", "server parent", ""}, {"C", "child", "P"}, {"D", "grandchild", "C"}}))
	_, err = db.Exec(`CREATE TRIGGER damage_q AFTER UPDATE ON nodes WHEN NEW.id='P' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='Q'; END`)
	require.NoError(t, err)
	require.NoError(t, c.applyStagedSnapshotLocked(context.Background(), s, snapshotApplyOptions{PreserveOutbox: true,
		RotateSource: true, NewSourceID: "a3-replacement", ReplacementReason: string(SourceRecoveryHistoryPruned), ClearSourceRecovery: true, RequireFreshRotatedSource: true}))
	require.Equal(t, "local parent", baselineText(t, db, `SELECT name FROM nodes WHERE id='P'`))
	require.Equal(t, "new clean Q", baselineText(t, db, `SELECT name FROM nodes WHERE id='Q'`))
	require.Equal(t, "server parent", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{"id":"P"}'`))
	require.Equal(t, frozen, baselineText(t, db, `SELECT canonical_request_hash FROM _sync_outbox_bundle`))
	require.Equal(t, "a3-replacement", baselineText(t, db, `SELECT source_id FROM _sync_outbox_bundle`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT source_bundle_id FROM _sync_outbox_bundle`))
	require.Equal(t, base, baselineText(t, db, `SELECT json_array(base_kind,base_payload,base_provenance,wire_payload,local_payload) FROM _sync_outbox_rows`))
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
}
