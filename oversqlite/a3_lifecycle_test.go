package oversqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func a3SnapshotRows(t *testing.T, values [][3]string) []oversync.SnapshotRow {
	t.Helper()
	rows := make([]oversync.SnapshotRow, 0, len(values))
	for _, value := range values {
		var parent any
		if value[2] != "" {
			parent = value[2]
		}
		rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "nodes", Key: mustBundleKey(value[0]), RowVersion: 2,
			Payload: mustJSONPayload(t, map[string]any{"id": value[0], "name": value[1], "parent_id": parent})})
	}
	return rows
}

func a3MarkSnapshot(t *testing.T, c *Client) {
	t.Helper()
	_, err := c.db.Exec(`UPDATE _sync_attachment_state SET rebuild_required=1`)
	require.NoError(t, err)
	c.syncWorkUsed = 0
}

func TestA3Lifecycle_AdmissionAndParents(t *testing.T) {
	for name, values := range map[string][][3]string{
		"new-parent-child-first": {{"N", "new child", "M"}, {"M", "new parent", ""}},
		"self":                   {{"S", "self", "S"}},
		"cycle":                  {{"A", "a", "B"}, {"B", "b", "A"}},
		"reparent-old-new-null":  {{"C", "child", "Q"}, {"Q", "other", ""}, {"D", "grandchild", ""}},
	} {
		t.Run(name, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			a3MarkSnapshot(t, c)
			s := stageUserSnapshotRowsForGate(t, c, "a3-"+name, 2, a3SnapshotRows(t, values))
			require.NoError(t, c.applyStagedSnapshotLocked(context.Background(), s, snapshotApplyOptions{}))
			require.EqualValues(t, len(values), countA3(t, db, `SELECT count(*) FROM nodes`))
			for _, value := range values {
				var actualName string
				var parent sql.NullString
				require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id=?`, value[0]).Scan(&actualName, &parent))
				require.Equal(t, value[1], actualName)
				require.Equal(t, value[2], parent.String)
				require.Equal(t, value[2] != "", parent.Valid)
			}
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
		})
	}
}

func TestA3Lifecycle_ActionsAndSurvivors(t *testing.T) {
	// Generation-visible SET DEFAULT remains rejected by existing admission.
	for _, action := range []string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL"} {
		t.Run(action, func(t *testing.T) {
			c, db := newA1SurvivorClientWithAction(t, action)
			a3MarkSnapshot(t, c)
			values := [][3]string{{"D", "final grandchild", "C"}, {"C", "final child", "Q"}, {"Q", "final parent", ""}}
			s := stageUserSnapshotRowsForGate(t, c, "a3-action", 2, a3SnapshotRows(t, values))
			_, err := db.Exec(`CREATE TRIGGER snapshot_rewrite AFTER INSERT ON nodes WHEN NEW.id='Q' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='D'; END`)
			require.NoError(t, err)
			before := qualificationState(t, db, "nodes")
			var blocked *FinalStateApplyBlockedError
			require.ErrorAs(t, c.applyStagedSnapshotLocked(context.Background(), s, snapshotApplyOptions{}), &blocked)
			require.Equal(t, "unsafe_trigger_or_dependency", blocked.Reason)
			require.Equal(t, before, qualificationState(t, db, "nodes"))
			_, err = db.Exec(`DROP TRIGGER snapshot_rewrite`)
			require.NoError(t, err)
			require.NoError(t, c.applyStagedSnapshotLocked(context.Background(), s, snapshotApplyOptions{}))
			require.Equal(t, "final grandchild", baselineText(t, db, `SELECT name FROM nodes WHERE id='D'`))
			require.Equal(t, "C", baselineText(t, db, `SELECT parent_id FROM nodes WHERE id='D'`))
			require.Equal(t, "Q", baselineText(t, db, `SELECT parent_id FROM nodes WHERE id='C'`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM nodes WHERE id='P'`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
		})
	}
}
