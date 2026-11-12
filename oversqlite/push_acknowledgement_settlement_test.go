package oversqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Settling a push whose committed effects are exactly the unchanged uploaded rows only records the
// authoritative baselines: there is no business write to protect, so no dependency index is built.
func TestPushAcknowledgementSettlement_UnchangedRowIndexesNoRelationships(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	before := countA3(t, db, `SELECT row_version FROM _sync_row_state WHERE table_name='notes' AND key_json='{"id":"N0"}'`)
	_, err := db.Exec(`UPDATE notes SET name='edited note' WHERE id='N0'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)

	require.Equal(t, "", indexedRelationships(t, db), "an acknowledgement-only settlement needs no dependency index")
	require.Equal(t, "edited note", scalarString(t, db, `SELECT name FROM notes WHERE id='N0'`))
	require.Greater(t, countA3(t, db, `SELECT row_version FROM _sync_row_state WHERE table_name='notes' AND key_json='{"id":"N0"}'`), before)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
}

// An application trigger on an affected table may have changed other rows, so settlement keeps the
// full survivor path, as before.
func TestPushAcknowledgementSettlement_ApplicationTriggerKeepsTheFullPath(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	_, err := db.Exec(`CREATE TRIGGER application_note_audit AFTER UPDATE ON notes BEGIN SELECT 1; END`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE notes SET name='edited note' WHERE id='N0'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)

	require.Contains(t, indexedRelationships(t, db), "crews<-sailors", "a table with an application trigger keeps the full index")
	require.Equal(t, "edited note", scalarString(t, db, `SELECT name FROM notes WHERE id='N0'`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
}
