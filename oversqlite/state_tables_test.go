package oversqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionTokenState_CorruptedInitializationIDsFailClosedOnLoad(t *testing.T) {
	ctx := context.Background()
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL)`)

	_, err := db.Exec(`UPDATE _sync_attachment_state SET pending_initialization_id = ' init-corrupt ' WHERE singleton_key = 1`)
	require.NoError(t, err)
	_, err = loadAttachmentState(ctx, db)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "init-corrupt")

	_, err = db.Exec(`UPDATE _sync_attachment_state SET pending_initialization_id = '' WHERE singleton_key = 1`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_outbox_bundle SET initialization_id = 'init-corrupt' WHERE singleton_key = 1`)
	require.NoError(t, err)
	_, err = loadOutboxBundle(ctx, db)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "init-corrupt")
}

func TestSessionTokenState_RejectsInvalidInitializationIDsBeforePersist(t *testing.T) {
	ctx := context.Background()
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL)`)

	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	attachment.PendingInitializationID = " init-corrupt "
	err = persistAttachmentState(ctx, db, attachment)
	require.Error(t, err)

	outbox, err := loadOutboxBundle(ctx, db)
	require.NoError(t, err)
	outbox.InitializationID = "init-corrupt"
	err = persistOutboxBundle(ctx, db, outbox)
	require.Error(t, err)
}
