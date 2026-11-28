package oversqlite

import (
	"context"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestAttachmentEpoch_ReopenAndRepeatedBinding(t *testing.T) {
	ctx := context.Background()
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})
	before, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Positive(t, before.AttachmentEpoch)
	_, err = client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	attached, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, before.AttachmentEpoch+1, attached.AttachmentEpoch)
	mustOpen(t, client, ctx)
	reopened, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, attached.AttachmentEpoch, reopened.AttachmentEpoch)
	// The same binding persisted by ordinary settlement/checkpoint work is not a new incarnation.
	reopened.LastBundleSeqSeen++
	require.NoError(t, persistAttachmentState(ctx, db, reopened))
	afterCheckpoint, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, attached.AttachmentEpoch, afterCheckpoint.AttachmentEpoch)
	mustDetach(t, client, ctx)
	detached, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, attached.AttachmentEpoch+1, detached.AttachmentEpoch)
	_, err = client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	reattached, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.Equal(t, attached.CurrentSourceID, reattached.CurrentSourceID)
	require.Equal(t, attached.AttachedUserID, reattached.AttachedUserID)
	require.Equal(t, detached.AttachmentEpoch+1, reattached.AttachmentEpoch)
}
func TestAttachmentEpoch_RollbackAndOverflow(t *testing.T) {
	ctx := context.Background()
	_, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})
	before := qualificationState(t, db, "users")
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, advanceAttachmentEpoch(ctx, tx))
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, qualificationState(t, db, "users"))
	_, err = db.Exec(`UPDATE _sync_attachment_state SET attachment_epoch=?`, int64(math.MaxInt64))
	require.NoError(t, err)
	before = qualificationState(t, db, "users")
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.Error(t, advanceAttachmentEpoch(ctx, tx))
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, qualificationState(t, db, "users"))
	state, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	state.CurrentSourceID = "another-source"
	require.Error(t, persistAttachmentState(ctx, db, state))
	require.Equal(t, before, qualificationState(t, db, "users"))
}
