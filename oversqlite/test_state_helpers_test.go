package oversqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireBundleState(t *testing.T, db *sql.DB) (string, int64, int64) {
	t.Helper()
	var sourceID string
	var nextSourceBundleID int64
	var lastBundleSeqSeen int64
	require.NoError(t, db.QueryRow(`
		SELECT a.current_source_id, COALESCE(s.next_source_bundle_id, 1), a.last_bundle_seq_seen
		FROM _sync_attachment_state AS a
		LEFT JOIN _sync_source_state AS s
			ON s.source_id = a.current_source_id
		WHERE a.singleton_key = 1
	`).Scan(&sourceID, &nextSourceBundleID, &lastBundleSeqSeen))
	return sourceID, nextSourceBundleID, lastBundleSeqSeen
}

func requirePendingInitializationID(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pendingInitializationID string
	require.NoError(t, db.QueryRow(`SELECT pending_initialization_id FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&pendingInitializationID))
	return pendingInitializationID
}

func requireApplyMode(t *testing.T, db *sql.DB) int {
	t.Helper()
	var applyMode int
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1`).Scan(&applyMode))
	return applyMode
}

func requireOperationState(t *testing.T, db *sql.DB) (string, string, string, int64, int64) {
	t.Helper()
	var kind, targetUserID, stagedSnapshotID string
	var bundleSeq, rowCount int64
	require.NoError(t, db.QueryRow(`
		SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
		FROM _sync_operation_state
		WHERE singleton_key = 1
	`).Scan(&kind, &targetUserID, &stagedSnapshotID, &bundleSeq, &rowCount))
	return kind, targetUserID, stagedSnapshotID, bundleSeq, rowCount
}

func setCurrentSourceBundleState(t *testing.T, db *sql.DB, nextSourceBundleID, lastBundleSeqSeen int64) {
	t.Helper()
	ctx := context.Background()
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.NotEmpty(t, attachment.CurrentSourceID)
	require.NoError(t, updateSourceNextBundleID(ctx, db, attachment.CurrentSourceID, nextSourceBundleID))
	attachment.LastBundleSeqSeen = lastBundleSeqSeen
	require.NoError(t, persistAttachmentState(ctx, db, attachment))
}

func setPendingInitializationID(t *testing.T, db *sql.DB, pendingInitializationID string) {
	t.Helper()
	ctx := context.Background()
	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	attachment.PendingInitializationID = pendingInitializationID
	require.NoError(t, persistAttachmentState(ctx, db, attachment))
}

func setApplyModeForTest(t *testing.T, db *sql.DB, enabled bool) {
	t.Helper()
	require.NoError(t, setApplyMode(context.Background(), db, enabled))
}

func setOperationStateForTest(t *testing.T, db *sql.DB, rec *operationStateRecord) {
	t.Helper()
	if rec == nil {
		rec = &operationStateRecord{Kind: operationKindNone}
	}
	require.NoError(t, persistOperationState(context.Background(), db, rec))
}
