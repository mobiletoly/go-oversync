package oversqlite_e2e

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireLastBundleSeqSeen(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var lastBundleSeq int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&lastBundleSeq))
	return lastBundleSeq
}

func requireNextSourceBundleID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var nextSourceBundleID int64
	require.NoError(t, db.QueryRow(`
		SELECT COALESCE(s.next_source_bundle_id, 1)
		FROM _sync_attachment_state AS a
		LEFT JOIN _sync_source_state AS s
			ON s.source_id = a.current_source_id
		WHERE a.singleton_key = 1
	`).Scan(&nextSourceBundleID))
	return nextSourceBundleID
}

func requirePendingInitializationID(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pendingInitializationID string
	require.NoError(t, db.QueryRow(`SELECT pending_initialization_id FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&pendingInitializationID))
	return pendingInitializationID
}
