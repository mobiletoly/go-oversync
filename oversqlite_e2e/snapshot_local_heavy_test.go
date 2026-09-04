package oversqlite_e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const (
	oversqliteLargeRestoreRowsEnv     = "OVERSQLITE_E2E_LARGE_RESTORE_ROWS"
	oversqliteLargeRestoreRowBytesEnv = "OVERSQLITE_E2E_LARGE_RESTORE_ROW_BYTES"
)

func TestLocalHeavy_EndToEndLargeMultiChunkRowByteSnapshotRestore(t *testing.T) {
	rawRows := strings.TrimSpace(os.Getenv(oversqliteLargeRestoreRowsEnv))
	if rawRows == "" {
		t.Skipf("set %s to 10000 or 100000 and %s to 256 or 1024 for the local-heavy real-server restore", oversqliteLargeRestoreRowsEnv, oversqliteLargeRestoreRowBytesEnv)
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("GITHUB_ACTIONS")), "true") {
		t.Fatalf("local-heavy real-server restore is forbidden when GITHUB_ACTIONS=true; refusing before database, server, or fixture setup")
	}
	rowCount, err := strconv.Atoi(rawRows)
	require.NoError(t, err)
	require.Contains(t, []int{10_000, 100_000}, rowCount)
	rowBytes, err := strconv.Atoi(strings.TrimSpace(os.Getenv(oversqliteLargeRestoreRowBytesEnv)))
	require.NoError(t, err)
	require.Contains(t, []int{256, 1024}, rowBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	schema := "e2e_phase2_large_restore_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-phase2-large-" + uuid.NewString()
	config := defaultOversqliteConfig(schema, syncTables("users"))
	config.SnapshotChunkRows = 250
	config.SnapshotChunkBytes = 4 << 20
	follower, followerDB := newSQLiteClient(t, server, userID, "phase2-follower", config, usersDDL)

	actor := oversync.Actor{UserID: userID, SourceID: "phase2-server-writer"}
	insertStarted := time.Now()
	err = server.SyncService.WithinSyncBundle(ctx, actor, oversync.BundleSource{
		SourceID: actor.SourceID, SourceBundleID: 1,
	}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		name := strings.Repeat("n", rowBytes-160)
		for ordinal := 1; ordinal <= rowCount; ordinal++ {
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", ordinal)
			if _, execErr := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s.users(id, name, email)
				VALUES ($1, $2, $3)
			`, pgx.Identifier{schema}.Sanitize()), id, name, id+"@example.com"); execErr != nil {
				return execErr
			}
		}
		return nil
	})
	require.NoError(t, err)
	insertElapsed := time.Since(insertStarted)

	follower.ResetSnapshotTransferDiagnostics()
	restoreStarted := time.Now()
	report, err := follower.Rebuild(ctx)
	require.NoError(t, err)
	restoreElapsed := time.Since(restoreStarted)
	require.NotNil(t, report.Restore)
	require.Equal(t, int64(rowCount), report.Restore.RowCount)

	var restoredRows int
	require.NoError(t, followerDB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&restoredRows))
	require.Equal(t, rowCount, restoredRows)
	stats := follower.SnapshotTransferDiagnostics()
	require.Greater(t, stats.ChunksFetched, int64(1))
	require.LessOrEqual(t, stats.MaxChunkRows, int64(config.SnapshotChunkRows))
	require.LessOrEqual(t, stats.MaxChunkWireBytes, config.SnapshotChunkBytes)
	require.Equal(t, int64(1), stats.MaxLiveStagedApplyRows)
	require.Equal(t, int64(1), stats.MaxAppliedInMemoryRows)
	t.Logf(
		"phase2_real_server_restore rows=%d approximate_row_bytes=%d insert=%s restore=%s sessions=%d chunks=%d max_chunk_rows=%d max_chunk_wire_bytes=%d max_chunk_decoded_body_bytes=%d max_live_staged_apply_rows=%d max_live_staged_apply_text_bytes=%d max_applied_in_memory_rows=%d",
		rowCount, rowBytes, insertElapsed, restoreElapsed, stats.SessionsCreated, stats.ChunksFetched,
		stats.MaxChunkRows, stats.MaxChunkWireBytes, stats.MaxChunkDecodedBodyBytes,
		stats.MaxLiveStagedApplyRows, stats.MaxLiveStagedApplyTextBytes, stats.MaxAppliedInMemoryRows,
	)
}
