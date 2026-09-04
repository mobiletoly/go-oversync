package oversqlite

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestPendingConflict_ProcessHelper(t *testing.T) {
	path := os.Getenv("M6_CONFLICT_FILE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	c, err := newTestClient(t, db, "http://example.invalid", tokenProviderForTests, cfg)
	require.NoError(t, err)
	stop := func() int {
		_ = os.WriteFile(os.Getenv("M6_CONFLICT_MARKER"), []byte("reached"), 0600)
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		return 0
	}
	if os.Getenv("M6_CONFLICT_BOUNDARY") == "before_commit" {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		require.NoError(t, conn.Raw(func(driverConn any) error {
			return driverConn.(*sqlite3.SQLiteConn).RegisterFunc("m6_stop", stop, false)
		}))
		require.NoError(t, conn.Close())
		_, err = db.Exec(`CREATE TEMP TRIGGER m6_kill BEFORE UPDATE ON _sync_conflict_state WHEN OLD.pending<>'' AND NEW.pending='' BEGIN SELECT m6_stop(); END`)
		require.NoError(t, err)
	}
	pending, err := c.PendingConflict(ctx)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.NoError(t, c.ResolveConflict(ctx, *pending, KeepLocal{}))
	stop()
	t.Fatal("process did not stop")
}

func TestPendingConflict_ProcessDeathKeepsAtomicDecision(t *testing.T) {
	for _, boundary := range []string{"before_commit", "after_commit"} {
		t.Run(boundary, func(t *testing.T) {
			c, id := pendingConflictFixture(t)
			ctx := context.Background()
			p, err := c.PendingConflict(ctx)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "conflict.sqlite")
			_, err = c.db.Exec(`VACUUM INTO ?`, path)
			require.NoError(t, err)
			marker := filepath.Join(t.TempDir(), "marker")
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			command := exec.CommandContext(bounded, os.Args[0], "-test.run=^TestPendingConflict_ProcessHelper$")
			command.Env = append(os.Environ(), "M6_CONFLICT_FILE="+path, "M6_CONFLICT_MARKER="+marker, "M6_CONFLICT_BOUNDARY="+boundary)
			output, err := command.CombinedOutput()
			require.Error(t, err, string(output))
			require.NoError(t, bounded.Err())
			_, err = os.Stat(marker)
			require.NoError(t, err, string(output))
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			reopened, err := newTestClient(t, db, "http://example.invalid", tokenProviderForTests, c.config)
			require.NoError(t, err)
			defer reopened.Close()
			pending, err := reopened.PendingConflict(ctx)
			require.NoError(t, err)
			if boundary == "before_commit" {
				require.NotNil(t, pending)
				require.Equal(t, p.ID, pending.ID)
				require.Equal(t, outboxStatePrepared, requireOutboxBundle(t, db).State)
			} else {
				require.Nil(t, pending)
				require.Equal(t, outboxStateNone, requireOutboxBundle(t, db).State)
				var base int64
				require.NoError(t, db.QueryRow(`SELECT base_row_version FROM _sync_dirty_rows`).Scan(&base))
				require.EqualValues(t, 2, base)
			}
			name, _, _ := loadUserForKey(t, db, id)
			require.Equal(t, "Offline", name)
		})
	}
}
