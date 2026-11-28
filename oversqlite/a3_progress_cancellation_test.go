package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestA3ProgressCancellation_ReadPhases(t *testing.T) {
	for _, phase := range []string{"connection", "query"} {
		for _, deadline := range []bool{false, true} {
			for _, workspace := range []bool{false, true} {
				t.Run(progressCancellationName(phase, deadline, workspace), func(t *testing.T) {
					c, db := newA1SurvivorClient(t)
					_, err := db.Exec(`UPDATE nodes SET name='retained intent' WHERE id='Q'`)
					require.NoError(t, err)
					before := qualificationState(t, db, "nodes")
					ctx, cancel, expected := progressCancellationContext(deadline)
					defer cancel()
					work := &SyncWorkLimitError{Unit: "a", Limit: 1, Used: 1, Progress: SyncProgress{DirtyRows: 77}}
					space := &SyncWorkspaceLimitError{Unit: "b", Dimension: "bytes", Limit: 7, Used: 6, Reserved: 2, Progress: work.Progress}
					var failure error = work
					if workspace {
						failure = space
					}
					conn, err := db.Conn(context.Background())
					require.NoError(t, err)
					defer conn.Close()
					release := make(chan struct{})
					var releaseOnce sync.Once
					releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
					defer releaseBarrier()
					entered := make(chan struct{})
					var armed atomic.Bool
					if phase == "query" {
						require.NoError(t, conn.Raw(func(raw any) error {
							raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(op int, table, _, _ string) int {
								if op == sqlite3.SQLITE_READ && table == "_sync_dirty_rows" && armed.CompareAndSwap(false, true) {
									close(entered)
									<-release
								}
								return sqlite3.SQLITE_OK
							})
							return nil
						}))
						require.NoError(t, conn.Close())
					}
					waits := db.Stats().WaitCount
					result := make(chan error, 1)
					go func() { result <- c.withSyncErrorProgress(ctx, failure) }()
					if phase == "connection" {
						require.Eventually(t, func() bool { return db.Stats().WaitCount > waits }, 500*time.Millisecond, time.Millisecond)
					} else {
						select {
						case <-entered:
						case <-ctx.Done():
							t.Fatal("progress query did not reach its barrier")
						}
					}
					if !deadline {
						cancel()
					}
					<-ctx.Done()
					releaseBarrier()
					actual := <-result
					if phase == "connection" {
						require.NoError(t, conn.Close())
					}
					progressCancellationClearAuthorizer(t, db)
					require.ErrorIs(t, actual, expected)
					require.False(t, errors.As(actual, &work), "resource result must not override cancellation")
					require.False(t, errors.As(actual, &space))
					require.EqualValues(t, 77, work.Progress.DirtyRows)
					require.EqualValues(t, 77, space.Progress.DirtyRows)
					require.Equal(t, before, qualificationState(t, db, "nodes"))
				})
			}
		}
	}
}

func progressCancellationClearAuthorizer(t *testing.T, db *sql.DB) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.Raw(func(raw any) error { raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(nil); return nil }))
}

func progressCancellationContext(deadline bool) (context.Context, context.CancelFunc, error) {
	if deadline {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		return ctx, cancel, context.DeadlineExceeded
	}
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, context.Canceled
}

func progressCancellationName(phase string, deadline, workspace bool) string {
	if deadline {
		phase += "/deadline"
	} else {
		phase += "/cancel"
	}
	if workspace {
		return phase + "/workspace"
	}
	return phase + "/work"
}

func TestA3ProgressCancellation_PublicExits(t *testing.T) {
	for _, operation := range []string{"pull", "rebuild"} {
		for _, deadline := range []bool{false, true} {
			t.Run(progressCancellationName(operation, deadline, true), func(t *testing.T) {
				c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
				ctx, cancel, expected := progressCancellationContext(deadline)
				defer cancel()
				reserved := make(chan *sql.Conn, 1)
				c.Token = func(ctx context.Context) (string, error) {
					conn, err := db.Conn(ctx)
					if err != nil {
						return "", err
					}
					reserved <- conn
					return "", &SyncWorkLimitError{Unit: "public finalization", Limit: 1, Used: 1}
				}
				waits := db.Stats().WaitCount
				result := make(chan error, 1)
				go func() {
					var err error
					if operation == "pull" {
						_, err = c.PullToStable(ctx)
					} else {
						_, err = c.Rebuild(ctx)
					}
					result <- err
				}()
				var conn *sql.Conn
				select {
				case conn = <-reserved:
				case err := <-result:
					t.Fatalf("operation returned before admission refusal: %v", err)
				case <-ctx.Done():
					t.Fatal("operation did not reach request barrier")
				}
				defer conn.Close()
				require.Eventually(t, func() bool { return db.Stats().WaitCount > waits }, 500*time.Millisecond, time.Millisecond)
				if !deadline {
					cancel()
				}
				<-ctx.Done()
				require.ErrorIs(t, <-result, expected)
				require.NoError(t, conn.Close())
				require.True(t, c.writeMu.TryLock(), "cancelled finalization must release operation ownership")
				c.writeMu.Unlock()
				require.NoError(t, c.Close())
			})
		}
	}
}
