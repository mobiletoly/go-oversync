package oversqlite

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestA3ProgressFinalization_OperationOwnership(t *testing.T) {
	for _, operation := range []string{"pull", "rebuild"} {
		t.Run(operation, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			failure := &SyncWorkspaceLimitError{Unit: "local admission", Dimension: "bytes", Limit: 7, Used: 6, Reserved: 2}
			reserved := make(chan *sql.Conn, 1)
			// Refuse locally at the request boundary, then exhaust the single-connection
			// pool so the final progress read has an observable acquisition barrier.
			c.Token = func(ctx context.Context) (string, error) {
				conn, err := db.Conn(ctx)
				if err != nil {
					return "", err
				}
				_, err = conn.ExecContext(ctx, `INSERT INTO users VALUES('late','Late','late@example.com')`)
				reserved <- conn
				if err != nil {
					return "", err
				}
				return "", failure
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
				t.Fatalf("operation exited before request refusal: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			defer conn.Close()
			require.Eventually(t, func() bool { return db.Stats().WaitCount > waits }, 3*time.Second, time.Millisecond,
				"final progress must be waiting for the reserved connection")
			unlocked := c.writeMu.TryLock()
			if unlocked {
				c.writeMu.Unlock()
			}
			require.False(t, unlocked, "operation ownership must cover final progress capture")
			_, err := c.PushPending(ctx)
			var busy *SyncOperationInProgressError
			require.ErrorAs(t, err, &busy)
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			select {
			case err := <-closed:
				t.Fatalf("Close completed during progress capture: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			require.Zero(t, atomic.LoadUint32(&c.closed), "Close must retain the pool until finalization finishes")
			require.NoError(t, conn.Close())
			select {
			case err := <-result:
				var actual *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &actual)
				require.Same(t, failure, actual)
				require.Equal(t, SyncProgress{DirtyRows: 1}, actual.Progress)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, <-closed)
		})
	}
}
