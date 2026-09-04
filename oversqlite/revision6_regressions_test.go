package oversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type detachGateDriver struct{ gate *detachWriterGate }
type detachWriterGate struct {
	armed            atomic.Bool
	entered, release chan struct{}
}

func (g *detachWriterGate) wait() {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
}

type detachGateConn struct {
	*sqlite3.SQLiteConn
	gate *detachWriterGate
}

func (d detachGateDriver) Open(name string) (driver.Conn, error) {
	c, err := (&sqlite3.SQLiteDriver{}).Open(name)
	if err != nil {
		return nil, err
	}
	return &detachGateConn{c.(*sqlite3.SQLiteConn), d.gate}, nil
}
func (c *detachGateConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if strings.EqualFold(query, "BEGIN IMMEDIATE") {
		c.gate.wait()
	}
	return c.SQLiteConn.ExecContext(ctx, query, args)
}
func (c *detachGateConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.gate.wait()
	return c.SQLiteConn.BeginTx(ctx, opts)
}

func TestDetach_ApplicationEditBeforeWriterReservation(t *testing.T) {
	for _, op := range []string{"UPDATE users SET name='Newer' WHERE id='row'", "DELETE FROM users WHERE id='row'"} {
		t.Run(op, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			seedSyncedUserRow(t, c, db, "row", "Original", 1)
			file := filepath.Join(t.TempDir(), "detach.sqlite")
			_, err := db.Exec(`VACUUM INTO ?`, file)
			require.NoError(t, err)
			gate := &detachWriterGate{entered: make(chan struct{}), release: make(chan struct{})}
			name := "detach_gate_" + uuid.NewString()
			sql.Register(name, detachGateDriver{gate})
			local, err := sql.Open(name, file+"?_busy_timeout=5000")
			require.NoError(t, err)
			defer local.Close()
			local.SetMaxOpenConns(1)
			client, err := newAdmittedFixtureClient(local, "http://example.invalid", tokenProviderForTests, c.config)
			require.NoError(t, err)
			defer client.Close()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			defer release()
			attachTestClient(t, client, "bundle-user", "bundle-device")
			app, err := sql.Open("sqlite3", file+"?_busy_timeout=5000")
			require.NoError(t, err)
			defer app.Close()
			gate.armed.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan DetachResult, 1)
			errs := make(chan error, 1)
			go func() { r, e := client.Detach(ctx); result <- r; errs <- e }()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("detach did not reach writer reservation")
			}
			// Commit application SQL after Detach starts and immediately before it reserves
			// the writer. The old check-before-BEGIN implementation erased this edit.
			_, err = app.ExecContext(ctx, op)
			require.NoError(t, err)
			before := vectorState(t, &Client{db: app})
			release()
			require.NoError(t, <-errs)
			require.Equal(t, DetachOutcomeBlockedUnsyncedData, (<-result).Outcome)
			require.Equal(t, before, vectorState(t, client))
		})
	}
}

func TestAcceptServer_RejectsSelectedRowTriggerDivergence(t *testing.T) {
	for _, body := range []string{"UPDATE users SET name='Derived' WHERE id=NEW.id;", "DELETE FROM users WHERE id=NEW.id;"} {
		t.Run(body, func(t *testing.T) {
			c, _ := pendingConflictFixture(t)
			_, err := c.db.Exec(`CREATE TRIGGER alter_accepted AFTER UPDATE ON users WHEN NEW.name='Original' BEGIN ` + body + ` END`)
			require.NoError(t, err)
			p, err := c.PendingConflict(context.Background())
			require.NoError(t, err)
			before := vectorState(t, c)
			require.ErrorContains(t, c.ResolveConflict(context.Background(), *p, AcceptServer{}), "AcceptServer final row")
			require.Equal(t, before, vectorState(t, c))
		})
	}
}
