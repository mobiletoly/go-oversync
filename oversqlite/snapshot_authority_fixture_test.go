package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Opening fixtures select a generation before they publish the client owner.
// The fixture seals its full declared S under that generation and request tuple.
func snapshotFixtureResponseForGeneration(t *testing.T, generation oversync.SchemaGenerationCapabilities, r *http.Request, id string, boundary int64, rows []oversync.SnapshotRow) *http.Response {
	t.Helper()
	var request oversync.SnapshotSessionCreateRequest
	raw, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &request))
	}
	source := r.Header.Get(oversync.SourceIDHeader)
	if request.SourceReplacement != nil {
		source = request.SourceReplacement.NewSourceID
	}
	return jsonResponse(sealedTestSnapshotForGeneration(t, generation, source, id, boundary, rows, request.Recovery))
}

// SQLite may discard a connection after SQLITE_FULL. A file owns the durable
// rollback vector independently of the lifetime of that poisoned connection.
func newFileSnapshotFailureClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "snapshot-failure.sqlite")+"?_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(usersTestDDL)
	require.NoError(t, err)
	config := testConfigWithLocalContract(t, db, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	c, err := newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	attachTestClient(t, c, "bundle-user", "bundle-device")
	require.NoError(t, db.PingContext(context.Background()))
	return c, db
}
