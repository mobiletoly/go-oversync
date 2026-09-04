package oversqlite_e2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// The client supplies table/key names and a generation ID only. The real server
// owns column contracts and its schema name; no test helper installs client codecs.
func TestContractDiscovery_RealOfflineSyncAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	schema := "discovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.BeforeBootstrap = func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			_, err := pool.Exec(ctx, "ALTER TABLE "+pgx.Identifier{schema, "typed_rows"}.Sanitize()+" ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb")
			return err
		}
		config.RegisteredTables = []oversync.RegisteredTable{{Schema: schema, Table: "typed_rows", SyncKeyColumns: []string{"id"}}}
		for _, table := range exampleserver.SchemaGenerationsForBusinessSchema(schema)[0].Tables {
			if table.Table == "typed_rows" {
				table.Columns = append(table.Columns, oversync.GenerationColumn{Name: "metadata", WireType: oversync.GenerationWireTypeJSON})
				config.SchemaGenerations = []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{table}}}
			}
		}
	})
	owner := "discovery-owner-" + uuid.NewString()
	filename := filepath.Join(t.TempDir(), "offline.sqlite")
	ddl := strings.Replace(typedRowsDDL, "created_at TEXT NULL", "created_at TEXT NULL, metadata TEXT NOT NULL DEFAULT '{}'", 1)
	clientConfig := &oversqlite.Config{
		GenerationID: 1, DatabaseID: "offline-db", ServiceID: "discovery-app", LocalVersion: 1,
		Tables: []oversqlite.SyncTable{{TableName: "typed_rows", SyncKeyColumns: []string{"id"}}}, UploadLimit: 100, DownloadLimit: 100,
	}
	token := func(context.Context) (string, error) { return server.GenerateToken(owner, time.Hour) }
	client := openFreshSQLiteClient(t, filename, server.URL(), token, clientConfig, ddl)
	db := client.Database()
	id := uuid.NewString()
	_, err := db.Exec(`INSERT INTO typed_rows(id,name,count_value,small_count,medium_count,exact_amount,enabled_flag,rating,float4_value,data) VALUES(?,?,?,?,?,?,?,?,?,?)`, id, "offline", int64(9007199254740993), 32767, 2147483647, "12345678901234567890.123456789", 1, 1.23456789012345, 1.23456789012345, []byte{0, 128, 255})
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE typed_rows SET metadata=? WHERE id=?`, ` { "z": 2, "a": 1 } `, id)
	require.NoError(t, err)
	status, err := client.SyncStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.ContractConfirmed)
	require.EqualValues(t, 1, status.Pending.PendingRowCount)
	require.NoError(t, client.Close())
	client = openReadySQLiteClient(t, filename, server.URL(), token, clientConfig)
	db = client.Database()
	result, err := client.Attach(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, result.Status)
	var metadata string
	require.NoError(t, db.QueryRow(`SELECT metadata FROM typed_rows WHERE id=?`, id).Scan(&metadata))
	require.Equal(t, ` { "z": 2, "a": 1 } `, metadata, "admission must not rewrite application JSON")
	report, err := client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, report.PushOutcome)
	require.False(t, report.Status.Pending.HasPendingSyncData)
	var integer int64
	var decimal string
	var flag int
	var blob []byte
	var real, rounded float64
	require.NoError(t, db.QueryRow(`SELECT count_value,exact_amount,enabled_flag,data,rating,float4_value FROM typed_rows WHERE id=?`, id).Scan(&integer, &decimal, &flag, &blob, &real, &rounded))
	require.Equal(t, int64(9007199254740993), integer)
	require.Equal(t, "12345678901234567890.123456789", decimal)
	require.Equal(t, 1, flag)
	require.Equal(t, []byte{0, 128, 255}, blob)
	require.Equal(t, 1.23456789012345, real)
	require.Equal(t, float64(float32(1.23456789012345)), rounded)
	require.NoError(t, db.QueryRow(`SELECT metadata FROM typed_rows WHERE id=?`, id).Scan(&metadata))
	require.Equal(t, `{"a":1,"z":2}`, metadata, "authoritative output must contain canonical JSON")
	require.NoError(t, client.Close())
	client = openReadySQLiteClient(t, filename, server.URL(), token, clientConfig)
	db = client.Database()
	status, err = client.SyncStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.ContractConfirmed)
	require.False(t, status.Pending.HasPendingSyncData)
	_, err = client.Attach(ctx, owner)
	require.NoError(t, err)
	report, err = client.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeNoChange, report.PushOutcome)

	peer := openFreshSQLiteClient(t, filepath.Join(t.TempDir(), "peer.sqlite"), server.URL(), token, clientConfig, ddl)
	peerDB := peer.Database()
	_, err = peer.Attach(ctx, owner)
	require.NoError(t, err)
	var peerName string
	require.NoError(t, peerDB.QueryRow(`SELECT name FROM typed_rows WHERE id=?`, id).Scan(&peerName))
	require.Equal(t, "offline", peerName)
	_, err = db.Exec(`UPDATE typed_rows SET name='from peer' WHERE id=?`, id)
	require.NoError(t, err)
	_, err = client.PushPending(ctx)
	require.NoError(t, err)
	pulled, err := peer.PullToStable(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedIncremental, pulled.Outcome)
	require.NoError(t, peerDB.QueryRow(`SELECT name FROM typed_rows WHERE id=?`, id).Scan(&peerName))
	require.Equal(t, "from peer", peerName)
}
