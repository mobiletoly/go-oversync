package oversqlite

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializeRowInTx_TypedRows_PreservesNullBlobAndTimestampText(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			note TEXT NULL,
			count_value INTEGER NULL,
			enabled_flag INTEGER NOT NULL,
			rating REAL NULL,
			data BLOB NULL,
			created_at TEXT NULL
		)
	`)

	_, err := db.Exec(`
		INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "typed-1", "Ada", nil, nil, 1, 6.57111473696007, nil, "2026-03-24T10:42:11-08:00")
	require.NoError(t, err)

	tx := mustBeginTx(t, db)
	defer tx.Rollback()

	payload, err := client.serializeRowInTx(ctx, tx, "typed_rows", "typed-1")
	require.NoError(t, err)
	require.JSONEq(t, `{
		"id":"typed-1",
		"name":"Ada",
		"note":null,
		"count_value":null,
		"enabled_flag":1,
		"rating":6.57111473696007,
		"data":null,
		"created_at":"2026-03-24T10:42:11-08:00"
	}`, string(payload))

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Contains(t, decoded, "data")
	require.Nil(t, decoded["data"])
	require.Equal(t, "2026-03-24T10:42:11-08:00", decoded["created_at"])
}

func TestProcessPayloadForUpload_TypedRows_Base64EncodesBlobAndKeepsNulls(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			note TEXT NULL,
			count_value INTEGER NULL,
			enabled_flag INTEGER NOT NULL,
			rating REAL NULL,
			data BLOB NULL,
			created_at TEXT NULL
		)
	`)

	rawPayload := `{
		"id":"typed-blob",
		"name":"Blob Row",
		"note":null,
		"count_value":null,
		"enabled_flag":0,
		"rating":1.25,
		"data":"001122ff",
		"created_at":"2026-03-24T18:42:11Z"
	}`
	out, err := client.processPayloadForUpload("typed_rows", rawPayload)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, "ABEi/w==", decoded["data"])
	require.Nil(t, decoded["note"])
	require.Nil(t, decoded["count_value"])
	require.Equal(t, "2026-03-24T18:42:11Z", decoded["created_at"])

	gotBytes, err := base64.StdEncoding.DecodeString(decoded["data"].(string))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x11, 0x22, 0xff}, gotBytes)
}

func TestDirtyRowCapture_TypedRows_NullBlobRemainsJsonNull(t *testing.T) {
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			note TEXT NULL,
			count_value INTEGER NULL,
			enabled_flag INTEGER NOT NULL,
			rating REAL NULL,
			data BLOB NULL,
			created_at TEXT NULL
		)
	`)

	_, err := db.Exec(`
		INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "typed-null-blob", "Ada", nil, nil, 1, 1.25, nil, nil)
	require.NoError(t, err)

	var raw string
	require.NoError(t, db.QueryRow(`
		SELECT payload
		FROM _sync_dirty_rows
		WHERE table_name = 'typed_rows' AND key_json = ?
	`, rowKeyJSON("typed-null-blob")).Scan(&raw))

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &payload))
	require.Contains(t, payload, "data")
	require.Nil(t, payload["data"])
}
