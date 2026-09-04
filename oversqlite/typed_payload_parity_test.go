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
			id TEXT PRIMARY KEY NOT NULL,
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
  "id": {
    "storage": "text",
    "value": "typed-1"
  },
  "name": {
    "storage": "text",
    "value": "Ada"
  },
  "note": {
    "storage": "null",
    "value": null
  },
  "count_value": {
    "storage": "null",
    "value": null
  },
  "enabled_flag": {
    "storage": "integer",
    "value": "1"
  },
  "rating": {
    "storage": "real",
    "value": "6.571114736960070424"
  },
  "data": {
    "storage": "null",
    "value": null
  },
  "created_at": {
    "storage": "text",
    "value": "2026-03-24T10:42:11-08:00"
  }
}`, string(payload))

	decoded := decodeDirtyPayload(t, string(payload))
	require.Contains(t, decoded, "data")
	require.Nil(t, decoded["data"])
	require.Equal(t, "2026-03-24T10:42:11-08:00", decoded["created_at"])
}

func TestProcessPayloadForUpload_TypedRows_Base64EncodesBlobAndKeepsNulls(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (
			id TEXT PRIMARY KEY NOT NULL,
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
  "id": {
    "storage": "text",
    "value": "typed-blob"
  },
  "name": {
    "storage": "text",
    "value": "Blob Row"
  },
  "note": {
    "storage": "null",
    "value": null
  },
  "count_value": {
    "storage": "null",
    "value": null
  },
  "enabled_flag": {
    "storage": "integer",
    "value": "0"
  },
  "rating": {
    "storage": "real",
    "value": "1.25"
  },
  "data": {
    "storage": "blob",
    "value": "001122ff"
  },
  "created_at": {
    "storage": "text",
    "value": "2026-03-24T18:42:11Z"
  }
}`
	out, err := client.processPayloadForUpload("typed_rows", rawPayload)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, "ABEi/w==", decoded["data"])
	require.Nil(t, decoded["note"])
	require.Nil(t, decoded["count_value"])
	require.Equal(t, "0", decoded["enabled_flag"])
	require.Equal(t, "1.25", decoded["rating"])
	require.Equal(t, "2026-03-24T18:42:11Z", decoded["created_at"])

	gotBytes, err := base64.StdEncoding.DecodeString(decoded["data"].(string))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x11, 0x22, 0xff}, gotBytes)
}

func TestDirtyRowCapture_TypedRows_NullBlobRemainsJsonNull(t *testing.T) {
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "typed_rows", SyncKeyColumnName: "id"}}, `
		CREATE TABLE typed_rows (
			id TEXT PRIMARY KEY NOT NULL,
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

	payload := decodeDirtyPayload(t, raw)
	require.Contains(t, payload, "data")
	require.Nil(t, payload["data"])
}
