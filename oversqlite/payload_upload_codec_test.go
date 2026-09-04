package oversqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestProcessPayloadForUpload_BlobUUIDPK_UsesUUIDStringForPK(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`
		CREATE TABLE files (
			id BLOB PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			data BLOB
		)
	`)
	require.NoError(t, err)

	cfg := admittedFixtureConfig("business", []SyncTable{{TableName: "files", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg = testConfigWithLocalContract(t, db, cfg)
	tokenFunc := func(ctx context.Context) (string, error) { return "mock-token", nil }
	client, err := newAdmittedFixtureClient(db, "http://localhost:8080", tokenFunc, cfg)
	require.NoError(t, err)

	id := uuid.New()
	idHex := hex.EncodeToString(id[:])
	data := []byte("hello")
	dataHex := hex.EncodeToString(data)

	rawPayload := map[string]any{
		"id":   map[string]any{"storage": "blob", "value": idHex},
		"name": map[string]any{"storage": "text", "value": "a.txt"},
		"data": map[string]any{"storage": "blob", "value": dataHex},
	}
	rawBytes, err := json.Marshal(rawPayload)
	require.NoError(t, err)

	out, err := client.processPayloadForUpload("files", string(rawBytes))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, id.String(), decoded["id"])
	require.Equal(t, "a.txt", decoded["name"])

	dataBase64, ok := decoded["data"].(string)
	require.True(t, ok)
	gotData, err := base64.StdEncoding.DecodeString(dataBase64)
	require.NoError(t, err)
	require.Equal(t, data, gotData)
}

func TestProcessPayloadForUpload_BlobUUIDReferenceColumns_UseUUIDStrings(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`
		CREATE TABLE files (
			id BLOB PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE file_reviews (
			id BLOB PRIMARY KEY NOT NULL,
			file_id BLOB NOT NULL REFERENCES files(id) DEFERRABLE INITIALLY DEFERRED,
			review TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	cfg := admittedFixtureConfig("business", []SyncTable{
		{TableName: "files", SyncKeyColumnName: "id"},
		{TableName: "file_reviews", SyncKeyColumnName: "id"},
	}, testSchemaGenerationProof())
	cfg = testConfigWithLocalContract(t, db, cfg)

	tokenFunc := func(ctx context.Context) (string, error) { return "mock-token", nil }
	client, err := newAdmittedFixtureClient(db, "http://localhost:8080", tokenFunc, cfg)
	require.NoError(t, err)

	reviewID := uuid.New()
	fileID := uuid.New()
	rawPayload := map[string]any{
		"id":      map[string]any{"storage": "blob", "value": hex.EncodeToString(reviewID[:])},
		"file_id": map[string]any{"storage": "blob", "value": hex.EncodeToString(fileID[:])},
		"review":  map[string]any{"storage": "text", "value": "looks good"},
	}
	rawBytes, err := json.Marshal(rawPayload)
	require.NoError(t, err)

	out, err := client.processPayloadForUpload("file_reviews", string(rawBytes))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, reviewID.String(), decoded["id"])
	require.Equal(t, fileID.String(), decoded["file_id"])
	require.Equal(t, "looks good", decoded["review"])
}
