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
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestApplyServerChangeInTx_BlobUUIDPK_ServerSendsUUIDString(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Match mobile_flow behavior (single-connection DB) to ensure tx paths never query via *sql.DB.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.Exec(`
		CREATE TABLE files (
			id BLOB PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			data BLOB
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE file_reviews (
			id BLOB PRIMARY KEY NOT NULL,
			file_id BLOB NOT NULL,
			review TEXT NOT NULL,
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
		)
	`)
	require.NoError(t, err)

	config := DefaultConfig("business", []SyncTable{
		{TableName: "files", SyncKeyColumnName: "id"},
		{TableName: "file_reviews", SyncKeyColumnName: "id"},
	})
	tokenFunc := func(ctx context.Context) (string, error) { return "mock-token", nil }

	client, err := NewClient(db, "http://localhost:8080", "test-user", "test-source", tokenFunc, config)
	require.NoError(t, err)

	require.NoError(t, client.Bootstrap(context.Background(), false))

	_, err = db.Exec(`UPDATE _sync_client_info SET apply_mode = 1 WHERE user_id = ?`, client.UserID)
	require.NoError(t, err)

	fileID := uuid.New()
	fileBytes := fileID[:]
	fileHex := hex.EncodeToString(fileBytes)
	fileBase64 := base64.StdEncoding.EncodeToString(fileBytes)

	dataBytes := []byte("hello")
	dataBase64 := base64.StdEncoding.EncodeToString(dataBytes)

	filePayload, err := json.Marshal(map[string]any{
		"id":   fileBase64,
		"name": "a.txt",
		"data": dataBase64,
	})
	require.NoError(t, err)

	// Server sends PK as UUID string (dashed), while SQLite stores BLOB PK meta as hex.
	insFile := &oversync.ChangeDownloadResponse{
		TableName:     "files",
		Op:            "INSERT",
		PK:            fileID.String(),
		Payload:       filePayload,
		ServerVersion: 10,
	}

	reviewID := uuid.New()
	reviewBytes := reviewID[:]
	reviewPayload, err := json.Marshal(map[string]any{
		"id":      base64.StdEncoding.EncodeToString(reviewBytes),
		"file_id": fileBase64,
		"review":  "ok",
	})
	require.NoError(t, err)

	insReview := &oversync.ChangeDownloadResponse{
		TableName:     "file_reviews",
		Op:            "INSERT",
		PK:            reviewID.String(),
		Payload:       reviewPayload,
		ServerVersion: 11,
	}

	// Ensure normalization helpers behave as expected.
	normalizedMetaPK, err := client.normalizePKForMeta("files", fileID.String())
	require.NoError(t, err)
	require.Equal(t, fileHex, normalizedMetaPK)

	normalizedWirePK, err := client.normalizePKForServer("files", fileHex)
	require.NoError(t, err)
	require.Equal(t, fileID.String(), normalizedWirePK)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, client.applyServerChangeInTx(ctx, tx, insFile))
	require.NoError(t, client.applyServerChangeInTx(ctx, tx, insReview))
	require.NoError(t, tx.Commit())

	var gotFileHexID, gotFileName string
	var gotFileData []byte
	err = db.QueryRow(`SELECT lower(hex(id)), name, data FROM files`).Scan(&gotFileHexID, &gotFileName, &gotFileData)
	require.NoError(t, err)
	require.Equal(t, fileHex, gotFileHexID)
	require.Equal(t, "a.txt", gotFileName)
	require.Equal(t, dataBytes, gotFileData)

	var metaCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM _sync_row_meta WHERE table_name='files' AND pk_uuid=?`, fileHex).Scan(&metaCount)
	require.NoError(t, err)
	require.Equal(t, 1, metaCount)

	err = db.QueryRow(`SELECT COUNT(*) FROM _sync_row_meta WHERE table_name='files' AND pk_uuid=?`, fileID.String()).Scan(&metaCount)
	require.NoError(t, err)
	require.Equal(t, 0, metaCount)

	var gotReviewFileHexID string
	err = db.QueryRow(`SELECT lower(hex(file_id)) FROM file_reviews`).Scan(&gotReviewFileHexID)
	require.NoError(t, err)
	require.Equal(t, fileHex, gotReviewFileHexID)

	delFile := &oversync.ChangeDownloadResponse{
		TableName:     "files",
		Op:            "DELETE",
		PK:            fileID.String(),
		ServerVersion: 12,
	}

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, client.applyServerChangeInTx(ctx, tx, delFile))
	require.NoError(t, tx.Commit())

	var filesCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&filesCount)
	require.NoError(t, err)
	require.Equal(t, 0, filesCount)

	var deletedInt int
	var gotServerVersion int64
	err = db.QueryRow(`SELECT deleted, server_version FROM _sync_row_meta WHERE table_name='files' AND pk_uuid=?`, fileHex).Scan(&deletedInt, &gotServerVersion)
	require.NoError(t, err)
	require.Equal(t, 1, deletedInt)
	require.Equal(t, int64(12), gotServerVersion)
}
