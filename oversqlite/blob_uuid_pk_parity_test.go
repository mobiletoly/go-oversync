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

func applyBundleRowForTest(t *testing.T, client *Client, tx *sql.Tx, row *oversync.BundleRow) {
	t.Helper()
	_, localPK, err := client.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
	require.NoError(t, err)
	require.NoError(t, client.applyBundleRowAuthoritativelyInTx(context.Background(), tx, row, localPK))
}

func TestApplyBundleRowAuthoritativelyInTx_BlobUUIDPK_ServerSendsUUIDString(t *testing.T) {
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

	_, err = db.Exec(`UPDATE _sync_client_state SET apply_mode = 1 WHERE user_id = ?`, client.UserID)
	require.NoError(t, err)

	fileID := uuid.New()
	fileBytes := fileID[:]
	fileHex := hex.EncodeToString(fileBytes)

	dataBytes := []byte("hello")
	dataBase64 := base64.StdEncoding.EncodeToString(dataBytes)

	filePayload, err := json.Marshal(map[string]any{
		"id":   fileID.String(),
		"name": "a.txt",
		"data": dataBase64,
	})
	require.NoError(t, err)

	insFile := &oversync.BundleRow{
		Schema:     "business",
		Table:      "files",
		Key:        oversync.SyncKey{"id": fileID.String()},
		Op:         oversync.OpInsert,
		Payload:    filePayload,
		RowVersion: 10,
	}

	reviewID := uuid.New()
	reviewPayload, err := json.Marshal(map[string]any{
		"id":      reviewID.String(),
		"file_id": fileID.String(),
		"review":  "ok",
	})
	require.NoError(t, err)

	insReview := &oversync.BundleRow{
		Schema:     "business",
		Table:      "file_reviews",
		Key:        oversync.SyncKey{"id": reviewID.String()},
		Op:         oversync.OpInsert,
		Payload:    reviewPayload,
		RowVersion: 11,
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

	applyBundleRowForTest(t, client, tx, insFile)
	applyBundleRowForTest(t, client, tx, insReview)
	require.NoError(t, tx.Commit())

	var gotFileHexID, gotFileName string
	var gotFileData []byte
	err = db.QueryRow(`SELECT lower(hex(id)), name, data FROM files`).Scan(&gotFileHexID, &gotFileName, &gotFileData)
	require.NoError(t, err)
	require.Equal(t, fileHex, gotFileHexID)
	require.Equal(t, "a.txt", gotFileName)
	require.Equal(t, dataBytes, gotFileData)

	var metaCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE schema_name='business' AND table_name='files' AND key_json=?`, `{"id":"`+fileHex+`"}`).Scan(&metaCount)
	require.NoError(t, err)
	require.Equal(t, 1, metaCount)

	var gotReviewFileHexID string
	err = db.QueryRow(`SELECT lower(hex(file_id)) FROM file_reviews`).Scan(&gotReviewFileHexID)
	require.NoError(t, err)
	require.Equal(t, fileHex, gotReviewFileHexID)

	delFile := &oversync.BundleRow{
		Schema:     "business",
		Table:      "files",
		Key:        oversync.SyncKey{"id": fileID.String()},
		Op:         oversync.OpDelete,
		RowVersion: 12,
	}

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	applyBundleRowForTest(t, client, tx, delFile)
	require.NoError(t, tx.Commit())

	var filesCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&filesCount)
	require.NoError(t, err)
	require.Equal(t, 0, filesCount)

	var deletedInt int
	var gotServerVersion int64
	err = db.QueryRow(`SELECT deleted, row_version FROM _sync_row_state WHERE schema_name='business' AND table_name='files' AND key_json=?`, `{"id":"`+fileHex+`"}`).Scan(&deletedInt, &gotServerVersion)
	require.NoError(t, err)
	require.Equal(t, 1, deletedInt)
	require.Equal(t, int64(12), gotServerVersion)
}

func TestApplyBundleRowAuthoritativelyInTx_UpsertPreservesChildrenForBlobPKTables(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

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

	client, err := NewClient(db, "http://localhost:8080", "test-user", "test-source", func(context.Context) (string, error) {
		return "mock-token", nil
	}, DefaultConfig("business", []SyncTable{
		{TableName: "files", SyncKeyColumnName: "id"},
		{TableName: "file_reviews", SyncKeyColumnName: "id"},
	}))
	require.NoError(t, err)
	require.NoError(t, client.Bootstrap(context.Background(), false))

	_, err = db.Exec(`UPDATE _sync_client_state SET apply_mode = 1 WHERE user_id = ?`, client.UserID)
	require.NoError(t, err)

	fileID := uuid.New()
	reviewID := uuid.New()

	insertFilePayload, err := json.Marshal(map[string]any{
		"id":   fileID.String(),
		"name": "original.txt",
		"data": base64.StdEncoding.EncodeToString([]byte("v1")),
	})
	require.NoError(t, err)
	insertReviewPayload, err := json.Marshal(map[string]any{
		"id":      reviewID.String(),
		"file_id": fileID.String(),
		"review":  "ok",
	})
	require.NoError(t, err)
	updateFilePayload, err := json.Marshal(map[string]any{
		"id":   fileID.String(),
		"name": "updated.txt",
		"data": base64.StdEncoding.EncodeToString([]byte("v2")),
	})
	require.NoError(t, err)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	applyBundleRowForTest(t, client, tx, &oversync.BundleRow{
		Schema:     "business",
		Table:      "files",
		Key:        oversync.SyncKey{"id": fileID.String()},
		Op:         oversync.OpInsert,
		Payload:    insertFilePayload,
		RowVersion: 1,
	})
	applyBundleRowForTest(t, client, tx, &oversync.BundleRow{
		Schema:     "business",
		Table:      "file_reviews",
		Key:        oversync.SyncKey{"id": reviewID.String()},
		Op:         oversync.OpInsert,
		Payload:    insertReviewPayload,
		RowVersion: 1,
	})
	require.NoError(t, tx.Commit())

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	applyBundleRowForTest(t, client, tx, &oversync.BundleRow{
		Schema:     "business",
		Table:      "files",
		Key:        oversync.SyncKey{"id": fileID.String()},
		Op:         oversync.OpUpdate,
		Payload:    updateFilePayload,
		RowVersion: 2,
	})
	require.NoError(t, tx.Commit())

	var reviewCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM file_reviews`).Scan(&reviewCount))
	require.Equal(t, 1, reviewCount)

	var fileName string
	require.NoError(t, db.QueryRow(`SELECT name FROM files`).Scan(&fileName))
	require.Equal(t, "updated.txt", fileName)
}

func TestApplyBundleRowAuthoritativelyInTx_RejectsNonCanonicalWireEncodingsForBlobTables(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

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

	client, err := NewClient(db, "http://localhost:8080", "test-user", "test-source", func(context.Context) (string, error) {
		return "mock-token", nil
	}, DefaultConfig("business", []SyncTable{
		{TableName: "files", SyncKeyColumnName: "id"},
	}))
	require.NoError(t, err)
	require.NoError(t, client.Bootstrap(context.Background(), false))

	_, err = db.Exec(`UPDATE _sync_client_state SET apply_mode = 1 WHERE user_id = ?`, client.UserID)
	require.NoError(t, err)

	fileID := uuid.New()
	fileHex := hex.EncodeToString(fileID[:])

	hexPayload, err := json.Marshal(map[string]any{
		"id":   fileID.String(),
		"name": "bad.txt",
		"data": "68656c6c6f",
	})
	require.NoError(t, err)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, localPK, err := client.bundleRowKeyToLocalKey(tx, "files", oversync.SyncKey{"id": fileID.String()})
	require.NoError(t, err)
	err = client.applyBundleRowAuthoritativelyInTx(ctx, tx, &oversync.BundleRow{
		Schema:     "business",
		Table:      "files",
		Key:        oversync.SyncKey{"id": fileID.String()},
		Op:         oversync.OpInsert,
		Payload:    hexPayload,
		RowVersion: 1,
	}, localPK)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to decode canonical wire blob payload field data")
	require.NoError(t, tx.Rollback())

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, _, err = client.bundleRowKeyToLocalKey(tx, "files", oversync.SyncKey{"id": fileHex})
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to decode canonical wire bundle row key")
	require.NoError(t, tx.Rollback())
}
