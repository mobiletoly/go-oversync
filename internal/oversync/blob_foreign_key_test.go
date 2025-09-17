package oversync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// TestBlobForeignKeyRoundtripSync tests BLOB primary keys and foreign key relationships
// This recreates the Kotlin test scenario: blob_foreign_key_roundtrip_sync()
func TestBlobForeignKeyRoundtripSync(t *testing.T) {
	// Create test harness with BLOB table handlers
	h := NewSimpleTestHarnessWithConfig(t, func(cfg *oversync.ServiceConfig) {
		// Add BLOB tables to the registered tables
		cfg.RegisteredTables = append(cfg.RegisteredTables,
			oversync.RegisteredTable{Schema: "business", Table: "files", Handler: &BlobFilesTableHandler{}},
			oversync.RegisteredTable{Schema: "business", Table: "file_reviews", Handler: &BlobFileReviewsTableHandler{}},
		)
	})
	defer h.Cleanup()
	h.Reset()

	// Setup business tables with BLOB primary keys and foreign keys
	h.setupBlobForeignKeyTables()

	// Generate test data
	fileName := "document.pdf"
	fileData := make([]byte, 512)
	_, err := rand.Read(fileData)
	require.NoError(t, err)

	reviewText := "Great document, very useful!"

	// Phase 1: Device A inserts file with BLOB primary key
	fileID := uuid.New()
	fileIDBytes := fileID[:]
	fileIDBase64 := base64.StdEncoding.EncodeToString(fileIDBytes)

	// Upload file from device A
	filePayload := map[string]interface{}{
		"id":   fileIDBase64, // Base64 encoded BLOB primary key
		"name": fileName,
		"data": fileData,
	}
	filePayloadBytes, err := json.Marshal(filePayload)
	require.NoError(t, err)

	uploadReq1 := &oversync.UploadRequest{
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "business",
				Table:          "files",
				Op:             "INSERT",
				PK:             fileID.String(),
				ServerVersion:  0,
				Payload:        filePayloadBytes,
			},
		},
	}

	uploadResp1, httpResp1 := h.DoUpload(h.client1Token, uploadReq1)
	require.Equal(t, 200, httpResp1.StatusCode)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, "applied", uploadResp1.Statuses[0].Status)

	// Phase 2: Device A inserts file review with BLOB foreign key
	reviewID := uuid.New()
	reviewPayload := map[string]interface{}{
		"id":      reviewID.String(),
		"review":  reviewText,
		"file_id": fileIDBase64, // Base64 encoded BLOB foreign key
	}
	reviewPayloadBytes, err := json.Marshal(reviewPayload)
	require.NoError(t, err)

	uploadReq2 := &oversync.UploadRequest{
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Schema:         "business",
				Table:          "file_reviews",
				Op:             "INSERT",
				PK:             reviewID.String(),
				ServerVersion:  0,
				Payload:        reviewPayloadBytes,
			},
		},
	}

	uploadResp2, httpResp2 := h.DoUpload(h.client1Token, uploadReq2)
	require.Equal(t, 200, httpResp2.StatusCode)
	require.True(t, uploadResp2.Accepted)
	require.Equal(t, "applied", uploadResp2.Statuses[0].Status)

	// Phase 3: Device B downloads changes (hydrate)
	downloadResp, httpResp3 := h.DoDownload(h.client2Token, 0, 500)
	require.Equal(t, 200, httpResp3.StatusCode)
	require.Len(t, downloadResp.Changes, 2) // File + Review

	// Verify file exists on device B by checking download response
	var fileChange, reviewChange *oversync.ChangeDownloadResponse
	for i := range downloadResp.Changes {
		change := &downloadResp.Changes[i]
		if change.TableName == "files" {
			fileChange = change
		} else if change.TableName == "file_reviews" {
			reviewChange = change
		}
	}

	require.NotNil(t, fileChange, "File change should be downloaded")
	require.NotNil(t, reviewChange, "Review change should be downloaded")

	// Verify file payload
	var downloadedFilePayload map[string]interface{}
	err = json.Unmarshal(fileChange.Payload, &downloadedFilePayload)
	require.NoError(t, err)
	require.Equal(t, fileName, downloadedFilePayload["name"])

	// Verify review payload and foreign key relationship
	var downloadedReviewPayload map[string]interface{}
	err = json.Unmarshal(reviewChange.Payload, &downloadedReviewPayload)
	require.NoError(t, err)
	require.Equal(t, reviewText, downloadedReviewPayload["review"])
	require.Equal(t, fileIDBase64, downloadedReviewPayload["file_id"])

	// Phase 4: Device B updates review and syncs back
	updatedReview := "Updated: Even better after second read!"
	updatedReviewPayload := map[string]interface{}{
		"id":      reviewID.String(),
		"review":  updatedReview,
		"file_id": fileIDBase64,
	}
	updatedReviewPayloadBytes, err := json.Marshal(updatedReviewPayload)
	require.NoError(t, err)

	uploadReq3 := &oversync.UploadRequest{
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "business",
				Table:          "file_reviews",
				Op:             "UPDATE",
				PK:             reviewID.String(),
				ServerVersion:  reviewChange.ServerVersion, // Use downloaded server version
				Payload:        updatedReviewPayloadBytes,
			},
		},
	}

	uploadResp3, httpResp4 := h.DoUpload(h.client2Token, uploadReq3)
	require.Equal(t, 200, httpResp4.StatusCode)
	require.True(t, uploadResp3.Accepted)
	require.Equal(t, "applied", uploadResp3.Statuses[0].Status)

	// Phase 5: Device A downloads updates
	downloadResp2, httpResp5 := h.DoDownload(h.client1Token, uploadResp2.HighestServerSeq, 100)
	require.Equal(t, 200, httpResp5.StatusCode)
	require.Len(t, downloadResp2.Changes, 1) // Updated review

	// Verify updated review
	updatedChange := &downloadResp2.Changes[0]
	require.Equal(t, "file_reviews", updatedChange.TableName)

	var finalReviewPayload map[string]interface{}
	err = json.Unmarshal(updatedChange.Payload, &finalReviewPayload)
	require.NoError(t, err)
	require.Equal(t, updatedReview, finalReviewPayload["review"])
	require.Equal(t, fileIDBase64, finalReviewPayload["file_id"])

	// Phase 6: Verify foreign key relationship works in business tables
	// This verifies that the materialization correctly handled BLOB foreign keys
	var joinResult struct {
		FileName string
		Review   string
	}
	err = h.service.Pool().QueryRow(h.ctx, `
		SELECT f.name, fr.review
		FROM business.files f
		JOIN business.file_reviews fr ON f.id = fr.file_id
		WHERE f.name = $1
	`, fileName).Scan(&joinResult.FileName, &joinResult.Review)
	require.NoError(t, err)
	require.Equal(t, fileName, joinResult.FileName)
	require.Equal(t, updatedReview, joinResult.Review)

	t.Logf("✅ BLOB foreign key roundtrip sync test completed successfully")
	t.Logf("   File: %s (ID: %s)", fileName, fileIDBase64)
	t.Logf("   Final review: %s", updatedReview)
}

// setupBlobForeignKeyTables creates business tables with BLOB primary keys and foreign keys
func (h *SimpleTestHarness) setupBlobForeignKeyTables() {
	migrations := []string{
		// Create business schema if it doesn't exist
		`CREATE SCHEMA IF NOT EXISTS business`,
		// Drop existing tables to start fresh
		`DROP TABLE IF EXISTS business.file_reviews CASCADE`,
		`DROP TABLE IF EXISTS business.files CASCADE`,
		// Files table with UUID primary key (stored as BYTEA for BLOB simulation)
		`CREATE TABLE business.files (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			data BYTEA NOT NULL
		)`,
		// File reviews table with UUID foreign key referencing files
		`CREATE TABLE business.file_reviews (
			id UUID PRIMARY KEY,
			review TEXT NOT NULL,
			file_id UUID REFERENCES business.files(id) DEFERRABLE INITIALLY DEFERRED
		)`,
		// Create index for FK performance
		`CREATE INDEX idx_file_reviews_file_id ON business.file_reviews(file_id)`,
	}

	err := pgx.BeginFunc(h.ctx, h.service.Pool(), func(tx pgx.Tx) error {
		for _, migration := range migrations {
			if _, err := tx.Exec(h.ctx, migration); err != nil {
				return fmt.Errorf("failed to create BLOB FK table: %w", err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)

	// Register the table handlers for BLOB FK tables
	// Note: This would normally be done in service config, but for testing we add them here
	h.logger.Info("BLOB foreign key tables created successfully")
}

// BlobFilesTableHandler handles files table with BLOB primary keys
type BlobFilesTableHandler struct{}

// ConvertReferenceKey implements the MaterializationHandler interface
func (h *BlobFilesTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *BlobFilesTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Parse the payload to extract the file data
	var file map[string]interface{}
	if err := json.Unmarshal(payload, &file); err != nil {
		return fmt.Errorf("invalid file payload: %w", err)
	}

	name, _ := file["name"].(string)

	// Handle data field - could be base64 string or byte array
	var data []byte
	switch v := file["data"].(type) {
	case string:
		// Assume it's base64 encoded
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
			data = decoded
		} else {
			data = []byte(v) // Fallback to raw string
		}
	case []byte:
		data = v
	case []interface{}:
		// JSON array of numbers (bytes)
		data = make([]byte, len(v))
		for i, b := range v {
			if num, ok := b.(float64); ok {
				data[i] = byte(num)
			}
		}
	default:
		return fmt.Errorf("unsupported data type for file data: %T", v)
	}

	// Materialize to business table
	_, err := tx.Exec(ctx, `
		INSERT INTO business.files (id, name, data)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			data = EXCLUDED.data`,
		pk, name, data)

	if err != nil {
		return fmt.Errorf("failed to materialize file: %w", err)
	}

	return nil
}

func (h *BlobFilesTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.files WHERE id = $1`, pk)
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

// BlobFileReviewsTableHandler handles file_reviews table with BLOB foreign keys
type BlobFileReviewsTableHandler struct{}

// ConvertReferenceKey implements the MaterializationHandler interface
func (h *BlobFileReviewsTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *BlobFileReviewsTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Parse the payload to extract the review data
	var review map[string]interface{}
	if err := json.Unmarshal(payload, &review); err != nil {
		return fmt.Errorf("invalid review payload: %w", err)
	}

	reviewText, _ := review["review"].(string)

	// Handle file_id - should be base64 encoded UUID
	var fileID uuid.UUID
	if fileIDStr, ok := review["file_id"].(string); ok {
		if converted, err := oversync.ConvertBase64EncodedUUID(fileIDStr); err == nil {
			fileID = converted
		} else {
			// Fallback to direct UUID parsing
			if parsed, err := uuid.Parse(fileIDStr); err == nil {
				fileID = parsed
			} else {
				return fmt.Errorf("failed to parse file_id: %w", err)
			}
		}
	} else {
		return fmt.Errorf("file_id is required")
	}

	// Materialize to business table
	_, err := tx.Exec(ctx, `
		INSERT INTO business.file_reviews (id, review, file_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			review = EXCLUDED.review,
			file_id = EXCLUDED.file_id`,
		pk, reviewText, fileID)

	if err != nil {
		return fmt.Errorf("failed to materialize file review: %w", err)
	}

	return nil
}

func (h *BlobFileReviewsTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.file_reviews WHERE id = $1`, pk)
	if err != nil {
		return fmt.Errorf("failed to delete file review: %w", err)
	}
	return nil
}
