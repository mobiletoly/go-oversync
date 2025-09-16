package oversqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// TestBlobPrimaryKeySupport tests BLOB primary key handling in triggers and serialization
func TestBlobPrimaryKeySupport(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with BLOB primary key
	_, err = db.Exec(`
		CREATE TABLE blob_files (
			id BLOB PRIMARY KEY,
			name TEXT NOT NULL,
			data BLOB NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create blob_files table: %v", err)
	}

	// Initialize database and create triggers
	if err := initializeDatabase(db); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	syncTable := SyncTable{
		TableName:         "blob_files",
		SyncKeyColumnName: "id",
	}

	err = createTriggersForTable(db, syncTable)
	if err != nil {
		t.Fatalf("Failed to create triggers: %v", err)
	}

	// Set up client info for triggers to work
	_, err = db.Exec(`
		INSERT INTO _sync_client_info (user_id, source_id, next_change_id, last_server_seq_seen, apply_mode)
		VALUES ('test-user', 'test-source', 1, 0, 0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert client info: %v", err)
	}

	// Generate test data
	fileID := uuid.New()
	fileIDBytes := fileID[:]
	fileName := "test-document.pdf"
	fileData := make([]byte, 256)
	_, err = rand.Read(fileData)
	if err != nil {
		t.Fatalf("Failed to generate random file data: %v", err)
	}

	// Insert test record with BLOB primary key
	_, err = db.Exec(`
		INSERT INTO blob_files (id, name, data)
		VALUES (?, ?, ?)
	`, fileIDBytes, fileName, fileData)
	if err != nil {
		t.Fatalf("Failed to insert blob file: %v", err)
	}

	// Verify that sync metadata was created with hex-encoded BLOB primary key
	var metaCount int
	fileIDHex := strings.ToLower(hex.EncodeToString(fileIDBytes))
	err = db.QueryRow(`
		SELECT COUNT(*) FROM _sync_row_meta
		WHERE table_name='blob_files' AND pk_uuid=?
	`, fileIDHex).Scan(&metaCount)
	if err != nil {
		t.Fatalf("Failed to query row meta: %v", err)
	}
	if metaCount != 1 {
		t.Errorf("Expected 1 row meta entry, got %d", metaCount)
	}

	// Verify that pending change was created with proper payload
	var pendingCount int
	var op string
	var payload sql.NullString
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(op), ''), COALESCE(MAX(payload), '')
		FROM _sync_pending
		WHERE table_name='blob_files' AND pk_uuid=?
	`, fileIDHex).Scan(&pendingCount, &op, &payload)
	if err != nil {
		t.Fatalf("Failed to query pending changes: %v", err)
	}
	if pendingCount != 1 {
		t.Errorf("Expected 1 pending change, got %d", pendingCount)
	}
	if op != "INSERT" {
		t.Errorf("Expected INSERT operation, got %s", op)
	}

	// Verify that the payload contains hex-encoded BLOB data
	if !payload.Valid {
		t.Fatal("Expected payload to be present")
	}

	var payloadData map[string]interface{}
	err = json.Unmarshal([]byte(payload.String), &payloadData)
	if err != nil {
		t.Fatalf("Failed to parse payload JSON: %v", err)
	}

	// Check that BLOB fields are hex encoded (lowercase)
	idField, ok := payloadData["id"].(string)
	if !ok {
		t.Fatal("Expected 'id' field to be a string (hex encoded)")
	}
	if idField != fileIDHex {
		t.Errorf("Expected id field to be %s, got %s", fileIDHex, idField)
	}

	dataField, ok := payloadData["data"].(string)
	if !ok {
		t.Fatal("Expected 'data' field to be a string (hex encoded)")
	}

	// Verify that data field can be decoded back to original bytes
	decodedData, err := hex.DecodeString(dataField)
	if err != nil {
		t.Fatalf("Failed to decode data field: %v", err)
	}
	if len(decodedData) != len(fileData) {
		t.Errorf("Expected decoded data length %d, got %d", len(fileData), len(decodedData))
	}

	// Verify name field is still a regular string
	nameField, ok := payloadData["name"].(string)
	if !ok {
		t.Fatal("Expected 'name' field to be a string")
	}
	if nameField != fileName {
		t.Errorf("Expected name field to be %s, got %s", fileName, nameField)
	}
}

// TestBlobSerializeRow tests that SerializeRow properly handles BLOB data
func TestBlobSerializeRow(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with BLOB columns
	_, err = db.Exec(`
		CREATE TABLE blob_documents (
			id BLOB PRIMARY KEY,
			title TEXT NOT NULL,
			content BLOB NOT NULL,
			metadata TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create blob_documents table: %v", err)
	}

	// Create configuration
	config := DefaultConfig("test", []SyncTable{
		{TableName: "blob_documents", SyncKeyColumnName: "id"},
	})

	// Mock token function
	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	// Create client
	client, err := NewClient(db, "http://localhost:8080", "test-user", "test-source", tokenFunc, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Generate test data
	docID := uuid.New()
	docIDBytes := docID[:]
	docIDHex := strings.ToLower(hex.EncodeToString(docIDBytes))
	title := "Test Document"
	content := make([]byte, 512)
	_, err = rand.Read(content)
	if err != nil {
		t.Fatalf("Failed to generate random content: %v", err)
	}
	metadata := "Some metadata"

	// Insert test record
	_, err = db.Exec(`
		INSERT INTO blob_documents (id, title, content, metadata)
		VALUES (?, ?, ?, ?)
	`, docIDBytes, title, content, metadata)
	if err != nil {
		t.Fatalf("Failed to insert blob document: %v", err)
	}

	// Test SerializeRow
	ctx := context.Background()
	payload, err := client.SerializeRow(ctx, "blob_documents", docIDHex)
	if err != nil {
		t.Fatalf("Failed to serialize row: %v", err)
	}

	// Parse the payload
	var payloadData map[string]interface{}
	err = json.Unmarshal(payload, &payloadData)
	if err != nil {
		t.Fatalf("Failed to parse payload JSON: %v", err)
	}

	// Verify BLOB primary key is hex encoded (lowercase)
	idField, ok := payloadData["id"].(string)
	if !ok {
		t.Fatal("Expected 'id' field to be a string (hex encoded)")
	}
	if idField != docIDHex {
		t.Errorf("Expected id field to be %s, got %s", docIDHex, idField)
	}

	// Verify BLOB content is hex encoded
	contentField, ok := payloadData["content"].(string)
	if !ok {
		t.Fatal("Expected 'content' field to be a string (hex encoded)")
	}

	// Decode and verify content
	decodedContent, err := hex.DecodeString(contentField)
	if err != nil {
		t.Fatalf("Failed to decode content field: %v", err)
	}
	if len(decodedContent) != len(content) {
		t.Errorf("Expected decoded content length %d, got %d", len(content), len(decodedContent))
	}

	// Verify text fields are still strings
	titleField, ok := payloadData["title"].(string)
	if !ok {
		t.Fatal("Expected 'title' field to be a string")
	}
	if titleField != title {
		t.Errorf("Expected title field to be %s, got %s", title, titleField)
	}

	metadataField, ok := payloadData["metadata"].(string)
	if !ok {
		t.Fatal("Expected 'metadata' field to be a string")
	}
	if metadataField != metadata {
		t.Errorf("Expected metadata field to be %s, got %s", metadata, metadataField)
	}
}

// TestBlobPayloadExtractor tests the BLOB payload extraction utilities
func TestBlobPayloadExtractor(t *testing.T) {
	// Create test payload with BLOB data (hex format from triggers)
	fileID := uuid.New()
	fileIDHex := EncodeBlobUUIDAsHex(fileID)

	testData := make([]byte, 256)
	_, err := rand.Read(testData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	testDataHex := EncodeBlobAsHex(testData)

	payload := map[string]interface{}{
		"id":       fileIDHex,
		"name":     "test-file.bin",
		"data":     testDataHex,
		"size":     len(testData),
		"metadata": nil,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal test payload: %v", err)
	}

	// Test BlobPayloadExtractor
	extractor, err := NewBlobPayloadExtractor(payloadBytes)
	if err != nil {
		t.Fatalf("Failed to create BlobPayloadExtractor: %v", err)
	}

	// Test BlobUUIDField
	extractedID := extractor.BlobUUIDField("id")
	if extractedID != fileID {
		t.Errorf("Expected extracted ID to be %s, got %s", fileID.String(), extractedID.String())
	}

	// Test BlobUUIDFieldRequired
	extractedIDRequired, err := extractor.BlobUUIDFieldRequired("id")
	if err != nil {
		t.Fatalf("Failed to extract required ID: %v", err)
	}
	if extractedIDRequired != fileID {
		t.Errorf("Expected extracted required ID to be %s, got %s", fileID.String(), extractedIDRequired.String())
	}

	// Test BlobField
	extractedData := extractor.BlobField("data")
	if len(extractedData) != len(testData) {
		t.Errorf("Expected extracted data length %d, got %d", len(testData), len(extractedData))
	}

	// Test BlobFieldRequired
	extractedDataRequired, err := extractor.BlobFieldRequired("data")
	if err != nil {
		t.Fatalf("Failed to extract required data: %v", err)
	}
	if len(extractedDataRequired) != len(testData) {
		t.Errorf("Expected extracted required data length %d, got %d", len(testData), len(extractedDataRequired))
	}

	// Test StringField
	extractedName := extractor.StringField("name")
	if extractedName == nil || *extractedName != "test-file.bin" {
		t.Errorf("Expected extracted name to be 'test-file.bin', got %v", extractedName)
	}

	// Test missing field
	missingBlob := extractor.BlobField("missing")
	if missingBlob != nil {
		t.Errorf("Expected missing BLOB field to return nil, got %v", missingBlob)
	}

	// Test null field
	nullBlob := extractor.BlobField("metadata")
	if nullBlob != nil {
		t.Errorf("Expected null BLOB field to return nil, got %v", nullBlob)
	}

	// Test error cases
	_, err = extractor.BlobUUIDFieldRequired("missing")
	if err == nil {
		t.Error("Expected error for missing required BLOB UUID field")
	}

	_, err = extractor.BlobFieldRequired("missing")
	if err == nil {
		t.Error("Expected error for missing required BLOB field")
	}
}

// TestBlobHelperFunctions tests the standalone BLOB helper functions
func TestBlobHelperFunctions(t *testing.T) {
	// Test EncodeBlobUUIDAsHex and DecodeBlobUUIDFromHex
	testID := uuid.New()
	encoded := EncodeBlobUUIDAsHex(testID)

	decoded, err := DecodeBlobUUIDFromHex(encoded)
	if err != nil {
		t.Fatalf("Failed to decode BLOB UUID: %v", err)
	}
	if decoded != testID {
		t.Errorf("Expected decoded UUID to be %s, got %s", testID.String(), decoded.String())
	}

	// Test EncodeBlobUUIDAsBase64 and DecodeBlobUUIDFromBase64
	encodedBase64 := EncodeBlobUUIDAsBase64(testID)
	decodedBase64, err := DecodeBlobUUIDFromBase64(encodedBase64)
	if err != nil {
		t.Fatalf("Failed to decode base64 BLOB UUID: %v", err)
	}
	if decodedBase64 != testID {
		t.Errorf("Expected decoded base64 UUID to be %s, got %s", testID.String(), decodedBase64.String())
	}

	// Test convertReferenceKeyForBlob with hex UUID
	converted, err := convertReferenceKeyForBlob(encoded)
	if err != nil {
		t.Errorf("Expected convertReferenceKeyForBlob to succeed for hex UUID, got error: %v", err)
	}
	if convertedUUID, ok := converted.(uuid.UUID); !ok || convertedUUID != testID {
		t.Errorf("Expected converted value to be UUID %s, got %v", testID.String(), converted)
	}

	// Test convertReferenceKeyForBlob with base64 UUID
	converted, err = convertReferenceKeyForBlob(encodedBase64)
	if err != nil {
		t.Errorf("Expected convertReferenceKeyForBlob to succeed for base64 UUID, got error: %v", err)
	}
	if convertedUUID, ok := converted.(uuid.UUID); !ok || convertedUUID != testID {
		t.Errorf("Expected converted value to be UUID %s, got %v", testID.String(), converted)
	}

	// Test convertReferenceKeyForBlob with regular UUID string
	uuidStr := testID.String()
	converted, err = convertReferenceKeyForBlob(uuidStr)
	if err != nil {
		t.Errorf("Expected convertReferenceKeyForBlob to succeed for UUID string, got error: %v", err)
	}
	if convertedUUID, ok := converted.(uuid.UUID); !ok || convertedUUID != testID {
		t.Errorf("Expected converted value to be UUID %s, got %v", testID.String(), converted)
	}

	// Test convertReferenceKeyForBlob with non-UUID string
	nonUUID := "not-a-uuid"
	converted, err = convertReferenceKeyForBlob(nonUUID)
	if err != nil {
		t.Errorf("Expected convertReferenceKeyForBlob to succeed for non-UUID string (no conversion), got error: %v", err)
	}
	if converted != nonUUID {
		t.Errorf("Expected converted value to be unchanged %s, got %v", nonUUID, converted)
	}

	// Test error cases
	_, err = DecodeBlobUUIDFromHex("invalid-hex!")
	if err == nil {
		t.Error("Expected error for invalid hex")
	}

	_, err = DecodeBlobUUIDFromBase64("invalid-base64!")
	if err == nil {
		t.Error("Expected error for invalid base64")
	}
}

// convertReferenceKeyForBlob is a helper function that can be used in TableHandler.ConvertReferenceKey
// to handle both regular UUID strings, base64-encoded BLOB UUIDs, and hex-encoded BLOB UUIDs.
func convertReferenceKeyForBlob(payloadValue any) (any, error) {
	if str, ok := payloadValue.(string); ok {
		// Check if it looks like a hex-encoded UUID (32 hex characters)
		if len(str) == 32 {
			if decoded, err := hex.DecodeString(str); err == nil && len(decoded) == 16 {
				if decodedUUID, err := uuid.FromBytes(decoded); err == nil {
					return decodedUUID, nil
				} else {
					// Valid hex but invalid UUID bytes - this is a parsing error
					return payloadValue, fmt.Errorf("hex decoded value is not a valid UUID: %w", err)
				}
			}
		}

		// Try to decode as base64 (server format)
		if decoded, err := base64.StdEncoding.DecodeString(str); err == nil && len(decoded) == 16 {
			if decodedUUID, err := uuid.FromBytes(decoded); err == nil {
				return decodedUUID, nil
			} else {
				// Valid base64 but invalid UUID bytes - this is a parsing error
				return payloadValue, fmt.Errorf("base64 decoded value is not a valid UUID: %w", err)
			}
		}

		// Fallback to regular UUID parsing
		if parsedUUID, err := uuid.Parse(str); err == nil {
			return parsedUUID, nil
		}

		// If none of the above worked, return original value (no conversion needed)
		return payloadValue, nil
	}

	return payloadValue, nil
}
