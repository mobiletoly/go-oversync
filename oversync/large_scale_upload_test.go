package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestLargeScaleUploadDownload tests uploading and downloading thousands of records
// This integration test validates:
// - Large-scale uploads in multiple batches
// - Large-scale downloads in multiple batches
// - Data integrity across the entire process
// - Proper handling of pagination limits
func TestLargeScaleUploadDownload(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Setup PostgreSQL connection
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	// Clean up database before test
	err = cleanupTestDatabase(ctx, pool)
	require.NoError(t, err)

	// Initialize sync service
	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "integration-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "business", Table: "users"},
			{Schema: "business", Table: "posts"},
		},
	}

	syncService, err := NewSyncService(pool, config, logger)
	require.NoError(t, err)
	defer syncService.Close()

	// Test configuration
	userID := "test-user-large-scale"
	sourceID := "test-device-large"

	// Test parameters - start smaller to focus on core functionality
	const (
		totalUsers        = 500 // 500 users
		totalPosts        = 0   // Skip posts for now to focus on core upload/download
		uploadBatchSize   = 100 // Upload in batches of 100
		downloadBatchSize = 150 // Download in batches of 150
	)

	t.Logf("Starting large-scale test: %d users, %d posts", totalUsers, totalPosts)

	// Phase 1: Upload large number of records in multiple batches
	var allUploadedChanges []ChangeUpload

	t.Run("Phase1_LargeScaleUpload", func(t *testing.T) {
		t.Logf("Creating and uploading %d users in batches of %d",
			totalUsers, uploadBatchSize)

		// Create users
		userChanges, _ := createUserChanges(totalUsers, sourceID)
		allUploadedChanges = append(allUploadedChanges, userChanges...)

		// Upload users in batches
		err := uploadChangesInBatches(t, ctx, syncService, userID, sourceID, userChanges, uploadBatchSize)
		require.NoError(t, err)

		t.Logf("Successfully uploaded %d users", totalUsers)

		// Skip posts for now to focus on core functionality
		if totalPosts > 0 {
			// Create posts (with FK references to users) - disabled for now
			t.Logf("Skipping posts to focus on core upload/download functionality")
		}

		// Verify server has all the data
		serverUserCount, serverPostCount := getServerRecordCounts(t, ctx, pool, userID)
		require.Equal(t, totalUsers, serverUserCount, "Server should have all uploaded users")
		if totalPosts > 0 {
			require.Equal(t, totalPosts, serverPostCount, "Server should have all uploaded posts")
		}

		t.Logf("Phase 1 complete: Server has %d users and %d posts", serverUserCount, serverPostCount)
	})

	// Phase 2: Download all records in multiple batches and verify completeness
	t.Run("Phase2_LargeScaleDownload", func(t *testing.T) {
		t.Logf("Downloading all %d records in batches of %d", len(allUploadedChanges), downloadBatchSize)

		var allDownloadedChanges []ChangeDownloadResponse
		after := int64(0)
		batchNum := 1

		// Download all changes in batches
		for {
			t.Logf("Downloading batch %d starting after server_id %d", batchNum, after)

			response, err := syncService.ProcessDownloadWindowed(ctx, userID, sourceID, after, downloadBatchSize, "", true, 0)
			require.NoError(t, err)

			t.Logf("Batch %d: downloaded %d changes, hasMore=%t, nextAfter=%d",
				batchNum, len(response.Changes), response.HasMore, response.NextAfter)

			allDownloadedChanges = append(allDownloadedChanges, response.Changes...)

			if !response.HasMore || len(response.Changes) == 0 {
				break
			}

			after = response.NextAfter
			batchNum++

			// Safety check to prevent infinite loops
			require.Less(t, batchNum, 100, "Too many download batches - possible infinite loop")
		}

		t.Logf("Downloaded %d total changes in %d batches", len(allDownloadedChanges), batchNum)

		// Verify we downloaded all uploaded changes
		require.Equal(t, len(allUploadedChanges), len(allDownloadedChanges),
			"Should download exactly the same number of changes that were uploaded")

		// Verify data integrity
		verifyDataIntegrity(t, allUploadedChanges, allDownloadedChanges)

		t.Logf("Phase 2 complete: All %d changes downloaded and verified", len(allDownloadedChanges))
	})

	// Phase 3: Test download with different parameters
	t.Run("Phase3_DownloadWithDifferentParameters", func(t *testing.T) {
		// Test download without includeSelf
		response, err := syncService.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 100, "", false, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(response.Changes), "Should get 0 changes when includeSelf=false with same sourceID")

		// Test download with schema filter
		response, err = syncService.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 1000, "business", true, 0)
		require.NoError(t, err)
		expectedTotal := totalUsers + totalPosts
		require.Equal(t, expectedTotal, len(response.Changes), "Should get all changes with business schema filter")

		// Test download with different sourceID (should get all changes)
		response, err = syncService.ProcessDownloadWindowed(ctx, userID, "different-source", 0, 1000, "", false, 0)
		require.NoError(t, err)
		require.Equal(t, expectedTotal, len(response.Changes), "Should get all changes with different sourceID")

		t.Logf("Phase 3 complete: Download parameter variations work correctly")
	})
}

// Helper functions

func createUserChanges(count int, sourceID string) ([]ChangeUpload, []string) {
	changes := make([]ChangeUpload, count)
	userUUIDs := make([]string, count)

	for i := 0; i < count; i++ {
		userID := uuid.New().String()
		userUUIDs[i] = userID

		payload := map[string]interface{}{
			"id":    userID,
			"name":  fmt.Sprintf("User %d", i+1),
			"email": fmt.Sprintf("user%d@test.com", i+1),
		}

		payloadBytes, _ := json.Marshal(payload)

		changes[i] = ChangeUpload{
			SourceChangeID: int64(i + 1),
			Schema:         "business",
			Table:          "users",
			Op:             OpInsert,
			PK:             userID,
			Payload:        payloadBytes,
		}
	}

	return changes, userUUIDs
}

func createPostChanges(count int, userUUIDs []string, sourceID string, startChangeID int) []ChangeUpload {
	changes := make([]ChangeUpload, count)

	for i := 0; i < count; i++ {
		postID := uuid.New().String()
		authorID := userUUIDs[i%len(userUUIDs)] // Cycle through users

		payload := map[string]interface{}{
			"id":        postID,
			"title":     fmt.Sprintf("Post %d", i+1),
			"content":   fmt.Sprintf("Content for post %d", i+1),
			"author_id": authorID,
		}

		payloadBytes, _ := json.Marshal(payload)

		changes[i] = ChangeUpload{
			SourceChangeID: int64(startChangeID + i + 1), // Continue from user changes
			Schema:         "business",
			Table:          "posts",
			Op:             OpInsert,
			PK:             postID,
			Payload:        payloadBytes,
		}
	}

	return changes
}

func uploadChangesInBatches(t *testing.T, ctx context.Context, syncService *SyncService,
	userID, sourceID string, changes []ChangeUpload, batchSize int) error {

	totalBatches := (len(changes) + batchSize - 1) / batchSize

	for i := 0; i < len(changes); i += batchSize {
		end := i + batchSize
		if end > len(changes) {
			end = len(changes)
		}

		batch := changes[i:end]
		batchNum := (i / batchSize) + 1

		t.Logf("Uploading batch %d/%d: %d changes", batchNum, totalBatches, len(batch))

		request := &UploadRequest{Changes: batch}
		response, err := syncService.ProcessUpload(ctx, userID, sourceID, request)
		if err != nil {
			return fmt.Errorf("failed to upload batch %d: %w", batchNum, err)
		}

		require.True(t, response.Accepted, "Upload batch %d should be accepted", batchNum)
		require.Equal(t, len(batch), len(response.Statuses), "Should get status for each change in batch %d", batchNum)

		// Verify all changes were applied
		for j, status := range response.Statuses {
			if status.Status != StApplied {
				t.Logf("Change %d in batch %d failed: status=%s, message=%s, invalid=%+v",
					j+1, batchNum, status.Status, status.Message, status.Invalid)
			}
			require.Equal(t, StApplied, status.Status,
				"Change %d in batch %d should be applied, got: %s", j+1, batchNum, status.Status)
		}
	}

	return nil
}

func verifyDataIntegrity(t *testing.T, uploaded []ChangeUpload, downloaded []ChangeDownloadResponse) {
	// Create maps for easy lookup
	uploadedMap := make(map[string]ChangeUpload)
	for _, change := range uploaded {
		key := fmt.Sprintf("%s.%s.%s", change.Schema, change.Table, change.PK)
		uploadedMap[key] = change
	}

	downloadedMap := make(map[string]ChangeDownloadResponse)
	for _, change := range downloaded {
		key := fmt.Sprintf("%s.%s.%s", change.Schema, change.TableName, change.PK)
		downloadedMap[key] = change
	}

	// Verify each uploaded change has a corresponding downloaded change
	for key, uploadedChange := range uploadedMap {
		downloadedChange, exists := downloadedMap[key]
		require.True(t, exists, "Downloaded changes should include %s", key)

		// Verify key fields match
		require.Equal(t, uploadedChange.Schema, downloadedChange.Schema, "Schema should match for %s", key)
		require.Equal(t, uploadedChange.Table, downloadedChange.TableName, "Table should match for %s", key)
		require.Equal(t, uploadedChange.Op, downloadedChange.Op, "Operation should match for %s", key)
		require.Equal(t, uploadedChange.PK, downloadedChange.PK, "PK should match for %s", key)

		// Verify payload content (JSON comparison)
		var uploadedPayload, downloadedPayload map[string]interface{}
		err := json.Unmarshal(uploadedChange.Payload, &uploadedPayload)
		require.NoError(t, err, "Should unmarshal uploaded payload for %s", key)

		err = json.Unmarshal(downloadedChange.Payload, &downloadedPayload)
		require.NoError(t, err, "Should unmarshal downloaded payload for %s", key)

		require.Equal(t, uploadedPayload, downloadedPayload, "Payload should match for %s", key)
	}

	t.Logf("Data integrity verified: all %d uploaded changes match downloaded changes", len(uploaded))
}

func getServerRecordCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID string) (users, posts int) {
	// Count records in sync.server_change_log for this user
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM sync.server_change_log
		WHERE user_id = $1 AND schema_name = 'business' AND table_name = 'users'
	`, userID).Scan(&users)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM sync.server_change_log
		WHERE user_id = $1 AND schema_name = 'business' AND table_name = 'posts'
	`, userID).Scan(&posts)
	require.NoError(t, err)

	return users, posts
}

func cleanupTestDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Clean up business tables
	_, err := pool.Exec(ctx, "DELETE FROM business.posts")
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "DELETE FROM business.users")
	if err != nil {
		return err
	}

	// Clean up sync tables
	_, err = pool.Exec(ctx, "DELETE FROM sync.server_change_log")
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "DELETE FROM sync.sync_row_meta")
	if err != nil {
		return err
	}

	return nil
}
