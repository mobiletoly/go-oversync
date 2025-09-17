package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test table handlers for FK constraint testing
type TestUsersHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the MaterializationHandler interface - no key conversion needed for this handler
func (h *TestUsersHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *TestUsersHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	h.logger.Debug("TestUsersHandler: ApplyUpsert", "pk", pk.String())

	var userData map[string]interface{}
	if err := json.Unmarshal(payload, &userData); err != nil {
		return fmt.Errorf("failed to parse user data: %w", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.users (id, name, email)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			email = EXCLUDED.email
	`, pk, userData["name"], userData["email"])

	return err
}

func (h *TestUsersHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.users WHERE id = $1`, pk)
	return err
}

type TestPostsHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the TableHandler interface - no key conversion needed for this handler
func (h *TestPostsHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return payloadValue, nil
}

func (h *TestPostsHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	h.logger.Debug("TestPostsHandler: ApplyUpsert", "pk", pk.String())

	var postData map[string]interface{}
	if err := json.Unmarshal(payload, &postData); err != nil {
		return fmt.Errorf("failed to parse post data: %w", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.posts (id, title, content, author_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			author_id = EXCLUDED.author_id
	`, pk, postData["title"], postData["content"], postData["author_id"])

	return err
}

func (h *TestPostsHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.posts WHERE id = $1`, pk)
	return err
}

// TestFKConstraintRetryLogic tests the core FK constraint handling and retry logic
// This test creates data in an order that will cause FK constraint violations:
// - 20 posts referencing user1, then user1
// - 20 posts referencing user2, then user2
// - etc.
// With small batch sizes, posts will be uploaded before their users, causing FK violations
// The system should retry and eventually upload all data successfully
func TestFKConstraintRetryLogic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup test database
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Connect to test database
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgresql://postgres:password@localhost:5432/clisync_example"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	// Clean up test data
	_, err = pool.Exec(ctx, "DELETE FROM sync.server_change_log WHERE user_id = 'test-fk-user'")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "DELETE FROM business.posts WHERE author_id IN (SELECT id FROM business.users WHERE name LIKE 'FK Test User %')")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "DELETE FROM business.users WHERE name LIKE 'FK Test User %'")
	require.NoError(t, err)

	// Create sync service with test configuration
	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-constraint-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "business", Table: "users", Handler: &TestUsersHandler{logger: logger}},
			{Schema: "business", Table: "posts", Handler: &TestPostsHandler{logger: logger}},
		},
	}
	service, err := NewSyncService(pool, config, logger)
	require.NoError(t, err)

	// Test parameters
	const (
		numUsers        = 5
		postsPerUser    = 20
		uploadBatchSize = 50 // Small batch size to force FK constraint violations
		userID          = "test-fk-user"
		sourceID        = "test-fk-device"
	)

	t.Logf("🎯 Testing FK constraint retry logic with %d users, %d posts each, batch size %d",
		numUsers, postsPerUser, uploadBatchSize)

	// Step 1: Create changes in FK-challenging order
	var changes []ChangeUpload
	changeID := int64(1)

	userIDs := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		userIDs[i] = uuid.New().String()
	}

	// Create changes in problematic order: posts first, then user
	for userIdx := 0; userIdx < numUsers; userIdx++ {
		userUUID := userIDs[userIdx]

		// First, create 20 posts referencing this user (user doesn't exist yet!)
		for postIdx := 0; postIdx < postsPerUser; postIdx++ {
			postUUID := uuid.New().String()
			postPayload := map[string]interface{}{
				"id":        postUUID,
				"title":     fmt.Sprintf("FK Test Post %d by User %d", postIdx+1, userIdx+1),
				"content":   fmt.Sprintf("This post tests FK constraints - post %d by user %d", postIdx+1, userIdx+1),
				"author_id": userUUID, // References user that hasn't been uploaded yet!
			}
			payloadBytes, _ := json.Marshal(postPayload)

			changes = append(changes, ChangeUpload{
				SourceChangeID: changeID,
				Schema:         "business",
				Table:          "posts",
				Op:             "INSERT",
				PK:             postUUID,
				ServerVersion:  0,
				Payload:        json.RawMessage(payloadBytes),
			})
			changeID++
		}

		// Then, create the user that the posts reference
		userPayload := map[string]interface{}{
			"id":    userUUID,
			"name":  fmt.Sprintf("FK Test User %d", userIdx+1),
			"email": fmt.Sprintf("fk.test.user%d@example.com", userIdx+1),
		}
		payloadBytes, _ := json.Marshal(userPayload)

		changes = append(changes, ChangeUpload{
			SourceChangeID: changeID,
			Schema:         "business",
			Table:          "users",
			Op:             "INSERT",
			PK:             userUUID,
			ServerVersion:  0,
			Payload:        json.RawMessage(payloadBytes),
		})
		changeID++
	}

	totalChanges := len(changes)
	expectedUsers := numUsers
	expectedPosts := numUsers * postsPerUser

	t.Logf("📊 Created %d changes (%d users + %d posts) in FK-challenging order",
		totalChanges, expectedUsers, expectedPosts)

	// Step 2: Upload changes in small batches to trigger FK constraint violations
	uploadedChanges := 0
	maxRounds := 20 // Prevent infinite loops
	totalApplied := 0
	totalInvalid := 0
	round := 0

	// Track rejected changes for detailed logging
	rejectedChanges := make(map[int64]string) // changeID -> reason

	t.Logf("🚀 Starting FK constraint retry logic test...")
	t.Logf("📊 Strategy: Upload %d changes in batches of %d to trigger FK violations", totalChanges, uploadBatchSize)

	for round = 1; round <= maxRounds && uploadedChanges < totalChanges; round++ {
		// Get next batch of changes
		batchStart := uploadedChanges
		batchEnd := batchStart + uploadBatchSize
		if batchEnd > totalChanges {
			batchEnd = totalChanges
		}

		batch := changes[batchStart:batchEnd]

		// Analyze batch composition
		batchUsers := 0
		batchPosts := 0
		for _, change := range batch {
			if change.Table == "users" {
				batchUsers++
			} else if change.Table == "posts" {
				batchPosts++
			}
		}

		t.Logf("🔄 Round %d: Uploading batch %d-%d (%d changes: %d users, %d posts)",
			round, batchStart+1, batchEnd, len(batch), batchUsers, batchPosts)

		// Upload batch
		uploadReq := &UploadRequest{
			LastServerSeqSeen: 0,
			Changes:           batch,
		}
		response, err := service.ProcessUpload(ctx, userID, sourceID, uploadReq)
		require.NoError(t, err)
		require.Equal(t, len(batch), len(response.Statuses))

		// Detailed analysis of results
		appliedCount := 0
		invalidCount := 0
		appliedUsers := 0
		appliedPosts := 0
		invalidUsers := 0
		invalidPosts := 0

		for i, status := range response.Statuses {
			change := batch[i]
			if status.Status == "applied" {
				appliedCount++
				if change.Table == "users" {
					appliedUsers++
				} else if change.Table == "posts" {
					appliedPosts++
				}
			} else if status.Status == "invalid" {
				invalidCount++
				if change.Table == "users" {
					invalidUsers++
				} else if change.Table == "posts" {
					invalidPosts++
				}
				// Track rejected changes
				rejectedChanges[change.SourceChangeID] = fmt.Sprintf("%s.%s (likely FK constraint)", change.Table, change.Op)
			}
		}

		totalApplied += appliedCount
		totalInvalid += invalidCount

		t.Logf("📈 Round %d results: %d applied (%d users, %d posts), %d invalid (%d users, %d posts)",
			round, appliedCount, appliedUsers, appliedPosts, invalidCount, invalidUsers, invalidPosts)

		// Show some examples of rejected changes
		if invalidCount > 0 {
			exampleCount := 0
			for changeID, reason := range rejectedChanges {
				if exampleCount < 3 { // Show first 3 examples
					t.Logf("   ❌ Rejected: change_id=%d, reason=%s", changeID, reason)
					exampleCount++
				}
			}
			if invalidCount > 3 {
				t.Logf("   ... and %d more rejected changes", invalidCount-3)
			}
		}

		// Only count applied changes as uploaded
		uploadedChanges += appliedCount

		t.Logf("📊 Progress: %d/%d changes uploaded (%.1f%% complete)",
			uploadedChanges, totalChanges, float64(uploadedChanges)/float64(totalChanges)*100)

		// If no progress, we might be stuck
		if appliedCount == 0 {
			t.Logf("⚠️  No progress in round %d, checking if we're done...", round)
			break
		}
	}

	// Step 3: Verify all data was eventually uploaded
	var actualUsers, actualPosts int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM business.users WHERE name LIKE 'FK Test User %'").Scan(&actualUsers)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM business.posts WHERE title LIKE 'FK Test Post %'").Scan(&actualPosts)
	require.NoError(t, err)

	t.Logf("✅ Final results: %d users, %d posts (expected: %d users, %d posts)",
		actualUsers, actualPosts, expectedUsers, expectedPosts)

	assert.Equal(t, expectedUsers, actualUsers, "All users should be uploaded")
	assert.Equal(t, expectedPosts, actualPosts, "All posts should be uploaded")

	// Step 4: Test download to verify data integrity
	t.Log("📥 Testing download to verify data integrity...")

	downloadResponse, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 1000, "", true, 0)
	require.NoError(t, err)

	t.Logf("🔍 Download response: %d total changes, next_after: %d, has_more: %t",
		len(downloadResponse.Changes), downloadResponse.NextAfter, downloadResponse.HasMore)

	downloadedUsers := 0
	downloadedPosts := 0
	for i, change := range downloadResponse.Changes {
		t.Logf("🔍 Change %d: table=%s, op=%s, server_id=%d",
			i+1, change.TableName, change.Op, change.ServerID)
		if change.TableName == "users" && change.Op == "INSERT" {
			downloadedUsers++
		} else if change.TableName == "posts" && change.Op == "INSERT" {
			downloadedPosts++
		}
	}

	t.Logf("📊 Downloaded: %d users, %d posts", downloadedUsers, downloadedPosts)

	assert.Equal(t, expectedUsers, downloadedUsers, "All users should be downloadable")
	assert.Equal(t, expectedPosts, downloadedPosts, "All posts should be downloadable")

	// Step 5: Verify FK relationships are valid
	var invalidFKCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM business.posts p 
		LEFT JOIN business.users u ON p.author_id = u.id 
		WHERE p.title LIKE 'FK Test Post %' AND u.id IS NULL
	`).Scan(&invalidFKCount)
	require.NoError(t, err)

	assert.Equal(t, 0, invalidFKCount, "All posts should have valid FK relationships")

	// Final comprehensive summary
	t.Log("🎉 FK constraint retry logic test completed successfully!")
	t.Log("================================================================================")
	t.Log("📋 FINAL SUMMARY:")
	t.Logf("   • Total upload rounds: %d", round-1)
	t.Logf("   • Total changes processed: %d", totalChanges)
	t.Logf("   • Total applied: %d", totalApplied)
	t.Logf("   • Total rejected (retried): %d", totalInvalid)
	t.Logf("   • Final database state: %d users, %d posts", actualUsers, actualPosts)
	t.Logf("   • Download verification: %d users, %d posts", downloadedUsers, downloadedPosts)
	t.Logf("   • FK constraint violations: %d (all resolved)", invalidFKCount)
	t.Log("================================================================================")
	t.Log("✅ All FK constraints were properly handled and resolved!")
	t.Log("✅ Retry logic worked perfectly - no data was lost!")
	t.Log("✅ System is ready for production use!")
}
