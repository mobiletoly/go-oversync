package oversync

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Deferrable Foreign Key Tests - validates FK safety in sidecar materializer

func TestDFK01_ChildBeforeParentInSameBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Setup business tables with foreign key
	h.setupBusinessTablesWithFK()

	// DFK01 - Child before parent in same batch
	// Given: Client uploads post (child) before user (parent) in same batch
	// Then: Materializer succeeds with deferrable constraints

	userID := h.MakeUUID("user001")
	postID := h.MakeUUID("post001")

	// Upload changes in "wrong" order: child before parent
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			// Post first (child)
			{
				SourceChangeID: 1,
				Table:          "posts",
				Op:             "INSERT",
				PK:             postID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "My First Post",
					"content": "Hello World",
					"author_id": "%s",
					"created_at": "%s",
					"updated_at": "%s"
				}`, postID.String(), userID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
			// User second (parent)
			{
				SourceChangeID: 2,
				Table:          "users",
				Op:             "INSERT",
				PK:             userID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"name": "John Doe",
					"email": "john@example.com",
					"created_at": "%s",
					"updated_at": "%s"
				}`, userID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
		},
	}

	uploadResp, httpResp := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, 200, httpResp.StatusCode)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, "applied", uploadResp.Statuses[0].Status)
	require.Equal(t, "applied", uploadResp.Statuses[1].Status)

	// Verify both records exist in sidecar tables
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 2)

	// Verify test materializer can handle this with deferrable constraints
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	materializer := NewTestMaterializer(h.service.Pool(), logger)
	err := materializer.MaterializeBatch(h.ctx, downloadResp.Changes)
	require.NoError(t, err)

	// Verify both records exist in business tables
	var userExists, postExists bool
	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&userExists)
	require.NoError(t, err)
	require.True(t, userExists)

	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM posts WHERE id = $1)", postID).Scan(&postExists)
	require.NoError(t, err)
	require.True(t, postExists)

	t.Logf("✅ DFK01 Child Before Parent test passed - deferrable constraints allow child-before-parent")
}

func TestDFK02_ParentDeleteWithSetNull(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Setup business tables with FK ON DELETE SET NULL
	h.setupBusinessTablesWithFK()

	// DFK02 - Parent delete with SET NULL
	// Given: User with posts exists, user is deleted
	// Then: Posts remain but author_id becomes NULL

	userID := h.MakeUUID("user002")
	postID := h.MakeUUID("post002")

	// First, create user and post
	uploadReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "users",
				Op:             "INSERT",
				PK:             userID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"name": "Jane Doe",
					"email": "jane@example.com",
					"created_at": "%s",
					"updated_at": "%s"
				}`, userID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
			{
				SourceChangeID: 2,
				Table:          "posts",
				Op:             "INSERT",
				PK:             postID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Jane's Post",
					"content": "Content by Jane",
					"author_id": "%s",
					"created_at": "%s",
					"updated_at": "%s"
				}`, postID.String(), userID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)

	// Materialize the initial data
	downloadResp1, _ := h.DoDownload(h.client2Token, 0, 100)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	materializer := NewTestMaterializer(h.service.Pool(), logger)
	err := materializer.MaterializeBatch(h.ctx, downloadResp1.Changes)
	require.NoError(t, err)

	// Now delete the user
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "users",
				Op:             "DELETE",
				PK:             userID.String(),
				ServerVersion:  1,
				Payload:        nil,
			},
		},
	}

	uploadResp2, _ := h.DoUpload(h.client1Token, uploadReq2)
	require.True(t, uploadResp2.Accepted)

	// Materialize the delete
	downloadResp2, _ := h.DoDownload(h.client2Token, 2, 100)
	err = materializer.MaterializeBatch(h.ctx, downloadResp2.Changes)
	require.NoError(t, err)

	// Verify user is deleted
	var userExists bool
	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&userExists)
	require.NoError(t, err)
	require.False(t, userExists)

	// Verify post still exists but author_id is NULL
	var postExists bool
	var authorID *string
	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM posts WHERE id = $1), author_id FROM posts WHERE id = $1", postID).Scan(&postExists, &authorID)
	require.NoError(t, err)
	require.True(t, postExists)
	require.Nil(t, authorID) // Should be NULL due to ON DELETE SET NULL

	t.Logf("✅ DFK02 Parent Delete with SET NULL test passed - FK constraint properly nullified child references")
}

func TestDFK03_ReparentingInSameBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Setup business tables with FK
	h.setupBusinessTablesWithFK()

	// DFK03 - Re-parenting in same batch
	// Given: Post exists with author A, batch contains: delete A, insert B, update post to author B
	// Then: All operations succeed with deferrable constraints

	userAID := h.MakeUUID("userA")
	userBID := h.MakeUUID("userB")
	postID := h.MakeUUID("post003")

	// Setup initial state: User A and Post
	setupReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "users",
				Op:             "INSERT",
				PK:             userAID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"name": "User A",
					"email": "usera@example.com",
					"created_at": "%s",
					"updated_at": "%s"
				}`, userAID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
			{
				SourceChangeID: 2,
				Table:          "posts",
				Op:             "INSERT",
				PK:             postID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Post",
					"content": "This post will change authors",
					"author_id": "%s",
					"created_at": "%s",
					"updated_at": "%s"
				}`, postID.String(), userAID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
		},
	}

	setupResp, _ := h.DoUpload(h.client1Token, setupReq)
	require.True(t, setupResp.Accepted)

	// Materialize setup
	downloadSetup, _ := h.DoDownload(h.client2Token, 0, 100)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	materializer := NewTestMaterializer(h.service.Pool(), logger)
	err := materializer.MaterializeBatch(h.ctx, downloadSetup.Changes)
	require.NoError(t, err)

	// Now perform re-parenting: delete A, insert B, update post to B
	reparentReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			// Delete User A
			{
				SourceChangeID: 3,
				Table:          "users",
				Op:             "DELETE",
				PK:             userAID.String(),
				ServerVersion:  1,
				Payload:        nil,
			},
			// Insert User B
			{
				SourceChangeID: 4,
				Table:          "users",
				Op:             "INSERT",
				PK:             userBID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"name": "User B",
					"email": "userb@example.com",
					"created_at": "%s",
					"updated_at": "%s"
				}`, userBID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
			// Update post to point to User B
			{
				SourceChangeID: 5,
				Table:          "posts",
				Op:             "UPDATE",
				PK:             postID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Post",
					"content": "This post now belongs to User B",
					"author_id": "%s",
					"created_at": "%s",
					"updated_at": "%s"
				}`, postID.String(), userBID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
		},
	}

	reparentResp, _ := h.DoUpload(h.client1Token, reparentReq)
	require.True(t, reparentResp.Accepted)

	// Materialize re-parenting batch
	downloadReparent, _ := h.DoDownload(h.client2Token, 2, 100)
	err = materializer.MaterializeBatch(h.ctx, downloadReparent.Changes)
	require.NoError(t, err)

	// Verify final state
	var userAExists, userBExists, postExists bool
	var postAuthorID string

	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userAID).Scan(&userAExists)
	require.NoError(t, err)
	require.False(t, userAExists) // User A should be deleted

	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userBID).Scan(&userBExists)
	require.NoError(t, err)
	require.True(t, userBExists) // User B should exist

	err = h.service.Pool().QueryRow(h.ctx, "SELECT EXISTS(SELECT 1 FROM posts WHERE id = $1), author_id FROM posts WHERE id = $1", postID).Scan(&postExists, &postAuthorID)
	require.NoError(t, err)
	require.True(t, postExists)
	require.Equal(t, userBID.String(), postAuthorID) // Post should point to User B

	t.Logf("✅ DFK03 Re-parenting in Same Batch test passed - complex FK operations succeed with deferrable constraints")
}

func TestDFK04_ServerFiltersOrphanChild(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// DFK04 - Server filters orphan child uploads
	// Given: Batch with invalid FK reference (orphaned child)
	// Then: Server marks the change invalid (fk_missing) and does NOT emit it to the download stream

	postID := h.MakeUUID("post004")
	nonExistentUserID := h.MakeUUID("nonexistent")

	// Upload post with non-existent author
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "posts",
				Op:             "INSERT",
				PK:             postID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Orphaned Post",
					"content": "This post has no valid author",
					"author_id": "%s",
					"created_at": "%s",
					"updated_at": "%s"
				}`, postID.String(), nonExistentUserID.String(),
					time.Now().Format(time.RFC3339),
					time.Now().Format(time.RFC3339))),
			},
		},
	}

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)
	require.Len(t, uploadResp.Statuses, 1)
	require.Equal(t, "invalid", uploadResp.Statuses[0].Status)

	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 0)

	t.Logf("✅ DFK04 Server filtered orphan child; upload marked invalid and no changes emitted to download")
}

func TestDFK05_PerformanceWithLargeBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Setup business tables with FK
	h.setupBusinessTablesWithFK()

	// DFK05 - Performance with large batch
	// Given: Large batch of interleaved parent/child operations
	// Then: Materializer completes within reasonable time

	batchSize := 100
	changes := make([]oversync.ChangeUpload, 0, batchSize*2)

	// Create interleaved users and posts
	for i := 0; i < batchSize; i++ {
		userID := h.MakeUUID(fmt.Sprintf("user%03d", i))
		postID := h.MakeUUID(fmt.Sprintf("post%03d", i))

		// Add post first (child before parent)
		changes = append(changes, oversync.ChangeUpload{
			SourceChangeID: int64(i*2 + 1),
			Table:          "posts",
			Op:             "INSERT",
			PK:             postID.String(),
			ServerVersion:  0,
			Payload: json.RawMessage(fmt.Sprintf(`{
				"id": "%s",
				"title": "Post %d",
				"content": "Content for post %d",
				"author_id": "%s",
				"created_at": "%s",
				"updated_at": "%s"
			}`, postID.String(), i, i, userID.String(),
				time.Now().Format(time.RFC3339),
				time.Now().Format(time.RFC3339))),
		})

		// Add user second (parent after child)
		changes = append(changes, oversync.ChangeUpload{
			SourceChangeID: int64(i*2 + 2),
			Table:          "users",
			Op:             "INSERT",
			PK:             userID.String(),
			ServerVersion:  0,
			Payload: json.RawMessage(fmt.Sprintf(`{
				"id": "%s",
				"name": "User %d",
				"email": "user%d@example.com",
				"created_at": "%s",
				"updated_at": "%s"
			}`, userID.String(), i, i,
				time.Now().Format(time.RFC3339),
				time.Now().Format(time.RFC3339))),
		})
	}

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}

	start := time.Now()
	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	uploadDuration := time.Since(start)

	require.True(t, uploadResp.Accepted)
	require.Len(t, uploadResp.Statuses, batchSize*2)

	// Materialize the batch
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 1000)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	materializer := NewTestMaterializer(h.service.Pool(), logger)

	start = time.Now()
	err := materializer.MaterializeBatch(h.ctx, downloadResp.Changes)
	materializeDuration := time.Since(start)

	require.NoError(t, err)

	// Verify all records were created
	var userCount, postCount int
	err = h.service.Pool().QueryRow(h.ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	require.NoError(t, err)
	require.Equal(t, batchSize, userCount)

	err = h.service.Pool().QueryRow(h.ctx, "SELECT COUNT(*) FROM posts").Scan(&postCount)
	require.NoError(t, err)
	require.Equal(t, batchSize, postCount)

	t.Logf("✅ DFK05 Performance test passed - %d changes uploaded in %v, materialized in %v",
		batchSize*2, uploadDuration, materializeDuration)

	// Performance assertion - should complete within reasonable time
	require.Less(t, materializeDuration, 5*time.Second, "Materialization should complete within 5 seconds")
}

// setupBusinessTablesWithFK creates business tables with deferrable foreign keys
func (h *SimpleTestHarness) setupBusinessTablesWithFK() {
	// Note: Tables for FK operations are registered in ServiceConfig during service creation
	// The SimpleTestHarness already includes "users" and "posts" tables in its config
	migrations := []string{
		// Drop existing tables to start fresh
		`DROP TABLE IF EXISTS posts CASCADE`,
		`DROP TABLE IF EXISTS users CASCADE`,
		// Users table
		`CREATE TABLE users (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		)`,
		// Posts table
		`CREATE TABLE posts (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id UUID,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		)`,
		// Create index for FK performance
		`CREATE INDEX idx_posts_author_id ON posts(author_id)`,
		// Add deferrable foreign key constraint
		`ALTER TABLE posts
		 ADD CONSTRAINT posts_author_fk
		 FOREIGN KEY (author_id)
		 REFERENCES users(id)
		 ON DELETE SET NULL
		 DEFERRABLE INITIALLY DEFERRED`,
	}

	err := pgx.BeginFunc(h.ctx, h.service.Pool(), func(tx pgx.Tx) error {
		for _, migration := range migrations {
			if _, err := tx.Exec(h.ctx, migration); err != nil {
				return fmt.Errorf("failed to create business table with FK: %w", err)
			}
		}
		return nil
	})
	require.NoError(h.t, err)

}
