package simulator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// FilesSyncScenario tests synchronization of files and file_reviews tables with BLOB primary keys
type FilesSyncScenario struct {
	*BaseScenario
}

// NewFilesSyncScenario creates a new files sync scenario
func NewFilesSyncScenario(simulator *Simulator) Scenario {
	return &FilesSyncScenario{
		BaseScenario: NewBaseScenario(simulator, "files-sync"),
	}
}

func (s *FilesSyncScenario) Name() string {
	return "Files Sync"
}

func (s *FilesSyncScenario) Description() string {
	return "Tests synchronization of files and file_reviews tables with BLOB primary keys and foreign key relationships"
}

func (s *FilesSyncScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Executing Files Sync Scenario")
	logger.Info("📱 This scenario tests BLOB primary key synchronization and foreign key relationships")

	// Phase 1: Sign in and prepare for testing
	logger.Info("📱 Phase 1: Sign in and prepare for files testing")

	if err := s.app.OnSignIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in: %w", err)
	}

	// Phase 2: Create files with BLOB data
	logger.Info("📱 Phase 2: Creating files with BLOB primary keys")

	fileIDs, err := s.createFiles()
	if err != nil {
		return fmt.Errorf("failed to create files: %w", err)
	}

	logger.Info("📱 Created files", "count", len(fileIDs))

	// Phase 3: Create file reviews with foreign key relationships
	logger.Info("📱 Phase 3: Creating file reviews with BLOB foreign keys")

	reviewIDs, err := s.createFileReviews(fileIDs)
	if err != nil {
		return fmt.Errorf("failed to create file reviews: %w", err)
	}

	logger.Info("📱 Created file reviews", "count", len(reviewIDs))

	// Phase 4: Sync to server
	logger.Info("📱 Phase 4: Syncing files and reviews to server")

	if err := s.app.PerformSyncUpload(ctx); err != nil {
		return fmt.Errorf("failed to upload files and reviews: %w", err)
	}

	// Phase 5: Test CASCADE delete by deleting a file
	logger.Info("📱 Phase 5: Testing CASCADE delete - removing first file")

	if len(fileIDs) > 0 {
		_, err := s.app.db.Exec(`DELETE FROM files WHERE id = ?`, fileIDs[0])
		if err != nil {
			return fmt.Errorf("failed to delete file: %w", err)
		}

		// Sync the delete operation
		if err := s.app.PerformSyncUpload(ctx); err != nil {
			return fmt.Errorf("failed to upload file deletion: %w", err)
		}

		logger.Info("📱 File deleted and synced - CASCADE should remove related reviews")
	}

	logger.Info("✅ Files sync scenario completed successfully")
	return nil
}

// createFiles creates test files with BLOB primary keys and binary data
func (s *FilesSyncScenario) createFiles() ([][]byte, error) {
	var fileIDs [][]byte

	files := []struct {
		name string
		data []byte
	}{
		{"document1.txt", []byte("This is the content of document 1")},
		{"image1.jpg", []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46}}, // JPEG header
		{"config.json", []byte(`{"setting1": "value1", "setting2": 42}`)},
		{"binary.dat", []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}},
	}

	for _, file := range files {
		// Generate proper UUID bytes for BLOB primary key (16 bytes)
		uuidObj := uuid.New()
		fileID, err := uuidObj.MarshalBinary() // This gives us the proper 16-byte UUID representation
		if err != nil {
			return nil, fmt.Errorf("failed to marshal UUID: %w", err)
		}

		// Insert file into database
		_, err = s.app.db.Exec(`
			INSERT INTO files (id, name, data) VALUES (?, ?, ?)
		`, fileID, file.name, file.data)

		if err != nil {
			return nil, fmt.Errorf("failed to insert file %s: %w", file.name, err)
		}

		fileIDs = append(fileIDs, fileID)
		s.simulator.GetLogger().Debug("Created file", "name", file.name, "size", len(file.data))
	}

	return fileIDs, nil
}

// createFileReviews creates test file reviews with BLOB foreign keys
func (s *FilesSyncScenario) createFileReviews(fileIDs [][]byte) ([][]byte, error) {
	var reviewIDs [][]byte

	reviews := []string{
		"Excellent document with clear structure",
		"Good image quality, needs better compression",
		"Configuration looks correct, all settings valid",
		"Binary data appears to be properly formatted",
		"Additional review for the first document",
		"Second opinion on the image file",
	}

	// Create multiple reviews for files (some files get multiple reviews)
	for i, review := range reviews {
		// Generate proper UUID bytes for BLOB primary key
		uuidObj := uuid.New()
		reviewID, err := uuidObj.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal review UUID: %w", err)
		}

		// Select file to review (cycle through files, some get multiple reviews)
		fileID := fileIDs[i%len(fileIDs)]

		// Insert file review into database
		_, err = s.app.db.Exec(`
			INSERT INTO file_reviews (id, file_id, review) VALUES (?, ?, ?)
		`, reviewID, fileID, review)

		if err != nil {
			return nil, fmt.Errorf("failed to insert file review: %w", err)
		}

		reviewIDs = append(reviewIDs, reviewID)
		s.simulator.GetLogger().Debug("Created file review", "review_length", len(review))
	}

	return reviewIDs, nil
}

func (s *FilesSyncScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔍 Verifying files sync scenario")

	// Verify files were synced to server (3 remaining after deletion)
	filesCount, err := verifier.CountRows("business.files")
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	if filesCount != 3 {
		return fmt.Errorf("expected 3 files (after deletion), got %d", filesCount)
	}

	// Verify file reviews were synced to server
	// The first file had 2 reviews (indices 0 and 4), so we should have 4 remaining reviews
	reviewsCount, err := verifier.CountRows("business.file_reviews")
	if err != nil {
		return fmt.Errorf("failed to count file reviews: %w", err)
	}

	if reviewsCount != 4 {
		return fmt.Errorf("expected 4 file reviews (after CASCADE delete), got %d", reviewsCount)
	}

	// Verify foreign key relationships are intact
	orphanedReviews, err := verifier.CountOrphanedReviews()
	if err != nil {
		return fmt.Errorf("failed to count orphaned reviews: %w", err)
	}

	if orphanedReviews > 0 {
		return fmt.Errorf("found %d orphaned file reviews", orphanedReviews)
	}

	logger.Info("✅ Files sync verification completed successfully")
	return nil
}
