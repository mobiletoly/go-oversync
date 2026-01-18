package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// InitializeApplicationTables creates clean business tables for sidecar sync
func InitializeApplicationTables(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	// Create application tables in a single transaction using pgx.BeginFunc
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// Create business schema
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS business`); err != nil {
			return fmt.Errorf("failed to create business schema: %w", err)
		}

		// Create users table (clean business table - no sync metadata)
		createUsersSQL :=
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.users (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now()
)
`
		if _, err := tx.Exec(ctx, createUsersSQL); err != nil {
			return fmt.Errorf("failed to create users table: %w", err)
		}

		// Create posts table with inline FK constraint (like batch_test tables)
		createPostsSQL :=
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.posts (
	id UUID PRIMARY KEY,
	title TEXT NOT NULL,
	content TEXT NOT NULL,
	author_id UUID REFERENCES business.users(id) DEFERRABLE INITIALLY DEFERRED,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now()
)
`
		if _, err := tx.Exec(ctx, createPostsSQL); err != nil {
			return fmt.Errorf("failed to create posts table: %w", err)
		}

		createFfilesSQL :=
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.files (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	data BYTEA NOT NULL
)
`
		if _, err := tx.Exec(ctx, createFfilesSQL); err != nil {
			return fmt.Errorf("failed to create files table: %w", err)
		}

		createFileReviewsSQL :=
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.file_reviews (
	id UUID PRIMARY KEY,
	review TEXT NOT NULL,
	file_id UUID REFERENCES business.files(id) DEFERRABLE INITIALLY DEFERRED
)
`
		if _, err := tx.Exec(ctx, createFileReviewsSQL); err != nil {
			return fmt.Errorf("failed to create files table: %w", err)
		}

		logger.Info("Created posts table with inline FK constraint", "constraint", "author_id -> business.users(id)")

		// Create index for FK performance
		createIndexSQL := `CREATE INDEX IF NOT EXISTS idx_posts_author_id ON business.posts(author_id)`
		if _, err := tx.Exec(ctx, createIndexSQL); err != nil {
			return fmt.Errorf("failed to create posts author index: %w", err)
		}

		logger.Info("Clean business tables initialized successfully for sidecar sync")
		logger.Info("Sync metadata will be stored in sidecar tables: sync_row_meta, sync_state, server_change_log")
		return nil
	})
}

// UserData represents the structure of user data in the payload
type UserData struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt int64  `json:"created_at,omitempty"` // Unix timestamp
	UpdatedAt int64  `json:"updated_at,omitempty"` // Unix timestamp
}

// PostData represents the structure of post data in the payload
type PostData struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Content   string `json:"content"`
	AuthorID  string `json:"author_id"`
	CreatedAt int64  `json:"created_at,omitempty"` // Unix timestamp
	UpdatedAt int64  `json:"updated_at,omitempty"` // Unix timestamp
}

type FileData struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type FileReviewData struct {
	ID     string `json:"id"`
	Review string `json:"review"`
	FileID string `json:"file_id"`
}

// UsersTableHandler implements oversync.MaterializationHandler for the users table
// This handler materializes changes from sidecar tables to the business users table
type UsersTableHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the MaterializationHandler interface - converts base64 encoded UUIDs
func (h *UsersTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *UsersTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	//h.logger.Info("DEBUG: UsersTableHandler.ApplyUpsert called", "schema", schema, "table", table, "pk", pk.String())

	// Parse the payload
	var userData UserData
	if err := json.Unmarshal(payload, &userData); err != nil {
		return fmt.Errorf("failed to parse user data: %w", err)
	}

	updatedAt := time.Now()
	if userData.UpdatedAt > 0 {
		updatedAt = time.Unix(userData.UpdatedAt, 0)
	}

	// Materialize to business table (users table)
	// Use UPSERT to handle both INSERT and UPDATE cases idempotently
	createdAt := updatedAt
	if userData.CreatedAt > 0 {
		createdAt = time.Unix(userData.CreatedAt, 0)
	}

	//h.logger.Info("Materializing user to business table", "user_id", pk, "name", userData.Name)

	_, err := tx.Exec(ctx, `
		INSERT INTO business.users (id, name, email, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			email = EXCLUDED.email,
			updated_at = EXCLUDED.updated_at`,
		pk, userData.Name, userData.Email, createdAt, updatedAt)

	if err != nil {
		return fmt.Errorf("failed to materialize user to business table: %w", err)
	}

	//h.logger.Debug("User materialized successfully",
	//	"user_id", pk.String(),
	//	"name", userData.Name,
	//	"table", table)

	return nil
}

func (h *UsersTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	// Note: This is idempotent - deleting a non-existent row is not an error
	_, err := tx.Exec(ctx, `DELETE FROM business.users WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete user from business table: %w", err)
	}

	//h.logger.Debug("User deleted successfully", "user_id", pk.String(), "table", table)
	return nil
}

func (h *UsersTableHandler) ApplyUpsertBatch(ctx context.Context, tx pgx.Tx, schema, table string, upserts []oversync.MaterializeUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(upserts))
	names := make([]string, 0, len(upserts))
	emails := make([]string, 0, len(upserts))
	createdAts := make([]time.Time, 0, len(upserts))
	updatedAts := make([]time.Time, 0, len(upserts))

	for _, u := range upserts {
		var userData UserData
		if err := json.Unmarshal(u.Payload, &userData); err != nil {
			return fmt.Errorf("failed to parse user data: %w", err)
		}

		updatedAt := time.Now()
		if userData.UpdatedAt > 0 {
			updatedAt = time.Unix(userData.UpdatedAt, 0)
		}
		createdAt := updatedAt
		if userData.CreatedAt > 0 {
			createdAt = time.Unix(userData.CreatedAt, 0)
		}

		ids = append(ids, u.PK)
		names = append(names, userData.Name)
		emails = append(emails, userData.Email)
		createdAts = append(createdAts, createdAt)
		updatedAts = append(updatedAts, updatedAt)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.users (id, name, email, created_at, updated_at)
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[])
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			email = EXCLUDED.email,
			updated_at = EXCLUDED.updated_at`,
		ids, names, emails, createdAts, updatedAts)
	if err != nil {
		return fmt.Errorf("failed to materialize users to business table: %w", err)
	}

	return nil
}

func (h *UsersTableHandler) ApplyDeleteBatch(ctx context.Context, tx pgx.Tx, schema, table string, pks []uuid.UUID) error {
	if len(pks) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM business.users WHERE id = ANY($1::uuid[])`, pks)
	if err != nil {
		return fmt.Errorf("failed to delete users from business table: %w", err)
	}
	return nil
}

// PostsTableHandler implements oversync.MaterializationHandler for the posts table
// This handler materializes changes from sidecar tables to the business posts table
type PostsTableHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the MaterializationHandler interface - converts base64 encoded UUIDs
func (h *PostsTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *PostsTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	//h.logger.Info("🔥 DEBUG: PostsTableHandler.ApplyUpsert called", "schema", schema, "table", table, "pk", pk.String())

	// Parse the payload
	var postData PostData
	if err := json.Unmarshal(payload, &postData); err != nil {
		return fmt.Errorf("failed to parse post data: %w", err)
	}

	updatedAt := time.Now()
	if postData.UpdatedAt > 0 {
		updatedAt = time.Unix(postData.UpdatedAt, 0)
	}

	// Materialize to business table (posts table)
	// Use UPSERT to handle both INSERT and UPDATE cases idempotently
	createdAt := updatedAt
	if postData.CreatedAt > 0 {
		createdAt = time.Unix(postData.CreatedAt, 0)
	}

	//h.logger.Info("Materializing post to business table", "post_id", pk, "title", postData.Title)

	_, err := tx.Exec(ctx, `
		INSERT INTO business.posts (id, title, content, author_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			author_id = EXCLUDED.author_id,
			updated_at = EXCLUDED.updated_at`,
		pk, postData.Title, postData.Content, postData.AuthorID, createdAt, updatedAt)

	if err != nil {
		return fmt.Errorf("failed to materialize post to business table: %w", err)
	}

	//h.logger.Debug("Post materialized successfully",
	//	"post_id", pk.String(),
	//	"title", postData.Title,
	//	"table", table)

	return nil
}

func (h *PostsTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Delete from business table (materialization)
	// Note: This is idempotent - deleting a non-existent row is not an error
	_, err := tx.Exec(ctx, `DELETE FROM business.posts WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete post from business table: %w", err)
	}

	//h.logger.Debug("Post deleted successfully", "post_id", pk.String(), "table", table)
	return nil
}

func (h *PostsTableHandler) ApplyUpsertBatch(ctx context.Context, tx pgx.Tx, schema, table string, upserts []oversync.MaterializeUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(upserts))
	titles := make([]string, 0, len(upserts))
	contents := make([]string, 0, len(upserts))
	authorIDs := make([]uuid.UUID, 0, len(upserts))
	createdAts := make([]time.Time, 0, len(upserts))
	updatedAts := make([]time.Time, 0, len(upserts))

	for _, u := range upserts {
		var postData PostData
		if err := json.Unmarshal(u.Payload, &postData); err != nil {
			return fmt.Errorf("failed to parse post data: %w", err)
		}

		updatedAt := time.Now()
		if postData.UpdatedAt > 0 {
			updatedAt = time.Unix(postData.UpdatedAt, 0)
		}
		createdAt := updatedAt
		if postData.CreatedAt > 0 {
			createdAt = time.Unix(postData.CreatedAt, 0)
		}

		authorUUID, err := uuid.Parse(postData.AuthorID)
		if err != nil {
			return fmt.Errorf("failed to parse author_id: %w", err)
		}

		ids = append(ids, u.PK)
		titles = append(titles, postData.Title)
		contents = append(contents, postData.Content)
		authorIDs = append(authorIDs, authorUUID)
		createdAts = append(createdAts, createdAt)
		updatedAts = append(updatedAts, updatedAt)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.posts (id, title, content, author_id, created_at, updated_at)
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::uuid[], $5::timestamptz[], $6::timestamptz[])
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			author_id = EXCLUDED.author_id,
			updated_at = EXCLUDED.updated_at`,
		ids, titles, contents, authorIDs, createdAts, updatedAts)
	if err != nil {
		return fmt.Errorf("failed to materialize posts to business table: %w", err)
	}

	return nil
}

func (h *PostsTableHandler) ApplyDeleteBatch(ctx context.Context, tx pgx.Tx, schema, table string, pks []uuid.UUID) error {
	if len(pks) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM business.posts WHERE id = ANY($1::uuid[])`, pks)
	if err != nil {
		return fmt.Errorf("failed to delete posts from business table: %w", err)
	}
	return nil
}

type FilesTableHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the MaterializationHandler interface - converts base64 encoded UUIDs
func (h *FilesTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *FilesTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	//h.logger.Info("DEBUG: FilesTableHandler.ApplyUpsert called", "schema", schema, "table", table, "pk", pk.String())
	var fileData FileData
	if err := json.Unmarshal(payload, &fileData); err != nil {
		return fmt.Errorf("failed to parse file data: %w", err)
	}

	// Materialize to business table (files table)
	_, err := tx.Exec(ctx, `
		INSERT INTO business.files (id, name, data)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			data = EXCLUDED.data`,
		pk, fileData.Name, fileData.Data)

	if err != nil {
		return fmt.Errorf("failed to materialize file to business table: %w", err)
	}

	return nil
}

func (h *FilesTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.files WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete file from business table: %w", err)
	}
	return nil
}

func (h *FilesTableHandler) ApplyUpsertBatch(ctx context.Context, tx pgx.Tx, schema, table string, upserts []oversync.MaterializeUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(upserts))
	names := make([]string, 0, len(upserts))
	datas := make([][]byte, 0, len(upserts))

	for _, u := range upserts {
		var fileData FileData
		if err := json.Unmarshal(u.Payload, &fileData); err != nil {
			return fmt.Errorf("failed to parse file data: %w", err)
		}
		ids = append(ids, u.PK)
		names = append(names, fileData.Name)
		datas = append(datas, fileData.Data)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.files (id, name, data)
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::bytea[])
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			data = EXCLUDED.data`,
		ids, names, datas)
	if err != nil {
		return fmt.Errorf("failed to materialize files to business table: %w", err)
	}

	return nil
}

func (h *FilesTableHandler) ApplyDeleteBatch(ctx context.Context, tx pgx.Tx, schema, table string, pks []uuid.UUID) error {
	if len(pks) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM business.files WHERE id = ANY($1::uuid[])`, pks)
	if err != nil {
		return fmt.Errorf("failed to delete files from business table: %w", err)
	}
	return nil
}

type FileReviewsTableHandler struct {
	logger *slog.Logger
}

// ConvertReferenceKey implements the MaterializationHandler interface - converts base64 encoded UUIDs
func (h *FileReviewsTableHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *FileReviewsTableHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	//h.logger.Info("DEBUG: FileReviewsTableHandler.ApplyUpsert called", "schema", schema, "table", table, "pk", pk.String())
	var fileReviewData FileReviewData
	if err := json.Unmarshal(payload, &fileReviewData); err != nil {
		return fmt.Errorf("failed to parse file review data: %w", err)
	}

	// Materialize to business table (file_reviews table)
	fileID, err := oversync.ConvertBase64EncodedUUID(fileReviewData.FileID)
	if err != nil {
		return fmt.Errorf("failed to parse file ID: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO business.file_reviews (id, review, file_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			review = EXCLUDED.review,
			file_id = EXCLUDED.file_id`,
		pk, fileReviewData.Review, fileID)

	if err != nil {
		return fmt.Errorf("failed to materialize file review to business table: %w", err)
	}

	return nil
}

func (h *FileReviewsTableHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM business.file_reviews WHERE id = @id`, pgx.NamedArgs{"id": pk})
	if err != nil {
		return fmt.Errorf("failed to delete file review from business table: %w", err)
	}
	return nil
}

func (h *FileReviewsTableHandler) ApplyUpsertBatch(ctx context.Context, tx pgx.Tx, schema, table string, upserts []oversync.MaterializeUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(upserts))
	reviews := make([]string, 0, len(upserts))
	fileIDs := make([]uuid.UUID, 0, len(upserts))

	for _, u := range upserts {
		var fileReviewData FileReviewData
		if err := json.Unmarshal(u.Payload, &fileReviewData); err != nil {
			return fmt.Errorf("failed to parse file review data: %w", err)
		}
		fileID, err := oversync.ConvertBase64EncodedUUID(fileReviewData.FileID)
		if err != nil {
			return fmt.Errorf("failed to parse file ID: %w", err)
		}
		ids = append(ids, u.PK)
		reviews = append(reviews, fileReviewData.Review)
		fileIDs = append(fileIDs, fileID)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO business.file_reviews (id, review, file_id)
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::uuid[])
		ON CONFLICT (id) DO UPDATE SET
			review = EXCLUDED.review,
			file_id = EXCLUDED.file_id`,
		ids, reviews, fileIDs)
	if err != nil {
		return fmt.Errorf("failed to materialize file reviews to business table: %w", err)
	}

	return nil
}

func (h *FileReviewsTableHandler) ApplyDeleteBatch(ctx context.Context, tx pgx.Tx, schema, table string, pks []uuid.UUID) error {
	if len(pks) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM business.file_reviews WHERE id = ANY($1::uuid[])`, pks)
	if err != nil {
		return fmt.Errorf("failed to delete file reviews from business table: %w", err)
	}
	return nil
}
