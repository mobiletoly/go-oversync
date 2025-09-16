package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/auth"
	"github.com/mobiletoly/go-oversync/oversync"
)

// InitializeApplicationTables creates `business` schema + business tables (CamelCase via quoted identifiers).
func InitializeApplicationTables(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS business`); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		// "Person" - with user scoping
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.person(
  id UUID PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  first_name TEXT NOT NULL,
  last_name  TEXT NOT NULL,
  email      TEXT NOT NULL,
  phone      TEXT,
  birth_date TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  score      DOUBLE PRECISION,
  is_active  BOOLEAN NOT NULL DEFAULT true,
  ssn        BIGINT,
  notes      TEXT
)`); err != nil {
			return fmt.Errorf("create person: %w", err)
		}

		// Add user-scoped unique constraint for email
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE UNIQUE INDEX IF NOT EXISTS person_user_email_unique
ON business.person(owner_user_id, email)`); err != nil {
			return fmt.Errorf("create person email index: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.person_address(
  id UUID PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  person_id UUID NOT NULL REFERENCES business.person(id) DEFERRABLE INITIALLY DEFERRED,
  address_type TEXT NOT NULL,
  street TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT,
  postal_code TEXT,
  country TEXT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
)`); err != nil {
			return fmt.Errorf("create person_address: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.comment(
  id UUID PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  person_id UUID NOT NULL REFERENCES business.person(id) DEFERRABLE INITIALLY DEFERRED,
  comment TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  tags TEXT
)`); err != nil {
			return fmt.Errorf("create comment: %w", err)
		}
		// Helpful indexes - user-scoped
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_person_user_name ON business.person(owner_user_id, last_name, first_name)`); err != nil {
			logger.Warn("Failed to create person name index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_user_person ON business.person_address(owner_user_id, person_id)`); err != nil {
			logger.Warn("Failed to create address person index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_user_primary ON business.person_address(owner_user_id, person_id, is_primary)`); err != nil {
			logger.Warn("Failed to create address primary index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_comment_user_person ON business.comment(owner_user_id, person_id)`); err != nil {
			logger.Warn("Failed to create comment person index", "error", err)
			return err
		}
		logger.Info("Initialized business schema and tables")
		return nil
	})
}

// PersonHandler materializes payload into business."Person"
type PersonHandler struct{ logger *slog.Logger }

// ConvertReferenceKey implements the TableHandler interface - converts base64 encoded UUIDs
func (h *PersonHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *PersonHandler) ApplyUpsert(
	ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte,
) error {
	h.logger.Info("PersonHandler: Starting ApplyUpsert",
		"schema", schema, "table", table, "pk", pk.String(), "payload_size", len(payload))

	// Extract user ID from context - this is critical for user scoping
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		h.logger.Error("PersonHandler: User ID not found in context")
		return fmt.Errorf("user ID not found in context")
	}
	h.logger.Debug("PersonHandler: Extracted user ID", "user_id", userID)

	// Use PayloadExtractor for field extraction
	extractor, err := oversync.NewPayloadExtractor(payload)
	if err != nil {
		h.logger.Error("PersonHandler: Failed to parse payload", "error", err)
		return err
	}

	// Extract required fields
	fn, err := extractor.StrFieldRequired("first_name")
	if err != nil {
		h.logger.Error("PersonHandler: Missing first_name", "error", err)
		return fmt.Errorf("missing first_name: %w", err)
	}
	ln, err := extractor.StrFieldRequired("last_name")
	if err != nil {
		h.logger.Error("PersonHandler: Missing last_name", "error", err)
		return fmt.Errorf("missing last_name: %w", err)
	}
	email, err := extractor.StrFieldRequired("email")
	if err != nil {
		h.logger.Error("PersonHandler: Missing email", "error", err)
		return fmt.Errorf("missing email: %w", err)
	}

	// Extract optional fields
	phone := extractor.StrField("phone")
	birth := extractor.StrField("birth_date")
	notes := extractor.StrField("notes")

	h.logger.Debug("PersonHandler: Extracted all fields",
		"first_name", fn, "last_name", ln, "email", email,
		"phone", phone, "birth_date", birth, "notes", notes)

	h.logger.Info("PersonHandler: Executing SQL upsert", "user_id", userID)
	_, err = tx.Exec(ctx,
		/*language=postgresql*/ `
INSERT INTO business.person(id, owner_user_id, first_name, last_name, email, phone, birth_date, score, is_active, ssn, notes)
VALUES(@id, @user_id, @first_name, @last_name, @email, @phone, @birth_date, @score, @is_active, @ssn, @notes)
ON CONFLICT(id)
    DO UPDATE SET owner_user_id=EXCLUDED.owner_user_id,
                  first_name=EXCLUDED.first_name,
                  last_name=EXCLUDED.last_name,
                  email=EXCLUDED.email,
                  phone=EXCLUDED.phone,
                  birth_date=EXCLUDED.birth_date,
                  score=EXCLUDED.score,
                  is_active=EXCLUDED.is_active,
                  ssn=EXCLUDED.ssn,
                  notes=EXCLUDED.notes`,
		pgx.NamedArgs{
			"id": pk, "user_id": userID, "first_name": fn, "last_name": ln, "email": email,
			"phone": phone, "birth_date": birth,
			"score": extractor.Float64Field("score"), "is_active": extractor.BoolField("is_active"),
			"ssn": extractor.Int64Field("ssn"), "notes": notes,
		})
	if err != nil {
		h.logger.Error("PersonHandler: SQL upsert failed", "error", err, "user_id", userID)
		return fmt.Errorf("upsert person: %w", err)
	}

	h.logger.Info("PersonHandler: Successfully completed ApplyUpsert", "user_id", userID)
	return nil
}

func (h *PersonHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Extract user ID from context for user-scoped deletion
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		return fmt.Errorf("user ID not found in context")
	}

	_, err := tx.Exec(ctx, `DELETE FROM business.person WHERE id=@id AND owner_user_id=@user_id`,
		pgx.NamedArgs{"id": pk, "user_id": userID})
	return err
}

// PersonAddressHandler materializes payload into business.person_address
type PersonAddressHandler struct{ logger *slog.Logger }

// ConvertReferenceKey implements the TableHandler interface - convert base64 person_id to bytes
func (h *PersonAddressHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *PersonAddressHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	h.logger.Info("PersonAddressHandler: Starting ApplyUpsert",
		"schema", schema, "table", table, "pk", pk.String(), "payload_size", len(payload))

	// Extract user ID from context - this is critical for user scoping
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		h.logger.Error("PersonAddressHandler: User ID not found in context")
		return fmt.Errorf("user ID not found in context")
	}
	h.logger.Debug("PersonAddressHandler: Extracted user ID", "user_id", userID)

	// Use PayloadExtractor for field extraction
	extractor, err := oversync.NewPayloadExtractor(payload)
	if err != nil {
		h.logger.Error("PersonAddressHandler: Failed to parse payload", "error", err)
		return fmt.Errorf("parse payload: %w", err)
	}

	// Extract required fields
	pid, err := extractor.Base64FieldRequired("person_id")
	if err != nil {
		return err
	}
	at, err := extractor.StrFieldRequired("address_type")
	if err != nil {
		return err
	}
	street, err := extractor.StrFieldRequired("street")
	if err != nil {
		return err
	}
	city, err := extractor.StrFieldRequired("city")
	if err != nil {
		return err
	}
	country, err := extractor.StrFieldRequired("country")
	if err != nil {
		return err
	}

	// Extract optional fields
	state := extractor.StrField("state")
	postal := extractor.StrField("postal_code")
	isPrimary := extractor.BoolField("is_primary")
	_, err = tx.Exec(ctx, `
INSERT INTO business.person_address(id, owner_user_id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES(@id,@user_id,@pid,@at,@st,@ci,@stt,@pc,@co,@ip)
ON CONFLICT(id) DO UPDATE SET
 owner_user_id=EXCLUDED.owner_user_id,
 person_id=EXCLUDED.person_id,
 address_type=EXCLUDED.address_type,
 street=EXCLUDED.street,
 city=EXCLUDED.city,
 state=EXCLUDED.state,
 postal_code=EXCLUDED.postal_code,
 country=EXCLUDED.country,
 is_primary=EXCLUDED.is_primary`,
		pgx.NamedArgs{
			"id": pk, "user_id": userID, "pid": pid, "at": at, "st": street, "ci": city,
			"stt": state, "pc": postal, "co": country, "ip": isPrimary,
		})
	if err != nil {
		return fmt.Errorf("upsert address: %w", err)
	}
	return nil
}

func (h *PersonAddressHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Extract user ID from context for user-scoped deletion
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		return fmt.Errorf("user ID not found in context")
	}

	_, err := tx.Exec(ctx, `DELETE FROM business.person_address WHERE id=@id AND owner_user_id=@user_id`,
		pgx.NamedArgs{"id": pk, "user_id": userID})
	return err
}

// CommentHandler materializes payload into business."Comment"
type CommentHandler struct{ logger *slog.Logger }

// ConvertReferenceKey implements the TableHandler interface
// This implementation decodes base64-encoded UUID key
func (h *CommentHandler) ConvertReferenceKey(fieldName string, payloadValue any) (any, error) {
	return oversync.OptionallyConvertBase64EncodedUUID(payloadValue.(string))
}

func (h *CommentHandler) ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error {
	// Extract user ID from context - this is critical for user scoping
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		return fmt.Errorf("user ID not found in context")
	}

	// Use PayloadExtractor for field extraction
	extractor, err := oversync.NewPayloadExtractor(payload)
	if err != nil {
		return err
	}

	// Extract required fields
	pid, err := extractor.Base64FieldRequired("person_id")
	if err != nil {
		return err
	}
	txt, err := extractor.StrFieldRequired("comment")
	if err != nil {
		return err
	}

	// Extract optional fields
	tags := extractor.StrField("tags")
	_, err = tx.Exec(ctx, `
INSERT INTO business.comment(id, owner_user_id, person_id, comment, tags)
VALUES(@id,@user_id,@pid,@cm,@tg)
ON CONFLICT(id) DO UPDATE SET
 owner_user_id=EXCLUDED.owner_user_id,
 person_id=EXCLUDED.person_id,
 comment=EXCLUDED.comment,
 tags=EXCLUDED.tags`,
		pgx.NamedArgs{"id": pk, "user_id": userID, "pid": pid, "cm": txt, "tg": tags})
	if err != nil {
		return fmt.Errorf("upsert comment: %w", err)
	}
	return nil
}

func (h *CommentHandler) ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error {
	// Extract user ID from context for user-scoped deletion
	userID, ok := auth.GetUserID(ctx)
	if !ok {
		return fmt.Errorf("user ID not found in context")
	}

	_, err := tx.Exec(ctx, `DELETE FROM business.comment WHERE id=@id AND owner_user_id=@user_id`,
		pgx.NamedArgs{"id": pk, "user_id": userID})
	return err
}
