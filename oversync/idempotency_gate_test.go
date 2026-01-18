package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyGate_ErrorStatesVerifyTriplet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc, err := NewSyncService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "idempotency-gate-test",
	}, logger)
	require.NoError(t, err)
	defer svc.Close()

	t.Run("Upsert_MissingTriplet_ReturnsInternalError", func(t *testing.T) {
		suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
		userID := "test-idem-user-" + suffix
		sourceID := "test-idem-src-" + suffix
		scid := int64(42)

		conn, err := pgx.Connect(ctx, dbURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			CREATE OR REPLACE FUNCTION pg_temp.fail_apply_upsert(
				user_id text, schema_name text, table_name text, op text,
				pk uuid, payload jsonb, source_id text, source_change_id bigint, base_version bigint
			) RETURNS void AS $$
			BEGIN
				RAISE EXCEPTION 'forced' USING ERRCODE = '40001';
			END
		$$ LANGUAGE plpgsql`)
		require.NoError(t, err)

		_, err = tx.Prepare(ctx, stmtApplyUpsert, `SELECT pg_temp.fail_apply_upsert($1,$2,$3,$4,$5::uuid,$6::jsonb,$7,$8,$9)`)
		require.NoError(t, err)

		change := ChangeUpload{
			SourceChangeID: scid,
			Schema:         "public",
			Table:          "t",
			Op:             OpInsert,
			PK:             uuid.New().String(),
			Payload:        []byte(`{"id":"` + uuid.New().String() + `"}`),
			ServerVersion:  0,
		}

		st, applyErr := svc.applyUpsert(ctx, tx, userID, sourceID, change)
		require.NoError(t, applyErr)
		require.Equal(t, StInvalid, st.Status)
		require.Equal(t, ReasonInternalError, st.Invalid["reason"])
	})

	t.Run("Upsert_ExistingTriplet_ReturnsAppliedIdempotent", func(t *testing.T) {
		suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
		userID := "test-idem-user-" + suffix
		sourceID := "test-idem-src-" + suffix
		scid := int64(43)

		conn, err := pgx.Connect(ctx, dbURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			CREATE OR REPLACE FUNCTION pg_temp.fail_apply_upsert(
				user_id text, schema_name text, table_name text, op text,
				pk uuid, payload jsonb, source_id text, source_change_id bigint, base_version bigint
			) RETURNS void AS $$
			BEGIN
				RAISE EXCEPTION 'forced' USING ERRCODE = '40001';
			END
		$$ LANGUAGE plpgsql`)
		require.NoError(t, err)

		_, err = tx.Prepare(ctx, stmtApplyUpsert, `SELECT pg_temp.fail_apply_upsert($1,$2,$3,$4,$5::uuid,$6::jsonb,$7,$8,$9)`)
		require.NoError(t, err)

		pk := uuid.New().String()
		_, err = tx.Exec(ctx, `
			INSERT INTO sync.server_change_log
				(user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
			VALUES ($1,$2,$3,$4,$5::uuid,$6::jsonb,$7,$8,$9)
		`, userID, "public", "t", OpInsert, pk, `{"id":"`+pk+`"}`, sourceID, scid, int64(1))
		require.NoError(t, err)

		change := ChangeUpload{
			SourceChangeID: scid,
			Schema:         "public",
			Table:          "t",
			Op:             OpInsert,
			PK:             pk,
			Payload:        []byte(`{"id":"` + pk + `"}`),
			ServerVersion:  0,
		}

		st, applyErr := svc.applyUpsert(ctx, tx, userID, sourceID, change)
		require.NoError(t, applyErr)
		require.Equal(t, StApplied, st.Status)
		require.Nil(t, st.NewServerVersion)
	})

	t.Run("Delete_MissingTriplet_ReturnsInternalError", func(t *testing.T) {
		suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
		userID := "test-idem-user-" + suffix
		sourceID := "test-idem-src-" + suffix
		scid := int64(44)

		conn, err := pgx.Connect(ctx, dbURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			CREATE OR REPLACE FUNCTION pg_temp.fail_apply_delete(
				user_id text, schema_name text, table_name text, op text,
				pk uuid, source_id text, source_change_id bigint, server_version bigint
			) RETURNS void AS $$
			BEGIN
				RAISE EXCEPTION 'forced' USING ERRCODE = '40P01';
			END
		$$ LANGUAGE plpgsql`)
		require.NoError(t, err)

		_, err = tx.Prepare(ctx, stmtApplyDelete, `SELECT pg_temp.fail_apply_delete($1,$2,$3,$4,$5::uuid,$6,$7,$8)`)
		require.NoError(t, err)

		change := ChangeUpload{
			SourceChangeID: scid,
			Schema:         "public",
			Table:          "t",
			Op:             OpDelete,
			PK:             uuid.New().String(),
			ServerVersion:  0,
		}

		st, applyErr := svc.applyDelete(ctx, tx, userID, sourceID, change)
		require.NoError(t, applyErr)
		require.Equal(t, StInvalid, st.Status)
		require.Equal(t, ReasonInternalError, st.Invalid["reason"])
	})

	t.Run("Delete_ExistingTriplet_ReturnsAppliedIdempotent", func(t *testing.T) {
		suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
		userID := "test-idem-user-" + suffix
		sourceID := "test-idem-src-" + suffix
		scid := int64(45)

		conn, err := pgx.Connect(ctx, dbURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			CREATE OR REPLACE FUNCTION pg_temp.fail_apply_delete(
				user_id text, schema_name text, table_name text, op text,
				pk uuid, source_id text, source_change_id bigint, server_version bigint
			) RETURNS void AS $$
			BEGIN
				RAISE EXCEPTION 'forced' USING ERRCODE = '40P01';
			END
		$$ LANGUAGE plpgsql`)
		require.NoError(t, err)

		_, err = tx.Prepare(ctx, stmtApplyDelete, `SELECT pg_temp.fail_apply_delete($1,$2,$3,$4,$5::uuid,$6,$7,$8)`)
		require.NoError(t, err)

		pk := uuid.New().String()
		_, err = tx.Exec(ctx, `
			INSERT INTO sync.server_change_log
				(user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
			VALUES ($1,$2,$3,$4,$5::uuid,$6::jsonb,$7,$8,$9)
		`, userID, "public", "t", OpDelete, pk, nil, sourceID, scid, int64(1))
		require.NoError(t, err)

		change := ChangeUpload{
			SourceChangeID: scid,
			Schema:         "public",
			Table:          "t",
			Op:             OpDelete,
			PK:             pk,
			ServerVersion:  0,
		}

		st, applyErr := svc.applyDelete(ctx, tx, userID, sourceID, change)
		require.NoError(t, applyErr)
		require.Equal(t, StApplied, st.Status)
		require.Nil(t, st.NewServerVersion)
	})
}

func TestServerChangeLogHasTriplet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc, err := NewSyncService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "idempotency-gate-test",
	}, logger)
	require.NoError(t, err)
	defer svc.Close()

	userID := "test-idem-exists-" + strings.ReplaceAll(uuid.New().String(), "-", "")
	sourceID := "src-" + strings.ReplaceAll(uuid.New().String(), "-", "")
	scid := int64(1)

	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		exists, err := svc.serverChangeLogHasTriplet(ctx, tx, userID, sourceID, scid)
		require.NoError(t, err)
		require.False(t, exists)

		pk := uuid.New().String()
		_, err = tx.Exec(ctx, `
			INSERT INTO sync.server_change_log
				(user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
			VALUES ($1,$2,$3,$4,$5::uuid,$6::jsonb,$7,$8,$9)
		`, userID, "public", "t", OpInsert, pk, fmt.Sprintf(`{"id":"%s"}`, pk), sourceID, scid, int64(1))
		if err != nil {
			return err
		}

		exists, err = svc.serverChangeLogHasTriplet(ctx, tx, userID, sourceID, scid)
		require.NoError(t, err)
		require.True(t, exists)
		return nil
	}))
}
