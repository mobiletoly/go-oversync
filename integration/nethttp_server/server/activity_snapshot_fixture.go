package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	activityfixture "github.com/mobiletoly/go-oversync/oversqlite/testdata"
	"github.com/mobiletoly/go-oversync/oversync"
)

// NewActivitySnapshotTestServer is an isolated qualification authority. Its
// maintained synthetic schema never reads or modifies an application installation.
func NewActivitySnapshotTestServer() (*TestServer, error) {
	const schema = "business"
	generation, registered, err := activityfixture.Definition(schema)
	if err != nil {
		return nil, err
	}
	return NewTestServer(&ServerConfig{
		AppName: "activity-snapshot-fixture", JWTSecret: "synthetic-snapshot-fixture",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BusinessSchema: schema,
		ApplicationSchemaPrepared: true, RegisteredTables: registered, SchemaGenerations: []oversync.SchemaGeneration{generation},
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			if _, err := pool.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
				return err
			}
			if _, err := pool.Exec(ctx, activityfixture.PostgreSQLSchema(schema)); err != nil {
				return err
			}
			_, err := pool.Exec(ctx, `CREATE FUNCTION business.automatic_notes() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.notes='normalize' THEN NEW.notes='occupied';END IF;RETURN NEW;END $$;CREATE TRIGGER automatic_notes BEFORE UPDATE ON business.daily_log FOR EACH ROW EXECUTE FUNCTION business.automatic_notes()`)
			return err
		},
	})
}

// ActivitySnapshotFixtureHandler exposes only predefined synthetic mutations.
// It is used by the local qualification executable, never a product endpoint.
func ActivitySnapshotFixtureHandler(authority *TestServer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/__snapshot-fixture/change" {
			authority.Handler.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var input struct {
			Scope    string `json:"scope"`
			Scenario string `json:"scenario"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil {
			http.Error(w, "invalid fixture request", 400)
			return
		}
		parsed, err := uuid.Parse(input.Scope)
		if err != nil || parsed.String() != input.Scope {
			http.Error(w, "invalid fixture scope", 400)
			return
		}
		if input.Scenario == "history_prune" {
			boundary, err := pruneActivitySnapshotHistory(r.Context(), authority, input.Scope)
			if err != nil {
				http.Error(w, "synthetic pruning failed", 500)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"bundle_seq": boundary})
			return
		}
		statements, ok := activitySnapshotChange(input.Scenario)
		if !ok {
			http.Error(w, "unknown fixture case", 400)
			return
		}
		bundleSeq, err := applyActivitySnapshotChange(r.Context(), authority, input.Scope, statements)
		w.Header().Set("Content-Type", "application/json")
		if input.Scenario == "DT06_invalid" {
			var constraint *pgconn.PgError
			if !errors.As(err, &constraint) || constraint.Code != "23514" {
				http.Error(w, "expected synthetic CHECK refusal", 500)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"rejected": true})
			return
		}
		if err != nil {
			http.Error(w, "synthetic trusted write failed", 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"bundle_seq": bundleSeq})
	})
}

// applyActivitySnapshotChange owns the synthetic trusted-writer transaction;
// HTTP validation and expected refusal responses remain with the handler.
func applyActivitySnapshotChange(ctx context.Context, authority *TestServer, scope, statements string) (int64, error) {
	id := uuid.New()
	result, err := oversync.NewScopeManager(authority.SyncService, oversync.ScopeManagerConfig{}).ExecWrite(ctx, scope,
		oversync.ScopeWriteOptions{WriterID: "snapshot-fixture-trusted", RequireExistingScope: true, RetryableWriteOptions: oversync.RetryableWriteOptions{
			OperationID: id, OperationHash: sha256.Sum256(id[:]), OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)}},
		func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			for _, statement := range strings.Split(statements, ";") {
				if statement == "" {
					continue
				}
				if _, err := tx.Exec(ctx, statement, scope); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	return result.Bundle.BundleSeq, nil
}

func activitySnapshotChange(scenario string) (string, bool) {
	changes := map[string]string{
		"DT01":         `UPDATE business.value_registry SET numeric_value=74.5 WHERE _sync_scope_id=$1 AND key='body-weight';UPDATE business.profile SET essential_plan=false,unlock_codes='["retained text"]' WHERE _sync_scope_id=$1 AND name='profile-name'`,
		"DT02":         `UPDATE business.profile SET body_weight=74 WHERE _sync_scope_id=$1 AND name='profile-name'`,
		"DT03":         `UPDATE business.activity SET sched_write_ref_start_at=NULL,sched_write_ref_repeat_after_days=NULL,sched_write_ref_days_duration=NULL WHERE _sync_scope_id=$1 AND id='A';UPDATE business.activity SET sched_write_ref_start_at='date-a',sched_write_ref_repeat_after_days='repeat-a',sched_write_ref_days_duration='duration-a' WHERE _sync_scope_id=$1 AND id='B';UPDATE business.program_item_input_goal SET input_key='weight-transferred' WHERE _sync_scope_id=$1 AND id='G'`,
		"DT04":         `DELETE FROM business.activity WHERE _sync_scope_id=$1 AND id='B'`,
		"DT05":         `UPDATE business.daily_log SET parent_daily_log_id=NULL WHERE _sync_scope_id=$1 AND id='L2';UPDATE business.daily_log SET parent_daily_log_id='L2',notes='child now' WHERE _sync_scope_id=$1 AND id='L1'`,
		"DT06":         `UPDATE business.program_item_input_goal_revision SET effective_day='2026-12-31',policy_json='{"min":7.25}' WHERE _sync_scope_id=$1 AND id='R'`,
		"DT06_invalid": `UPDATE business.program_item_input_goal_revision SET effective_day='2026-1-01' WHERE _sync_scope_id=$1 AND id='R'`,
	}
	value, ok := changes[scenario]
	return value, ok
}

// Only the run-owned fixture scope is locked and pruned; business state,
// source identities and exact push outcomes are untouched.
func pruneActivitySnapshotHistory(ctx context.Context, authority *TestServer, scope string) (int64, error) {
	tx, err := authority.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var user, boundary int64
	if err = tx.QueryRow(ctx, `SELECT user_pk,next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1 FOR UPDATE`, scope).Scan(&user, &boundary); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM sync.bundle_log WHERE user_pk=$1`, user); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor=$2 WHERE user_pk=$1`, user, boundary); err != nil {
		return 0, err
	}
	return boundary, tx.Commit(ctx)
}
