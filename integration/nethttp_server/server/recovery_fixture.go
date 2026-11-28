package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

// RecoveryFixtureHandler is installed only by the isolated category fixture
// executable. These predefined synthetic fault controls are not product routes.
func RecoveryFixtureHandler(authority *TestServer) http.Handler {
	return BrowserTestCORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/__recovery-fixture/"
		if !strings.HasPrefix(r.URL.Path, prefix) {
			authority.Handler.ServeHTTP(w, r)
			return
		}
		var input struct {
			Scope  string `json:"scope"`
			Source string `json:"source"`
			Key    string `json:"key"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if r.Method != http.MethodPost || decoder.Decode(&input) != nil || input.Scope == "" || len(input.Scope) > 256 {
			http.Error(w, "invalid fixture request", http.StatusBadRequest)
			return
		}
		action := strings.TrimPrefix(r.URL.Path, prefix)
		boundary, err := mutateRecoveryFixture(r.Context(), authority, action, input.Scope, input.Source, input.Key)
		if err != nil {
			http.Error(w, "synthetic fixture action failed", http.StatusBadRequest)
			return
		}
		var raw json.RawMessage
		err = authority.Pool.QueryRow(r.Context(), `SELECT json_build_object(
			'history',(SELECT count(*) FROM sync.bundle_log WHERE user_pk=u.user_pk),
			'outcomes',(SELECT count(*) FROM sync.push_outcomes WHERE user_pk=u.user_pk),
			'categories',(SELECT count(*) FROM business.categories WHERE _sync_scope_id=u.user_id),
			'acknowledged',COALESCE((SELECT max(acknowledged_sequence) FROM sync.push_source_progress WHERE user_pk=u.user_pk),0),
			'bundle_seq',$2::bigint) FROM sync.user_state u WHERE user_id=$1`, input.Scope, boundary).Scan(&raw)
		if err != nil {
			http.Error(w, "synthetic fixture state failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
}

func mutateRecoveryFixture(ctx context.Context, authority *TestServer, action, scope, source, key string) (int64, error) {
	switch action {
	case "state":
		return 0, nil
	case "prune":
		return pruneActivitySnapshotHistory(ctx, authority, scope)
	case "retire":
		return 0, retireRecoveryFixtureSource(ctx, authority, scope, source)
	case "historical_bad", "historical_good", "post_snapshot_write":
		return writeCategoryRecoveryFixture(ctx, authority, scope, action, key)
	default:
		return 0, errors.New("unknown synthetic fixture action")
	}
}

// This deliberately exercises the same synthetic retirement/binding-loss fault
// as the accepted upload proof, without deleting the compact original ack fence.
func retireRecoveryFixtureSource(ctx context.Context, authority *TestServer, scope, source string) error {
	if source == "" {
		return errors.New("fixture source required")
	}
	tx, err := authority.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var user, outstanding int64
	if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1 FOR UPDATE`, scope).Scan(&user); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM sync.push_outcomes WHERE user_pk=$1 AND source_id=$2`, user, source).Scan(&outstanding); err != nil {
		return err
	}
	if outstanding != 0 {
		return errors.New("fixture cannot retire an outstanding outcome")
	}
	if _, err := tx.Exec(ctx, `UPDATE sync.source_state SET state='retired',replaced_by_source_id='recovery-successor',retirement_reason='recovery-test' WHERE user_pk=$1 AND source_id=$2`, user, source); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM sync.source_generation_bindings WHERE user_pk=$1 AND source_id=$2`, user, source); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func writeCategoryRecoveryFixture(ctx context.Context, authority *TestServer, scope, action, key string) (int64, error) {
	statements := map[string]string{
		"historical_bad":      `UPDATE business.categories SET name='collision' WHERE _sync_scope_id=$1`,
		"historical_good":     `UPDATE business.categories SET name=CASE WHEN id=$2 THEN 'right' ELSE 'left' END WHERE _sync_scope_id=$1`,
		"post_snapshot_write": `UPDATE business.categories SET name='post-T' WHERE _sync_scope_id=$1 AND id=$2`,
	}
	if action != "historical_bad" && key == "" {
		return 0, errors.New("fixture row key required")
	}
	id := uuid.New()
	result, err := oversync.NewScopeManager(authority.SyncService, oversync.ScopeManagerConfig{}).ExecWrite(ctx, scope,
		oversync.ScopeWriteOptions{WriterID: "recovery-fixture-trusted", RequireExistingScope: true, RetryableWriteOptions: oversync.RetryableWriteOptions{
			OperationID: id, OperationHash: sha256.Sum256(id[:]), OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)}},
		func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			args := []any{scope}
			if action != "historical_bad" {
				args = append(args, key)
			}
			_, err := tx.Exec(ctx, statements[action], args...)
			return err
		})
	if err != nil {
		return 0, err
	}
	return result.Bundle.BundleSeq, nil
}
