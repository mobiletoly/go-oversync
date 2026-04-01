package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

func TestResetEndpointClearsExampleDatabase(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{})
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	defer ts.Close()

	ctx := context.Background()
	_, err = ts.SyncService.Connect(ctx, oversync.Actor{UserID: "test-user", SourceID: "device-a"}, &oversync.ConnectRequest{
		SourceID:            "device-a",
		HasLocalPendingRows: false,
	})
	if err != nil {
		t.Fatalf("initialize test-user scope: %v", err)
	}
	err = ts.SyncService.WithinSyncBundle(
		ctx,
		oversync.Actor{UserID: "test-user", SourceID: "device-a"},
		oversync.BundleSource{SourceID: "device-a", SourceBundleID: 1},
		func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
			INSERT INTO business.users(id, name, email)
			VALUES ('00000000-0000-0000-0000-000000000001', 'Ada', 'ada@example.com')
		`)
			return err
		},
	)
	if err != nil {
		t.Fatalf("seed user row: %v", err)
	}

	resp, err := http.Post(ts.URL()+"/test/reset", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("reset request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	var out struct {
		Status string `json:"status"`
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Status != "ok" {
		t.Fatalf("unexpected status payload: %+v", out)
	}
	if out.Schema != "business" {
		t.Fatalf("unexpected schema payload: %+v", out)
	}

	var businessRows int
	if err := ts.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM business.users`).Scan(&businessRows); err != nil {
		t.Fatalf("count business.users rows: %v", err)
	}
	if businessRows != 0 {
		t.Fatalf("expected reset business.users to be empty, got %d rows", businessRows)
	}

	var userStateRows int
	if err := ts.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.user_state`).Scan(&userStateRows); err != nil {
		t.Fatalf("count sync.user_state rows: %v", err)
	}
	if userStateRows != 0 {
		t.Fatalf("expected reset sync.user_state to be empty, got %d rows", userStateRows)
	}

	var captureTriggerExists bool
	if err := ts.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_trigger t
			JOIN pg_class c ON c.oid = t.tgrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'business'
			  AND c.relname = 'users'
			  AND t.tgname = $1
		)
	`, "oversync_bundle_capture_row").Scan(&captureTriggerExists); err != nil {
		t.Fatalf("query trigger existence: %v", err)
	}
	if !captureTriggerExists {
		t.Fatalf("expected oversync capture trigger to be reinstalled after reset")
	}

	var bundleLogRows int
	if err := ts.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleLogRows); err != nil {
		t.Fatalf("count sync.bundle_log rows: %v", err)
	}
	if bundleLogRows != 0 {
		t.Fatalf("expected reset sync.bundle_log to be empty, got %d rows", bundleLogRows)
	}
}
