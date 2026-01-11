package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestShouldDropInvalid(t *testing.T) {
	cases := []struct {
		reason string
		drop   bool
	}{
		{oversync.ReasonBadPayload, true},
		{oversync.ReasonUnregisteredTable, true},
		{oversync.ReasonFKMissing, false},
		{oversync.ReasonBatchTooLarge, false},
		{oversync.ReasonPrecheckError, false},
	}

	for _, tc := range cases {
		status := oversync.ChangeUploadStatus{
			Status: oversync.StInvalid,
			Invalid: map[string]any{
				"reason": tc.reason,
			},
		}
		if got := shouldDropInvalid(status); got != tc.drop {
			t.Fatalf("reason %s: expected drop=%v got %v", tc.reason, tc.drop, got)
		}
	}
}

// Verify adaptive rechunking on batch_too_large responses.
func TestUploadChangesAdaptive_RechunksOnBatchTooLarge(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Business table required before client setup.
	if _, err := db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cfg := DefaultConfig("public", []SyncTable{{TableName: "items"}})
	token := func(ctx context.Context) (string, error) { return "token", nil }
	client, err := NewClient(db, "http://example.com", "user1", "source1", token, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	// Fake server that rejects batches larger than 2.
	var requestSizes []int
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != "POST" || r.URL.Path != "/sync/upload" {
			return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}

		var req oversync.UploadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		requestSizes = append(requestSizes, len(req.Changes))
		resp := oversync.UploadResponse{
			HighestServerSeq: 10,
			Statuses:         make([]oversync.ChangeUploadStatus, len(req.Changes)),
			Accepted:         true,
		}

		if len(req.Changes) > 2 {
			resp.Accepted = false
			for i, ch := range req.Changes {
				resp.Statuses[i] = oversync.ChangeUploadStatus{
					SourceChangeID: ch.SourceChangeID,
					Status:         oversync.StInvalid,
					Invalid: map[string]any{
						"reason": oversync.ReasonBatchTooLarge,
					},
				}
			}
		} else {
			for i, ch := range req.Changes {
				resp.Statuses[i] = oversync.ChangeUploadStatus{
					SourceChangeID:   ch.SourceChangeID,
					Status:           oversync.StApplied,
					NewServerVersion: intPtr(1),
				}
			}
		}

		return jsonResponse(resp), nil
	})}
	client.BaseURL = "http://example"

	changes := []oversync.ChangeUpload{
		{SourceChangeID: 1, Schema: "public", Table: "items", Op: "INSERT", PK: "1"},
		{SourceChangeID: 2, Schema: "public", Table: "items", Op: "INSERT", PK: "2"},
		{SourceChangeID: 3, Schema: "public", Table: "items", Op: "INSERT", PK: "3"},
	}

	if err := client.uploadChangesAdaptive(context.Background(), changes, 0, 1); err != nil {
		t.Fatalf("uploadChangesAdaptive: %v", err)
	}

	if len(requestSizes) < 2 {
		t.Fatalf("expected multiple requests after rechunking, got %v", requestSizes)
	}
	if requestSizes[0] != 3 {
		t.Fatalf("expected first request size 3, got %d", requestSizes[0])
	}
	if requestSizes[len(requestSizes)-1] > 2 {
		t.Fatalf("expected final request size <=2 after rechunking, got %d", requestSizes[len(requestSizes)-1])
	}
}

func intPtr(v int64) *int64 { return &v }
