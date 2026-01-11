package oversync

import (
	"context"
	"log/slog"
	"testing"
)

// Ensure batch size overflow responds with batch_too_large and rejects the batch.
func TestProcessUpload_BatchTooLargeIsRejected(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			MaxUploadBatchSize: 1,
		},
		logger: slog.Default(),
	}

	req := &UploadRequest{
		Changes: []ChangeUpload{
			{SourceChangeID: 1, Schema: "public", Table: "t", Op: OpInsert, PK: "11111111-1111-1111-1111-111111111111"},
			{SourceChangeID: 2, Schema: "public", Table: "t", Op: OpInsert, PK: "22222222-2222-2222-2222-222222222222"},
		},
	}

	resp, err := svc.ProcessUpload(context.Background(), "user", "device", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("expected batch to be rejected when over limit")
	}
	for _, st := range resp.Statuses {
		reason := st.Invalid["reason"]
		if reason != ReasonBatchTooLarge {
			t.Fatalf("expected reason %s, got %v", ReasonBatchTooLarge, reason)
		}
	}
}
