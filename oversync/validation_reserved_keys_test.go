package oversync

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestValidateChange_AllowsDeletedAndServerVersionInPayload(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			MaxPayloadBytes: 0,
		},
		registeredTables: map[string]bool{
			"business.activity": true,
		},
	}

	payload, err := json.Marshal(map[string]any{
		"deleted":        true,
		"server_version": 123,
		"title":          "Example",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	change := &ChangeUpload{
		Schema:  "business",
		Table:   "activity",
		Op:      OpInsert,
		PK:      uuid.New().String(),
		Payload: payload,
	}

	if _, err := svc.validateChangeAndMaybeParsePayload(change, nil); err != nil {
		t.Fatalf("expected payload to be accepted, got error: %v", err)
	}
}
