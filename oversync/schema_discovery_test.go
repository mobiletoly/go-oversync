package oversync

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type mockHandler struct {
	convertValue any
	err          error
}

func (m *mockHandler) ApplyUpsert(_ context.Context, _ pgx.Tx, _ string, _ string, _ uuid.UUID, _ []byte) error {
	return nil
}

func (m *mockHandler) ApplyDelete(_ context.Context, _ pgx.Tx, _ string, _ string, _ uuid.UUID) error {
	return nil
}

func (m *mockHandler) ConvertReferenceKey(_ string, _ any) (any, error) {
	return m.convertValue, m.err
}

// Test helper to ensure FK conversion does not panic on non-string payloads.
func TestConvertFKValueForHandler_AllowsNonStringPayload(t *testing.T) {
	handler := &mockHandler{convertValue: 42}
	val, bad := convertFKValueForHandler(handler, "user_id", 42)
	if bad {
		t.Fatalf("expected non-bad payload")
	}
	if val != 42 {
		t.Fatalf("expected converted value to be preserved, got %v", val)
	}
}

func TestConvertFKValueForHandler_ReportsBadPayloadOnError(t *testing.T) {
	handler := &mockHandler{err: fmt.Errorf("parse error")}
	_, bad := convertFKValueForHandler(handler, "user_id", "abc")
	if !bad {
		t.Fatalf("expected bad payload flag on handler error")
	}
}
