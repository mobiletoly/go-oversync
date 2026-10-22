//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// Audit reproducers assert the intended contract rather than the current
// implementation. A failing audit-tagged test is evidence for the matching
// OS-AUD finding; it must move into the default suite when that finding is
// remediated.

func TestAuditCanonicalJSON_PreservesExactNumbers(t *testing.T) {
	raw := json.RawMessage(`{"integer":"9007199254740993","decimal":"1234567890.123456789"}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, `{"decimal":"1234567890.123456789","integer":"9007199254740993"}`, string(canonical),
		"uniform numeric strings must survive JCS byte-for-byte")
}

func TestAuditPayloadExtractor_Int64RejectsFractionalNumbers(t *testing.T) {
	extractor, err := NewPayloadExtractor([]byte(`{"value":1.5}`))
	require.NoError(t, err)

	require.Nil(t, extractor.Int64Field("value"), "fractional JSON numbers must not be truncated to int64")
}

func TestAuditRetainedFloor_RejectsZeroCheckpointAfterPruning(t *testing.T) {
	err := enforceRetainedBundleFloor("audit-user", 0, 2)

	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr, "checkpoint zero cannot reconstruct state once history has been pruned")
}

func TestAuditActorMiddleware_PreservesExactVisibleASCIISourceToken(t *testing.T) {
	const (
		userID   = " audit-user "
		sourceID = "audit-source!~"
	)

	var actor Actor
	handler := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(context.Context) (string, error) {
			return userID, nil
		},
	})(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		actor, _ = ActorFromContext(request.Context())
	}))

	request := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	request.Header.Set(SourceIDHeader, sourceID)
	handler.ServeHTTP(httptest.NewRecorder(), request)

	require.Equal(t, userID, actor.UserID, "middleware must preserve the validated user id exactly")
	require.Equal(t, sourceID, actor.SourceID, "middleware must not silently normalize source_id")
}

func FuzzAuditCanonicalJSON_Idempotent(f *testing.F) {
	for _, seed := range []string{
		`null`,
		`{"b":2,"a":1}`,
		`{"integer":"9007199254740993"}`,
		`{"nested":[true,false,null,{"value":"text"}]}`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if !json.Valid([]byte(input)) {
			t.Skip()
		}
		first, err := canonicalJSON(json.RawMessage(input))
		if err != nil {
			// RFC 8785 has a narrower input domain than generic JSON. Inputs such
			// as non-finite binary64 conversions are valid JSON but must reject.
			return
		}
		second, err := canonicalJSON(first)
		if err != nil {
			t.Fatalf("canonicalize canonical JSON: %v", err)
		}
		if string(first) != string(second) {
			t.Fatalf("canonical JSON is not idempotent: first=%s second=%s", first, second)
		}
	})
}

func BenchmarkAuditCanonicalJSON(b *testing.B) {
	payload := json.RawMessage(`{"z":[{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","value":1234567890.123456789}],"a":{"enabled":true,"label":"audit"}}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		if _, err := canonicalJSON(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuditCommittedBundleHash(b *testing.B) {
	for _, rowCount := range []int{1, 100, 1_000, 5_000} {
		rows := make([]BundleRow, rowCount)
		for i := range rows {
			id := fmt.Sprintf("audit-%08d", i)
			rows[i] = BundleRow{
				Schema:     "audit",
				Table:      "records",
				Key:        SyncKey{"id": id},
				Op:         OpInsert,
				RowVersion: int64(i + 1),
				Payload:    json.RawMessage(fmt.Sprintf(`{"id":%q,"value":%d,"label":"benchmark"}`, id, i)),
			}
		}

		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := computeCommittedBundleHash(rows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
