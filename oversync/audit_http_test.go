//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newAuditPushHTTPFixture(t *testing.T) (*directPushFixture, string) {
	t.Helper()
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	initializationID := resolveConnectForPush(t, ctx, fixture.svc, fixture.writer, true)
	return fixture, initializationID
}

func auditCompactPushBody(t *testing.T, fixture *directPushFixture, initializationID string) string {
	t.Helper()
	row := fixture.userRow(uuid.New(), "Audit")
	proof := testSchemaGenerationProofForService(fixture.svc)
	contract, ok := fixture.svc.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	groups, err := CompactGroupsForRows(generationDescriptor(contract), []PushRequestRow{row})
	require.NoError(t, err)
	hash, err := computeGenerationPinnedPushRequestHash(proof, []PushRequestRow{row})
	require.NoError(t, err)
	body, err := json.Marshal(CompactPushRequest{
		SourceBundleID:       1,
		CanonicalRequestHash: hash,
		InitializationID:     initializationID,
		Rows:                 groups,
	})
	require.NoError(t, err)
	return string(body)
}

func runAuditPushRequest(
	t *testing.T,
	fixture *directPushFixture,
	body string,
	contentType string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(body))
	request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(ContextWithActor(request.Context(), fixture.writer))
	proof := testSchemaGenerationProofForService(fixture.svc)
	request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
	request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	handlers.HandlePush(recorder, request)
	return recorder
}

func TestAuditHTTPPush_RejectsUnknownFields(t *testing.T) {
	fixture, initializationID := newAuditPushHTTPFixture(t)
	body := strings.TrimSuffix(auditCompactPushBody(t, fixture, initializationID), "}") + `,"unknown":true}`

	recorder := runAuditPushRequest(t, fixture, body, "application/json")
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestAuditHTTPPush_RejectsTrailingJSONDocument(t *testing.T) {
	fixture, initializationID := newAuditPushHTTPFixture(t)
	body := auditCompactPushBody(t, fixture, initializationID) + ` {"extra":true}`

	recorder := runAuditPushRequest(t, fixture, body, "application/json")
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestAuditHTTPPush_RejectsUnsupportedContentType(t *testing.T) {
	fixture, initializationID := newAuditPushHTTPFixture(t)
	body := auditCompactPushBody(t, fixture, initializationID)

	recorder := runAuditPushRequest(t, fixture, body, "text/plain")
	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code)
}
