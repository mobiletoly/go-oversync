//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Existing audit owners intentionally remain authoritative for overlapping
// Phase 5 evidence:
//   - TestAuditActorMiddleware_PreservesExactVisibleASCIISourceToken covers
//     exact valid source identities.
//   - TestAuditHTTPPush_RejectsUnknownFields,
//     TestAuditHTTPPush_RejectsTrailingJSONDocument, and
//     TestAuditHTTPPush_RejectsUnsupportedContentType cover the
//     corresponding direct-push decoder contracts.

func runAuditProtocolActorRequest(
	t *testing.T,
	userID string,
	sourceID string,
	includeSource bool,
) (*httptest.ResponseRecorder, Actor, bool) {
	t.Helper()

	var actor Actor
	called := false
	handler := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(context.Context) (string, error) {
			return userID, nil
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		called = true
		actor, _ = ActorFromContext(request.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	if includeSource {
		request.Header.Set(SourceIDHeader, sourceID)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder, actor, called
}

func TestAuditProtocolGreen_ActorMiddlewareRejectsMissingAndBlankActorIDs(t *testing.T) {
	tests := []struct {
		name          string
		userID        string
		sourceID      string
		includeSource bool
		wantStatus    int
	}{
		{name: "missing user", sourceID: "source", includeSource: true, wantStatus: http.StatusUnauthorized},
		{name: "blank user", userID: " \t ", sourceID: "source", includeSource: true, wantStatus: http.StatusUnauthorized},
		{name: "missing source", userID: "user", wantStatus: http.StatusBadRequest},
		{name: "blank source", userID: "user", sourceID: " \t ", includeSource: true, wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder, _, called := runAuditProtocolActorRequest(t, test.userID, test.sourceID, test.includeSource)
			require.Equal(t, test.wantStatus, recorder.Code)
			require.False(t, called)
		})
	}
}

func TestAuditProtocolGreen_ActorMiddlewarePreservesCaseDistinctTokens(t *testing.T) {
	tests := []struct {
		userID   string
		sourceID string
	}{
		{userID: "AuditUser", sourceID: "AuditSource"},
		{userID: "audituser", sourceID: "auditsource"},
	}

	actors := make([]Actor, 0, len(tests))
	for _, test := range tests {
		recorder, actor, called := runAuditProtocolActorRequest(t, test.userID, test.sourceID, true)
		require.Equal(t, http.StatusNoContent, recorder.Code)
		require.True(t, called)
		require.Equal(t, Actor{UserID: test.userID, SourceID: test.sourceID}, actor)
		actors = append(actors, actor)
	}
	require.NotEqual(t, actors[0], actors[1], "case-distinct exact tokens must remain distinct")
}

func TestAuditProtocolContract_ActorMiddlewareRejectsControlAndOversizedActorIDs(t *testing.T) {
	oversized := strings.Repeat("a", 64*1024+1)
	tests := []struct {
		name       string
		userID     string
		sourceID   string
		wantStatus int
	}{
		{name: "user control character", userID: "user\x01id", sourceID: "source", wantStatus: http.StatusUnauthorized},
		{name: "source control character", userID: "user", sourceID: "source\x01id", wantStatus: http.StatusBadRequest},
		{name: "oversized user", userID: oversized, sourceID: "source", wantStatus: http.StatusUnauthorized},
		{name: "oversized source", userID: "user", sourceID: oversized, wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder, _, called := runAuditProtocolActorRequest(t, test.userID, test.sourceID, true)
			assert.Equal(t, test.wantStatus, recorder.Code, "invalid actor identifiers must fail before handler dispatch")
			assert.False(t, called, "invalid actor identifiers must not reach the handler")
		})
	}
}

func TestAuditProtocolGreen_HTTPRejectsMalformedAndSnapshotUnknownJSON(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	actor := fixture.writer
	tests := []struct {
		name      string
		path      string
		body      string
		handle    func(http.ResponseWriter, *http.Request)
		wantError string
	}{
		{name: "push malformed", path: "/sync/push", body: `{`, handle: handlers.HandlePush, wantError: "invalid_request"},
		{name: "connect malformed", path: "/sync/connect", body: `{`, handle: handlers.HandleConnect, wantError: "invalid_request"},
		{name: "snapshot unknown field", path: "/sync/snapshot-sessions", body: `{"unknown":true}`, handle: handlers.HandleCreateSnapshotSession, wantError: "invalid_request"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			setTestSchemaGenerationHeaders(request, fixture.svc)
			request = request.WithContext(ContextWithActor(request.Context(), actor))
			recorder := httptest.NewRecorder()
			test.handle(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, test.wantError, decodeErrorResponse(t, recorder).Error)
		})
	}
}

func TestAuditProtocolGreen_PushAcceptsJSONMediaTypeWithCharset(t *testing.T) {
	fixture, _ := newAuditPushHTTPFixture(t)
	body := auditCompactPushBody(t, fixture, "")

	recorder := runAuditPushRequest(t, fixture, body, "application/json; charset=utf-8")
	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestAuditProtocolContract_PushRejectsDuplicateOrCaseCollidingFields(t *testing.T) {
	tests := []struct {
		name       string
		fieldBytes string
	}{
		{name: "duplicate key", fieldBytes: `"source_bundle_id":1,"source_bundle_id":1`},
		{name: "case-colliding key", fieldBytes: `"source_bundle_id":1,"SOURCE_BUNDLE_ID":1`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, _ := newAuditPushHTTPFixture(t)
			body := `{` + test.fieldBytes + `,"canonical_request_hash":"` + strings.Repeat("0", 64) + `","rows":[]}`

			recorder := runAuditPushRequest(t, fixture, body, "application/json")
			require.Equal(t, http.StatusBadRequest, recorder.Code, "ambiguous object fields must be rejected")
		})
	}
}

func TestAuditProtocolContract_SnapshotCreateRejectsTrailingJSONDocument(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	mustInitializeEmptyScope(t, ctx, fixture.svc, fixture.writer.UserID, fixture.writer.SourceID)

	request := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", strings.NewReader(`{} {"extra":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(request, fixture.svc)
	request = request.WithContext(ContextWithActor(request.Context(), fixture.writer))
	recorder := httptest.NewRecorder()
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	handlers.HandleCreateSnapshotSession(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, "snapshot creation must consume exactly one JSON document")
}

func TestAuditProtocolContract_PushRejectsDuplicateAndCaseCollidingFieldsBeforePersistence(t *testing.T) {
	tests := []struct {
		name          string
		payloadFields string
	}{
		{name: "duplicate request key", payloadFields: `"source_bundle_id":1,"source_bundle_id":1`},
		{name: "case-colliding request key", payloadFields: `"source_bundle_id":1,"SOURCE_BUNDLE_ID":1`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture, initializationID := newAuditPushHTTPFixture(t)
			body := `{` + test.payloadFields + `,"canonical_request_hash":"` + strings.Repeat("0", 64) + `","initialization_id":` + fmt.Sprintf("%q", initializationID) + `,"rows":[]}`

			recorder := runAuditPushRequest(t, fixture, body, "application/json")

			assert.Equal(t, http.StatusBadRequest, recorder.Code, "ambiguous JSON must be rejected before the direct push reaches storage")
			assert.Zero(t, fixture.businessUserCount(t, ctx), "rejected ambiguous JSON must leave no business rows")
		})
	}
}

func TestAuditProtocolGreen_MalformedPullCursorReturnsBadRequest(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	request := httptest.NewRequest(http.MethodGet, "/sync/pull?after_bundle_seq=not-an-integer", nil)
	request = request.WithContext(ContextWithActor(request.Context(), fixture.reader))
	setTestSchemaGenerationHeaders(request, fixture.svc)
	recorder := httptest.NewRecorder()

	handlers.HandlePull(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, "invalid_request", decodeErrorResponse(t, recorder).Error)
}

func TestAuditProtocolContract_MalformedUUIDPathIDsReturnBadRequest(t *testing.T) {
	tests := []struct {
		name   string
		handle func(*HTTPSyncHandlers, http.ResponseWriter, *http.Request)
		method string
		path   string
		key    string
	}{
		{
			name: "snapshot session",
			handle: func(handlers *HTTPSyncHandlers, w http.ResponseWriter, request *http.Request) {
				handlers.HandleGetSnapshotChunk(w, request)
			},
			method: http.MethodGet,
			path:   "/sync/snapshot-sessions/not-a-uuid",
			key:    "snapshot_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
			handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
			request := httptest.NewRequest(test.method, test.path, nil)
			request.SetPathValue(test.key, "not-a-uuid")
			request = request.WithContext(ContextWithActor(request.Context(), fixture.writer))
			recorder := httptest.NewRecorder()

			test.handle(handlers, recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, "malformed UUID path values must be classified as invalid input")
		})
	}
}

func TestAuditProtocolGreen_IdentifierAndSchemaTableParsersRejectUnsupportedShapes(t *testing.T) {
	identifierTests := []struct {
		name     string
		validate func(string) bool
	}{
		{name: "schema", validate: isValidSchemaName},
		{name: "table", validate: isValidTableName},
		{name: "column", validate: isValidColumnName},
	}
	for _, test := range identifierTests {
		t.Run(test.name, func(t *testing.T) {
			require.True(t, test.validate("audit_name_1"))
			for _, invalid := range []string{"", "Audit", "audit-name", "audit name", "audit.name", "audit\x00name", "é"} {
				require.Falsef(t, test.validate(invalid), "input %q must be rejected", invalid)
			}
		})
	}

	schemaTableTests := []struct {
		raw  string
		want string
		ok   bool
	}{
		{raw: "events", want: "public.events", ok: true},
		{raw: " AUDIT.EVENTS ", want: "audit.events", ok: true},
		{raw: "", ok: false},
		{raw: ".events", ok: false},
		{raw: "audit.", ok: false},
		{raw: "audit.events.extra", ok: false},
		{raw: "audit.event-name", ok: false},
	}
	for _, test := range schemaTableTests {
		got, ok := normalizeSchemaTableKey(test.raw)
		require.Equal(t, test.ok, ok, "input %q", test.raw)
		require.Equal(t, test.want, got, "input %q", test.raw)
	}
}

func newAuditProtocolParserService() *SyncService {
	const tableKey = "audit.records"
	return &SyncService{
		registeredTables: map[string]bool{tableKey: true},
		registeredTableInfo: map[string]registeredTableRuntimeInfo{
			tableKey: {
				schemaName:    "audit",
				tableName:     "records",
				tableID:       1,
				syncKeyColumn: "id",
				syncKeyType:   syncKeyTypeUUID,
			},
		},
		columnTypesByTable: map[string]map[string]string{
			tableKey: {"id": "uuid", "name": "text"},
		},
	}
}

func newAuditProtocolValidRow() PushRequestRow {
	id := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8").String()
	return PushRequestRow{
		Schema:         "audit",
		Table:          "records",
		Key:            SyncKey{"id": id},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"audit"}`, id)),
	}
}

func TestAuditProtocolGreen_PushParserRejectsMalformedIdentifiersAndSyncKeys(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PushRequestRow)
	}{
		{name: "invalid schema", mutate: func(row *PushRequestRow) { row.Schema = "audit-bad" }},
		{name: "invalid table", mutate: func(row *PushRequestRow) { row.Table = "records.bad" }},
		{name: "unregistered table", mutate: func(row *PushRequestRow) { row.Table = "other" }},
		{name: "invalid column", mutate: func(row *PushRequestRow) {
			row.Payload = json.RawMessage(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","bad-name":true}`)
		}},
		{name: "missing sync key", mutate: func(row *PushRequestRow) { row.Key = SyncKey{} }},
		{name: "non-string sync key", mutate: func(row *PushRequestRow) { row.Key = SyncKey{"id": 7} }},
		{name: "malformed UUID sync key", mutate: func(row *PushRequestRow) { row.Key = SyncKey{"id": "not-a-uuid"} }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newAuditProtocolParserService()
			row := newAuditProtocolValidRow()
			test.mutate(&row)

			_, err := service.preparePushRowsWithOptions([]PushRequestRow{row}, false)
			var validationErr *PushValidationError
			require.ErrorAs(t, err, &validationErr)
		})
	}
}

func TestAuditProtocolGreen_SyncKeyEncodingRejectsMalformedAndPreservesExactText(t *testing.T) {
	const canonicalUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	keyBytes, dbValue, err := encodeKeyBytes(syncKeyTypeUUID, canonicalUUID)
	require.NoError(t, err)
	require.Len(t, keyBytes, 16)
	require.Equal(t, uuid.MustParse(canonicalUUID), dbValue)
	decoded, decodedValue, err := decodeKeyBytes(syncKeyTypeUUID, keyBytes)
	require.NoError(t, err)
	require.Equal(t, canonicalUUID, decoded)
	require.Equal(t, dbValue, decodedValue)

	for _, malformed := range []string{"not-a-uuid", "6BA7B810-9DAD-11D1-80B4-00C04FD430C8", "6ba7b8109dad11d180b400c04fd430c8"} {
		_, _, err := encodeKeyBytes(syncKeyTypeUUID, malformed)
		require.Error(t, err, "UUID input %q must be rejected", malformed)
	}
	_, _, err = encodeKeyBytes("unsupported", canonicalUUID)
	require.Error(t, err)
	_, _, err = decodeKeyBytes(syncKeyTypeUUID, []byte("short"))
	require.Error(t, err)

	const exactText = " exact Text key "
	keyBytes, dbValue, err = encodeKeyBytes(syncKeyTypeText, exactText)
	require.NoError(t, err)
	require.Equal(t, []byte(exactText), keyBytes)
	require.Equal(t, exactText, dbValue)
	decoded, decodedValue, err = decodeKeyBytes(syncKeyTypeText, keyBytes)
	require.NoError(t, err)
	require.Equal(t, exactText, decoded)
	require.Equal(t, exactText, decodedValue)
}

func TestAuditProtocolContract_TextSyncKeyRejectsNULBeforePersistence(t *testing.T) {
	_, _, err := encodeKeyBytes(syncKeyTypeText, "invalid\x00key")
	require.Error(t, err, "text keys that PostgreSQL cannot represent must fail during request validation")
}

func TestAuditProtocolContract_PushParserRejectsUnknownColumnsBeforeStaging(t *testing.T) {
	service := newAuditProtocolParserService()
	row := newAuditProtocolValidRow()
	row.Payload = json.RawMessage(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"audit","unknown_column":true}`)

	_, err := service.preparePushRowsWithOptions([]PushRequestRow{row}, false)
	require.Error(t, err, "columns absent from the discovered registered-table shape must fail before staging")
}

func TestAuditProtocolContract_IdentifierValidatorsRejectPostgresOverlengthNames(t *testing.T) {
	overlength := strings.Repeat("a", 64)
	assert.False(t, isValidSchemaName(overlength))
	assert.False(t, isValidTableName(overlength))
	assert.False(t, isValidColumnName(overlength))
	_, ok := normalizeSchemaTableKey(overlength + ".records")
	assert.False(t, ok)
}
