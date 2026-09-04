package oversync

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type phase3RowFunc func(...any) error

func (fn phase3RowFunc) Scan(dest ...any) error { return fn(dest...) }

type phase3Tx struct {
	pgx.Tx
	row pgx.Row
}

func (tx *phase3Tx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }

func TestPhase3GenerationDiscoveryDecodeBoundary(t *testing.T) {
	valid := `{"protocol_version":"v1","schema_generation":{"id":"1","fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tables":[]},"limits":{"max_push_rows":10000,"max_push_decoded_bytes":16777216,"max_pull_bundles":5000,"max_pull_decoded_bytes":33554432,"max_snapshot_rows_per_chunk":2000,"max_snapshot_decoded_bytes_per_chunk":8388608,"max_snapshot_rows":50000,"max_snapshot_logical_bytes":134217728,"max_row_decoded_bytes":1048576},"watch_enabled":true}`
	var response GenerationDiscoveryResponse
	require.NoError(t, json.Unmarshal([]byte(valid), &response))
	require.Equal(t, SyncProtocolVersion, response.ProtocolVersion)
	require.Equal(t, "1", response.SchemaGeneration.ID)
	require.Equal(t, 10000, response.Limits.MaxPushRows)
	require.EqualValues(t, 16777216, response.Limits.MaxPushDecodedBytes)
	require.True(t, response.WatchEnabled)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(valid), &document))
	delete(document, "limits")
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	response = GenerationDiscoveryResponse{}
	require.NoError(t, json.Unmarshal(encoded, &response))
	require.Zero(t, response.Limits)
}

func TestPhase3SourceRetiredJSONPresenceDistinctions(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "absent", body: `{"error":"source_retired","message":"retired","source_id":"source-a"}`},
		{name: "present null", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":null}`, wantErr: true},
		{name: "present empty", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":""}`, wantErr: true},
		{name: "valid same", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-a"}`},
		{name: "valid different", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-b"}`},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var response SourceRetiredResponse
			err := json.Unmarshal([]byte(testCase.body), &response)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestPhase3SourceRetiredWireMessageIsFixedAndRedacted(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	handler.writeSourceRetired(recorder, nil, "request-source", &SourceRetiredError{
		UserID:             "private-user",
		SourceID:           "request-source",
		ReplacedBySourceID: "replacement-source",
	})

	require.Equal(t, http.StatusConflict, recorder.Code)
	var response SourceRetiredResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "source is retired", response.Message)
	require.Equal(t, "request-source", response.SourceID)
	require.False(t, strings.Contains(response.Message, "private-user"))
	require.False(t, strings.Contains(response.Message, "request-source"))
	require.False(t, strings.Contains(response.Message, "replacement-source"))
}

func TestPhase3GenerationOrSourceLifecycleWriterMapsRetiredSource(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	require.True(t, handler.writeGenerationOrSourceLifecycleError(recorder, nil, "request-source", &SourceRetiredError{
		UserID:             "private-user",
		SourceID:           "request-source",
		ReplacedBySourceID: "replacement-source",
	}))

	require.Equal(t, http.StatusConflict, recorder.Code)
	var response SourceRetiredResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "source_retired", response.Error)
	require.Equal(t, "request-source", response.SourceID)
	require.Equal(t, "replacement-source", response.ReplacedBySourceID)
}

func TestPhase3SourceRetiredWriterRejectsMismatchedRequestSource(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	handler.writeSourceRetired(recorder, nil, "request-source", &SourceRetiredError{
		UserID:   "private-user",
		SourceID: "different-source",
	})

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "request-source")
	require.NotContains(t, recorder.Body.String(), "different-source")
	require.NotContains(t, recorder.Body.String(), "private-user")
}

func TestPhase3TerminalSourceErrorsAreRedacted(t *testing.T) {
	const (
		userSentinel   = "private-user-sentinel"
		sourceSentinel = "private-source-sentinel"
	)
	errorsToCheck := []error{
		&SourceTupleHistoryPrunedError{UserID: userSentinel, SourceID: sourceSentinel, SourceBundleID: 1, MaxCommittedSourceBundleIDHint: 2},
		&SourceSequenceOutOfOrderError{UserID: userSentinel, SourceID: sourceSentinel, Expected: 2, Actual: 3},
		&SourceSequenceChangedError{UserID: userSentinel, SourceID: sourceSentinel, Expected: 2, Actual: 1},
	}
	for _, err := range errorsToCheck {
		require.NotContains(t, err.Error(), userSentinel)
		require.NotContains(t, err.Error(), sourceSentinel)
	}
}

func TestPhase3PersistedAndNotificationSourceIDsFailClosed(t *testing.T) {
	invalidSourceID := "private source"

	t.Run("scope_state initializer", func(t *testing.T) {
		tx := &phase3Tx{row: phase3RowFunc(func(dest ...any) error {
			*dest[0].(*int64) = 1
			*dest[1].(*string) = "user"
			*dest[2].(*int16) = scopeStateCodeInitializing
			*dest[3].(*sql.NullString) = sql.NullString{String: invalidSourceID, Valid: true}
			*dest[4].(*sql.NullString) = sql.NullString{String: "00000000-0000-0000-0000-000000000001", Valid: true}
			*dest[5].(*sql.NullTime) = sql.NullTime{Time: time.Now().Add(time.Minute), Valid: true}
			*dest[6].(*sql.NullTime) = sql.NullTime{}
			*dest[7].(*sql.NullString) = sql.NullString{}
			return nil
		})}
		_, err := loadScopeStateForUpdate(context.Background(), tx, "user")
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})

	t.Run("bundle_log", func(t *testing.T) {
		tx := &phase3Tx{row: phase3RowFunc(func(dest ...any) error {
			planned := int64(1)
			*dest[0].(*int64) = 1
			*dest[1].(*string) = invalidSourceID
			*dest[2].(*int64) = 1
			*dest[3].(*int64) = 0
			*dest[4].(**int64) = &planned
			*dest[5].(*[]byte) = []byte{1}
			*dest[6].(*string) = strings.Repeat("a", 64)
			return nil
		})}
		_, err := loadCommittedPushMetadataBySourceTuple(context.Background(), tx, 1, "request-source", 1)
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})

	t.Run("notification", func(t *testing.T) {
		service := &SyncService{}
		payload := `{"user_pk":1,"bundle_seq":1,"source_id":"private source","source_bundle_id":1}`
		err := service.publishBundleChangeNotification(payload)
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})
}
