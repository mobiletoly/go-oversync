// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBoundedJSONRequest_GzipAndLimits(t *testing.T) {
	identity := []byte(`{"source_bundle_id":1}`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(identity)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	request := httptest.NewRequest("POST", "/sync/push", bytes.NewReader(compressed.Bytes()))
	request.Header.Set("Content-Encoding", "gzip")
	var decoded map[string]any
	require.NoError(t, decodeBoundedJSONRequest(request, &decoded, 1024))
	require.Equal(t, float64(1), decoded["source_bundle_id"])

	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(append(compressed.Bytes(), []byte("trailing")...)))
	request.Header.Set("Content-Encoding", "gzip")
	err = decodeBoundedJSONRequest(request, &decoded, 1024)
	require.Equal(t, "invalid_gzip", err.(*boundedBodyError).code)

	concatenated := append([]byte(nil), compressed.Bytes()...)
	concatenated = append(concatenated, compressed.Bytes()...)
	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(concatenated))
	request.Header.Set("Content-Encoding", "gzip")
	err = decodeBoundedJSONRequest(request, &decoded, 1024)
	require.Equal(t, "invalid_gzip", err.(*boundedBodyError).code)

	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(compressed.Bytes()))
	request.Header.Add("Content-Encoding", "gzip")
	request.Header.Add("Content-Encoding", "identity")
	err = decodeBoundedJSONRequest(request, &decoded, 1024)
	require.Equal(t, "unsupported_content_encoding", err.(*boundedBodyError).code)

	request = httptest.NewRequest("POST", "/sync/push", strings.NewReader("not gzip"))
	request.Header.Set("Content-Encoding", "gzip")
	err = decodeBoundedJSONRequest(request, &decoded, 1024)
	require.Equal(t, "invalid_gzip", err.(*boundedBodyError).code)

	var expansion bytes.Buffer
	expansionWriter := gzip.NewWriter(&expansion)
	_, err = expansionWriter.Write([]byte(`{"pad":"` + strings.Repeat("A", 4096) + `"}`))
	require.NoError(t, err)
	require.NoError(t, expansionWriter.Close())
	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(expansion.Bytes()))
	request.Header.Set("Content-Encoding", "gzip")
	err = decodeBoundedJSONRequest(request, &decoded, 1024)
	require.Equal(t, "decoded_body_too_large", err.(*boundedBodyError).code)

	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(identity))
	err = decodeBoundedJSONRequest(request, &decoded, 4)
	require.Equal(t, "encoded_body_too_large", err.(*boundedBodyError).code)

	request = httptest.NewRequest("POST", "/sync/push", bytes.NewReader(identity))
	request.Header.Set("Content-Encoding", "identity")
	require.NoError(t, decodeBoundedJSONRequest(request, &decoded, 1024))

	for _, values := range [][]string{{""}, {"gzip", "gzip"}, {"gzip, identity"}, {"br"}, {" gzip"}} {
		request = httptest.NewRequest("POST", "/sync/push", strings.NewReader(strings.Repeat("x", 2048)))
		request.Header["Content-Encoding"] = values
		err = decodeBoundedJSONRequest(request, &decoded, 4)
		require.Equal(t, "unsupported_content_encoding", err.(*boundedBodyError).code, values)
	}
}

func TestWriteCompactPushError_MapsLogicalValidationToPushInvalid(t *testing.T) {
	handler := NewHTTPSyncHandlers(nil, slog.Default())
	request := httptest.NewRequest(http.MethodPost, "/sync/push", nil)
	recorder := httptest.NewRecorder()

	handler.writeCompactPushError(recorder, request, "source-a", &PushValidationError{Message: "hidden column"})

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var response ErrorResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "push_invalid", response.Error)
}

func TestWriteBoundedJSON_SelectsBeneficialGzipOnlyAtThreshold(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.Default()}
	request := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	recorder := httptest.NewRecorder()
	handler.writeBoundedJSON(recorder, request, http.StatusOK, map[string]string{"pad": strings.Repeat("A", 4096)}, 8192, "encode")
	require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))
	decoded, err := gzip.NewReader(bytes.NewReader(recorder.Body.Bytes()))
	require.NoError(t, err)
	var body map[string]string
	require.NoError(t, json.NewDecoder(decoded).Decode(&body))
	require.Equal(t, strings.Repeat("A", 4096), body["pad"])
	require.NoError(t, decoded.Close())

	recorder = httptest.NewRecorder()
	handler.writeBoundedJSON(recorder, request, http.StatusOK, map[string]string{"pad": "short"}, 8192, "encode")
	require.Empty(t, recorder.Header().Get("Content-Encoding"))

	request.Header.Set("Accept-Encoding", "gzip;q=0, identity")
	recorder = httptest.NewRecorder()
	handler.writeBoundedJSON(recorder, request, http.StatusOK, map[string]string{"pad": strings.Repeat("A", 4096)}, 8192, "encode")
	require.Empty(t, recorder.Header().Get("Content-Encoding"))
}

func TestWriteError_UsesTheSameRequestAwareJSONBoundary(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.Default()}
	request := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	recorder := httptest.NewRecorder()
	handler.writeError(recorder, http.StatusConflict, "test_error", strings.Repeat("A", 4096), request)
	require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))
	decoded, err := gzip.NewReader(bytes.NewReader(recorder.Body.Bytes()))
	require.NoError(t, err)
	var body ErrorResponse
	require.NoError(t, json.NewDecoder(decoded).Decode(&body))
	require.Equal(t, "test_error", body.Error)
	require.Equal(t, strings.Repeat("A", 4096), body.Message)
	require.NoError(t, decoded.Close())

	request.Header.Set("Accept-Encoding", "gzip;q=0")
	recorder = httptest.NewRecorder()
	handler.writeError(recorder, http.StatusConflict, "test_error", strings.Repeat("A", 4096), request)
	require.Empty(t, recorder.Header().Get("Content-Encoding"))
}

func TestSpecializedErrorsUseTheSameRequestAwareJSONBoundary(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.Default()}
	request := httptest.NewRequest(http.MethodGet, "/sync/generations/1", nil)
	request.Header.Set("Accept-Encoding", "gzip")

	recorder := httptest.NewRecorder()
	require.True(t, handler.writeGenerationError(recorder, request, &SchemaGenerationRequestError{
		Code:    SchemaGenerationUnknown,
		Message: strings.Repeat("A", 4096),
	}))
	require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))

	recorder = httptest.NewRecorder()
	handler.writeDiscoveryError(recorder, request, http.StatusConflict, "unknown_schema_generation", strings.Repeat("A", 4096), "upgrade_or_reconfigure_client", 1)
	require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))
}

func TestValidateJSONRequestContentType_RequiresJSONAndUTF8Parameters(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		contentType string
		wantError   bool
	}{
		{name: "json", contentType: "application/json"},
		{name: "utf8", contentType: "application/json; charset=UTF-8"},
		{name: "missing", wantError: true},
		{name: "wrong media", contentType: "text/json", wantError: true},
		{name: "wrong charset", contentType: "application/json; charset=latin1", wantError: true},
		{name: "unknown parameter", contentType: "application/json; profile=test", wantError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(`{}`))
			if testCase.contentType != "" {
				request.Header.Set("Content-Type", testCase.contentType)
			}
			err := validateJSONRequestContentType(request, false)
			if testCase.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCompactPushRequest_RequiresEnvelopeFields(t *testing.T) {
	for _, body := range []string{
		`{"canonical_request_hash":"` + strings.Repeat("a", 64) + `","rows":[]}`,
		`{"source_bundle_id":1,"rows":[]}`,
		`{"source_bundle_id":1,"canonical_request_hash":"` + strings.Repeat("a", 64) + `"}`,
	} {
		var request CompactPushRequest
		require.Error(t, decodeStrictJSONRequest(strings.NewReader(body), &request, true))
	}
}
