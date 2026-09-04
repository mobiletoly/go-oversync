package oversync

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func doJSONHandlerRequest(
	t *testing.T,
	service *SyncService,
	handler func(http.ResponseWriter, *http.Request),
	method string,
	path string,
	actor Actor,
	body any,
	pathValues map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var requestBody []byte
	if body != nil {
		var err error
		requestBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(requestBody))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	setTestSchemaGenerationHeaders(req, service)
	req = req.WithContext(ContextWithActor(req.Context(), actor))
	for key, value := range pathValues {
		req.SetPathValue(key, value)
	}

	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func decodeErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()

	var response ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	return response
}

func decodePushConflictResponse(t *testing.T, rec *httptest.ResponseRecorder) PushConflictResponse {
	t.Helper()

	var response PushConflictResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	return response
}
