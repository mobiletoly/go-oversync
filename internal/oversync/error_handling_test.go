package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Error Handling Tests (E01-E05) from spec section 5

func TestE01_InvalidJWTToken(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E01 – Invalid JWT token
	// Upload with invalid token → 401 Unauthorized.
	noteID := h.MakeUUID("000000000001")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Test Note",
					"content": "This should fail",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// Upload with invalid token
	invalidToken := "invalid.jwt.token"
	_, httpResp := h.DoUpload(invalidToken, uploadReq)

	// Should return 401 Unauthorized
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	t.Logf("✅ E01 Invalid JWT Token test passed - 401 Unauthorized returned")
}

func TestE02_MissingAuthHeader(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E02 – Missing auth header
	// Upload without Authorization header → 401 Unauthorized.
	noteID := h.MakeUUID("000000000001")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Test Note",
					"content": "This should fail",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// Upload with empty token (simulates missing Authorization header)
	_, httpResp := h.DoUpload("", uploadReq)

	// Should return 401 Unauthorized
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	t.Logf("✅ E02 Missing Auth Header test passed - 401 Unauthorized returned")
}

func TestE03_MalformedJSON(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E03 – Malformed JSON
	// Upload with malformed JSON → 400 Bad Request.

	// Create malformed JSON request manually
	malformedJSON := `{"source_id": "test", "changes": [{"source_change_id": 1, "table": "note", "op": "INSERT", "pk": "test", "payload": {malformed json}`

	body := []byte(malformedJSON)
	httpReq := h.createHTTPRequest("POST", "/sync/upload", body)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+h.client1Token)

	recorder := h.executeHTTPRequest(httpReq)

	// Should return 400 Bad Request
	require.Equal(t, http.StatusBadRequest, recorder.Code)

	t.Logf("✅ E03 Malformed JSON test passed - 400 Bad Request returned")
}

func TestE04_InvalidTableName(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E04 – Invalid table name
	// Upload with syntactically invalid table name → validation error
	noteID := h.MakeUUID("000000000001")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "123invalid-table!", // Syntactically invalid table name
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Test Note",
					"content": "This should fail",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	resp, httpResp := h.DoUpload(h.client1Token, uploadReq)

	// Should reject syntactically invalid table names
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status) // Invalid table name format
	require.Contains(t, resp.Statuses[0].Message, "invalid table name")

	t.Logf("✅ E04 Invalid Table Name test passed - syntactically invalid table names rejected")
}

func TestE05_InvalidUUID(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E05 – Invalid UUID
	// Upload with invalid UUID format → error handling.
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             "invalid-uuid-format", // Invalid UUID
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "invalid-uuid-format",
					"title": "Test Note",
					"content": "This should fail",
					"updated_at": %d
				}`, time.Now().Unix())),
			},
		},
	}

	resp, httpResp := h.DoUpload(h.client1Token, uploadReq)

	// Sidecar v2: Returns HTTP 200 with "invalid" status for validation errors
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status)
	require.Contains(t, resp.Statuses[0].Message, "invalid UUID format")

	t.Logf("✅ E05 Invalid UUID test passed - sidecar v2 validation working correctly")
}

func TestE06_DownloadWithInvalidParameters(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E06 – Download with invalid parameters
	// Negative after -> 400
	_, httpResp1 := h.DoDownload(h.client1Token, -1, 100)
	require.Equal(t, http.StatusBadRequest, httpResp1.StatusCode)

	// Zero limit -> 400
	_, httpResp2 := h.DoDownload(h.client1Token, 0, 0)
	require.Equal(t, http.StatusBadRequest, httpResp2.StatusCode)

	// Excessive limit -> 400
	_, httpResp3 := h.DoDownload(h.client1Token, 0, 10000)
	require.Equal(t, http.StatusBadRequest, httpResp3.StatusCode)

	// Invalid schema name -> 400
	req := h.createHTTPRequest("GET", "/sync/download?after=0&limit=10&schema=Invalid-Name", nil)
	req.Header.Set("Authorization", "Bearer "+h.client1Token)
	rec := h.executeHTTPRequest(req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Negative until -> 400
	req2 := h.createHTTPRequest("GET", "/sync/download?after=0&limit=10&until=-5", nil)
	req2.Header.Set("Authorization", "Bearer "+h.client1Token)
	rec2 := h.executeHTTPRequest(req2)
	require.Equal(t, http.StatusBadRequest, rec2.Code)

	t.Logf("✅ E06 Download Invalid Parameters test passed - 400 Bad Request for invalid params")
}

func TestE07_EmptyUploadRequest(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E07 – Empty upload request
	// Upload with no changes → should be accepted but do nothing.
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           []oversync.ChangeUpload{}, // Empty changes
	}

	response, httpResp := h.DoUpload(h.client1Token, uploadReq)

	// Should return 200 and be accepted
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, response.Accepted)
	require.Len(t, response.Statuses, 0)                  // No statuses for empty changes
	require.Equal(t, int64(0), response.HighestServerSeq) // No changes processed

	t.Logf("✅ E07 Empty Upload Request test passed - empty upload accepted")
}

func TestE08_InvalidOperation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// E08 – Invalid operation
	// Upload with invalid operation type → error handling.
	noteID := h.MakeUUID("000000000001")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INVALID_OP", // Invalid operation
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Test Note",
					"content": "This should fail",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	resp, httpResp := h.DoUpload(h.client1Token, uploadReq)

	// Sidecar v2: Returns HTTP 200 with "invalid" status for validation errors
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status)
	require.Contains(t, resp.Statuses[0].Message, "invalid operation")

	t.Logf("✅ E08 Invalid Operation test passed - sidecar v2 validation working correctly")
}
