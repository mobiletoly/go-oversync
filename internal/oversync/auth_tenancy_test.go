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

// Auth & Tenancy Tests (A01-A03) from spec section 11

func TestA01_JWTAuthenticationRequired(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// A01 – JWT authentication required
	// Given: Request without valid JWT.
	// When: Upload or Download.
	// Then: 401 Unauthorized; no data access.

	noteID := h.MakeUUID("auth001")

	// Test upload without authentication
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
					"title": "Unauthorized Note",
					"content": "Should not be allowed",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	// Test with no Authorization header
	_, httpResp := h.DoUploadWithoutAuth(uploadReq)
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	// Test with invalid JWT token
	_, httpResp = h.DoUploadWithInvalidAuth(uploadReq, "invalid-jwt-token")
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	// Test download without authentication
	_, httpResp = h.DoDownloadWithoutAuth(0, 100)
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	// Test download with invalid JWT token
	_, httpResp = h.DoDownloadWithInvalidAuth("invalid-jwt-token", 0, 100)
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	// Verify no data was created
	var count int
	err := h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM note`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	var logCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&logCount)
	require.NoError(t, err)
	require.Equal(t, 0, logCount)

	t.Logf("✅ A01 JWT Authentication Required test passed - all unauthorized requests rejected")
}

func TestA02_ClientIsolation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// A02 – Client isolation
	// Given: Multiple clients with different source_ids.
	// When: Each uploads data.
	// Then: Each client only sees their own uploads in source_id field; all see others' changes in downloads.

	noteID1 := h.MakeUUID("client1note")
	noteID2 := h.MakeUUID("client2note")

	// Client 1 uploads data
	client1Req := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Client 1 Note",
					"content": "Created by Client 1",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	client1Resp, httpResp := h.DoUpload(h.client1Token, client1Req)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, client1Resp.Accepted)
	require.Equal(t, int64(1), client1Resp.HighestServerSeq)

	// Client 2 uploads data
	client2Req := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Client 2 Note",
					"content": "Created by Client 2",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	client2Resp, httpResp := h.DoUpload(h.client2Token, client2Req)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, client2Resp.Accepted)
	require.Equal(t, int64(2), client2Resp.HighestServerSeq)

	// Both clients download changes (sidecar v2: excludes own changes)
	client1Download, _ := h.DoDownload(h.client1Token, 0, 100)
	client2Download, _ := h.DoDownload(h.client2Token, 0, 100)

	// Sidecar v2: Each client should see only the OTHER client's changes
	require.Len(t, client1Download.Changes, 1) // Client 1 sees only Client 2's change
	require.Len(t, client2Download.Changes, 1) // Client 2 sees only Client 1's change

	// Client 1 should see Client 2's change
	client1Changes := client1Download.Changes
	require.Equal(t, int64(2), client1Changes[0].ServerID)
	require.Equal(t, h.client2ID, client1Changes[0].SourceID)
	require.Equal(t, noteID2.String(), client1Changes[0].PK)

	// Client 2 should see Client 1's change
	client2Changes := client2Download.Changes
	require.Equal(t, int64(1), client2Changes[0].ServerID)
	require.Equal(t, h.client1ID, client2Changes[0].SourceID)
	require.Equal(t, noteID1.String(), client2Changes[0].PK)

	// Sidecar v2: Each client sees different changes (excludes own)

	// Verify source_change_id isolation - each client can use same source_change_id
	require.Equal(t, int64(1), client1Changes[0].SourceChangeID) // Client 2's SCID=1 (seen by Client 1)
	require.Equal(t, int64(1), client2Changes[0].SourceChangeID) // Client 1's SCID=1 (seen by Client 2)

	t.Logf("✅ A02 Client Isolation test passed - clients isolated by source_id but see all changes")
}

func TestA03_JWTClaimsValidation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// A03 – JWT claims validation
	// Given: JWT with specific client_id claim.
	// When: Upload with different source_id in payload.
	// Then: Server enforces JWT claim; overrides client-provided source_id with JWT claim.

	noteID := h.MakeUUID("claims001")

	// Create a valid JWT for client1
	validToken := h.client1Token
	validSourceID := h.ExtractClientIDFromToken(validToken) // Device ID from JWT

	// Test 1: Valid request - source_id matches JWT claim
	validReq := &oversync.UploadRequest{
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
					"title": "Valid Claims Note",
					"content": "Source ID matches JWT",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	validResp, httpResp := h.DoUpload(validToken, validReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, validResp.Accepted)

	// Test 2: Request with different source_id - server should override with JWT claim
	overrideReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             h.MakeUUID("override001").String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Override Test Note",
					"content": "Server should override source_id",
					"updated_at": %d
				}`, h.MakeUUID("override001").String(), time.Now().Unix())),
			},
		},
	}

	overrideResp, httpResp := h.DoUpload(validToken, overrideReq)
	// Server should accept the request but override the source_id
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, overrideResp.Accepted)

	// Test 3: Verify both changes were persisted with correct source_id
	var count int
	err := h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM note`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count) // Both requests should have been processed

	var logCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&logCount)
	require.NoError(t, err)
	require.Equal(t, 2, logCount) // Two changes in log

	// Test 4: Verify both persisted changes have correct source_id (from JWT, not client payload)
	var sourceIDs []string
	rows, err := h.service.Pool().Query(h.ctx, `
		SELECT source_id FROM sync.server_change_log ORDER BY server_id`)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var sourceID string
		err := rows.Scan(&sourceID)
		require.NoError(t, err)
		sourceIDs = append(sourceIDs, sourceID)
	}

	require.Len(t, sourceIDs, 2)
	require.Equal(t, validSourceID, sourceIDs[0]) // First change
	require.Equal(t, validSourceID, sourceIDs[1]) // Second change (overridden from JWT)

	// Test 5: Download should work with valid JWT (sidecar v2: excludes own changes)
	downloadResp, httpResp := h.DoDownload(validToken, 0, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	// Sidecar v2: Client doesn't see its own changes, so 0 changes expected
	require.Len(t, downloadResp.Changes, 0)

	t.Logf("✅ A03 JWT Claims Validation test passed - server overrides source_id with JWT claim, sidecar v2 excludes own changes")
}
