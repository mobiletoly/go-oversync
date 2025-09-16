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

// Multi-Schema Support Tests - validates schema-aware sync operations

func TestMS01_DefaultSchemaPublic(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MS01 - Default schema is "public"
	// Given: Client uploads without specifying schema
	// Then: Schema defaults to "public"

	noteID := h.MakeUUID("ms01-note")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				// Schema omitted - should default to "public"
				Table:         "note",
				Op:            "INSERT",
				PK:            noteID.String(),
				ServerVersion: 0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "MS01 Note",
					"content": "Default schema test",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp, httpResp := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, "applied", uploadResp.Statuses[0].Status)

	// Verify the change was stored with "public" schema
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 1)
	require.Equal(t, "public", downloadResp.Changes[0].Schema)
	require.Equal(t, "note", downloadResp.Changes[0].TableName)

	t.Logf("✅ MS01 Default Schema Public test passed - schema defaults to 'public'")
}

func TestMS02_ExplicitSchemaSupport(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MS02 - Explicit schema support
	// Given: Client uploads with explicit schema
	// Then: Schema is preserved and used correctly

	noteID := h.MakeUUID("ms02-note")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "crm", // Explicit schema
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "MS02 CRM Note",
					"content": "CRM schema test",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	uploadResp, httpResp := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, "applied", uploadResp.Statuses[0].Status)

	// Verify the change was stored with "crm" schema
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 1)
	require.Equal(t, "crm", downloadResp.Changes[0].Schema)
	require.Equal(t, "note", downloadResp.Changes[0].TableName)

	t.Logf("✅ MS02 Explicit Schema Support test passed - explicit schema preserved")
}

func TestMS03_SchemaIsolation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MS03 - Schema isolation
	// Given: Same table/PK in different schemas
	// Then: Changes are isolated by schema

	noteID := h.MakeUUID("ms03-note")

	// Upload to public schema
	uploadReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Public Note",
					"content": "In public schema"
				}`, noteID.String())),
			},
		},
	}

	uploadResp1, _ := h.DoUpload(h.client1Token, uploadReq1)
	require.True(t, uploadResp1.Accepted)
	require.Equal(t, "applied", uploadResp1.Statuses[0].Status)

	// Upload to crm schema (same table/PK)
	uploadReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Schema:         "crm",
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(), // Same PK as public
				ServerVersion:  0,               // New record in crm schema
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "CRM Note",
					"content": "In CRM schema"
				}`, noteID.String())),
			},
		},
	}

	uploadResp2, _ := h.DoUpload(h.client1Token, uploadReq2)
	require.True(t, uploadResp2.Accepted)
	require.Equal(t, "applied", uploadResp2.Statuses[0].Status)

	// Verify both records exist independently
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 2)

	// Find public and crm changes
	var publicChange, crmChange *oversync.ChangeDownloadResponse
	for i := range downloadResp.Changes {
		if downloadResp.Changes[i].Schema == "public" {
			publicChange = &downloadResp.Changes[i]
		} else if downloadResp.Changes[i].Schema == "crm" {
			crmChange = &downloadResp.Changes[i]
		}
	}

	require.NotNil(t, publicChange)
	require.NotNil(t, crmChange)
	require.Equal(t, noteID.String(), publicChange.PK)
	require.Equal(t, noteID.String(), crmChange.PK)

	// Verify content is different
	var publicPayload, crmPayload map[string]interface{}
	err := json.Unmarshal(publicChange.Payload, &publicPayload)
	require.NoError(t, err)
	err = json.Unmarshal(crmChange.Payload, &crmPayload)
	require.NoError(t, err)

	require.Equal(t, "Public Note", publicPayload["title"])
	require.Equal(t, "CRM Note", crmPayload["title"])

	t.Logf("✅ MS03 Schema Isolation test passed - same table/PK isolated by schema")
}

func TestMS04_SchemaFilteredDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MS04 - Schema-filtered download
	// Given: Changes in multiple schemas
	// Then: Download can filter by schema

	note1ID := h.MakeUUID("ms04-note1")
	note2ID := h.MakeUUID("ms04-note2")

	// Upload to different schemas
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "note",
				Op:             "INSERT",
				PK:             note1ID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Public Note"
				}`, note1ID.String())),
			},
			{
				SourceChangeID: 2,
				Schema:         "crm",
				Table:          "note",
				Op:             "INSERT",
				PK:             note2ID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "CRM Note"
				}`, note2ID.String())),
			},
		},
	}

	uploadResp, _ := h.DoUpload(h.client1Token, uploadReq)
	require.True(t, uploadResp.Accepted)

	// Test unfiltered download (should see both)
	allChanges, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, allChanges.Changes, 2)

	// Test schema-filtered download (public only)
	publicChanges, _ := h.DoDownloadWithSchema(h.client2Token, 0, 100, "public")
	require.Len(t, publicChanges.Changes, 1)
	require.Equal(t, "public", publicChanges.Changes[0].Schema)

	// Test schema-filtered download (crm only)
	crmChanges, _ := h.DoDownloadWithSchema(h.client2Token, 0, 100, "crm")
	require.Len(t, crmChanges.Changes, 1)
	require.Equal(t, "crm", crmChanges.Changes[0].Schema)

	t.Logf("✅ MS04 Schema Filtered Download test passed - schema filtering works correctly")
}

func TestMS05_InvalidSchemaRejected(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MS05 - Invalid schema rejected
	// Given: Client uploads with invalid schema name
	// Then: Upload is rejected with validation error

	noteID := h.MakeUUID("ms05-note")

	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "invalid-schema!", // Invalid schema name
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Invalid Schema Test"
				}`, noteID.String())),
			},
		},
	}

	uploadResp, httpResp := h.DoUpload(h.client1Token, uploadReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, uploadResp.Accepted)
	require.Equal(t, "invalid", uploadResp.Statuses[0].Status)
	require.Contains(t, uploadResp.Statuses[0].Message, "invalid schema name")

	t.Logf("✅ MS05 Invalid Schema Rejected test passed - invalid schema names rejected")
}
