package oversync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Sidecar vIntegration Tests
// Based on docs/techreq/08_integration_tests_v2.md

// Helper methods for sidecar tests

// Upload is a convenience wrapper around DoUpload for client1
func (h *SimpleTestHarness) Upload(changes []oversync.ChangeUpload) *oversync.UploadResponse {
	req := &oversync.UploadRequest{Changes: changes}
	resp, httpResp := h.DoUpload(h.client1Token, req)
	require.Equal(h.t, 200, httpResp.StatusCode)
	return resp
}

// Download is a convenience wrapper around DoDownload for client1
func (h *SimpleTestHarness) Download(after int64, limit int) *oversync.DownloadResponse {
	resp, httpResp := h.DoDownload(h.client1Token, after, limit)
	require.Equal(h.t, 200, httpResp.StatusCode)
	return resp
}

// MakeNotePayload creates a JSON payload for a note
func (h *SimpleTestHarness) MakeNotePayload(id uuid.UUID, title, content string, updatedAt time.Time) json.RawMessage {
	payload := map[string]interface{}{
		"id":         id.String(),
		"title":      title,
		"content":    content,
		"updated_at": updatedAt.Format(time.RFC3339),
	}
	data, err := json.Marshal(payload)
	require.NoError(h.t, err)
	return data
}

// MakeTaskPayload creates a JSON payload for a task
func (h *SimpleTestHarness) MakeTaskPayload(id uuid.UUID, title string, done bool, updatedAt time.Time) json.RawMessage {
	payload := map[string]interface{}{
		"id":         id.String(),
		"title":      title,
		"done":       done,
		"updated_at": updatedAt.Format(time.RFC3339),
	}
	data, err := json.Marshal(payload)
	require.NoError(h.t, err)
	return data
}

// TC1: Schema Initialization - Start server with fresh DB
func TestSidecarTC01_SchemaInitialization(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Verify all three sidecar tables exist with proper indexes
	tables := []string{"sync_row_meta", "sync_state", "server_change_log"}
	for _, table := range tables {
		var exists bool
		err := h.service.Pool().QueryRow(h.ctx,
			`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)`,
			table).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "Table %s should exist", table)
	}

	// Verify indexes exist (user-scoped architecture)
	requiredIndexes := []string{
		"scl_user_seq_idx",
		"scl_user_schema_seq_idx",
		"scl_user_pk_seq_idx",
		"scl_user_delete_seq_idx",
	}
	for _, index := range requiredIndexes {
		var exists bool
		err := h.service.Pool().QueryRow(h.ctx,
			`SELECT EXISTS (SELECT FROM pg_indexes WHERE indexname = $1)`,
			index).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "Index %s should exist", index)
	}

	// Verify redundant historical indexes are not present.
	redundantIndexes := []string{
		"scl_seq_idx",     // redundant with server_change_log_pkey
		"scl_triplet_idx", // redundant with scl_user_pk_seq_idx prefix
	}
	for _, index := range redundantIndexes {
		var exists bool
		err := h.service.Pool().QueryRow(h.ctx,
			`SELECT EXISTS (SELECT FROM pg_indexes WHERE indexname = $1)`,
			index).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists, "Index %s should not exist", index)
	}

	t.Log("✅ TC1: Schema initialization test passed")
}

// TC2: Restart server with existing schema
func TestSidecarTC02_SchemaRestart(t *testing.T) {
	// First initialization
	h1 := NewSimpleTestHarness(t)
	h1.Cleanup()

	// Second initialization (restart simulation)
	h2 := NewSimpleTestHarness(t)
	defer h2.Cleanup()

	// Should not cause any errors
	t.Log("✅ TC2: Schema restart test passed")
}

// TC3: Insert new row creates all sidecar entries
func TestSidecarTC03_InsertCreatesAllEntries(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc3-note")

	// Upload INSERT
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "TC3 Note", "Content", time.Now()),
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "applied", resp.Statuses[0].Status)
	require.NotNil(t, resp.Statuses[0].NewServerVersion)
	require.Equal(t, int64(1), *resp.Statuses[0].NewServerVersion)

	// Verify sync_row_meta entry
	meta, err := h.GetSyncRowMeta("public", "note", noteID)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.ServerVersion)
	require.False(t, meta.Deleted)

	// Verify sync_state entry
	state, err := h.GetSyncState("public", "note", noteID)
	require.NoError(t, err)
	require.NotEmpty(t, state.Payload)

	// Verify server_change_log entry
	change, err := h.GetChangeLogEntry(h.client1ID, 1)
	require.NoError(t, err)
	require.Equal(t, "INSERT", change.Op)
	require.Greater(t, change.ServerID, int64(0)) // Just verify it's a positive ID

	t.Log("✅ TC3: Insert creates all sidecar entries test passed")
}

// TC4: Update existing row with matching server_version
func TestSidecarTC04_UpdateWithMatchingVersion(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc4-note")

	// First insert
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Original", "Content", time.Now()),
	}})

	// Update with correct version
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "UPDATE",
		PK:             noteID.String(),
		ServerVersion:  1, // Correct version
		Payload:        h.MakeNotePayload(noteID, "Updated", "New Content", time.Now()),
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "applied", resp.Statuses[0].Status)
	require.NotNil(t, resp.Statuses[0].NewServerVersion)
	require.Equal(t, int64(2), *resp.Statuses[0].NewServerVersion)

	// Verify server_version bumped
	var serverVersion int64
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version FROM sync.sync_row_meta 
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&serverVersion)
	require.NoError(t, err)
	require.Equal(t, int64(2), serverVersion)

	// Verify payload updated in sync_state
	var payload json.RawMessage
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT payload FROM sync.sync_state 
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&payload)
	require.NoError(t, err)

	var note map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &note))
	require.Equal(t, "Updated", note["title"])

	t.Log("✅ TC4: Update with matching version test passed")
}

// TC5: Conflict on update (wrong server_version)
func TestSidecarTC05_UpdateConflict(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc5-note")

	// First insert
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Original", "Content", time.Now()),
	}})

	// Update with wrong version
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "UPDATE",
		PK:             noteID.String(),
		ServerVersion:  0, // Wrong version (should be 1)
		Payload:        h.MakeNotePayload(noteID, "Conflicted", "New Content", time.Now()),
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "conflict", resp.Statuses[0].Status)
	require.Nil(t, resp.Statuses[0].NewServerVersion)
	require.NotNil(t, resp.Statuses[0].ServerRow)

	// Verify server row contains current state
	var serverRow map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Statuses[0].ServerRow, &serverRow))
	require.Equal(t, "note", serverRow["table_name"])
	require.Equal(t, noteID.String(), serverRow["id"])
	require.Equal(t, float64(1), serverRow["server_version"]) // JSON numbers are float64
	require.Equal(t, false, serverRow["deleted"])
	require.NotNil(t, serverRow["payload"])

	// Verify original state unchanged
	var serverVersion int64
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version FROM sync.sync_row_meta 
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&serverVersion)
	require.NoError(t, err)
	require.Equal(t, int64(1), serverVersion) // Should still be 1

	t.Log("✅ TC5: Update conflict test passed")
}

// TC6: Idempotency - repeat same (source_id, source_change_id)
func TestSidecarTC06_Idempotency(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc6-note")
	change := oversync.ChangeUpload{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "TC6 Note", "Content", time.Now()),
	}

	// First upload
	resp1 := h.Upload([]oversync.ChangeUpload{change})
	require.True(t, resp1.Accepted)
	require.Equal(t, "applied", resp1.Statuses[0].Status)

	// Second upload (identical)
	resp2 := h.Upload([]oversync.ChangeUpload{change})
	require.True(t, resp2.Accepted)
	require.Equal(t, "applied", resp2.Statuses[0].Status) // Should be idempotent

	// Verify only one entry in server_change_log
	var count int
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.server_change_log 
		 WHERE source_id = $1 AND source_change_id = 1`,
		h.client1ID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify server_version is still 1 (not incremented twice)
	var serverVersion int64
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version FROM sync.sync_row_meta 
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&serverVersion)
	require.NoError(t, err)
	require.Equal(t, int64(1), serverVersion)

	t.Log("✅ TC6: Idempotency test passed")
}

// TC7: Delete row with matching server_version
func TestSidecarTC07_DeleteWithMatchingVersion(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc7-note")

	// First insert
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "To Delete", "Content", time.Now()),
	}})

	// Delete with correct version
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "DELETE",
		PK:             noteID.String(),
		ServerVersion:  1,   // Correct version
		Payload:        nil, // DELETE doesn't need payload
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "applied", resp.Statuses[0].Status)
	require.NotNil(t, resp.Statuses[0].NewServerVersion)
	require.Equal(t, int64(2), *resp.Statuses[0].NewServerVersion)

	// Verify sync_row_meta: deleted=true, version bumped
	var serverVersion int64
	var deleted bool
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version, deleted FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&serverVersion, &deleted)
	require.NoError(t, err)
	require.Equal(t, int64(2), serverVersion)
	require.True(t, deleted)

	// Verify sync_state: payload removed
	var count int
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.sync_state
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count) // Should be removed

	// Verify server_change_log entry
	var logOp string
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT op FROM sync.server_change_log
		 WHERE source_id = $1 AND source_change_id = 2`,
		h.client1ID).Scan(&logOp)
	require.NoError(t, err)
	require.Equal(t, "DELETE", logOp)

	t.Log("✅ TC7: Delete with matching version test passed")
}

// TC8: Conflict on delete (wrong version)
func TestSidecarTC08_DeleteConflict(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc8-note")

	// First insert
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "To Delete", "Content", time.Now()),
	}})

	// Delete with wrong version
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "DELETE",
		PK:             noteID.String(),
		ServerVersion:  0, // Wrong version (should be 1)
		Payload:        nil,
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "conflict", resp.Statuses[0].Status)
	require.Nil(t, resp.Statuses[0].NewServerVersion)
	require.NotNil(t, resp.Statuses[0].ServerRow)

	// Verify server row contains current state
	var serverRow map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Statuses[0].ServerRow, &serverRow))
	require.Equal(t, "note", serverRow["table_name"])
	require.Equal(t, float64(1), serverRow["server_version"])
	require.Equal(t, false, serverRow["deleted"])

	// Verify original state unchanged
	var serverVersion int64
	var deleted bool
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version, deleted FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&serverVersion, &deleted)
	require.NoError(t, err)
	require.Equal(t, int64(1), serverVersion)
	require.False(t, deleted) // Should still be false

	t.Log("✅ TC8: Delete conflict test passed")
}

// TC9: Idempotent delete
func TestSidecarTC09_IdempotentDelete(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc9-note")

	// Insert and delete
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "To Delete", "Content", time.Now()),
	}})

	deleteChange := oversync.ChangeUpload{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "DELETE",
		PK:             noteID.String(),
		ServerVersion:  1,
		Payload:        nil,
	}

	// First delete
	resp1 := h.Upload([]oversync.ChangeUpload{deleteChange})
	require.Equal(t, "applied", resp1.Statuses[0].Status)

	// Second delete (identical)
	resp2 := h.Upload([]oversync.ChangeUpload{deleteChange})
	require.Equal(t, "applied", resp2.Statuses[0].Status) // Should be idempotent

	// Verify only one DELETE entry in server_change_log
	var count int
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.server_change_log
		 WHERE source_id = $1 AND source_change_id = 2 AND op = 'DELETE'`,
		h.client1ID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	t.Log("✅ TC9: Idempotent delete test passed")
}

// TC10: Download changes after given server_id
func TestSidecarTC10_DownloadAfterServerID(t *testing.T) {
	h := NewSimpleTestHarness(t) // Same user, multiple devices
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID1 := h.MakeUUID("tc10-note1")
	noteID2 := h.MakeUUID("tc10-note2")

	// Device 1 uploads two changes
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID1.String(),
				ServerVersion:  0,
				Payload:        h.MakeNotePayload(noteID1, "Note 1", "Content 1", time.Now()),
			},
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload:        h.MakeNotePayload(noteID2, "Note 2", "Content 2", time.Now()),
			},
		},
	}
	h.DoUpload(h.client1Token, uploadReq)

	// Device 2 downloads after server_id=0 (should get both from device 1)
	resp, _ := h.DoDownload(h.client2Token, 0, 10)
	require.Len(t, resp.Changes, 2)
	require.False(t, resp.HasMore)
	require.Equal(t, int64(2), resp.NextAfter)

	// Verify sidecar fields in response
	change1 := resp.Changes[0]
	require.Equal(t, int64(1), change1.ServerID)
	require.Equal(t, "note", change1.TableName)
	require.Equal(t, "INSERT", change1.Op)
	require.Equal(t, int64(1), change1.ServerVersion) // From sync_row_meta
	require.False(t, change1.Deleted)                 // From sync_row_meta
	require.NotNil(t, change1.Payload)

	// Device 2 downloads after server_id=1 (should get only second change)
	resp2, _ := h.DoDownload(h.client2Token, 1, 10)
	require.Len(t, resp2.Changes, 1)
	require.Equal(t, noteID2.String(), resp2.Changes[0].PK)

	t.Log("✅ TC10: Download after server_id test passed")
}

// TC11: Pagination with has_more flag
func TestSidecarTC11_DownloadPagination(t *testing.T) {
	h := NewSimpleTestHarness(t) // Same user, multiple devices
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Device 1 uploads 3 changes
	changes := make([]oversync.ChangeUpload, 3)
	for i := 0; i < 3; i++ {
		noteID := h.MakeUUID("tc11-note" + string(rune('1'+i)))
		changes[i] = oversync.ChangeUpload{
			SourceChangeID: int64(i + 1),
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID, "Note", "Content", time.Now()),
		}
	}
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}
	h.DoUpload(h.client1Token, uploadReq)

	// Device 2 downloads with limit=2 (should get 2 changes, has_more=true)
	resp, _ := h.DoDownload(h.client2Token, 0, 2)
	require.Len(t, resp.Changes, 2)
	require.True(t, resp.HasMore)
	require.Equal(t, int64(2), resp.NextAfter)

	// Device 2 downloads remaining (should get 1 change, has_more=false)
	resp2, _ := h.DoDownload(h.client2Token, 2, 2)
	require.Len(t, resp2.Changes, 1)
	require.False(t, resp2.HasMore)
	require.Equal(t, int64(3), resp2.NextAfter)

	t.Log("✅ TC11: Download pagination test passed")
}

// TC12: Download excludes own changes
func TestSidecarTC12_DownloadExcludesOwnChanges(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc12-note")

	// Upload change from client1
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Note", "Content", time.Now()),
	}})

	// Download from same client (should exclude own change)
	resp := h.Download(0, 10)
	require.Len(t, resp.Changes, 0) // Should be empty

	// Upload from client2 (same user, different device) and download from client1
	noteID2 := h.MakeUUID("tc12-note2")
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{{
			SourceChangeID: 1,
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID2.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID2, "Note 2", "Content 2", time.Now()),
		}},
	}
	h.DoUpload(h.client2Token, uploadReq)

	// Download from client1 (should see client2's change since same user)
	resp2 := h.Download(0, 10)
	require.Len(t, resp2.Changes, 1)
	require.Equal(t, h.client2ID, resp2.Changes[0].SourceID)

	t.Log("✅ TC12: Download excludes own changes test passed")
}

// TC13: Insert updates both sync_row_meta and sync_state consistently
func TestSidecarTC13_MetaStateConsistency(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc13-note")
	payload := h.MakeNotePayload(noteID, "Consistent", "Content", time.Now())

	// Upload INSERT
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        payload,
	}})

	// Verify sync_row_meta
	var metaVersion int64
	var metaDeleted bool
	var metaUpdatedAt time.Time
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version, deleted, updated_at FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&metaVersion, &metaDeleted, &metaUpdatedAt)
	require.NoError(t, err)
	require.Equal(t, int64(1), metaVersion)
	require.False(t, metaDeleted)

	// Verify sync_state
	var statePayload json.RawMessage
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT payload FROM sync.sync_state
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&statePayload)
	require.NoError(t, err)
	require.JSONEq(t, string(payload), string(statePayload))

	t.Log("✅ TC13: Meta/state consistency test passed")
}

// TC14: Delete removes payload from sync_state but keeps sync_row_meta
func TestSidecarTC14_DeleteStateCleanup(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc14-note")

	// Insert then delete
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "To Delete", "Content", time.Now()),
	}})

	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "DELETE",
		PK:             noteID.String(),
		ServerVersion:  1,
		Payload:        nil,
	}})

	// Verify sync_row_meta still exists with deleted=true
	var metaVersion int64
	var metaDeleted bool
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version, deleted FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&metaVersion, &metaDeleted)
	require.NoError(t, err)
	require.Equal(t, int64(2), metaVersion)
	require.True(t, metaDeleted)

	// Verify sync_state entry removed
	var stateCount int
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.sync_state
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&stateCount)
	require.NoError(t, err)
	require.Equal(t, 0, stateCount)

	t.Log("✅ TC14: Delete state cleanup test passed")
}

// TC16: Multi-table upload in one batch
func TestSidecarTC16_MultiTableBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc16-note")
	taskID := h.MakeUUID("tc16-task")

	// Upload changes for both tables in one batch
	resp := h.Upload([]oversync.ChangeUpload{
		{
			SourceChangeID: 1,
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID, "Multi Note", "Content", time.Now()),
		},
		{
			SourceChangeID: 2,
			Table:          "task",
			Op:             "INSERT",
			PK:             taskID.String(),
			ServerVersion:  0,
			Payload:        h.MakeTaskPayload(taskID, "Multi Task", false, time.Now()),
		},
	})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 2)
	require.Equal(t, "applied", resp.Statuses[0].Status)
	require.Equal(t, "applied", resp.Statuses[1].Status)

	// Verify both tables have independent metadata
	var noteVersion, taskVersion int64
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&noteVersion)
	require.NoError(t, err)
	require.Equal(t, int64(1), noteVersion)

	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT server_version FROM sync.sync_row_meta
		 WHERE table_name = 'task' AND pk_uuid = $1`,
		taskID).Scan(&taskVersion)
	require.NoError(t, err)
	require.Equal(t, int64(1), taskVersion)

	t.Log("✅ TC16: Multi-table batch test passed")
}

// TC17: Conflict in one table doesn't block other table changes
func TestSidecarTC17_PartialConflictBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc17-note")
	taskID := h.MakeUUID("tc17-task")

	// Insert note first
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Existing", "Content", time.Now()),
	}})

	// Batch with conflict on note but valid task
	resp := h.Upload([]oversync.ChangeUpload{
		{
			SourceChangeID: 2,
			Table:          "note",
			Op:             "UPDATE",
			PK:             noteID.String(),
			ServerVersion:  0, // Wrong version - conflict
			Payload:        h.MakeNotePayload(noteID, "Conflicted", "Content", time.Now()),
		},
		{
			SourceChangeID: 3,
			Table:          "task",
			Op:             "INSERT",
			PK:             taskID.String(),
			ServerVersion:  0, // Valid
			Payload:        h.MakeTaskPayload(taskID, "Valid Task", false, time.Now()),
		},
	})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 2)
	require.Equal(t, "conflict", resp.Statuses[0].Status)
	require.Equal(t, "applied", resp.Statuses[1].Status)

	// Verify task was created despite note conflict
	var taskExists bool
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT EXISTS(SELECT 1 FROM sync.sync_row_meta
		 WHERE table_name = 'task' AND pk_uuid = $1)`,
		taskID).Scan(&taskExists)
	require.NoError(t, err)
	require.True(t, taskExists)

	t.Log("✅ TC17: Partial conflict batch test passed")
}

// TC23: Upload with invalid UUID
func TestSidecarTC23_InvalidUUID(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Upload with invalid UUID
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             "invalid-uuid", // Invalid UUID format
		ServerVersion:  0,
		Payload:        json.RawMessage(`{"title": "Test"}`),
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status)
	require.Contains(t, resp.Statuses[0].Message, "invalid UUID format")

	// Verify no database entries created
	var count int
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.server_change_log
		 WHERE source_id = $1 AND source_change_id = 1`,
		h.client1ID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	t.Log("✅ TC23: Invalid UUID test passed")
}

// TC24: Upload with invalid operation
func TestSidecarTC24_InvalidOperation(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc24-note")

	// Upload with invalid operation
	resp := h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INVALID_OP", // Invalid operation
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Test", "Content", time.Now()),
	}})

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)
	require.Equal(t, "invalid", resp.Statuses[0].Status)
	require.Contains(t, resp.Statuses[0].Message, "invalid operation")

	t.Log("✅ TC24: Invalid operation test passed")
}

// TC25: Download exactly limit records
func TestSidecarTC25_DownloadExactLimit(t *testing.T) {
	h := NewSimpleTestHarness(t) // Same user, multiple devices
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Device 1 uploads exactly 3 changes
	changes := make([]oversync.ChangeUpload, 3)
	for i := 0; i < 3; i++ {
		noteID := h.MakeUUID("tc25-note" + string(rune('1'+i)))
		changes[i] = oversync.ChangeUpload{
			SourceChangeID: int64(i + 1),
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID, "Note", "Content", time.Now()),
		}
	}
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}
	h.DoUpload(h.client1Token, uploadReq)

	// Device 2 downloads with limit=3 (exactly the number of records)
	resp, _ := h.DoDownload(h.client2Token, 0, 3)
	require.Len(t, resp.Changes, 3)
	require.False(t, resp.HasMore) // Should be false since we got exactly the limit
	require.Equal(t, int64(3), resp.NextAfter)

	t.Log("✅ TC25: Download exact limit test passed")
}

// TC26: Download limit+1 records
func TestSidecarTC26_DownloadOverLimit(t *testing.T) {
	h := NewSimpleTestHarness(t) // Same user, multiple devices
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Device 1 uploads 4 changes
	changes := make([]oversync.ChangeUpload, 4)
	for i := 0; i < 4; i++ {
		noteID := h.MakeUUID("tc26-note" + string(rune('1'+i)))
		changes[i] = oversync.ChangeUpload{
			SourceChangeID: int64(i + 1),
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID, "Note", "Content", time.Now()),
		}
	}
	uploadReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes:           changes,
	}
	h.DoUpload(h.client1Token, uploadReq)

	// Device 2 downloads with limit=3 (less than total records)
	resp, _ := h.DoDownload(h.client2Token, 0, 3)
	require.Len(t, resp.Changes, 3) // Should get only 3
	require.True(t, resp.HasMore)   // Should be true since there's 1 more
	require.Equal(t, int64(3), resp.NextAfter)

	t.Log("✅ TC26: Download over limit test passed")
}

// TC27: updated_at in sync_row_meta is updated on version bump
func TestSidecarTC27_UpdatedAtBump(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	noteID := h.MakeUUID("tc27-note")

	// Insert
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 1,
		Table:          "note",
		Op:             "INSERT",
		PK:             noteID.String(),
		ServerVersion:  0,
		Payload:        h.MakeNotePayload(noteID, "Original", "Content", time.Now()),
	}})

	// Get initial updated_at
	var initialUpdatedAt time.Time
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT updated_at FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&initialUpdatedAt)
	require.NoError(t, err)

	// Wait a bit to ensure timestamp difference
	time.Sleep(10 * time.Millisecond)

	// Update
	h.Upload([]oversync.ChangeUpload{{
		SourceChangeID: 2,
		Table:          "note",
		Op:             "UPDATE",
		PK:             noteID.String(),
		ServerVersion:  1,
		Payload:        h.MakeNotePayload(noteID, "Updated", "Content", time.Now()),
	}})

	// Get new updated_at
	var newUpdatedAt time.Time
	err = h.service.Pool().QueryRow(h.ctx,
		`SELECT updated_at FROM sync.sync_row_meta
		 WHERE table_name = 'note' AND pk_uuid = $1`,
		noteID).Scan(&newUpdatedAt)
	require.NoError(t, err)

	// Verify updated_at was bumped
	require.True(t, newUpdatedAt.After(initialUpdatedAt))

	t.Log("✅ TC27: updated_at bump test passed")
}

// TC29: Performance test - Upload 500 changes in single batch
func TestSidecarTC29_PerformanceBatch(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset() // Clear any existing data

	// Create 500 changes
	changes := make([]oversync.ChangeUpload, 500)
	for i := 0; i < 500; i++ {
		noteID := h.MakeUUID("perf-note" + string(rune(i)))
		changes[i] = oversync.ChangeUpload{
			SourceChangeID: int64(i + 1),
			Table:          "note",
			Op:             "INSERT",
			PK:             noteID.String(),
			ServerVersion:  0,
			Payload:        h.MakeNotePayload(noteID, "Perf Note", "Content", time.Now()),
		}
	}

	// Measure upload time
	start := time.Now()
	resp := h.Upload(changes)
	duration := time.Since(start)

	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 500)

	// All should be applied
	for i, status := range resp.Statuses {
		require.Equal(t, "applied", status.Status, "Change %d should be applied", i)
	}

	// Performance assertion (should be under 5 seconds for 500 changes)
	require.Less(t, duration, 5*time.Second, "Upload should complete within 5 seconds")

	t.Logf("✅ TC29: Performance batch test passed - 500 changes in %v", duration)
}
