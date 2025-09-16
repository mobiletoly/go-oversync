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

// Delete in metadata tests

func TestT01_DeleteCreatesMetadataWithDeletedFlag(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// T01 – Delete creates metadata with deleted=true
	// Given: Client deletes note(A) with server_version=3.
	// Then: sync_row_meta(A).deleted=true, server_version incremented, change_log DELETE appended.

	noteID := h.MakeUUID("000000000001")

	// First, create a note on the server
	insertReq := &oversync.UploadRequest{
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
					"title": "Note to be deleted",
					"content": "This will be deleted",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)
	require.Equal(t, int64(1), *insertResp.Statuses[0].NewServerVersion)

	// Update the note to increment server_version to 3
	updateReq1 := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated Note",
					"content": "Updated content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	updateResp1, _ := h.DoUpload(h.client1Token, updateReq1)
	require.True(t, updateResp1.Accepted)
	require.Equal(t, int64(2), *updateResp1.Statuses[0].NewServerVersion)

	// Update again to get server_version=3
	updateReq2 := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  2,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Final Update",
					"content": "Final content before delete",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	updateResp2, _ := h.DoUpload(h.client1Token, updateReq2)
	require.True(t, updateResp2.Accepted)
	require.Equal(t, int64(3), *updateResp2.Statuses[0].NewServerVersion)

	// Now delete the note with server_version=3
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 3,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 4,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  3, // Correct server_version
				Payload:        nil,
			},
		},
	}

	deleteResp, httpResp := h.DoUpload(h.client1Token, deleteReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, deleteResp.Accepted)
	require.Equal(t, "applied", deleteResp.Statuses[0].Status)

	// Verify note is deleted from business table
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.Nil(t, note) // Should not exist in business table

	// Verify sync_row_meta shows deleted=true
	var deleted bool
	var serverVersion int64
	err = h.service.Pool().QueryRow(h.ctx, `
		SELECT deleted, server_version FROM sync.sync_row_meta
		WHERE table_name = $1 AND pk_uuid = $2`,
		"note", noteID).Scan(&deleted, &serverVersion)
	require.NoError(t, err)
	require.True(t, deleted, "sync_row_meta.deleted should be true")
	require.Equal(t, int64(4), serverVersion, "server_version should be incremented")

	// Verify DELETE change is in server_change_log
	changeLog, err := h.GetChangeLogEntry(h.client1ID, 4)
	require.NoError(t, err)
	require.NotNil(t, changeLog)
	require.Equal(t, "DELETE", changeLog.Op)
	require.Equal(t, noteID.String(), changeLog.PkUUID)

	t.Logf("✅ T01 Delete Creates Metadata with Deleted Flag test passed - sync_row_meta.deleted=true")
}

func TestT02_DeletedMetadataPreventsResurrection(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// T02 – Deleted metadata prevents resurrection
	// Given: sync_row_meta.deleted=true for a record.
	// Then: Attempts to INSERT with same PK are rejected with conflict.

	noteID := h.MakeUUID("000000000002")

	// Create and delete a note
	insertReq := &oversync.UploadRequest{
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
					"title": "Temporary Note",
					"content": "Will be deleted",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Delete the note
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload:        nil,
			},
		},
	}

	deleteResp, _ := h.DoUpload(h.client1Token, deleteReq)
	require.True(t, deleteResp.Accepted)

	// Verify sync_row_meta shows deleted=true
	var deleted bool
	var serverVersion int64
	err := h.service.Pool().QueryRow(h.ctx, `
		SELECT deleted, server_version FROM sync.sync_row_meta
		WHERE table_name = $1 AND pk_uuid = $2`,
		"note", noteID).Scan(&deleted, &serverVersion)
	require.NoError(t, err)
	require.True(t, deleted, "sync_row_meta.deleted should be true")

	// Verify note is deleted from business table
	note, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.Nil(t, note)

	// Another client tries to download - should see the DELETE
	downloadResp, _ := h.DoDownload(h.client2Token, 0, 100)
	require.Len(t, downloadResp.Changes, 2) // INSERT + DELETE

	// Verify DELETE is included in download
	deleteChange := downloadResp.Changes[1]
	require.Equal(t, "DELETE", deleteChange.Op)
	require.Equal(t, noteID.String(), deleteChange.PK)

	// The deleted metadata prevents resurrection
	// If client 2 tries to insert the same ID, it should conflict
	recreateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0, // New insert
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Recreated Note",
					"content": "New note with same ID",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	recreateResp, _ := h.DoUpload(h.client2Token, recreateReq)
	require.True(t, recreateResp.Accepted)
	// Sidecar v2: INSERT after DELETE with same PK conflicts with deleted metadata
	require.Equal(t, "conflict", recreateResp.Statuses[0].Status)

	// Verify the conflict response includes the deleted metadata
	var serverState map[string]interface{}
	err = json.Unmarshal(recreateResp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, true, serverState["deleted"])
	require.Equal(t, float64(2), serverState["server_version"])

	t.Logf("✅ T02 Deleted Metadata Prevents Resurrection test passed - sync_row_meta.deleted=true prevents INSERT")
}

func TestT03_ChangeLogRetentionAndVersionGatedConflicts(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// T03 – Change log retention and version-gated conflicts
	// Given: Old change log entries may be pruned for retention.
	// Then: sync_row_meta.deleted=true still prevents resurrection via version conflicts.

	noteID := h.MakeUUID("000000000003")

	// Create and delete a note
	insertReq := &oversync.UploadRequest{
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
					"title": "Note for Pruning Test",
					"content": "Will be deleted and pruned",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	insertResp, _ := h.DoUpload(h.client1Token, insertReq)
	require.True(t, insertResp.Accepted)

	// Delete the note
	deleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 1,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "DELETE",
				PK:             noteID.String(),
				ServerVersion:  1,
				Payload:        nil,
			},
		},
	}

	deleteResp, _ := h.DoUpload(h.client1Token, deleteReq)
	require.True(t, deleteResp.Accepted)

	// Verify sync_row_meta shows deleted=true
	var deleted bool
	var serverVersion int64
	err := h.service.Pool().QueryRow(h.ctx, `
		SELECT deleted, server_version FROM sync.sync_row_meta
		WHERE table_name = $1 AND pk_uuid = $2`,
		"note", noteID).Scan(&deleted, &serverVersion)
	require.NoError(t, err)
	require.True(t, deleted, "sync_row_meta.deleted should be true")

	// Simulate change log retention by removing old entries
	// (In a real implementation, this would be handled by a background job)
	retentionDays := 90
	oldTimestamp := time.Now().AddDate(0, 0, -retentionDays-1) // 91 days ago

	// Update the change log entry to be old
	_, err = h.service.Pool().Exec(h.ctx, `
		UPDATE sync.server_change_log
		SET ts = $1
		WHERE source_id = $2 AND source_change_id = $3`,
		oldTimestamp, h.client1ID, 2)
	require.NoError(t, err)

	// Simulate pruning old change log entries (but keep sync_row_meta)
	_, err = h.service.Pool().Exec(h.ctx,
		fmt.Sprintf("DELETE FROM sync.server_change_log WHERE ts < NOW() - INTERVAL '%d days'", retentionDays))
	require.NoError(t, err)

	// Verify change log entry was pruned
	changeLog, err := h.GetChangeLogEntry(h.client1ID, 2)
	require.NoError(t, err)
	require.Nil(t, changeLog, "Change log entry should be pruned")

	// Key point: Even after change log pruning, sync_row_meta.deleted=true prevents resurrection
	// Verify metadata still exists and shows deleted=true
	err = h.service.Pool().QueryRow(h.ctx, `
		SELECT deleted, server_version FROM sync.sync_row_meta
		WHERE table_name = $1 AND pk_uuid = $2`,
		"note", noteID).Scan(&deleted, &serverVersion)
	require.NoError(t, err)
	require.True(t, deleted, "sync_row_meta.deleted should still be true after pruning")

	// Attempt to INSERT with same PK should still conflict due to version check
	resurrectReq := &oversync.UploadRequest{
		LastServerSeqSeen: 100, // High value to simulate after pruning
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0, // Trying to insert as new
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Resurrection Attempt",
					"content": "Should be blocked",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
		},
	}

	resurrectResp, _ := h.DoUpload(h.client2Token, resurrectReq)
	require.True(t, resurrectResp.Accepted)
	require.Equal(t, "conflict", resurrectResp.Statuses[0].Status)

	// Verify conflict response shows deleted metadata
	var serverState map[string]interface{}
	err = json.Unmarshal(resurrectResp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)
	require.Equal(t, true, serverState["deleted"])

	t.Logf("✅ T03 Change Log Retention and Version Gated Conflicts test passed - sync_row_meta prevents resurrection even after change log pruning")
}
