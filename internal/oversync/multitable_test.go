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

// Multi-Table Tests (MT01-MT02) from spec section 8

func TestMT01_TableIndependence(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MT01 – Table independence
	// Given: Changes to note and task tables with different server_versions.
	// When: Upload mixed batch.
	// Then: Each table's version logic independent; no cross-table interference.

	noteID := h.MakeUUID("note001")
	taskID := h.MakeUUID("task001")

	// Phase 1: Create initial records in both tables
	setupReq := &oversync.UploadRequest{
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
					"title": "Initial Note",
					"content": "Initial content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Initial Task",
					"done": false,
					"updated_at": %d
				}`, taskID.String(), time.Now().Unix())),
			},
		},
	}

	setupResp, httpResp := h.DoUpload(h.client1Token, setupReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, setupResp.Accepted)
	require.Equal(t, int64(2), setupResp.HighestServerSeq)

	// Phase 2: Update note multiple times to advance its server_version
	for i := 3; i <= 5; i++ {
		updateNoteReq := &oversync.UploadRequest{
			LastServerSeqSeen: int64(i - 1),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(i),
					Table:          "note",
					Op:             "UPDATE",
					PK:             noteID.String(),
					ServerVersion:  int64(i - 2), // Note server_version: 1, 2, 3
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Updated Note %d",
						"content": "Updated content %d",
						"updated_at": %d
					}`, noteID.String(), i-2, i-2, time.Now().Unix())),
				},
			},
		}

		updateResp, _ := h.DoUpload(h.client1Token, updateNoteReq)
		require.True(t, updateResp.Accepted)
		require.Equal(t, "applied", updateResp.Statuses[0].Status)
	}

	// Check actual server_versions before mixed batch
	finalNote, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, finalNote)
	noteServerVersion := finalNote.ServerVersion

	finalTask, err := h.GetServerTask(taskID)
	require.NoError(t, err)
	require.NotNil(t, finalTask)
	taskServerVersion := finalTask.ServerVersion

	// Phase 3: Mixed batch with different server_versions per table
	mixedReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 6,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID.String(),
				ServerVersion:  noteServerVersion, // Current note version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Final Note Update",
					"content": "Final note content",
					"updated_at": %d
				}`, noteID.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 7,
				Table:          "task",
				Op:             "UPDATE",
				PK:             taskID.String(),
				ServerVersion:  taskServerVersion, // Current task version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated Task",
					"done": true,
					"updated_at": %d
				}`, taskID.String(), time.Now().Unix())),
			},
		},
	}

	mixedResp, httpResp := h.DoUpload(h.client1Token, mixedReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, mixedResp.Accepted)
	require.Len(t, mixedResp.Statuses, 2)

	// Verify both updates applied successfully (table independence)
	require.Equal(t, "applied", mixedResp.Statuses[0].Status) // Note update
	require.Equal(t, "applied", mixedResp.Statuses[1].Status) // Task update

	// Verify final server_versions are independent
	require.Equal(t, noteServerVersion+1, *mixedResp.Statuses[0].NewServerVersion) // Note incremented
	require.Equal(t, taskServerVersion+1, *mixedResp.Statuses[1].NewServerVersion) // Task incremented

	// Verify database state
	updatedNote, err := h.GetServerNote(noteID)
	require.NoError(t, err)
	require.NotNil(t, updatedNote)
	require.Equal(t, "Final Note Update", updatedNote.Title)
	require.Equal(t, noteServerVersion+1, updatedNote.ServerVersion)

	updatedTask, err := h.GetServerTask(taskID)
	require.NoError(t, err)
	require.NotNil(t, updatedTask)
	require.Equal(t, "Updated Task", updatedTask.Title)
	require.True(t, updatedTask.Done)
	require.Equal(t, taskServerVersion+1, updatedTask.ServerVersion)

	t.Logf("✅ MT01 Table Independence test passed - each table maintains independent server_versions")
}

func TestMT02_CrossTableOperations(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// MT02 – Cross-table operations
	// Given: Batch contains operations across multiple tables.
	// When: Some succeed, some conflict.
	// Then: Per-table results; no rollback across tables; atomic within each table operation.

	noteID1 := h.MakeUUID("mt2n1orig")
	noteID2 := h.MakeUUID("mt2n2new")
	taskID1 := h.MakeUUID("mt2t1orig")
	taskID2 := h.MakeUUID("mt2t2new")

	// Phase 1: Setup initial data
	setupReq := &oversync.UploadRequest{
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
					"title": "Existing Note",
					"content": "Will be updated",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID1.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Existing Task",
					"done": false,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
		},
	}

	setupResp, _ := h.DoUpload(h.client1Token, setupReq)
	require.True(t, setupResp.Accepted)

	// Phase 2: Another client updates the note (creates conflict scenario)
	conflictSetupReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID1.String(),
				ServerVersion:  1, // Current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated by Client 2",
					"content": "Client 2 update",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	conflictSetupResp, _ := h.DoUpload(h.client2Token, conflictSetupReq)
	require.True(t, conflictSetupResp.Accepted)

	// Phase 3: Client 1 attempts mixed batch with some conflicts
	mixedReq := &oversync.UploadRequest{
		LastServerSeqSeen: 2, // Client 1 hasn't seen Client 2's update
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID1.String(),
				ServerVersion:  1, // Stale version - will conflict
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Client 1 Update",
					"content": "This will conflict",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 4,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "New Note",
					"content": "This should succeed",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 5,
				Table:          "task",
				Op:             "UPDATE",
				PK:             taskID1.String(),
				ServerVersion:  1, // Correct version - should succeed
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated Task",
					"done": true,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 6,
				Table:          "task",
				Op:             "INSERT",
				PK:             taskID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "New Task",
					"done": false,
					"updated_at": %d
				}`, taskID2.String(), time.Now().Unix())),
			},
		},
	}

	mixedResp, httpResp := h.DoUpload(h.client1Token, mixedReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, mixedResp.Accepted)
	require.Len(t, mixedResp.Statuses, 4)

	// Verify per-table results
	// Note UPDATE should conflict (stale version)
	require.Equal(t, int64(3), mixedResp.Statuses[0].SourceChangeID)
	require.Equal(t, "conflict", mixedResp.Statuses[0].Status)
	require.NotNil(t, mixedResp.Statuses[0].ServerRow)

	// Note INSERT should succeed
	require.Equal(t, int64(4), mixedResp.Statuses[1].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[1].Status)
	require.NotNil(t, mixedResp.Statuses[1].NewServerVersion)

	// Task UPDATE should succeed
	require.Equal(t, int64(5), mixedResp.Statuses[2].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[2].Status)
	require.NotNil(t, mixedResp.Statuses[2].NewServerVersion)

	// Task INSERT should succeed
	require.Equal(t, int64(6), mixedResp.Statuses[3].SourceChangeID)
	require.Equal(t, "applied", mixedResp.Statuses[3].Status)
	require.NotNil(t, mixedResp.Statuses[3].NewServerVersion)

	// Verify database state - successful operations persisted despite conflicts
	// Note 1 should have Client 2's update (conflict prevented Client 1's update)
	note1, err := h.GetServerNote(noteID1)
	require.NoError(t, err)
	require.NotNil(t, note1)
	require.Equal(t, "Updated by Client 2", note1.Title)

	// Note 2 should exist (INSERT succeeded)
	note2, err := h.GetServerNote(noteID2)
	require.NoError(t, err)
	require.NotNil(t, note2)
	require.Equal(t, "New Note", note2.Title)

	// Task 1 should be updated (UPDATE succeeded)
	task1, err := h.GetServerTask(taskID1)
	require.NoError(t, err)
	require.NotNil(t, task1)
	require.Equal(t, "Updated Task", task1.Title)
	require.True(t, task1.Done)

	// Task 2 should exist (INSERT succeeded)
	task2, err := h.GetServerTask(taskID2)
	require.NoError(t, err)
	require.NotNil(t, task2)
	require.Equal(t, "New Task", task2.Title)
	require.False(t, task2.Done)

	t.Logf("✅ MT02 Cross-Table Operations test passed - per-table results with no cross-table rollback")
}
