package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Concrete Examples Tests (X01-X02) from spec section 15

func TestX01_NoteAppScenario(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// X01 – Note app scenario
	// Given: User has note app on phone and laptop.
	// When: Creates/edits notes on both devices offline, then syncs.
	// Then: All changes merged correctly; conflicts resolved; consistent state.

	// Simulate two devices for the same user
	phoneToken := h.client1Token
	laptopToken := h.client2Token

	// Phase 1: Both devices start offline and create notes
	phoneNoteID := h.MakeUUID("phone001")
	laptopNoteID := h.MakeUUID("laptop001")
	sharedNoteID := h.MakeUUID("shared001")

	// Phone creates notes offline
	phoneOfflineReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             phoneNoteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Phone Note",
					"content": "Created on phone while offline",
					"device": "phone",
					"created_at": %d,
					"updated_at": %d
				}`, phoneNoteID.String(), time.Now().Add(-2*time.Hour).Unix(), time.Now().Add(-2*time.Hour).Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "INSERT",
				PK:             sharedNoteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Note",
					"content": "Created on phone first",
					"device": "phone",
					"created_at": %d,
					"updated_at": %d
				}`, sharedNoteID.String(), time.Now().Add(-90*time.Minute).Unix(), time.Now().Add(-90*time.Minute).Unix())),
			},
		},
	}

	// Laptop creates notes offline (different notes + same shared note)
	laptopOfflineReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             laptopNoteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Laptop Note",
					"content": "Created on laptop while offline",
					"device": "laptop",
					"created_at": %d,
					"updated_at": %d
				}`, laptopNoteID.String(), time.Now().Add(-1*time.Hour).Unix(), time.Now().Add(-1*time.Hour).Unix())),
			},
		},
	}

	// Phase 2: Phone comes online first and syncs
	phoneResp, httpResp := h.DoUpload(phoneToken, phoneOfflineReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, phoneResp.Accepted)
	require.Equal(t, int64(2), phoneResp.HighestServerSeq)

	// Phone downloads to see current state (excludes own changes)
	phoneDownload1, _ := h.DoDownload(phoneToken, 0, 100)
	require.Len(t, phoneDownload1.Changes, 0) // Phone doesn't see its own changes

	// Phase 3: Laptop comes online and syncs
	laptopResp, httpResp := h.DoUpload(laptopToken, laptopOfflineReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, laptopResp.Accepted)
	require.Equal(t, int64(3), laptopResp.HighestServerSeq)

	// Laptop downloads to get all changes (excludes own changes)
	laptopDownload1, _ := h.DoDownload(laptopToken, 0, 100)
	require.Len(t, laptopDownload1.Changes, 2) // Phone's 2 changes only (laptop doesn't see its own)

	// Phase 4: Both devices now edit the shared note (conflict scenario)
	// Phone edits shared note
	phoneEditReq := &oversync.UploadRequest{
		LastServerSeqSeen: 3,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "UPDATE",
				PK:             sharedNoteID.String(),
				ServerVersion:  1, // Version when phone created it
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Note - Phone Edit",
					"content": "Updated on phone with important changes",
					"device": "phone",
					"last_editor": "phone_user",
					"updated_at": %d
				}`, sharedNoteID.String(), time.Now().Unix())),
			},
		},
	}

	phoneEditResp, _ := h.DoUpload(phoneToken, phoneEditReq)
	require.True(t, phoneEditResp.Accepted)
	require.Equal(t, "applied", phoneEditResp.Statuses[0].Status)

	// Laptop also edits shared note (will conflict)
	laptopEditReq := &oversync.UploadRequest{
		LastServerSeqSeen: 3,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "note",
				Op:             "UPDATE",
				PK:             sharedNoteID.String(),
				ServerVersion:  1, // Same version - will conflict
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Note - Laptop Edit",
					"content": "Updated on laptop with different changes",
					"device": "laptop",
					"last_editor": "laptop_user",
					"updated_at": %d
				}`, sharedNoteID.String(), time.Now().Unix())),
			},
		},
	}

	laptopEditResp, _ := h.DoUpload(laptopToken, laptopEditReq)
	require.True(t, laptopEditResp.Accepted)
	require.Equal(t, "conflict", laptopEditResp.Statuses[0].Status)

	// Phase 5: Laptop resolves conflict by merging changes
	var serverState map[string]interface{}
	err := json.Unmarshal(laptopEditResp.Statuses[0].ServerRow, &serverState)
	require.NoError(t, err)

	// Laptop creates merged version
	mergedContent := fmt.Sprintf("MERGED: Phone: %s | Laptop: %s",
		"Updated on phone with important changes",
		"Updated on laptop with different changes")

	laptopMergeReq := &oversync.UploadRequest{
		LastServerSeqSeen: 4,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "UPDATE",
				PK:             sharedNoteID.String(),
				ServerVersion:  int64(serverState["server_version"].(float64)), // Current server version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Shared Note - Merged",
					"content": "%s",
					"device": "laptop",
					"last_editor": "laptop_user_merged",
					"merge_timestamp": %d,
					"updated_at": %d
				}`, sharedNoteID.String(), mergedContent, time.Now().Unix(), time.Now().Unix())),
			},
		},
	}

	laptopMergeResp, _ := h.DoUpload(laptopToken, laptopMergeReq)
	require.True(t, laptopMergeResp.Accepted)
	require.Equal(t, "applied", laptopMergeResp.Statuses[0].Status)

	// Phase 6: Both devices sync to final consistent state (each sees other's changes)
	finalPhoneDownload, _ := h.DoDownload(phoneToken, 0, 100)
	finalLaptopDownload, _ := h.DoDownload(laptopToken, 0, 100)

	// Sidecar: Each device sees the OTHER device's changes
	// Phone sees: laptop note, laptop merge (laptop's conflicting edit is not logged per new spec)
	// Laptop sees: phone note, shared note, phone edit (all successful)
	require.Len(t, finalPhoneDownload.Changes, 2)  // laptop's successful changes
	require.Len(t, finalLaptopDownload.Changes, 3) // phone's successful changes

	// Verify phone sees laptop's changes
	_ = finalPhoneDownload.Changes // phoneSeesChanges

	// Verify laptop sees phone's changes
	_ = finalLaptopDownload.Changes // laptopSeesChanges

	// Verify the scenario worked correctly by checking key changes
	// Phone should see laptop's final merge
	var foundMerge bool
	for _, change := range finalPhoneDownload.Changes {
		if change.TableName == "note" && change.PK == sharedNoteID.String() && change.Op == "UPDATE" {
			var payload map[string]interface{}
			err := json.Unmarshal(change.Payload, &payload)
			require.NoError(t, err)
			if title, ok := payload["title"].(string); ok && title == "Shared Note - Merged" {
				foundMerge = true
				break
			}
		}
	}
	require.True(t, foundMerge, "Phone should see laptop's merge of the shared note")

	// Laptop should see phone's original changes
	var foundPhoneNote bool
	for _, change := range finalLaptopDownload.Changes {
		if change.TableName == "note" && change.PK == phoneNoteID.String() {
			foundPhoneNote = true
			break
		}
	}
	require.True(t, foundPhoneNote, "Laptop should see phone's note")

	t.Logf("✅ X01 Note App Scenario test passed - sidecar sync with conflict resolution working correctly")
}

func TestX02_TaskManagerScenario(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// X02 – Task manager scenario
	// Given: Team uses task manager; members create/assign/complete tasks.
	// When: Multiple team members work simultaneously.
	// Then: Task states synchronized; assignments tracked; completion status consistent.

	// Simulate team members
	aliceToken := h.client1Token
	bobToken := h.client2Token

	// Phase 1: Alice creates project tasks
	task1ID := h.MakeUUID("taska")
	task2ID := h.MakeUUID("taskb")
	task3ID := h.MakeUUID("taskc")

	aliceCreateReq := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "task",
				Op:             "INSERT",
				PK:             task1ID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Setup Database",
					"description": "Create database schema and initial data",
					"assignee": "unassigned",
					"creator": "alice",
					"priority": "high",
					"status": "todo",
					"done": false,
					"created_at": %d,
					"updated_at": %d
				}`, task1ID.String(), time.Now().Unix(), time.Now().Unix())),
			},
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "INSERT",
				PK:             task2ID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Implement API",
					"description": "Build REST API endpoints",
					"assignee": "unassigned",
					"creator": "alice",
					"priority": "medium",
					"status": "todo",
					"done": false,
					"created_at": %d,
					"updated_at": %d
				}`, task2ID.String(), time.Now().Unix(), time.Now().Unix())),
			},
			{
				SourceChangeID: 3,
				Table:          "task",
				Op:             "INSERT",
				PK:             task3ID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Write Tests",
					"description": "Create comprehensive test suite",
					"assignee": "unassigned",
					"creator": "alice",
					"priority": "medium",
					"status": "todo",
					"done": false,
					"created_at": %d,
					"updated_at": %d
				}`, task3ID.String(), time.Now().Unix(), time.Now().Unix())),
			},
		},
	}

	aliceCreateResp, httpResp := h.DoUpload(aliceToken, aliceCreateReq)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, aliceCreateResp.Accepted)

	// Phase 2: Bob syncs and sees all tasks
	bobDownload1, _ := h.DoDownload(bobToken, 0, 100)
	require.Len(t, bobDownload1.Changes, 3)

	// Phase 3: Both Alice and Bob assign themselves tasks simultaneously
	// Get current server_versions of tasks
	task1, err := h.GetServerTask(task1ID)
	require.NoError(t, err)
	require.NotNil(t, task1)
	task1ServerVersion := task1.ServerVersion

	// Alice assigns task1 to herself
	aliceAssignReq := &oversync.UploadRequest{
		LastServerSeqSeen: 3,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 4,
				Table:          "task",
				Op:             "UPDATE",
				PK:             task1ID.String(),
				ServerVersion:  task1ServerVersion, // Use actual current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Setup Database",
					"description": "Create database schema and initial data",
					"assignee": "alice",
					"creator": "alice",
					"priority": "high",
					"status": "in_progress",
					"done": false,
					"assigned_at": %d,
					"updated_at": %d
				}`, task1ID.String(), time.Now().Unix(), time.Now().Unix())),
			},
		},
	}

	// Get current server_version of task2
	task2, err := h.GetServerTask(task2ID)
	require.NoError(t, err)
	require.NotNil(t, task2)
	task2ServerVersion := task2.ServerVersion

	// Bob assigns task2 to himself
	bobAssignReq := &oversync.UploadRequest{
		LastServerSeqSeen: 4, // After Alice's assignment
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "task",
				Op:             "UPDATE",
				PK:             task2ID.String(),
				ServerVersion:  task2ServerVersion, // Use actual current version
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Implement API",
					"description": "Build REST API endpoints",
					"assignee": "bob",
					"creator": "alice",
					"priority": "medium",
					"status": "in_progress",
					"done": false,
					"assigned_at": %d,
					"updated_at": %d
				}`, task2ID.String(), time.Now().Unix(), time.Now().Unix())),
			},
		},
	}

	// Upload Alice's assignment first
	aliceAssignResp, _ := h.DoUpload(aliceToken, aliceAssignReq)
	require.True(t, aliceAssignResp.Accepted)
	require.Equal(t, "applied", aliceAssignResp.Statuses[0].Status)

	// Then Bob's assignment (should also succeed since different tasks)
	bobAssignResp, _ := h.DoUpload(bobToken, bobAssignReq)
	require.True(t, bobAssignResp.Accepted)
	require.Equal(t, "applied", bobAssignResp.Statuses[0].Status)

	// Phase 4: Alice completes her task
	aliceCompleteReq := &oversync.UploadRequest{
		LastServerSeqSeen: 5, // After both assignments
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 5,
				Table:          "task",
				Op:             "UPDATE",
				PK:             task1ID.String(),
				ServerVersion:  2, // After her assignment
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Setup Database",
					"description": "Create database schema and initial data",
					"assignee": "alice",
					"creator": "alice",
					"priority": "high",
					"status": "completed",
					"done": true,
					"assigned_at": %d,
					"completed_at": %d,
					"updated_at": %d
				}`, task1ID.String(), time.Now().Add(-10*time.Minute).Unix(), time.Now().Unix(), time.Now().Unix())),
			},
		},
	}

	aliceCompleteResp, _ := h.DoUpload(aliceToken, aliceCompleteReq)
	require.True(t, aliceCompleteResp.Accepted)
	require.Equal(t, "applied", aliceCompleteResp.Statuses[0].Status)

	// Phase 5: Bob also tries to assign task3 to Alice (team coordination)
	bobAssignTask3Req := &oversync.UploadRequest{
		LastServerSeqSeen: 6,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 2,
				Table:          "task",
				Op:             "UPDATE",
				PK:             task3ID.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Write Tests",
					"description": "Create comprehensive test suite",
					"assignee": "alice",
					"creator": "alice",
					"priority": "medium",
					"status": "todo",
					"done": false,
					"assigned_by": "bob",
					"updated_at": %d
				}`, task3ID.String(), time.Now().Unix())),
			},
		},
	}

	bobAssignTask3Resp, _ := h.DoUpload(bobToken, bobAssignTask3Req)
	require.True(t, bobAssignTask3Resp.Accepted)

	// Phase 6: Final sync - both team members download changes (sidecar: excludes own changes)
	finalAliceDownload, _ := h.DoDownload(aliceToken, 0, 100)
	finalBobDownload, _ := h.DoDownload(bobToken, 0, 100)

	// Sidecar: Each client sees only the OTHER's changes
	// Alice sees Bob's changes (initial 3 tasks + Bob's updates)
	// Bob sees Alice's changes (Alice's updates)
	totalChanges := len(finalAliceDownload.Changes) + len(finalBobDownload.Changes)
	require.GreaterOrEqual(t, totalChanges, 4) // At least 4 changes (3 initial + updates)

	// Verify task states by combining all changes from both clients
	allChanges := append(finalAliceDownload.Changes, finalBobDownload.Changes...)
	require.GreaterOrEqual(t, len(allChanges), 2) // At least 2 operations (each client's final change)

	// Sort changes by ServerID to apply them in correct order
	sort.Slice(allChanges, func(i, j int) bool {
		return allChanges[i].ServerID < allChanges[j].ServerID
	})

	// Build final task states by applying all changes in order
	taskStates := make(map[string]map[string]interface{})
	for _, change := range allChanges {
		if change.TableName == "task" {
			var payload map[string]interface{}
			err := json.Unmarshal(change.Payload, &payload)
			require.NoError(t, err)
			taskStates[change.PK] = payload
		}
	}

	// Verify final task assignments and states
	require.Len(t, taskStates, 3)

	// Task 1: Should be completed by Alice
	task1State := taskStates[task1ID.String()]
	require.Equal(t, "alice", task1State["assignee"])
	require.Equal(t, "completed", task1State["status"])
	require.Equal(t, true, task1State["done"])

	// Task 2: Should be assigned to Bob and in progress
	task2State := taskStates[task2ID.String()]
	require.Equal(t, "bob", task2State["assignee"])
	require.Equal(t, "in_progress", task2State["status"])
	require.Equal(t, false, task2State["done"])

	// Task 3: Should be assigned to Alice by Bob
	task3State := taskStates[task3ID.String()]
	require.Equal(t, "alice", task3State["assignee"])
	require.Equal(t, "bob", task3State["assigned_by"])

	t.Logf("✅ X02 Task Manager Scenario test passed - %d tasks synchronized across team members", len(taskStates))
}
