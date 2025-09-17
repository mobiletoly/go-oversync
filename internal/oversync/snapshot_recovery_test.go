package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Snapshot & Recovery Tests (S01-S03) from spec section 9

func TestS01_NewClientBootstrap(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// S01 – New client bootstrap
	// Given: Server has existing data from other clients.
	// When: New client downloads from after=0.
	// Then: Receives all changes in server_id order; can reconstruct current state.

	noteID1 := h.MakeUUID("note001")
	noteID2 := h.MakeUUID("note002")
	taskID1 := h.MakeUUID("task001")

	// Phase 1: Client 1 creates initial data
	client1Req1 := &oversync.UploadRequest{
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
					"title": "First Note",
					"content": "Created by Client 1",
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
					"title": "First Task",
					"done": false,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
		},
	}

	client1Resp1, _ := h.DoUpload(h.client1Token, client1Req1)
	require.True(t, client1Resp1.Accepted)
	require.Equal(t, int64(2), client1Resp1.HighestServerSeq)

	// Phase 2: Client 2 adds more data
	client2Req1 := &oversync.UploadRequest{
		LastServerSeqSeen: 2,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID2.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Second Note",
					"content": "Created by Client 2",
					"updated_at": %d
				}`, noteID2.String(), time.Now().Unix())),
			},
		},
	}

	client2Resp1, _ := h.DoUpload(h.client2Token, client2Req1)
	require.True(t, client2Resp1.Accepted)
	require.Equal(t, int64(3), client2Resp1.HighestServerSeq)

	// Phase 3: Client 1 updates existing data
	client1Req2 := &oversync.UploadRequest{
		LastServerSeqSeen: 3,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 3,
				Table:          "note",
				Op:             "UPDATE",
				PK:             noteID1.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated First Note",
					"content": "Updated by Client 1",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
			{
				SourceChangeID: 4,
				Table:          "task",
				Op:             "UPDATE",
				PK:             taskID1.String(),
				ServerVersion:  1,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Updated First Task",
					"done": true,
					"updated_at": %d
				}`, taskID1.String(), time.Now().Unix())),
			},
		},
	}

	client1Resp2, _ := h.DoUpload(h.client1Token, client1Req2)
	require.True(t, client1Resp2.Accepted)
	require.Equal(t, int64(5), client1Resp2.HighestServerSeq)

	// Phase 4: New device (Device 3) for same user bootstraps from scratch
	// Generate a third device token for the same user
	testUserID := h.ExtractUserIDFromToken(h.client1Token)
	device3ID := "device3-" + uuid.New().String()
	client3Token, err := h.jwtAuth.GenerateToken(testUserID, device3ID, time.Hour)
	require.NoError(t, err)

	bootstrapResp, httpResp := h.DoDownload(client3Token, 0, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, bootstrapResp.Changes, 5) // All 5 changes
	require.Equal(t, int64(5), bootstrapResp.NextAfter)
	require.False(t, bootstrapResp.HasMore)

	// Verify changes are in server_id order
	expectedServerIDs := []int64{1, 2, 3, 4, 5}
	for i, change := range bootstrapResp.Changes {
		require.Equal(t, expectedServerIDs[i], change.ServerID)
	}

	// Verify change sequence tells the complete story
	changes := bootstrapResp.Changes

	// Change 1: Note INSERT
	require.Equal(t, "note", changes[0].TableName)
	require.Equal(t, "INSERT", changes[0].Op)
	require.Equal(t, noteID1.String(), changes[0].PK)
	require.Equal(t, h.client1ID, changes[0].SourceID)

	// Change 2: Task INSERT
	require.Equal(t, "task", changes[1].TableName)
	require.Equal(t, "INSERT", changes[1].Op)
	require.Equal(t, taskID1.String(), changes[1].PK)
	require.Equal(t, h.client1ID, changes[1].SourceID)

	// Change 3: Note INSERT (by Client 2)
	require.Equal(t, "note", changes[2].TableName)
	require.Equal(t, "INSERT", changes[2].Op)
	require.Equal(t, noteID2.String(), changes[2].PK)
	require.Equal(t, h.client2ID, changes[2].SourceID)

	// Change 4: Note UPDATE
	require.Equal(t, "note", changes[3].TableName)
	require.Equal(t, "UPDATE", changes[3].Op)
	require.Equal(t, noteID1.String(), changes[3].PK)
	require.Equal(t, h.client1ID, changes[3].SourceID)

	// Change 5: Task UPDATE
	require.Equal(t, "task", changes[4].TableName)
	require.Equal(t, "UPDATE", changes[4].Op)
	require.Equal(t, taskID1.String(), changes[4].PK)
	require.Equal(t, h.client1ID, changes[4].SourceID)

	// Verify Client 3 can reconstruct current state by applying changes
	// (In a real client, this would involve applying each change to local state)
	// For this test, we verify the final payloads match current server state

	// Final note1 state should match the UPDATE payload
	var note1Payload map[string]interface{}
	err = json.Unmarshal(changes[3].Payload, &note1Payload)
	require.NoError(t, err)
	require.Equal(t, "Updated First Note", note1Payload["title"])

	// Final task1 state should match the UPDATE payload
	var task1Payload map[string]interface{}
	err = json.Unmarshal(changes[4].Payload, &task1Payload)
	require.NoError(t, err)
	require.Equal(t, "Updated First Task", task1Payload["title"])
	require.Equal(t, true, task1Payload["done"])

	t.Logf("✅ S01 New Client Bootstrap test passed - client received all %d changes in order", len(bootstrapResp.Changes))
}

func TestS02_ClientRecoveryAfterOffline(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// S02 – Client recovery after offline
	// Given: Client was offline; server accumulated changes.
	// When: Client downloads from last known server_seq.
	// Then: Receives only new changes; can catch up incrementally.

	noteID1 := h.MakeUUID("note001")
	noteID2 := h.MakeUUID("note002")

	// Phase 1: Client 1 is online and syncs initial data
	initialReq := &oversync.UploadRequest{
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
					"title": "Initial Note",
					"content": "Before offline",
					"updated_at": %d
				}`, noteID1.String(), time.Now().Unix())),
			},
		},
	}

	// Sidecar: Client 2 uploads initial change so Client 1 can see it
	initialResp, _ := h.DoUpload(h.client2Token, initialReq)
	require.True(t, initialResp.Accepted)
	require.Equal(t, int64(1), initialResp.HighestServerSeq)

	// Client 1 downloads and knows about server_seq=1 (sees Client 2's change)
	downloadResp1, _ := h.DoDownload(h.client1Token, 0, 100)
	require.Len(t, downloadResp1.Changes, 1)
	require.Equal(t, int64(1), downloadResp1.NextAfter)
	lastKnownSeq := downloadResp1.NextAfter

	// Phase 2: Client 1 goes offline; other clients make changes
	// Client 2 makes changes while Client 1 is offline
	offlineChanges := []struct {
		sourceID string
		token    string
		changes  []oversync.ChangeUpload
	}{
		{
			sourceID: h.client2ID,
			token:    h.client2Token,
			changes: []oversync.ChangeUpload{
				{
					SourceChangeID: 2, // Client 2 already used SourceChangeID=1 for initial upload
					Table:          "note",
					Op:             "INSERT",
					PK:             noteID2.String(),
					ServerVersion:  0,
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Note by Client 2",
						"content": "While Client 1 offline",
						"updated_at": %d
					}`, noteID2.String(), time.Now().Unix())),
				},
			},
		},
		{
			sourceID: h.client2ID,
			token:    h.client2Token,
			changes: []oversync.ChangeUpload{
				{
					SourceChangeID: 3, // Client 2 used SourceChangeID=1,2 already
					Table:          "note",
					Op:             "UPDATE",
					PK:             noteID1.String(),
					ServerVersion:  1,
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Updated by Client 2",
						"content": "Client 1 was offline",
						"updated_at": %d
					}`, noteID1.String(), time.Now().Unix())),
				},
			},
		},
	}

	currentSeq := lastKnownSeq
	for _, batch := range offlineChanges {
		req := &oversync.UploadRequest{
			LastServerSeqSeen: currentSeq,
			Changes:           batch.changes,
		}

		resp, _ := h.DoUpload(batch.token, req)
		require.True(t, resp.Accepted)
		currentSeq = resp.HighestServerSeq
	}

	finalSeqWhileOffline := currentSeq
	require.Equal(t, int64(3), finalSeqWhileOffline) // Should be 3 total changes

	// Phase 3: Client 1 comes back online and recovers
	recoveryResp, httpResp := h.DoDownload(h.client1Token, lastKnownSeq, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, recoveryResp.Changes, 2) // Only new changes since offline
	require.Equal(t, int64(3), recoveryResp.NextAfter)
	require.False(t, recoveryResp.HasMore)

	// Verify Client 1 received only the changes that happened while offline
	changes := recoveryResp.Changes

	// Change 1: Note INSERT by Client 2
	require.Equal(t, int64(2), changes[0].ServerID)
	require.Equal(t, "note", changes[0].TableName)
	require.Equal(t, "INSERT", changes[0].Op)
	require.Equal(t, noteID2.String(), changes[0].PK)
	require.Equal(t, h.client2ID, changes[0].SourceID)

	// Change 2: Note UPDATE by Client 2
	require.Equal(t, int64(3), changes[1].ServerID)
	require.Equal(t, "note", changes[1].TableName)
	require.Equal(t, "UPDATE", changes[1].Op)
	require.Equal(t, noteID1.String(), changes[1].PK)
	require.Equal(t, h.client2ID, changes[1].SourceID)

	// Verify payloads contain the expected data
	var insertPayload map[string]interface{}
	err := json.Unmarshal(changes[0].Payload, &insertPayload)
	require.NoError(t, err)
	require.Equal(t, "Note by Client 2", insertPayload["title"])

	var updatePayload map[string]interface{}
	err = json.Unmarshal(changes[1].Payload, &updatePayload)
	require.NoError(t, err)
	require.Equal(t, "Updated by Client 2", updatePayload["title"])

	t.Logf("✅ S02 Client Recovery After Offline test passed - client caught up with %d new changes", len(recoveryResp.Changes))
}

func TestS03_PartialSyncWithPagination(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// S03 – Partial sync with pagination
	// Given: Large number of changes accumulated.
	// When: Client downloads with small limit.
	// Then: Receives partial results; has_more=true; can continue with next batch.

	// Phase 1: Create many changes (more than typical pagination limit)
	numChanges := 15
	noteIDs := make([]string, numChanges)

	for i := 0; i < numChanges; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("note%03d", i))
		noteIDs[i] = noteID.String()

		req := &oversync.UploadRequest{
			LastServerSeqSeen: int64(i),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(i + 1),
					Table:          "note",
					Op:             "INSERT",
					PK:             noteID.String(),
					ServerVersion:  0,
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Note %d",
						"content": "Content for note %d",
						"updated_at": %d
					}`, noteID.String(), i, i, time.Now().Unix())),
				},
			},
		}

		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
		require.Equal(t, int64(i+1), resp.HighestServerSeq)
	}

	// Phase 2: Client downloads with small pagination limit
	pageSize := 5
	var allChanges []oversync.ChangeDownloadResponse
	currentAfter := int64(0)
	pageCount := 0

	for {
		pageCount++
		downloadResp, httpResp := h.DoDownload(h.client2Token, currentAfter, pageSize)
		require.Equal(t, http.StatusOK, httpResp.StatusCode)
		require.LessOrEqual(t, len(downloadResp.Changes), pageSize)

		allChanges = append(allChanges, downloadResp.Changes...)

		t.Logf("Page %d: received %d changes, has_more=%t, next_after=%d",
			pageCount, len(downloadResp.Changes), downloadResp.HasMore, downloadResp.NextAfter)

		if !downloadResp.HasMore {
			break
		}

		// Verify pagination consistency
		require.True(t, downloadResp.HasMore)
		require.Greater(t, downloadResp.NextAfter, currentAfter)
		currentAfter = downloadResp.NextAfter
	}

	// Phase 3: Verify complete sync through pagination
	require.Equal(t, numChanges, len(allChanges))
	require.GreaterOrEqual(t, pageCount, 3) // Should have taken multiple pages

	// Verify all changes received in correct order
	for i, change := range allChanges {
		expectedServerID := int64(i + 1)
		require.Equal(t, expectedServerID, change.ServerID)
		require.Equal(t, "note", change.TableName)
		require.Equal(t, "INSERT", change.Op)
		require.Equal(t, noteIDs[i], change.PK)
		require.Equal(t, h.client1ID, change.SourceID)
		require.Equal(t, int64(i+1), change.SourceChangeID)

		// Verify payload
		var payload map[string]interface{}
		err := json.Unmarshal(change.Payload, &payload)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("Note %d", i), payload["title"])
	}

	// Phase 4: Verify client can continue from any point
	// Download from middle of the sequence
	midPoint := int64(7)
	partialResp, _ := h.DoDownload(h.client2Token, midPoint, 100)
	expectedRemaining := numChanges - int(midPoint)
	require.Len(t, partialResp.Changes, expectedRemaining)
	require.False(t, partialResp.HasMore)

	// Verify the partial download starts from the correct point
	require.Equal(t, midPoint+1, partialResp.Changes[0].ServerID)

	t.Logf("✅ S03 Partial Sync with Pagination test passed - %d changes synced in %d pages", numChanges, pageCount)
}
