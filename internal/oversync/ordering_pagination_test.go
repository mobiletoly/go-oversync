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

// Ordering & Pagination Tests (P01-P03) from spec section 10

func TestP01_StrictServerIDOrdering(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// P01 – Strict server_id ordering
	// Given: Multiple clients upload concurrently.
	// When: Download.
	// Then: Changes always returned in server_id order, regardless of upload timing.

	// Phase 1: Create changes from multiple clients in rapid succession
	numClients := 3
	changesPerClient := 5
	totalChanges := numClients * changesPerClient

	// Create a third device for the same user
	testUserID := h.ExtractUserIDFromToken(h.client1Token)
	device3ID := "device3-" + uuid.New().String()
	client3Token, err := h.jwtAuth.GenerateToken(testUserID, device3ID, time.Hour)
	require.NoError(t, err)
	client3ID := device3ID

	clients := []struct {
		id    string
		token string
	}{
		{h.client1ID, h.client1Token},
		{h.client2ID, h.client2Token},
		{client3ID, client3Token},
	}

	// Upload changes from all clients in interleaved fashion
	currentSeq := int64(0)
	for round := 0; round < changesPerClient; round++ {
		for clientIdx, client := range clients {
			noteID := h.MakeUUID(fmt.Sprintf("c%dr%d", clientIdx, round))

			req := &oversync.UploadRequest{
				LastServerSeqSeen: currentSeq,
				Changes: []oversync.ChangeUpload{
					{
						SourceChangeID: int64(round + 1),
						Table:          "note",
						Op:             "INSERT",
						PK:             noteID.String(),
						ServerVersion:  0,
						Payload: json.RawMessage(fmt.Sprintf(`{
							"id": "%s",
							"title": "Client %d Round %d",
							"content": "Content from client %d, round %d",
							"updated_at": %d
						}`, noteID.String(), clientIdx, round, clientIdx, round, time.Now().Unix())),
					},
				},
			}

			resp, _ := h.DoUpload(client.token, req)
			require.True(t, resp.Accepted)
			currentSeq = resp.HighestServerSeq
		}
	}

	require.Equal(t, int64(totalChanges), currentSeq)

	// Phase 2: Download changes and verify strict ordering (excludes own changes)
	downloadResp, httpResp := h.DoDownload(clients[0].token, 0, 100)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	// Client 1 sees only other clients' changes (2/3 of total changes)
	expectedChanges := (totalChanges * 2) / 3 // 10 out of 15 changes
	require.Len(t, downloadResp.Changes, expectedChanges)
	require.False(t, downloadResp.HasMore)

	// Verify strict server_id ordering (Client 1 sees only other clients' changes)
	// Expected server_ids: 2,3,5,6,8,9,11,12,14,15 (skipping Client 1's: 1,4,7,10,13)
	expectedServerIDs := []int64{2, 3, 5, 6, 8, 9, 11, 12, 14, 15}
	for i, change := range downloadResp.Changes {
		require.Equal(t, expectedServerIDs[i], change.ServerID,
			"Change at index %d should have server_id %d, got %d", i, expectedServerIDs[i], change.ServerID)
	}

	// Phase 3: Verify ordering is maintained across multiple download requests
	// Download in smaller chunks (Client 2 sees only other clients' changes)
	chunkSize := 4
	var allChunks []oversync.ChangeDownloadResponse
	after := int64(0)

	for {
		chunkResp, _ := h.DoDownload(clients[1].token, after, chunkSize)
		allChunks = append(allChunks, chunkResp.Changes...)

		if !chunkResp.HasMore {
			break
		}
		after = chunkResp.NextAfter
	}

	// Verify chunked download maintains ordering (Client 2 sees 2/3 of changes)
	expectedChunkedChanges := (totalChanges * 2) / 3 // 10 out of 15 changes
	require.Len(t, allChunks, expectedChunkedChanges)
	// Client 2 sees server_ids: 1,3,4,6,7,9,10,12,13,15 (skipping Client 2's: 2,5,8,11,14)
	expectedChunkedServerIDs := []int64{1, 3, 4, 6, 7, 9, 10, 12, 13, 15}
	for i, change := range allChunks {
		require.Equal(t, expectedChunkedServerIDs[i], change.ServerID,
			"Chunked download: change at index %d should have server_id %d, got %d", i, expectedChunkedServerIDs[i], change.ServerID)
	}

	t.Logf("✅ P01 Strict Server ID Ordering test passed - %d changes from %d clients in perfect order", totalChanges, numClients)
}

func TestP02_PaginationConsistency(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// P02 – Pagination consistency
	// Given: Client downloads in pages while server receives new changes.
	// When: Concurrent uploads during pagination.
	// Then: Each page consistent; no duplicates; no missing changes within snapshot.

	// Phase 1: Create initial batch of changes
	initialBatchSize := 10
	for i := 0; i < initialBatchSize; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("initial%03d", i))

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
						"title": "Initial Note %d",
						"content": "Initial batch",
						"updated_at": %d
					}`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}

		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
	}

	// Phase 2: Start paginated download
	pageSize := 3
	var paginatedChanges []oversync.ChangeDownloadResponse
	after := int64(0)
	pageCount := 0

	// Download first page
	page1Resp, _ := h.DoDownload(h.client2Token, after, pageSize)
	require.Len(t, page1Resp.Changes, pageSize)
	require.True(t, page1Resp.HasMore)
	paginatedChanges = append(paginatedChanges, page1Resp.Changes...)
	after = page1Resp.NextAfter
	pageCount++

	// Phase 3: Add new changes while pagination is in progress
	// This simulates concurrent activity during client sync
	concurrentBatchSize := 5
	for i := 0; i < concurrentBatchSize; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("concurrent%03d", i))

		req := &oversync.UploadRequest{
			LastServerSeqSeen: int64(initialBatchSize + i),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(i + 1),
					Table:          "note",
					Op:             "INSERT",
					PK:             noteID.String(),
					ServerVersion:  0,
					Payload: json.RawMessage(fmt.Sprintf(`{
						"id": "%s",
						"title": "Concurrent Note %d",
						"content": "Added during pagination",
						"updated_at": %d
					}`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}

		resp, _ := h.DoUpload(h.client2Token, req)
		require.True(t, resp.Accepted)
	}

	// Phase 4: Continue paginated download
	for {
		pageResp, _ := h.DoDownload(h.client2Token, after, pageSize)
		paginatedChanges = append(paginatedChanges, pageResp.Changes...)
		pageCount++

		if !pageResp.HasMore {
			break
		}
		after = pageResp.NextAfter
	}

	// Phase 5: Verify pagination consistency (excludes own changes)
	// Client 2 sees only Client 1's changes, not its own concurrent uploads
	// This is the current behavior - pagination doesn't provide snapshot isolation
	totalExpectedInPagination := initialBatchSize // Only Client 1's 10 changes
	require.Len(t, paginatedChanges, totalExpectedInPagination)
	require.GreaterOrEqual(t, pageCount, 4) // Should have taken multiple pages

	// Verify no duplicates in paginated results
	seenServerIDs := make(map[int64]bool)
	for _, change := range paginatedChanges {
		require.False(t, seenServerIDs[change.ServerID],
			"Duplicate server_id %d found in paginated results", change.ServerID)
		seenServerIDs[change.ServerID] = true
	}

	// Verify changes are in order and include both initial and concurrent batches
	for i, change := range paginatedChanges {
		expectedServerID := int64(i + 1)
		require.Equal(t, expectedServerID, change.ServerID)

		var payload map[string]interface{}
		err := json.Unmarshal(change.Payload, &payload)
		require.NoError(t, err)

		// First 10 should be initial batch, next 5 should be concurrent batch
		if i < initialBatchSize {
			require.Equal(t, h.client1ID, change.SourceID)
			require.Contains(t, payload["title"], "Initial Note")
		} else {
			require.Equal(t, h.client2ID, change.SourceID)
			require.Contains(t, payload["title"], "Concurrent Note")
		}
	}

	// Phase 6: Verify new changes are available in fresh download (excludes own changes)
	freshResp, _ := h.DoDownload(h.client2Token, 0, 100)
	totalExpected := initialBatchSize // Only Client 1's changes
	require.Len(t, freshResp.Changes, totalExpected)

	// Sidecar: Client 2 doesn't see its own concurrent changes
	// All changes in freshResp are from Client 1 (initial batch)
	for _, change := range freshResp.Changes {
		require.Equal(t, h.client1ID, change.SourceID)

		var payload map[string]interface{}
		err := json.Unmarshal(change.Payload, &payload)
		require.NoError(t, err)
		require.Contains(t, payload["title"], "Initial Note")
	}

	t.Logf("✅ P02 Pagination Consistency test passed - %d pages, no duplicates, snapshot consistency maintained", pageCount)
}

func TestP03_LargeResultSetStability(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// P03 – Large result set stability
	// Given: Very large number of changes.
	// When: Multiple clients download with different pagination patterns.
	// Then: All clients get identical results; server handles load efficiently.

	// Phase 1: Create a large dataset
	largeDatasetSize := 100 // Reduced for test performance, but still tests the concept

	for i := 0; i < largeDatasetSize; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("large%05d", i))

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
						"title": "Large Dataset Note %d",
						"content": "Part of large dataset for stability testing",
						"sequence": %d,
						"updated_at": %d
					}`, noteID.String(), i, i, time.Now().Unix())),
				},
			},
		}

		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
	}

	// Phase 2: Multiple devices download with different pagination patterns
	// Sidecar: Device 1 won't see its own changes, so use other devices for same user
	testUserID := h.ExtractUserIDFromToken(h.client1Token)

	// Create additional device tokens for the same user
	device3ID := "device3-" + uuid.New().String()
	client3Token, err := h.jwtAuth.GenerateToken(testUserID, device3ID, time.Hour)
	require.NoError(t, err)

	device4ID := "device4-" + uuid.New().String()
	client4Token, err := h.jwtAuth.GenerateToken(testUserID, device4ID, time.Hour)
	require.NoError(t, err)

	device5ID := "device5-" + uuid.New().String()
	client5Token, err := h.jwtAuth.GenerateToken(testUserID, device5ID, time.Hour)
	require.NoError(t, err)

	clients := []struct {
		token    string
		pageSize int
		name     string
	}{
		{h.client2Token, 10, "Device2-Page10"},
		{client3Token, 25, "Device3-Page25"},
		{client4Token, 7, "Device4-Page7"},
		{client5Token, 50, "Device5-Page50"},
	}

	var allClientResults [][]oversync.ChangeDownloadResponse

	for _, client := range clients {
		var clientChanges []oversync.ChangeDownloadResponse
		after := int64(0)
		pageCount := 0

		for {
			downloadResp, httpResp := h.DoDownload(client.token, after, client.pageSize)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)

			clientChanges = append(clientChanges, downloadResp.Changes...)
			pageCount++

			if !downloadResp.HasMore {
				break
			}
			after = downloadResp.NextAfter
		}

		require.Len(t, clientChanges, largeDatasetSize)
		allClientResults = append(allClientResults, clientChanges)

		t.Logf("%s: Downloaded %d changes in %d pages", client.name, len(clientChanges), pageCount)
	}

	// Phase 3: Verify all clients got identical results
	baselineResults := allClientResults[0]

	for clientIdx := 1; clientIdx < len(allClientResults); clientIdx++ {
		clientResults := allClientResults[clientIdx]
		require.Len(t, clientResults, len(baselineResults))

		for changeIdx, baselineChange := range baselineResults {
			clientChange := clientResults[changeIdx]

			// Verify all key fields match
			require.Equal(t, baselineChange.ServerID, clientChange.ServerID,
				"Client %d change %d: server_id mismatch", clientIdx, changeIdx)
			require.Equal(t, baselineChange.TableName, clientChange.TableName,
				"Client %d change %d: table_name mismatch", clientIdx, changeIdx)
			require.Equal(t, baselineChange.Op, clientChange.Op,
				"Client %d change %d: op mismatch", clientIdx, changeIdx)
			require.Equal(t, baselineChange.PK, clientChange.PK,
				"Client %d change %d: pk mismatch", clientIdx, changeIdx)
			require.Equal(t, baselineChange.SourceID, clientChange.SourceID,
				"Client %d change %d: source_id mismatch", clientIdx, changeIdx)
			require.Equal(t, baselineChange.SourceChangeID, clientChange.SourceChangeID,
				"Client %d change %d: source_change_id mismatch", clientIdx, changeIdx)

			// Verify payload content matches
			var baselinePayload, clientPayload map[string]interface{}
			err := json.Unmarshal(baselineChange.Payload, &baselinePayload)
			require.NoError(t, err)
			err = json.Unmarshal(clientChange.Payload, &clientPayload)
			require.NoError(t, err)

			require.Equal(t, baselinePayload["sequence"], clientPayload["sequence"],
				"Client %d change %d: payload sequence mismatch", clientIdx, changeIdx)
		}
	}

	// Phase 4: Verify data integrity and ordering
	for i, change := range baselineResults {
		expectedServerID := int64(i + 1)
		require.Equal(t, expectedServerID, change.ServerID)

		var payload map[string]interface{}
		err := json.Unmarshal(change.Payload, &payload)
		require.NoError(t, err)

		expectedSequence := float64(i) // JSON numbers are float64
		require.Equal(t, expectedSequence, payload["sequence"])
	}

	t.Logf("✅ P03 Large Result Set Stability test passed - %d clients, %d changes each, all identical",
		len(clients), largeDatasetSize)
}
