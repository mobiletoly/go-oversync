package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Performance & Load Tests (L01-L03) from spec section 12

func TestL01_ConcurrentUploads(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// L01 – Concurrent uploads
	// Given: Multiple clients upload simultaneously.
	// When: High concurrency.
	// Then: All uploads processed correctly; no data corruption; server_id sequence maintained.

	numClients := 10
	uploadsPerClient := 5
	totalUploads := numClients * uploadsPerClient

	// Create multiple client tokens
	clients := make([]struct {
		id    string
		token string
	}, numClients)

	for i := 0; i < numClients; i++ {
		token := h.GenerateJWT(fmt.Sprintf("client%d", i))
		clients[i] = struct {
			id    string
			token string
		}{
			id:    h.ExtractClientIDFromToken(token),
			token: token,
		}
	}

	// Concurrent upload goroutines
	var wg sync.WaitGroup
	results := make(chan struct {
		clientIdx int
		uploadIdx int
		success   bool
		serverSeq int64
	}, totalUploads)

	startTime := time.Now()

	// Pre-generate UUIDs to avoid race conditions in fmt.Sprintf
	type uploadJob struct {
		clientIdx int
		uploadIdx int
		client    struct {
			id    string
			token string
		}
		noteID    uuid.UUID
		noteIDStr string
	}

	var jobs []uploadJob
	for clientIdx, client := range clients {
		for uploadIdx := 0; uploadIdx < uploadsPerClient; uploadIdx++ {
			noteID := h.MakeUUID(fmt.Sprintf("c%du%d", clientIdx, uploadIdx))
			jobs = append(jobs, uploadJob{
				clientIdx: clientIdx,
				uploadIdx: uploadIdx,
				client:    client,
				noteID:    noteID,
				noteIDStr: noteID.String(),
			})
		}
	}

	// Launch concurrent uploads
	for _, job := range jobs {
		wg.Add(1)
		go func(j uploadJob) {
			defer wg.Done()
			req := &oversync.UploadRequest{
				LastServerSeqSeen: 0, // Simplified for concurrent test
				Changes: []oversync.ChangeUpload{
					{
						SourceChangeID: int64(j.uploadIdx + 1),
						Table:          "note",
						Op:             "INSERT",
						PK:             j.noteIDStr,
						ServerVersion:  0,
						Payload: json.RawMessage(fmt.Sprintf(`{
								"id": "%s",
								"title": "Concurrent Note C%d U%d",
								"content": "Created concurrently",
								"client_idx": %d,
								"upload_idx": %d,
								"updated_at": %d
							}`, j.noteIDStr, j.clientIdx, j.uploadIdx, j.clientIdx, j.uploadIdx, time.Now().Unix())),
					},
				},
			}

			resp, httpResp := h.DoUpload(j.client.token, req)
			success := httpResp.StatusCode == http.StatusOK && resp.Accepted

			results <- struct {
				clientIdx int
				uploadIdx int
				success   bool
				serverSeq int64
			}{
				clientIdx: j.clientIdx,
				uploadIdx: j.uploadIdx,
				success:   success,
				serverSeq: resp.HighestServerSeq,
			}
		}(job)
	}

	// Wait for all uploads to complete
	wg.Wait()
	close(results)

	duration := time.Since(startTime)

	// Collect results
	var successCount int
	var serverSeqs []int64
	for result := range results {
		if result.success {
			successCount++
			serverSeqs = append(serverSeqs, result.serverSeq)
		}
	}

	// Verify all uploads succeeded
	require.Equal(t, totalUploads, successCount, "All concurrent uploads should succeed")

	// Verify database consistency
	var noteCount int
	err := h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM note`).Scan(&noteCount)
	require.NoError(t, err)
	require.Equal(t, totalUploads, noteCount)

	var logCount int
	err = h.service.Pool().QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.server_change_log`).Scan(&logCount)
	require.NoError(t, err)
	require.Equal(t, totalUploads, logCount)

	// Verify server_id sequence integrity
	var maxServerID int64
	err = h.service.Pool().QueryRow(h.ctx, `SELECT MAX(server_id) FROM sync.server_change_log`).Scan(&maxServerID)
	require.NoError(t, err)
	require.Equal(t, int64(totalUploads), maxServerID)

	// Verify no gaps in server_id sequence
	var actualCount int64
	err = h.service.Pool().QueryRow(h.ctx, `
		SELECT COUNT(*) FROM generate_series(1, $1) s(id) 
		WHERE EXISTS (SELECT 1 FROM sync.server_change_log WHERE server_id = s.id)`,
		maxServerID).Scan(&actualCount)
	require.NoError(t, err)
	require.Equal(t, maxServerID, actualCount, "No gaps in server_id sequence")

	throughput := float64(totalUploads) / duration.Seconds()
	t.Logf("✅ L01 Concurrent Uploads test passed - %d uploads in %.2fs (%.1f uploads/sec)",
		totalUploads, duration.Seconds(), throughput)
}

func TestL02_LargePayloadHandling(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// L02 – Large payload handling
	// Given: Upload with large JSON payloads.
	// When: Process large data.
	// Then: Handled efficiently; no memory issues; correct storage/retrieval.

	// Create large content (simulate rich text document)
	largeContent := ""
	for i := 0; i < 1000; i++ {
		largeContent += fmt.Sprintf("This is paragraph %d with some substantial content that makes the payload larger. ", i)
	}

	// Create metadata with many fields
	metadata := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		metadata[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d_with_some_additional_text", i)
	}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	noteID := h.MakeUUID("largepayload")

	// Create large payload
	largePayload := map[string]interface{}{
		"id":          noteID.String(),
		"title":       "Large Payload Note",
		"content":     largeContent,
		"metadata":    string(metadataJSON),
		"tags":        []string{"tag1", "tag2", "tag3", "performance", "test", "large", "payload"},
		"created_at":  time.Now().Unix(),
		"updated_at":  time.Now().Unix(),
		"version":     "1.0.0",
		"description": "This is a test note with a very large payload to test the system's ability to handle substantial amounts of data efficiently without memory issues or performance degradation.",
	}

	payloadBytes, err := json.Marshal(largePayload)
	require.NoError(t, err)
	payloadSize := len(payloadBytes)

	req := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: 1,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload:        json.RawMessage(payloadBytes),
			},
		},
	}

	// Measure upload performance
	startTime := time.Now()
	resp, httpResp := h.DoUpload(h.client1Token, req)
	uploadDuration := time.Since(startTime)

	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.True(t, resp.Accepted)

	// Measure download performance
	startTime = time.Now()
	downloadResp, httpResp := h.DoDownload(h.client2Token, 0, 100)
	downloadDuration := time.Since(startTime)

	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, downloadResp.Changes, 1)

	// Verify payload integrity
	retrievedPayload := downloadResp.Changes[0].Payload
	var retrievedData map[string]interface{}
	err = json.Unmarshal(retrievedPayload, &retrievedData)
	require.NoError(t, err)

	// Verify key fields
	require.Equal(t, largePayload["title"], retrievedData["title"])
	require.Equal(t, largePayload["content"], retrievedData["content"])
	require.Equal(t, largePayload["metadata"], retrievedData["metadata"])

	// Verify metadata integrity
	var retrievedMetadata map[string]interface{}
	err = json.Unmarshal([]byte(retrievedData["metadata"].(string)), &retrievedMetadata)
	require.NoError(t, err)
	require.Equal(t, metadata, retrievedMetadata)

	// Performance assertions (reasonable thresholds)
	require.Less(t, uploadDuration, 5*time.Second, "Upload should complete within 5 seconds")
	require.Less(t, downloadDuration, 5*time.Second, "Download should complete within 5 seconds")

	uploadThroughput := float64(payloadSize) / uploadDuration.Seconds() / 1024 / 1024     // MB/s
	downloadThroughput := float64(payloadSize) / downloadDuration.Seconds() / 1024 / 1024 // MB/s

	t.Logf("✅ L02 Large Payload Handling test passed - %.1f KB payload, upload: %.1f MB/s, download: %.1f MB/s",
		float64(payloadSize)/1024, uploadThroughput, downloadThroughput)
}

func TestL03_HighVolumeDownload(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// L03 – High volume download
	// Given: Large number of changes in server.
	// When: Client downloads all.
	// Then: Efficient pagination; reasonable memory usage; consistent performance.

	// Create a substantial dataset
	numChanges := 500 // Reduced for test performance but still substantial
	batchSize := 50   // Upload in batches for efficiency

	t.Logf("Creating %d changes in batches of %d...", numChanges, batchSize)

	// Upload data in batches
	for batch := 0; batch < numChanges/batchSize; batch++ {
		changes := make([]oversync.ChangeUpload, batchSize)

		for i := 0; i < batchSize; i++ {
			changeIdx := batch*batchSize + i
			noteID := h.MakeUUID(fmt.Sprintf("vol%05d", changeIdx))

			changes[i] = oversync.ChangeUpload{
				SourceChangeID: int64(changeIdx + 1),
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{
					"id": "%s",
					"title": "Volume Note %d",
					"content": "Content for high volume test %d",
					"batch": %d,
					"index": %d,
					"updated_at": %d
				}`, noteID.String(), changeIdx, changeIdx, batch, i, time.Now().Unix())),
			}
		}

		req := &oversync.UploadRequest{
			LastServerSeqSeen: int64(batch * batchSize),
			Changes:           changes,
		}

		resp, httpResp := h.DoUpload(h.client1Token, req)
		require.Equal(t, http.StatusOK, httpResp.StatusCode)
		require.True(t, resp.Accepted)
	}

	t.Logf("Data creation complete. Starting high volume download test...")

	// Test high volume download with different pagination sizes
	pageSizes := []int{10, 50, 100}

	for _, pageSize := range pageSizes {
		t.Logf("Testing download with page size %d...", pageSize)

		startTime := time.Now()
		var allChanges []oversync.ChangeDownloadResponse
		after := int64(0)
		pageCount := 0

		for {
			downloadResp, httpResp := h.DoDownload(h.client2Token, after, pageSize)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)

			allChanges = append(allChanges, downloadResp.Changes...)
			pageCount++

			if !downloadResp.HasMore {
				break
			}
			after = downloadResp.NextAfter
		}

		duration := time.Since(startTime)

		// Verify completeness
		require.Len(t, allChanges, numChanges)

		// Verify ordering
		for i, change := range allChanges {
			expectedServerID := int64(i + 1)
			require.Equal(t, expectedServerID, change.ServerID)
		}

		// Performance metrics
		throughput := float64(numChanges) / duration.Seconds()
		avgPageTime := duration / time.Duration(pageCount)

		t.Logf("Page size %d: %d changes in %d pages, %.2fs total (%.1f changes/sec, %.1fms/page)",
			pageSize, numChanges, pageCount, duration.Seconds(), throughput, float64(avgPageTime.Nanoseconds())/1e6)

		// Performance assertions
		require.Less(t, duration, 30*time.Second, "Download should complete within 30 seconds")
		require.Greater(t, throughput, 10.0, "Should achieve at least 10 changes/sec throughput")
	}

	t.Logf("✅ L03 High Volume Download test passed - efficient handling of %d changes", numChanges)
}
