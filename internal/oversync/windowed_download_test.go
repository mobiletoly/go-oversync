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

// Windowed Download Tests (W01-W02): snapshot-consistent hydration with `until`

// W01 – Multi-page windowed hydration remains stable under concurrent writes.
// Steps:
//  1. Seed initial batch that spans multiple pages.
//  2. Start windowed download (no until) to get first page and window_until=W.
//  3. Concurrently upload more changes after first page.
//  4. Continue paging with until=W; ensure no rows > W are returned and final cursor equals W.
func TestW01_WindowedHydrationMultiPage_ConcurrentWrites(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Seed initial dataset from Client 1 (device1)
	initial := 25
	for i := 0; i < initial; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("init%03d", i))
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
                        "title": "Initial %d",
                        "content": "batch",
                        "updated_at": %d
                    }`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}
		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
	}

	// Client 2 begins hydration with small page size, no until on first call
	pageSize := 10
	page1, httpResp := h.DoDownloadWindowed(h.client2Token, 0, pageSize, "", false, 0)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, page1.Changes, pageSize)
	require.True(t, page1.HasMore)
	// The frozen window for this hydration
	W := page1.WindowUntil
	require.Equal(t, int64(initial), W, "window_until should equal initial highest server_id")

	// Concurrent writes after first page: add 5 more changes from Client 1
	extra := 5
	for i := 0; i < extra; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("extra%03d", i))
		req := &oversync.UploadRequest{
			LastServerSeqSeen: int64(initial + i),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(1000 + i + 1),
					Table:          "note",
					Op:             "INSERT",
					PK:             noteID.String(),
					ServerVersion:  0,
					Payload: json.RawMessage(fmt.Sprintf(`{
                        "id": "%s",
                        "title": "Concurrent %d",
                        "content": "after freeze",
                        "updated_at": %d
                    }`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}
		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
	}

	// Continue paging within the frozen window: until=W
	collected := append([]oversync.ChangeDownloadResponse{}, page1.Changes...)
	after := page1.NextAfter
	for {
		pr, _ := h.DoDownloadWindowed(h.client2Token, after, pageSize, "", false, W)
		collected = append(collected, pr.Changes...)
		if !pr.HasMore {
			// Must stop exactly at W
			require.Equal(t, W, pr.NextAfter)
			break
		}
		after = pr.NextAfter
	}

	// Verify we saw exactly the initial rows 1..W and none beyond W
	require.Len(t, collected, initial)
	var maxSeen int64
	for _, c := range collected {
		if c.ServerID > maxSeen {
			maxSeen = c.ServerID
		}
		require.LessOrEqual(t, c.ServerID, W, "should not leak rows beyond window")
	}
	require.Equal(t, W, maxSeen)

	// Fresh non-windowed download shows new rows exist
	fresh, _ := h.DoDownload(h.client2Token, 0, 1000)
	// Client 2 does not see its own changes; here all uploads were from Client 1
	// so fresh should include initial + extra
	require.Len(t, fresh.Changes, initial+extra)
}

// W02 – Recovery hydration with include_self=true and frozen window
// Steps mirror W01 but the downloading device asks for include_self.
func TestW02_WindowedHydration_IncludeSelf_ConcurrentWrites(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	// Seed initial dataset from Client 1 (device1)
	initial := 10
	for i := 0; i < initial; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("self%03d", i))
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
                        "title": "Self %d",
                        "content": "seed",
                        "updated_at": %d
                    }`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}
		resp, _ := h.DoUpload(h.client1Token, req)
		require.True(t, resp.Accepted)
	}

	// Same device performs recovery hydration: include_self=true
	pageSize := 4
	page1, httpResp := h.DoDownloadWindowed(h.client1Token, 0, pageSize, "", true, 0)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.Len(t, page1.Changes, pageSize)
	require.True(t, page1.HasMore)
	W := page1.WindowUntil
	require.Equal(t, int64(initial), W)

	// Concurrent uploads from another device after the first page
	concurrent := 3
	for i := 0; i < concurrent; i++ {
		noteID := h.MakeUUID(fmt.Sprintf("other%03d", i))
		req := &oversync.UploadRequest{
			LastServerSeqSeen: int64(initial + i),
			Changes: []oversync.ChangeUpload{
				{
					SourceChangeID: int64(500 + i + 1),
					Table:          "note",
					Op:             "INSERT",
					PK:             noteID.String(),
					ServerVersion:  0,
					Payload: json.RawMessage(fmt.Sprintf(`{
                        "id": "%s",
                        "title": "Other %d",
                        "content": "after",
                        "updated_at": %d
                    }`, noteID.String(), i, time.Now().Unix())),
				},
			},
		}
		// Upload as Client 2
		resp, _ := h.DoUpload(h.client2Token, req)
		require.True(t, resp.Accepted)
	}

	// Continue paging with include_self=true and until=W
	collected := append([]oversync.ChangeDownloadResponse{}, page1.Changes...)
	after := page1.NextAfter
	for {
		pr, _ := h.DoDownloadWindowed(h.client1Token, after, pageSize, "", true, W)
		collected = append(collected, pr.Changes...)
		if !pr.HasMore {
			require.Equal(t, W, pr.NextAfter)
			break
		}
		after = pr.NextAfter
	}

	// Verify we saw exactly the initial rows 1..W (including own changes due to include_self=true)
	require.Len(t, collected, initial)
	for _, c := range collected {
		require.LessOrEqual(t, c.ServerID, W)
	}

	// Fresh download from same device without window will include other device's writes too
	fresh, _ := h.DoDownload(h.client1Token, 0, 1000)
	// include_self default is false, so same device won't see its own changes in this fresh call
	// but should see the other device's concurrent writes
	require.GreaterOrEqual(t, len(fresh.Changes), concurrent)
}
