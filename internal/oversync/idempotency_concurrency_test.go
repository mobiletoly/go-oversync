package oversync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// High-concurrency idempotency tests to validate the insert-first gate under race

// IHC01 – Many concurrent identical uploads (same SCID) from one device
// Expect exactly one applied with a new_server_version, others idempotent (applied without version).
func TestIHC01_ConcurrentDuplicateSCID_SingleChange(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	noteID := h.MakeUUID("idempo-single-0001")
	scid := int64(1)

	req := &oversync.UploadRequest{
		LastServerSeqSeen: 0,
		Changes: []oversync.ChangeUpload{
			{
				SourceChangeID: scid,
				Table:          "note",
				Op:             "INSERT",
				PK:             noteID.String(),
				ServerVersion:  0,
				Payload: json.RawMessage(fmt.Sprintf(`{"id":"%s","title":"IHC01","updated_at":%d}`,
					noteID.String(), time.Now().Unix())),
			},
		},
	}

	const concurr = 20
	var wg sync.WaitGroup
	wg.Add(concurr)

	// Track results
	var appliedWithVersion int
	var appliedIdempotent int
	var mu sync.Mutex

	for i := 0; i < concurr; i++ {
		go func() {
			defer wg.Done()
			resp, httpResp := h.DoUpload(h.client1Token, req)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)
			require.True(t, resp.Accepted)
			require.Len(t, resp.Statuses, 1)

			st := resp.Statuses[0]
			require.Equal(t, scid, st.SourceChangeID)
			require.Equal(t, "applied", st.Status)

			mu.Lock()
			if st.NewServerVersion != nil {
				appliedWithVersion++
			} else {
				appliedIdempotent++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Exactly one should have advanced the version; others idempotent
	require.Equal(t, 1, appliedWithVersion)
	require.Equal(t, concurr-1, appliedIdempotent)

	// Verify exactly one change log row exists for this (user, source, scid)
	userID := h.ExtractUserIDFromToken(h.client1Token)
	var cnt int
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.server_change_log WHERE user_id=$1 AND source_id=$2 AND source_change_id=$3`,
		userID, h.client1ID, scid,
	).Scan(&cnt)
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
}

// IHC02 – Many concurrent batches hitting the same SCID set
// K goroutines each attempt M distinct SCIDs for the same device; expect exactly M rows in change_log.
func TestIHC02_ConcurrentDuplicateSCIDs_Many(t *testing.T) {
	h := NewSimpleTestHarness(t)
	defer h.Cleanup()
	h.Reset()

	M := 30 // number of distinct SCIDs
	K := 5  // number of contenders per SCID

	// Pre-generate SCIDs and PKs
	type job struct {
		scid int64
		pk   string
	}
	jobs := make([]job, 0, M)
	for i := 0; i < M; i++ {
		pk := h.MakeUUID(fmt.Sprintf("idempo-many-%04d", i)).String()
		jobs = append(jobs, job{scid: int64(i + 1), pk: pk})
	}

	var wg sync.WaitGroup
	wg.Add(K)
	// Counters
	var appliedWithVersion int64
	var appliedIdempotent int64
	var mu sync.Mutex

	uploader := func() {
		defer wg.Done()
		for _, j := range jobs {
			req := &oversync.UploadRequest{
				LastServerSeqSeen: 0, // not used for idempotency gate
				Changes: []oversync.ChangeUpload{{
					SourceChangeID: j.scid,
					Table:          "note",
					Op:             "INSERT",
					PK:             j.pk,
					ServerVersion:  0,
					Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","title":"IHC02-%d","updated_at":%d}`, j.pk, j.scid, time.Now().Unix())),
				}},
			}
			resp, httpResp := h.DoUpload(h.client1Token, req)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)
			require.True(t, resp.Accepted)
			require.Len(t, resp.Statuses, 1)
			st := resp.Statuses[0]
			require.Equal(t, j.scid, st.SourceChangeID)
			require.Equal(t, "applied", st.Status)
			mu.Lock()
			if st.NewServerVersion != nil {
				appliedWithVersion++
			} else {
				appliedIdempotent++
			}
			mu.Unlock()
		}
	}

	for i := 0; i < K; i++ {
		go uploader()
	}
	wg.Wait()

	// We expect exactly M rows inserted total; there should be exactly M applied-with-version across K contenders
	require.Equal(t, int64(M), appliedWithVersion)
	require.Equal(t, int64(M*(K-1)), appliedIdempotent)

	// Verify change_log cardinality for the device
	userID := h.ExtractUserIDFromToken(h.client1Token)
	var cnt int
	err := h.service.Pool().QueryRow(h.ctx,
		`SELECT COUNT(*) FROM sync.server_change_log WHERE user_id=$1 AND source_id=$2`, userID, h.client1ID,
	).Scan(&cnt)
	require.NoError(t, err)
	require.Equal(t, M, cnt)

	// Another device should see all M changes
	dresp, _ := h.DoDownload(h.client2Token, 0, 1000)
	require.Len(t, dresp.Changes, M)
}
