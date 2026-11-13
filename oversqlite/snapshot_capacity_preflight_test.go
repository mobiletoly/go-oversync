// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRetainedEntriesLowerBound(t *testing.T) {
	require.EqualValues(t, 1, snapshotRetainedEntriesLowerBound(0))
	require.EqualValues(t, 200_001, snapshotRetainedEntriesLowerBound(100_000))
	require.EqualValues(t, math.MaxInt64, snapshotRetainedEntriesLowerBound(math.MaxInt64))
	require.EqualValues(t, math.MaxInt64, snapshotRetainedEntriesLowerBound(-1))
}

// rebuildAgainstSnapshot runs Rebuild against a snapshot of the given row
// count and reports how often each snapshot request was made.
func rebuildAgainstSnapshot(t *testing.T, entryQuota int64, rowCount int) (err error, creates, chunks, deletes, staged int) {
	t.Helper()
	err, creates, chunks, deleteSources, staged := rebuildAgainstSnapshotAs(t, entryQuota, rowCount, "")
	return err, creates, chunks, len(deleteSources), staged
}

// rebuildAgainstSnapshotAs is rebuildAgainstSnapshot that first requires a
// source rotation to rotatedSourceID when that is not empty, and reports the
// source each session release was sent as.
func rebuildAgainstSnapshotAs(t *testing.T, entryQuota int64, rowCount int, rotatedSourceID string) (err error, creates, chunks int, deleteSources []string, staged int) {
	t.Helper()
	client, db := newUsersBundleClient(t)
	_, execErr := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, execErr)
	if rotatedSourceID != "" {
		require.NoError(t, client.markSourceRecoveryRequiredLocked(context.Background(), SourceRecoveryHistoryPruned, "", rotatedSourceID))
		client.sourceIDGenerator = func() string { return rotatedSourceID }
	}
	client.config.MaxSyncWorkspaceEntries = entryQuota

	rows := make([]oversync.SnapshotRow, 0, rowCount)
	for i := range rowCount {
		id := fmt.Sprintf("user-%d", i)
		rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: mustBundleKey(id), RowVersion: 4, Payload: mustBundlePayload(t, id, "name", id+"@example.com")})
	}
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			creates++
			return snapshotFixtureResponse(t, client, r, "snapshot-quota", 4, rows), nil
		case "/sync/snapshot-sessions/snapshot-quota":
			switch r.Method {
			case http.MethodGet:
				chunks++
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-quota", SnapshotBundleSeq: 4, Rows: rows, NextRowOrdinal: int64(rowCount), HasMore: false}), nil
			case http.MethodDelete:
				deleteSources = append(deleteSources, r.Header.Get(oversync.SourceIDHeader))
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
			}
		}
		return nil, io.EOF
	})}

	_, err = client.Rebuild(context.Background())
	return err, creates, chunks, deleteSources, snapshotStageCount(t, db)
}

func TestRebuild_ReleasesARefusedSnapshotAsTheSourceThatOwnsIt(t *testing.T) {
	err, creates, chunks, deleteSources, staged := rebuildAgainstSnapshotAs(t, 200, 96, "rotated-device")

	var limit *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, 1, creates)
	require.Zero(t, chunks)
	require.Equal(t, []string{"rotated-device"}, deleteSources, "the session was created for the new source")
	require.Zero(t, staged)
}

func TestRebuild_RefusesASnapshotThatCannotFitTheEntryQuotaBeforeDownloadingIt(t *testing.T) {
	// One owner, two entries per row and eight to admit a row: 96 rows need 201.
	err, creates, chunks, deletes, staged := rebuildAgainstSnapshot(t, 200, 96)

	var limit *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "entries", limit.Dimension)
	require.EqualValues(t, 200, limit.Limit)
	require.EqualValues(t, 193, limit.Used)
	require.EqualValues(t, snapshotRowAdmissionEntries, limit.Reserved)
	require.Equal(t, 1, creates)
	require.Zero(t, chunks, "nothing is downloaded")
	require.Equal(t, 1, deletes, "the server session is released")
	require.Zero(t, staged, "nothing is staged")
}

func TestRebuild_DoesNotRefuseEarlyASnapshotAtTheLowerBound(t *testing.T) {
	// 95 rows need 199 of 200 by the lower bound, so the snapshot is downloaded.
	_, creates, chunks, _, _ := rebuildAgainstSnapshot(t, 200, 95)

	require.Equal(t, 1, creates)
	require.Positive(t, chunks)
}
