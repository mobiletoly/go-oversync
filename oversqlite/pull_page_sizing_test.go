// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPullPageSizing_FirstRequestIsSmallAndHeldToTheByteTarget(t *testing.T) {
	pages := newPullPageSizing(1000, pullPageTargetDecodedBytes)
	require.Equal(t, pullPageInitialBundles, pages.requestBundles)
	require.Equal(t, pullPageTargetDecodedBytes, pages.responseDecodedByteLimit(32<<20))
	require.EqualValues(t, 2048, pages.responseDecodedByteLimit(2048), "a smaller advertised limit wins")
	require.Equal(t, 5, newPullPageSizing(5, pullPageTargetDecodedBytes).requestBundles)
}

func TestPullPageSizing_SmallBundlesGrowTheRequestFourfoldPerPageUpToTheMaximum(t *testing.T) {
	pages := newPullPageSizing(1000, pullPageTargetDecodedBytes)
	requested := []int{pages.requestBundles}
	for range 4 {
		pages.pageDecoded(pages.requestBundles, int64(pages.requestBundles)*220)
		requested = append(requested, pages.requestBundles)
	}
	require.Equal(t, []int{16, 64, 256, 1000, 1000}, requested)
}

func TestPullPageSizing_RequestSettlesWhereBundlesFillHalfTheTarget(t *testing.T) {
	pages := newPullPageSizing(1000, pullPageTargetDecodedBytes)
	const bundleBytes = int64(105_000)
	pages.pageDecoded(16, 16*bundleBytes)
	require.Equal(t, 19, pages.requestBundles) // 2 MiB / 105 kB
	pages.pageDecoded(19, 19*bundleBytes)
	require.Equal(t, 19, pages.requestBundles)
	// A short last page says nothing new about bundle size.
	pages.pageDecoded(3, 3*bundleBytes)
	require.Equal(t, 19, pages.requestBundles)
}

func TestPullPageSizing_OversizedPagesShrinkTheRequestUntilItIsOneBundle(t *testing.T) {
	pages := newPullPageSizing(1000, pullPageTargetDecodedBytes)
	requested := []int{pages.requestBundles}
	for pages.shrinkAfterOversizedPage() {
		requested = append(requested, pages.requestBundles)
	}
	require.Equal(t, []int{16, 4, 1}, requested)
	require.False(t, pages.shrinkAfterOversizedPage())

	// One bundle is limited only by what the server advertised, and a bundle
	// above the target keeps the request at one.
	require.EqualValues(t, 32<<20, pages.responseDecodedByteLimit(32<<20))
	pages.pageDecoded(1, 10<<20)
	require.Equal(t, 1, pages.requestBundles)
	// Smaller bundles let it grow again.
	pages.pageDecoded(1, 1000)
	require.Equal(t, 4, pages.requestBundles)
	require.Equal(t, pullPageTargetDecodedBytes, pages.responseDecodedByteLimit(32<<20))
}

func TestPullPageSizing_RequestStaysWithinItsBoundsForAnySequenceOfPages(t *testing.T) {
	sizes := []int64{1, 300, 105_000, 3 << 20, 64 << 20, 1<<63 - 1}
	for _, maxBundles := range []int{1, 2, 16, 17, 1000, 5000} {
		pages := newPullPageSizing(maxBundles, pullPageTargetDecodedBytes)
		seed := uint64(0x2545F491) + uint64(maxBundles)
		for range 2000 {
			seed = seed*6364136223846793005 + 1442695040888963407
			pick := int(seed >> 33)
			before := pages.requestBundles
			if pick%5 == 0 {
				shrank := pages.shrinkAfterOversizedPage()
				require.Equal(t, before > 1, shrank)
				if shrank {
					require.Less(t, pages.requestBundles, before)
				}
			} else {
				pages.pageDecoded(1+pick%before, sizes[pick%len(sizes)])
				require.LessOrEqual(t, pages.requestBundles, before*pullPageGrowthFactor)
			}
			require.GreaterOrEqual(t, pages.requestBundles, 1)
			require.LessOrEqual(t, pages.requestBundles, maxBundles)
		}
	}
	pages := newPullPageSizing(1000, pullPageTargetDecodedBytes)
	pages.pageDecoded(0, 60)
	pages.pageDecoded(3, 0)
	require.Equal(t, pullPageInitialBundles, pages.requestBundles)
}

// pagedPullServer answers pull requests from a list of bundles, honouring
// after_bundle_seq and max_bundles, and records each requested max_bundles.
type pagedPullServer struct {
	t         *testing.T
	bundles   []oversync.Bundle
	requested []int
	// gzip answers pages compressed, as the real server does.
	gzip bool
	// respond, when set, may answer a request itself; nil falls through.
	respond func(request int) *http.Response
}

func (s *pagedPullServer) add(rows ...oversync.BundleRow) {
	seq := int64(len(s.bundles) + 1)
	for i := range rows {
		rows[i].RowVersion = seq
	}
	s.bundles = append(s.bundles, pullBundle(seq, "peer", seq, rows...))
}

func (s *pagedPullServer) addUser(id, name string) {
	s.add(validUserInsertRow(s.t, id, 0, name, id+"@example.com"))
}

func (s *pagedPullServer) RoundTrip(r *http.Request) (*http.Response, error) {
	require.Equal(s.t, "/sync/pull", r.URL.Path)
	after, err := strconv.ParseInt(r.URL.Query().Get("after_bundle_seq"), 10, 64)
	require.NoError(s.t, err)
	maxBundles, err := strconv.Atoi(r.URL.Query().Get("max_bundles"))
	require.NoError(s.t, err)
	s.requested = append(s.requested, maxBundles)
	if s.respond != nil {
		if response := s.respond(len(s.requested)); response != nil {
			return response, nil
		}
	}
	var page []oversync.Bundle
	for _, bundle := range s.bundles {
		if bundle.BundleSeq > after && len(page) < maxBundles {
			page = append(page, bundle)
		}
	}
	stable := int64(len(s.bundles))
	last := after
	if len(page) > 0 {
		last = page[len(page)-1].BundleSeq
	}
	response := jsonResponse(oversync.PullResponse{StableBundleSeq: stable, Bundles: page, HasMore: last < stable})
	if s.gzip {
		raw, err := io.ReadAll(response.Body)
		require.NoError(s.t, err)
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, err = writer.Write(raw)
		require.NoError(s.t, err)
		require.NoError(s.t, writer.Close())
		response.Body = io.NopCloser(&compressed)
		response.ContentLength = int64(compressed.Len())
		response.Header.Set("Content-Encoding", "gzip")
	}
	return response, nil
}

func newPagedPullClient(t *testing.T, admittedDecodedBytes int64) (*Client, *sql.DB, *pagedPullServer) {
	t.Helper()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	client.config.RecoveryMode = RecoveryDisabled
	client.discovery.Limits.MaxPullDecodedBytes = admittedDecodedBytes
	server := &pagedPullServer{t: t}
	client.HTTP = &http.Client{Transport: roundTripFunc(server.RoundTrip)}
	return client, db, server
}

func TestPullToStable_PullsABacklogOfSmallBundlesInGrowingPages(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	for i := range 100 {
		server.addUser(fmt.Sprintf("small-%d", i), "name")
	}

	_, err := client.PullToStable(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{16, 64, 256}, server.requested)
	requireTotalUserCount(t, db, 100)
	requireLastBundleSeqSeen(t, client, context.Background(), 100)
}

func TestPullToStable_PullsAFewBundlesInOneRequest(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	for i := range pullPageInitialBundles {
		server.addUser(fmt.Sprintf("few-%d", i), "name")
	}

	_, err := client.PullToStable(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{pullPageInitialBundles}, server.requested)
	requireTotalUserCount(t, db, pullPageInitialBundles)
}

// The cases below use a 48 KiB page target so that their bundles stay small;
// the sizing rule does not depend on the scale. The target must differ from
// snapshotControlBodyLimit, at which error bodies are refused.
const smallPullPageTarget = int64(48 << 10)

// pullWithPageTarget runs one incremental pull as PullToStable does, with the
// given page target.
func pullWithPageTarget(t *testing.T, client *Client, pageTargetDecodedBytes int64) error {
	t.Helper()
	require.NoError(t, client.tryBeginSyncOperation())
	defer client.writeMu.Unlock()
	ctx := context.WithValue(context.Background(), syncInvocationKey{}, &syncInvocation{})
	after, err := client.lastSeenBundleSeq(ctx)
	require.NoError(t, err)
	_, err = client.pullIncrementalPagesLocked(ctx, after, client.discovery.Limits.MaxPullBundles, pageTargetDecodedBytes)
	return err
}

func TestPullPages_AsksAgainWithFewerBundlesForAPageAboveTheByteTarget(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	name := strings.Repeat("n", 3_600) // sixteen of these bundles decode to about 61 kB
	for i := range 20 {
		server.addUser(fmt.Sprintf("large-%d", i), name)
	}

	require.NoError(t, pullWithPageTarget(t, client, smallPullPageTarget))

	require.Equal(t, []int{16, 4, 6, 6, 6}, server.requested)
	requireTotalUserCount(t, db, 20)
	requireLastBundleSeqSeen(t, client, context.Background(), 20)
}

func TestPullPages_ShrinkingAfterPagesWereAppliedNeitherSkipsNorRepeatsABundle(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	name := strings.Repeat("n", 3_600)
	for i := range 20 {
		server.addUser(fmt.Sprintf("small-%d", i), "name")
	}
	for i := range 20 {
		server.addUser(fmt.Sprintf("large-%d", i), name)
	}

	require.NoError(t, pullWithPageTarget(t, client, smallPullPageTarget))

	// 16 small bundles; then 64 asked: 4 small and 20 large, about 77 kB,
	// refused; then 16 (4 small, 12 large, about 47 kB); then the remaining 8.
	require.Equal(t, []int{16, 64, 16, 8}, server.requested)
	requireTotalUserCount(t, db, 40)
	requireLastBundleSeqSeen(t, client, context.Background(), 40)
}

func TestPullPages_ShrinksForACompressedPageWhoseDecodedSizeIsAboveTheTarget(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	server.gzip = true
	name := strings.Repeat("n", 3_600) // small on the wire, about 61 kB decoded for sixteen bundles
	for i := range 20 {
		server.addUser(fmt.Sprintf("large-%d", i), name)
	}

	require.NoError(t, pullWithPageTarget(t, client, smallPullPageTarget))

	require.Equal(t, []int{16, 4, 6, 6, 6}, server.requested)
	requireTotalUserCount(t, db, 20)
	requireLastBundleSeqSeen(t, client, context.Background(), 20)
}

func TestPullPages_FailsOnACompressedResponseAboveTheAdvertisedLimit(t *testing.T) {
	client, db, server := newPagedPullClient(t, smallPullPageTarget)
	server.gzip = true
	name := strings.Repeat("n", 12_500)
	for i := range 6 { // about 76 kB decoded across six bundles
		server.addUser(fmt.Sprintf("over-%d", i), name)
	}

	err := pullWithPageTarget(t, client, smallPullPageTarget)

	var tooLarge *SnapshotResponseBodyTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, smallPullPageTarget, tooLarge.Limit)
	require.Equal(t, []int{16}, server.requested)
	requireTotalUserCount(t, db, 0)
}

func TestPullPageRefusedAtLimit_NeverTreatsTheErrorBodyLimitAsAPage(t *testing.T) {
	page := fmt.Errorf("failed to send pull request: %w", &SnapshotResponseBodyTooLargeError{Operation: "pull_request", Limit: 4 << 20})
	require.True(t, pullPageRefusedAtLimit(page, 4<<20))
	require.False(t, pullPageRefusedAtLimit(page, 5<<20))
	encoded := fmt.Errorf("failed to send pull request: %w", &EncodedResponseBodyTooLargeError{Operation: "pull_request", Limit: 4 << 20})
	require.True(t, pullPageRefusedAtLimit(encoded, 4<<20))
	errorBody := fmt.Errorf("failed to send pull request: %w", &SnapshotResponseBodyTooLargeError{Operation: "pull_request", Limit: snapshotControlBodyLimit})
	require.False(t, pullPageRefusedAtLimit(errorBody, snapshotControlBodyLimit))
	require.False(t, pullPageRefusedAtLimit(fmt.Errorf("other"), 4<<20))
}

func hugeUserBundle(t *testing.T, rows int) []oversync.BundleRow {
	t.Helper()
	name := strings.Repeat("n", 12_500)
	bundle := make([]oversync.BundleRow, 0, rows)
	for i := range rows {
		id := fmt.Sprintf("huge-%d", i)
		bundle = append(bundle, validUserInsertRow(t, id, 0, name, id+"@example.com"))
	}
	return bundle
}

func TestPullPages_StillPullsOneBundleAboveTheByteTarget(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	server.add(hugeUserBundle(t, 6)...) // about 76 kB in one bundle
	server.addUser("after", "small")

	require.NoError(t, pullWithPageTarget(t, client, smallPullPageTarget))

	require.Equal(t, []int{16, 4, 1, 1}, server.requested)
	requireTotalUserCount(t, db, 7)
	requireLastBundleSeqSeen(t, client, context.Background(), 2)
}

func TestPullPages_FailsOnAResponseAboveTheAdvertisedLimit(t *testing.T) {
	requireRefusedAt := func(t *testing.T, err error, limit int64) {
		t.Helper()
		// The fixture answers uncompressed, so the encoded limit trips first.
		var tooLarge *EncodedResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
		require.Equal(t, limit, tooLarge.Limit)
	}
	t.Run("advertised limit at the target: one request", func(t *testing.T) {
		client, db, server := newPagedPullClient(t, smallPullPageTarget)
		name := strings.Repeat("n", 12_500)
		for i := range 6 { // about 76 kB across six bundles
			server.addUser(fmt.Sprintf("over-%d", i), name)
		}

		err := pullWithPageTarget(t, client, smallPullPageTarget)

		requireRefusedAt(t, err, smallPullPageTarget)
		require.Equal(t, []int{16}, server.requested)
		requireTotalUserCount(t, db, 0)
		requireLastBundleSeqSeen(t, client, context.Background(), 0)
	})
	t.Run("one bundle above a larger advertised limit: shrinks to it, then fails", func(t *testing.T) {
		const advertised = int64(80_000)
		client, db, server := newPagedPullClient(t, advertised)
		server.add(hugeUserBundle(t, 7)...) // about 88 kB in one bundle

		err := pullWithPageTarget(t, client, smallPullPageTarget)

		requireRefusedAt(t, err, advertised)
		require.Equal(t, []int{16, 4, 1}, server.requested)
		requireTotalUserCount(t, db, 0)
		requireLastBundleSeqSeen(t, client, context.Background(), 0)
	})
}

func TestPullToStable_DoesNotShrinkForAnOversizedErrorBody(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	server.addUser("one", "name")
	server.respond = func(int) *http.Response {
		// Larger than any control-body limit and than the 4 MiB page target.
		return errorJSONResponse(http.StatusBadRequest, map[string]any{"error": "invalid_request", "message": strings.Repeat("x", 5<<20)})
	}

	_, err := client.PullToStable(context.Background())

	require.Error(t, err)
	require.NotEmpty(t, server.requested)
	for _, requested := range server.requested {
		require.Equal(t, pullPageInitialBundles, requested)
	}
	requireTotalUserCount(t, db, 0)
}

func TestPullToStable_AppliesEveryBundleOnceAcrossATransientFailureMidBacklog(t *testing.T) {
	client, db, server := newPagedPullClient(t, 64<<20)
	for i := range 100 {
		server.addUser(fmt.Sprintf("small-%d", i), "name")
	}
	server.respond = func(request int) *http.Response {
		if request == 2 {
			return errorJSONResponse(http.StatusServiceUnavailable, map[string]any{"error": "server_unavailable", "message": "injected"})
		}
		return nil
	}

	_, err := client.PullToStable(context.Background())

	require.NoError(t, err)
	// The failed second request is retried at the same size.
	require.Equal(t, []int{16, 64, 64, 256}, server.requested)
	requireTotalUserCount(t, db, 100)
	requireLastBundleSeqSeen(t, client, context.Background(), 100)
}
