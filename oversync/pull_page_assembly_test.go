// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// wholePagePullRetainedAllocations is the reference the running totals must
// equal: it walks every admitted bundle each time it is asked.
func wholePagePullRetainedAllocations(admitted []Bundle, current ...*Bundle) (int64, int64) {
	var buffers, bytes int64
	if admitted != nil {
		buffers++
		bytes += int64(cap(admitted)) * int64(unsafe.Sizeof(Bundle{}))
	}
	for i := range admitted {
		rowBuffers, rowBytes := retainedBundleAllocations(&admitted[i])
		buffers += rowBuffers
		bytes += rowBytes
	}
	currentBuffers, currentBytes := retainedBundleAllocations(current...)
	return buffers + currentBuffers, bytes + currentBytes
}

func pullPageTestBundle(seq int64) Bundle {
	user := func(id, name string) BundleRow {
		payload, _ := json.Marshal(map[string]any{"id": id, "name": name})
		return BundleRow{Schema: "main", Table: "users", Key: SyncKey{"id": id}, Op: "INSERT", RowVersion: seq, Payload: payload}
	}
	bundle := Bundle{
		BundleSeq:      seq,
		SourceID:       fmt.Sprintf("source-%d", seq%3),
		SourceBundleID: seq * 7,
		BundleHash:     strings.Repeat(fmt.Sprintf("%x", seq%16), 64),
	}
	switch seq % 6 {
	case 0:
		bundle.Rows = []BundleRow{}
	case 1:
		bundle.Rows = []BundleRow{user(fmt.Sprintf("u%d", seq), "Ada")}
	case 2:
		bundle.Rows = []BundleRow{
			user(fmt.Sprintf("u%d", seq), `<Grace & "Hopper">`),
			{Schema: "main", Table: "users", Key: SyncKey{"id": fmt.Sprintf("gone-%d", seq)}, Op: "DELETE", RowVersion: seq + 1},
		}
	case 3:
		bundle.Rows = []BundleRow{{
			Schema: "main", Table: "projects", Key: SyncKey{"project_id": seq, "tenant_id": "tenant-é "},
			Op: "UPDATE", RowVersion: 1 << 40, Payload: json.RawMessage(`{"name":"Renamed","tags":[1,2,3],"nested":{"k":null}}`),
		}}
	case 4:
		for i := range 25 {
			bundle.Rows = append(bundle.Rows, user(fmt.Sprintf("u%d-%d", seq, i), strings.Repeat("n", i*13)))
		}
	default:
		bundle.Rows = nil
	}
	bundle.RowCount = int64(len(bundle.Rows))
	return bundle
}

func TestPullPageAssembly_SizeAndRetentionEqualTheWholePageComputation(t *testing.T) {
	generation := compactTestGeneration()
	const bundleCount = 48
	for _, test := range []struct {
		name            string
		stableBundleSeq int64
		hasMore         bool
	}{
		{name: "last page", stableBundleSeq: 48, hasMore: false},
		{name: "more pages follow", stableBundleSeq: 9_876_543_210, hasMore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			page, err := newPullPageAssembly(generation, test.stableBundleSeq, test.hasMore, bundleCount)
			require.NoError(t, err)
			emptyBytes, err := compactPullResponseDecodedBytes(generation, test.stableBundleSeq, nil, test.hasMore)
			require.NoError(t, err)
			require.Equal(t, emptyBytes, page.responseBytes)

			for seq := int64(1); seq <= bundleCount; seq++ {
				bundle := pullPageTestBundle(seq)
				canonical := pullPageTestBundle(seq + 1000)

				wantBuffers, wantBytes := wholePagePullRetainedAllocations(page.bundles, &canonical, &bundle)
				gotBuffers, gotBytes := page.retainedWith(&canonical, &bundle)
				require.Equal(t, wantBuffers, gotBuffers, "retained buffers before bundle %d", seq)
				require.Equal(t, wantBytes, gotBytes, "retained bytes before bundle %d", seq)

				candidate := append(append([]Bundle(nil), page.bundles...), bundle)
				wantResponseBytes, err := compactPullResponseDecodedBytes(generation, test.stableBundleSeq, candidate, test.hasMore)
				require.NoError(t, err)
				gotResponseBytes, err := page.responseBytesWith(&bundle)
				require.NoError(t, err)
				require.Equal(t, wantResponseBytes, gotResponseBytes, "response bytes with bundle %d", seq)
				require.Len(t, page.bundles, int(seq-1), "sizing must not change the page")

				page.admit(&bundle, gotResponseBytes)
				require.Equal(t, candidate, page.bundles)
			}

			encoded, err := compactPullResponseDecodedBytes(generation, test.stableBundleSeq, page.bundles, test.hasMore)
			require.NoError(t, err)
			require.Equal(t, encoded, page.responseBytes)
		})
	}
}

func TestPullPageAssembly_EmptyPageHasANonNilBundleList(t *testing.T) {
	page, err := newPullPageAssembly(compactTestGeneration(), 0, false, 0)
	require.NoError(t, err)
	require.NotNil(t, page.bundles)
	require.Empty(t, page.bundles)
	wantBuffers, wantBytes := wholePagePullRetainedAllocations(page.bundles)
	gotBuffers, gotBytes := page.retainedWith()
	require.Equal(t, wantBuffers, gotBuffers)
	require.Equal(t, wantBytes, gotBytes)
}

func TestPullPageAssembly_RejectsABundleItCannotEncodeAndKeepsThePage(t *testing.T) {
	generation := compactTestGeneration()
	page, err := newPullPageAssembly(generation, 2, false, 2)
	require.NoError(t, err)
	first := pullPageTestBundle(1)
	firstBytes, err := page.responseBytesWith(&first)
	require.NoError(t, err)
	page.admit(&first, firstBytes)

	unknown := Bundle{BundleSeq: 2, SourceID: "source", SourceBundleID: 2, Rows: []BundleRow{{
		Schema: "main", Table: "not_registered", Key: SyncKey{"id": "x"}, Op: "INSERT", Payload: json.RawMessage(`{"id":"x"}`),
	}}}
	_, wholePageErr := compactPullResponseDecodedBytes(generation, 2, []Bundle{first, unknown}, false)
	require.Error(t, wholePageErr)
	_, err = page.responseBytesWith(&unknown)
	require.Error(t, err)
	require.Equal(t, []Bundle{first}, page.bundles)
	require.Equal(t, firstBytes, page.responseBytes)
}

func TestPullPageAssembly_WorkPerBundleDoesNotGrowWithThePage(t *testing.T) {
	generation := compactTestGeneration()
	const bundleCount = 1000
	const window = 100
	bundles := make([]Bundle, bundleCount)
	for i := range bundles {
		bundles[i] = pullPageTestBundle(int64(6*i + 1)) // one-row bundles of the same shape
	}
	page, err := newPullPageAssembly(generation, bundleCount, false, bundleCount)
	require.NoError(t, err)

	allocatedFor := func(from, to int) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := from; i < to; i++ {
			page.retainedWith(&bundles[i], &bundles[i])
			responseBytes, err := page.responseBytesWith(&bundles[i])
			require.NoError(t, err)
			page.admit(&bundles[i], responseBytes)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	firstWindow := allocatedFor(0, window)
	_ = allocatedFor(window, bundleCount-window)
	lastWindow := allocatedFor(bundleCount-window, bundleCount)

	require.Len(t, page.bundles, bundleCount)
	// Re-encoding the page for every bundle would make the last window cost
	// about nineteen times the first; a running total keeps them equal.
	require.Less(t, lastWindow, firstWindow*2, "first window %d bytes, last window %d bytes", firstWindow, lastWindow)
}
