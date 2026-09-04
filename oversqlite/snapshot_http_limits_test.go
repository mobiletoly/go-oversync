package oversqlite

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func rawJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func gzipResponse(status int, decoded string) *http.Response {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(decoded))
	_ = writer.Close()
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type":     []string{"application/json"},
			"Content-Encoding": []string{"gzip"},
		},
		Body: io.NopCloser(bytes.NewReader(compressed.Bytes())),
	}
}

type repeatedByteReadCloser struct {
	remaining int64
	closed    bool
}

func (r *repeatedByteReadCloser) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	count := int64(len(buffer))
	if count > r.remaining {
		count = r.remaining
	}
	for index := int64(0); index < count; index++ {
		buffer[index] = 'x'
	}
	r.remaining -= count
	return int(count), nil
}

func (r *repeatedByteReadCloser) Close() error {
	r.closed = true
	return nil
}

func gzipRepeatedResponse(t *testing.T, decodedBytes int64) *http.Response {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := io.Copy(writer, &repeatedByteReadCloser{remaining: decodedBytes})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Encoding": []string{"gzip"}}, Body: io.NopCloser(bytes.NewReader(compressed.Bytes()))}
}

func TestSnapshotDecodedBodyLimit_ExactExtraCompressedAndEncoding(t *testing.T) {
	t.Run("exact limit succeeds", func(t *testing.T) {
		resp := rawJSONResponse(http.StatusOK, "1234")
		body, err := readDecodedBodyBounded(context.Background(), "test", resp, 4)
		require.NoError(t, err)
		require.Equal(t, []byte("1234"), body)
	})

	t.Run("one extra decoded byte fails", func(t *testing.T) {
		resp := rawJSONResponse(http.StatusOK, "12345")
		_, err := readDecodedBodyBounded(context.Background(), "test", resp, 4)
		var tooLarge *EncodedResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
	})

	t.Run("compressed expansion is bounded after decoding", func(t *testing.T) {
		resp := gzipResponse(http.StatusOK, strings.Repeat("a", 70_000))
		_, err := readDecodedBodyBounded(context.Background(), "test", resp, 64<<10)
		var tooLarge *SnapshotResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
	})

	t.Run("unsupported encoding fails before JSON decode", func(t *testing.T) {
		resp := rawJSONResponse(http.StatusOK, `{}`)
		resp.Header.Set("Content-Encoding", "br")
		_, err := readDecodedBodyBounded(context.Background(), "test", resp, 100)
		var unsupported *SnapshotUnsupportedContentEncodingError
		require.ErrorAs(t, err, &unsupported)
	})
}

func TestGenerationDataPlaneReaders_ProtocolHardCeilingsAndCeilingPlusOne(t *testing.T) {
	require.Equal(t, int64(67112960), pushConflictErrorBodyLimit)

	for _, testCase := range []struct {
		name  string
		limit int64
	}{
		{name: "discovered pull success", limit: 4096},
		{name: "structured push conflict", limit: pushConflictErrorBodyLimit},
	} {
		t.Run(testCase.name+" exact", func(t *testing.T) {
			reader := &repeatedByteReadCloser{remaining: testCase.limit}
			body, err := readDecodedBodyBounded(context.Background(), testCase.name, &http.Response{Header: http.Header{}, Body: reader}, testCase.limit)
			require.NoError(t, err)
			require.Len(t, body, int(testCase.limit))
			require.NoError(t, reader.Close())
			require.True(t, reader.closed)
		})
		t.Run(testCase.name+" ceiling plus one", func(t *testing.T) {
			reader := &repeatedByteReadCloser{remaining: testCase.limit + 1}
			_, err := readDecodedBodyBounded(context.Background(), testCase.name, &http.Response{Header: http.Header{}, Body: reader}, testCase.limit)
			var tooLarge *EncodedResponseBodyTooLargeError
			require.ErrorAs(t, err, &tooLarge)
			require.NoError(t, reader.Close())
			require.True(t, reader.closed)
		})
	}

	t.Run("gzip is bounded after decompression at the hard conflict ceiling", func(t *testing.T) {
		response := gzipRepeatedResponse(t, pushConflictErrorBodyLimit+1)
		_, err := readDecodedBodyBounded(context.Background(), "structured push conflict", response, pushConflictErrorBodyLimit)
		var tooLarge *SnapshotResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
	})

	t.Run("duplicate content encoding is rejected", func(t *testing.T) {
		response := gzipResponse(http.StatusOK, `{}`)
		response.Header["content-encoding"] = []string{"identity"}
		_, err := readDecodedBodyBounded(context.Background(), "duplicate encoding", response, 1024)
		var unsupported *SnapshotUnsupportedContentEncodingError
		require.ErrorAs(t, err, &unsupported)
	})

	t.Run("concatenated gzip member is rejected", func(t *testing.T) {
		first := gzipResponse(http.StatusOK, `{}`)
		firstBytes, err := io.ReadAll(first.Body)
		require.NoError(t, err)
		second := gzipResponse(http.StatusOK, `{}`)
		secondBytes, err := io.ReadAll(second.Body)
		require.NoError(t, err)
		response := &http.Response{Header: http.Header{"Content-Encoding": []string{"gzip"}}, Body: io.NopCloser(bytes.NewReader(append(firstBytes, secondBytes...)))}
		_, err = readDecodedBodyBounded(context.Background(), "concatenated gzip", response, 1024)
		var failure *boundedHTTPFailureError
		require.ErrorAs(t, err, &failure)
	})
}
func TestSnapshotChunkRequest_SendsRowsAndBytesAndRequiresDeclaredCount(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)

	t.Run("both budgets", func(t *testing.T) {
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "17", r.URL.Query().Get("max_rows"))
			require.Equal(t, "2048", r.URL.Query().Get("max_bytes"))
			return rawJSONResponse(http.StatusOK, `{"rows":[],"next_row_ordinal":0,"has_more":false,"logical_byte_count":0}`), nil
		})}
		chunk, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 17, 2048)
		require.NoError(t, err)
		require.Empty(t, chunk.Rows)
	})

	t.Run("missing byte count", func(t *testing.T) {
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return rawJSONResponse(http.StatusOK, `{"rows":[],"next_row_ordinal":0,"has_more":false}`), nil
		})}
		_, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 17, 2048)
		require.ErrorContains(t, err, "missing required logical_byte_count")
	})
}

func TestSnapshotSourceRetiredWithoutReplacementRemainsTypedAndRedacted(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.sourceID = "previous-source"
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return rawJSONResponse(
			http.StatusConflict,
			`{"error":"source_retired","message":"hostile-private-message","source_id":"previous-source"}`,
		), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), &oversync.SnapshotSessionCreateRequest{
		SourceReplacement: &oversync.SnapshotSourceReplacement{
			PreviousSourceID: "previous-source",
			NewSourceID:      "local-replacement",
			Reason:           "source_retired",
		},
	})

	var recoveryErr *SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, SourceRecoveryRetired, recoveryErr.Code)
	require.NotContains(t, err.Error(), "previous-source")
	require.NotContains(t, err.Error(), "local-replacement")
	require.NotContains(t, err.Error(), "hostile-private-message")
}

func TestSnapshotSessionMissingByteCount_AttemptsRetirement(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	var retired atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			retired.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
		}
		return rawJSONResponse(http.StatusOK, `{"snapshot_id":"snapshot-invalid","snapshot_bundle_seq":4,"row_count":0,"expires_at":"2030-01-01T00:00:00Z"}`), nil
	})}
	_, err := client.createSnapshotSession(context.Background(), nil)
	require.ErrorContains(t, err, "missing required logical_byte_count")
	require.Equal(t, int64(1), retired.Load())
}

func TestSnapshotResponsePresence_OrdinaryAndRotatedDownloadsRequireExplicitEmptyFraming(t *testing.T) {
	for _, tc := range []struct {
		name       string
		request    *oversync.SnapshotSessionCreateRequest
		snapshotID string
		sourceID   string
		body       string
		operation  string
		want       string
	}{
		{
			name: "ordinary session omits row_count", sourceID: "bundle-device",
			body:      `{"snapshot_id":"ordinary","snapshot_bundle_seq":8,"logical_byte_count":0,"expires_at":"2030-01-01T00:00:00Z"}`,
			operation: "session", want: "missing required row_count",
		},
		{
			name: "rotated session omits row_count", sourceID: "replacement",
			request: &oversync.SnapshotSessionCreateRequest{SourceReplacement: &oversync.SnapshotSourceReplacement{
				PreviousSourceID: "bundle-device", NewSourceID: "replacement", Reason: "history_pruned",
			}},
			body:      `{"snapshot_id":"rotated","snapshot_bundle_seq":8,"logical_byte_count":0,"expires_at":"2030-01-01T00:00:00Z"}`,
			operation: "session", want: "missing required row_count",
		},
		{
			name: "ordinary chunk omits rows", snapshotID: "ordinary", sourceID: "bundle-device",
			body:      `{"next_row_ordinal":0,"has_more":false,"logical_byte_count":0}`,
			operation: "chunk", want: "missing required rows",
		},
		{
			name: "rotated chunk omits has_more", snapshotID: "rotated", sourceID: "replacement",
			body:      `{"rows":[],"next_row_ordinal":0,"logical_byte_count":0}`,
			operation: "chunk", want: "missing required has_more",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return rawJSONResponse(http.StatusOK, tc.body), nil
			})}
			var err error
			if tc.operation == "session" {
				_, err = client.createSnapshotSession(context.Background(), tc.request)
			} else {
				_, _, err = client.fetchSnapshotChunkAsSource(context.Background(), tc.snapshotID, 8, 0, 1, 1024, tc.sourceID)
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSnapshotChunkValidation_CountsOrdinalsIdentityAndEmptyShape(t *testing.T) {
	valid := oversync.SnapshotChunkResponse{
		SnapshotID: "snapshot-1", SnapshotBundleSeq: 4,
		Rows:           []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "1"}, RowVersion: 1, Payload: []byte(`{"id":"1","name":"A","email":"a@example.com"}`)}},
		NextRowOrdinal: 1, ByteCount: 10,
	}
	tests := []struct {
		name string
		edit func(*oversync.SnapshotChunkResponse)
		want string
	}{
		{name: "identity", edit: func(v *oversync.SnapshotChunkResponse) { v.SnapshotID = "other" }, want: "snapshot_id"},
		{name: "sequence", edit: func(v *oversync.SnapshotChunkResponse) { v.SnapshotBundleSeq++ }, want: "snapshot_bundle_seq"},
		{name: "ordinal", edit: func(v *oversync.SnapshotChunkResponse) { v.NextRowOrdinal++ }, want: "next_row_ordinal"},
		{name: "row budget", edit: func(v *oversync.SnapshotChunkResponse) { v.Rows = append(v.Rows, v.Rows[0]); v.NextRowOrdinal++ }, want: "max_rows"},
		{name: "byte budget", edit: func(v *oversync.SnapshotChunkResponse) { v.ByteCount = 101 }, want: "max_bytes"},
		{name: "zero bytes with row", edit: func(v *oversync.SnapshotChunkResponse) { v.ByteCount = 0 }, want: "positive byte_count"},
		{name: "empty progress", edit: func(v *oversync.SnapshotChunkResponse) {
			v.Rows = nil
			v.NextRowOrdinal = 0
			v.ByteCount = 0
			v.HasMore = true
		}, want: "has_more=true"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := valid
			candidate.Rows = append([]oversync.SnapshotRow(nil), valid.Rows...)
			testCase.edit(&candidate)
			err := validateSnapshotChunkResponse(&candidate, "snapshot-1", 4, 0, 1, 100)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateSnapshotChunkResponse(&oversync.SnapshotChunkResponse{
		SnapshotID: "snapshot-empty", SnapshotBundleSeq: 0, Rows: nil,
		NextRowOrdinal: 0, HasMore: false, ByteCount: 0,
	}, "snapshot-empty", 0, 0, 1, 100))
	require.Error(t, validateSnapshotSession(&oversync.SnapshotSession{SnapshotID: "snapshot-empty", RowCount: 0, ByteCount: 1, ExpiresAt: "2030-01-01T00:00:00Z"}))
}

func TestSnapshotCheckedArithmetic(t *testing.T) {
	_, err := checkedSnapshotChunkBodyLimit(math.MaxInt64-1, 2)
	require.ErrorContains(t, err, "overflow")
	_, err = checkedAddInt64(math.MaxInt64, 1)
	require.ErrorContains(t, err, "overflow")
}

func TestSnapshotChunkStructuredErrors_TransientAndActionable(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	retryBody := `{"error":"snapshot_chunk_capacity","message":"` + strings.Repeat("x", 2048) + `"}`
	successBody := `{"rows":[],"next_row_ordinal":0,"has_more":false,"logical_byte_count":0}`

	var capacityAttempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempt := capacityAttempts.Add(1)
		if attempt < 3 {
			return rawJSONResponse(http.StatusTooManyRequests, retryBody), nil
		}
		return rawJSONResponse(http.StatusOK, successBody), nil
	})}
	_, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 1, 1024)
	require.NoError(t, err)
	require.Equal(t, int64(3), capacityAttempts.Load())
	require.Equal(t, int64(len(retryBody)), client.SnapshotTransferDiagnostics().MaxChunkDecodedBodyBytes)

	client.ResetSnapshotTransferDiagnostics()
	tooSmallBody := `{"error":"snapshot_chunk_too_small","required_decoded_body_bytes":2049,"message":"` + strings.Repeat("y", 1024) + `"}`
	var tooSmallAttempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		tooSmallAttempts.Add(1)
		return rawJSONResponse(http.StatusUnprocessableEntity, tooSmallBody), nil
	})}
	_, _, err = client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 1, 1024)
	var tooSmall *SnapshotChunkTooSmallError
	require.ErrorAs(t, err, &tooSmall)
	require.Equal(t, int64(2049), tooSmall.RequiredBytes)
	require.Equal(t, int64(1), tooSmallAttempts.Load())
	require.Equal(t, int64(len(tooSmallBody)), client.SnapshotTransferDiagnostics().MaxChunkDecodedBodyBytes)
}

func TestSnapshotSessionBuildCapacity_RetriesOnlyLockedStructuredCode(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	var attempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if attempts.Add(1) < 3 {
			return errorJSONResponse(http.StatusTooManyRequests, oversync.ErrorResponse{Error: "snapshot_build_capacity"}), nil
		}
		return rawJSONResponse(http.StatusOK, `{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,"row_count":0,"logical_byte_count":0,"expires_at":"2030-01-01T00:00:00Z"}`), nil
	})}
	_, err := client.createSnapshotSession(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), attempts.Load())

	attempts.Store(0)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return errorJSONResponse(http.StatusTooManyRequests, oversync.ErrorResponse{Error: "unrelated_capacity"}), nil
	})}
	_, err = client.createSnapshotSession(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, int64(1), attempts.Load())
}

func TestSnapshotControlResponsesUseOperationSpecificDecodedLimits(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)

	t.Run("session success", func(t *testing.T) {
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return gzipResponse(http.StatusOK, `{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,"row_count":0,"logical_byte_count":0,"expires_at":"2030-01-01T00:00:00Z","padding":"`+strings.Repeat("x", 70<<10)+`"}`), nil
		})}
		_, err := client.createSnapshotSession(context.Background(), nil)
		var tooLarge *SnapshotResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
		require.Equal(t, snapshotControlBodyLimit, tooLarge.Limit)
	})

	t.Run("session non-2xx", func(t *testing.T) {
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return gzipResponse(http.StatusBadRequest, `{"error":"bad_request","message":"`+strings.Repeat("x", 70<<10)+`"}`), nil
		})}
		_, err := client.createSnapshotSession(context.Background(), nil)
		var tooLarge *SnapshotResponseBodyTooLargeError
		require.ErrorAs(t, err, &tooLarge)
		require.Equal(t, snapshotControlBodyLimit, tooLarge.Limit)
	})

}

type closeTrackingBody struct {
	reader io.Reader
	closed atomic.Int64
}

type cancelAtEOFReader struct {
	reader *strings.Reader
	cancel context.CancelFunc
}

type cancelAndAwaitCloseBody struct {
	reader           *strings.Reader
	cancel           context.CancelFunc
	closed           atomic.Int64
	closeDone        chan struct{}
	callbackObserved atomic.Bool
}

type readErrorBody struct {
	reader *strings.Reader
	closed atomic.Int64
}

type hostileBoundedBody struct {
	reader   io.Reader
	readErr  error
	closeErr error
	closed   atomic.Int64
}

func (b *hostileBoundedBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.reader.Read(p)
}

func (b *hostileBoundedBody) Close() error {
	b.closed.Add(1)
	return b.closeErr
}

func (b *readErrorBody) Read(p []byte) (int, error) {
	if b.reader == nil {
		b.reader = strings.NewReader("{")
	}
	n, err := b.reader.Read(p)
	if err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}

func (b *readErrorBody) Close() error {
	b.closed.Add(1)
	return nil
}

func (b *closeTrackingBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *closeTrackingBody) Close() error {
	b.closed.Add(1)
	return nil
}

func (r *cancelAtEOFReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

func (b *cancelAndAwaitCloseBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.cancel()
		select {
		case <-b.closeDone:
			b.callbackObserved.Store(true)
		case <-time.After(time.Second):
		}
	}
	return n, err
}

func (b *cancelAndAwaitCloseBody) Close() error {
	if b.closed.Add(1) == 1 {
		close(b.closeDone)
	}
	return nil
}

func TestSnapshotChunkSemanticFailure_ClosesBodyAndRecordsDecodedHighWater(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	body := &closeTrackingBody{reader: strings.NewReader(`{"rows":[],"next_row_ordinal":1,"has_more":false,"logical_byte_count":0}`)}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
	})}
	_, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 1, 1024)
	require.ErrorContains(t, err, "next_row_ordinal")
	require.Equal(t, int64(1), body.closed.Load())
	require.Greater(t, client.SnapshotTransferDiagnostics().MaxChunkDecodedBodyBytes, int64(0))
}

func TestSnapshotChunkPartialReadError_RetriesAndClosesEveryBody(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	badBody := &readErrorBody{reader: strings.NewReader(strings.Repeat("partial", 512))}
	successBody := `{"rows":[],"next_row_ordinal":0,"has_more":false,"logical_byte_count":0}`
	var attempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: badBody}, nil
		}
		return rawJSONResponse(http.StatusOK, successBody), nil
	})}
	_, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 1, 1024)
	require.NoError(t, err)
	require.Equal(t, int64(2), attempts.Load())
	require.Equal(t, int64(1), badBody.closed.Load())
	require.Equal(t, int64(len(successBody)), client.SnapshotTransferDiagnostics().MaxChunkDecodedBodyBytes)
}

func TestPhase3BoundedHTTPFailuresAreRedactedWithRetriesDisabled(t *testing.T) {
	const hostileText = "private-source-sentinel connection reset by peer"
	validBody := `{"rows":[],"next_row_ordinal":0,"has_more":false,"logical_byte_count":0}`

	tests := []struct {
		name      string
		transport rawRoundTripFunc
	}{
		{
			name: "transport",
			transport: func(*http.Request) (*http.Response, error) {
				return nil, phase3RetryableHostileError{}
			},
		},
		{
			name: "read",
			transport: func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       &hostileBoundedBody{readErr: phase3RetryableHostileError{}},
				}, nil
			},
		},
		{
			name: "close",
			transport: func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: &hostileBoundedBody{
						reader:   strings.NewReader(validBody),
						closeErr: phase3RetryableHostileError{},
					},
				}, nil
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
			cfg.RetryPolicy = &RetryPolicy{Enabled: false}
			client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
			client.HTTP = &http.Client{Transport: testCase.transport}

			_, _, err := client.fetchSnapshotChunk(context.Background(), "snapshot-1", 4, 0, 1, 1024)
			require.Error(t, err)
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				require.NotContains(t, cause.Error(), hostileText)
			}
		})
	}
}

func TestSnapshotConfigDefaultsAndNegativeValidation(t *testing.T) {
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	require.Equal(t, int64(4<<20), config.SnapshotChunkBytes)
	require.Equal(t, 1000, config.SnapshotChunkRows)

	for name, mutate := range map[string]func(*Config){
		"rows":  func(c *Config) { c.SnapshotChunkRows = -1 },
		"bytes": func(c *Config) { c.SnapshotChunkBytes = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec(usersTestDDL)
			require.NoError(t, err)
			bad := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
			mutate(bad)
			_, err = newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, bad)
			require.ErrorContains(t, err, "must be positive")
		})
	}
}

func TestSnapshotChunkBodyLimitFormula(t *testing.T) {
	limit, err := checkedSnapshotChunkBodyLimit(1000, 17)
	require.NoError(t, err)
	require.Equal(t, int64(1000+17+(64<<10)), limit)
}

func TestReadDecodedBodyBounded_CancellationAndCallerCleanupCloseExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	body := &cancelAndAwaitCloseBody{
		reader:    strings.NewReader(`{"error":"snapshot_chunk_capacity"}`),
		cancel:    cancel,
		closeDone: make(chan struct{}),
	}
	resp := &http.Response{Header: http.Header{}, Body: body}

	_, err := readDecodedBodyBounded(ctx, "snapshot_chunk", resp, snapshotControlBodyLimit)
	require.True(t, errors.Is(err, context.Canceled))
	require.True(t, body.callbackObserved.Load(), "cancellation close callback must complete before caller cleanup")
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int64(1), body.closed.Load())
}

func TestSnapshotCancellationStopsCapacityRetry(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int64
	body := &closeTrackingBody{reader: &cancelAtEOFReader{reader: strings.NewReader(`{"error":"snapshot_chunk_capacity"}`), cancel: cancel}}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
	})}
	_, _, err := client.fetchSnapshotChunk(ctx, "snapshot-1", 4, 0, 1, 1024)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, int64(1), attempts.Load())
	require.Equal(t, int64(1), body.closed.Load())
}
