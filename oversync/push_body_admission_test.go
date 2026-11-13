// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func gzipForPushBodyTest(t *testing.T, decoded []byte) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer := gzip.NewWriter(&encoded)
	_, err := writer.Write(decoded)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return encoded.Bytes()
}

func gzipPushBodyRequest(encoded []byte) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(encoded))
	request.Header.Set("Content-Encoding", "gzip")
	return request
}

func requirePushBodyCapacityError(t *testing.T, err error) {
	t.Helper()
	var projectionErr *GenerationProjectionError
	require.ErrorAs(t, err, &projectionErr)
	require.Equal(t, HistoryProjectionCapacityExceeded, projectionErr.Code)
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

func TestPushBodyAdmission_RefusesBeyondDecodePermitsWithoutDecoding(t *testing.T) {
	const decodedBytes = 8 << 20
	const limit = int64(16 << 20)
	service := &SyncService{pushBodies: newPushBodyAdmission(1, limit)}
	encoded := gzipForPushBodyTest(t, bytes.Repeat([]byte{'a'}, decodedBytes))
	require.Less(t, len(encoded), 64<<10)

	holdPermit, admitted := service.pushBodies.tryAcquireDecode()
	require.True(t, admitted)

	request := gzipPushBodyRequest(encoded)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	decoded, release, err := service.admitPushBody(request, limit)
	runtime.ReadMemStats(&after)

	requirePushBodyCapacityError(t, err)
	require.Nil(t, decoded)
	require.Nil(t, release)
	// A refused body must cost about its encoded size, not its decoded size.
	// The bound is half the decoded size because the counter is process-wide.
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(decodedBytes/2))
	require.Zero(t, service.pushBodies.chargedBytes.Load())
	require.EqualValues(t, 1, service.historyProjectionMetrics.capacityFailures.Load())

	holdPermit()
	runtime.ReadMemStats(&before)
	decoded, release, err = service.admitPushBody(gzipPushBodyRequest(encoded), limit)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Len(t, decoded, decodedBytes)
	// The same body costs at least its decoded size once it is admitted.
	require.GreaterOrEqual(t, after.TotalAlloc-before.TotalAlloc, uint64(decodedBytes))
	require.Zero(t, service.pushBodies.chargedBytes.Load())
	release()
	release()
	require.Empty(t, service.pushBodies.decodePermits)
}

func TestPushBodyAdmission_AdmitsOnlyPermittedConcurrentBodies(t *testing.T) {
	const permits = 2
	const requests = 16
	const limit = int64(1 << 20)
	service := &SyncService{pushBodies: newPushBodyAdmission(permits, limit)}
	encoded := gzipForPushBodyTest(t, bytes.Repeat([]byte{'b'}, int(limit)))

	type outcome struct {
		decoded int
		err     error
	}
	outcomes := make(chan outcome, requests)
	holdAdmitted := make(chan struct{})
	var workers sync.WaitGroup
	for range requests {
		workers.Add(1)
		go func() {
			defer workers.Done()
			decoded, release, err := service.admitPushBody(gzipPushBodyRequest(encoded), limit)
			outcomes <- outcome{decoded: len(decoded), err: err}
			if err == nil {
				<-holdAdmitted
				release()
			}
		}()
	}

	admitted := 0
	for range requests {
		result := <-outcomes
		if result.err == nil {
			admitted++
			require.EqualValues(t, limit, result.decoded)
			continue
		}
		requirePushBodyCapacityError(t, result.err)
	}
	require.Equal(t, permits, admitted)
	require.EqualValues(t, requests-permits, service.historyProjectionMetrics.capacityFailures.Load())

	close(holdAdmitted)
	workers.Wait()
	require.Empty(t, service.pushBodies.decodePermits)
	require.Zero(t, service.pushBodies.chargedBytes.Load())
}

type admittedPushBody struct {
	decoded []byte
	release func()
	err     error
}

// startStalledPushBody starts admitting a body that declares its full length
// and has delivered only the given prefix. finish delivers the rest.
func startStalledPushBody(t *testing.T, service *SyncService, limit int64, body []byte, prefix int) (finish func() admittedPushBody) {
	t.Helper()
	reader, writer := io.Pipe()
	request := httptest.NewRequest(http.MethodPost, "/sync/push", reader)
	request.ContentLength = int64(len(body))
	done := make(chan admittedPushBody, 1)
	go func() {
		decoded, release, err := service.admitPushBody(request, limit)
		done <- admittedPushBody{decoded: decoded, release: release, err: err}
	}()
	_, err := writer.Write(body[:prefix])
	require.NoError(t, err)
	return func() admittedPushBody {
		_, err := writer.Write(body[prefix:])
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		return <-done
	}
}

func TestPushBodyAdmission_AnAnnouncedBodyHoldsOnlyWhatHasArrived(t *testing.T) {
	const limit = int64(1 << 20)
	service := &SyncService{pushBodies: newPushBodyAdmission(1, limit)}
	announced := bytes.Repeat([]byte{'c'}, int(limit))

	// A body announces the largest length and stalls after 100 bytes.
	finishStalled := startStalledPushBody(t, service, limit, announced, 100)
	require.Equal(t, pushBodyInitialBufferBytes, service.pushBodies.chargedBytes.Load())

	// Another body of more than half the budget is still received and admitted.
	other := bytes.Repeat([]byte{'d'}, 600<<10)
	request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(other))
	decoded, release, err := service.admitPushBody(request, limit)
	require.NoError(t, err)
	require.Equal(t, other, decoded)
	require.Equal(t, pushBodyInitialBufferBytes, service.pushBodies.chargedBytes.Load())
	release()

	stalled := finishStalled()
	require.NoError(t, stalled.err)
	require.Equal(t, announced, stalled.decoded)
	require.Zero(t, service.pushBodies.chargedBytes.Load())
	stalled.release()
	require.Empty(t, service.pushBodies.decodePermits)
}

func TestPushBodyAdmission_BodiesThatHaveArrivedAreChargedAndBoundOthers(t *testing.T) {
	const limit = int64(1 << 20)
	service := &SyncService{pushBodies: newPushBodyAdmission(1, limit)}
	first := bytes.Repeat([]byte{'c'}, int(limit))

	// 700 kB of the first body has arrived; its buffer has doubled to 1 MiB,
	// one byte short of the whole budget.
	finishFirst := startStalledPushBody(t, service, limit, first, 700<<10)
	require.Eventually(t, func() bool { return service.pushBodies.chargedBytes.Load() == limit }, time.Second, time.Millisecond)

	// A second body that does not fit beside it is refused and received
	// without being retained.
	second := &countingReader{reader: bytes.NewReader(bytes.Repeat([]byte{'d'}, 600<<10))}
	request := httptest.NewRequest(http.MethodPost, "/sync/push", second)
	request.ContentLength = 600 << 10
	decoded, release, err := service.admitPushBody(request, limit)
	requirePushBodyCapacityError(t, err)
	require.Nil(t, decoded)
	require.Nil(t, release)
	require.Equal(t, 600<<10, second.read)
	require.Equal(t, limit, service.pushBodies.chargedBytes.Load())

	admitted := finishFirst()
	require.NoError(t, admitted.err)
	require.Equal(t, first, admitted.decoded)
	require.Zero(t, service.pushBodies.chargedBytes.Load())
	admitted.release()
}

func TestPushBodyAdmission_RefundsAfterABodyReadFailure(t *testing.T) {
	const limit = int64(1 << 20)
	service := &SyncService{pushBodies: newPushBodyAdmission(1, limit)}
	failure := errors.New("connection lost")
	body := io.MultiReader(bytes.NewReader(make([]byte, 100<<10)), errorReader{err: failure})
	request := httptest.NewRequest(http.MethodPost, "/sync/push", body)
	request.ContentLength = -1

	decoded, release, err := service.admitPushBody(request, limit)

	require.ErrorIs(t, err, failure)
	require.Nil(t, decoded)
	require.Nil(t, release)
	require.Zero(t, service.pushBodies.chargedBytes.Load())
	require.Empty(t, service.pushBodies.decodePermits)
	require.Zero(t, service.historyProjectionMetrics.capacityFailures.Load())
}

func TestPushBodyAdmission_RejectsOversizedBodiesBeforeCapacity(t *testing.T) {
	const limit = int64(1024)
	service := &SyncService{pushBodies: newPushBodyAdmission(1, limit)}
	requireTooLarge := func(t *testing.T, err error, code string) {
		t.Helper()
		var bodyErr *boundedBodyError
		require.ErrorAs(t, err, &bodyErr)
		require.Equal(t, code, bodyErr.code)
		require.Zero(t, service.pushBodies.chargedBytes.Load())
		require.Empty(t, service.pushBodies.decodePermits)
	}

	t.Run("declared length is received up to the limit and not retained", func(t *testing.T) {
		body := &countingReader{reader: bytes.NewReader(make([]byte, 4*limit))}
		request := httptest.NewRequest(http.MethodPost, "/sync/push", body)
		request.ContentLength = 4 * limit
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, _, err := service.admitPushBody(request, limit)
		runtime.ReadMemStats(&after)
		requireTooLarge(t, err, "encoded_body_too_large")
		require.EqualValues(t, limit+1, body.read)
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(256<<10))
	})
	t.Run("undeclared length stops at the limit", func(t *testing.T) {
		body := &countingReader{reader: bytes.NewReader(make([]byte, 4*limit))}
		request := httptest.NewRequest(http.MethodPost, "/sync/push", body)
		request.ContentLength = -1
		_, _, err := service.admitPushBody(request, limit)
		requireTooLarge(t, err, "encoded_body_too_large")
		require.EqualValues(t, limit+1, body.read)
	})
	t.Run("decoded size is checked under a permit and releases it", func(t *testing.T) {
		encoded := gzipForPushBodyTest(t, make([]byte, limit+1))
		_, _, err := service.admitPushBody(gzipPushBodyRequest(encoded), limit)
		requireTooLarge(t, err, "decoded_body_too_large")
	})
	t.Run("unsupported encoding is reported while capacity is exhausted", func(t *testing.T) {
		hold, admitted := service.pushBodies.tryAcquireDecode()
		require.True(t, admitted)
		defer hold()
		request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader([]byte("{}")))
		request.Header.Set("Content-Encoding", "br")
		_, _, err := service.admitPushBody(request, limit)
		var bodyErr *boundedBodyError
		require.ErrorAs(t, err, &bodyErr)
		require.Equal(t, "unsupported_content_encoding", bodyErr.code)
	})
}

func TestPushBodyAdmission_ReadEncodedChargesItsBuffer(t *testing.T) {
	const limit = int64(1 << 20)
	body := bytes.Repeat([]byte{'d'}, 100<<10)
	for _, test := range []struct {
		name     string
		sizeHint int64
	}{
		{name: "undeclared length", sizeHint: -1},
		{name: "declared length", sizeHint: int64(len(body))},
		{name: "declared length too small", sizeHint: 10},
		{name: "declared length too large", sizeHint: limit},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := newPushBodyAdmission(1, limit)
			encoded, charged, err := admission.readEncoded(bytes.NewReader(body), test.sizeHint, limit)
			require.NoError(t, err)
			require.Equal(t, body, encoded)
			require.EqualValues(t, cap(encoded), charged)
			require.Equal(t, charged, admission.chargedBytes.Load())
			admission.refund(charged)
			require.Zero(t, admission.chargedBytes.Load())
		})
	}

	t.Run("a full budget stops the read", func(t *testing.T) {
		admission := newPushBodyAdmission(1, limit)
		require.True(t, admission.tryCharge(admission.budgetBytes-pushBodyInitialBufferBytes))
		encoded, charged, err := admission.readEncoded(bytes.NewReader(body), -1, limit)
		require.ErrorIs(t, err, errPushBodyBudgetExhausted)
		require.Nil(t, encoded)
		require.Equal(t, pushBodyInitialBufferBytes, charged)
		require.Equal(t, admission.budgetBytes, admission.chargedBytes.Load())
	})
	t.Run("a read failure keeps its charge for the caller", func(t *testing.T) {
		admission := newPushBodyAdmission(1, limit)
		failure := errors.New("connection lost")
		encoded, charged, err := admission.readEncoded(io.MultiReader(bytes.NewReader(body[:10]), errorReader{err: failure}), -1, limit)
		require.ErrorIs(t, err, failure)
		require.Nil(t, encoded)
		require.Equal(t, charged, admission.chargedBytes.Load())
	})
	t.Run("a missing admission refuses", func(t *testing.T) {
		var admission *pushBodyAdmission
		_, _, err := admission.readEncoded(bytes.NewReader(body), -1, limit)
		require.ErrorIs(t, err, errPushBodyBudgetExhausted)
		_, admitted := admission.tryAcquireDecode()
		require.False(t, admitted)
	})
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestPushBodyAdmission_HTTPRefusalIsRetryableAndTheSamePushThenCommits(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})
	initializationID := resolveConnectForPush(t, ctx, fixture.svc, fixture.writer, true)
	proof := testSchemaGenerationProofForService(fixture.svc)
	contract, ok := fixture.svc.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	rows := []PushRequestRow{fixture.userRow(uuid.New(), "Admitted")}
	groups, err := CompactGroupsForRows(generationDescriptor(contract), rows)
	require.NoError(t, err)
	hash, err := computeGenerationPinnedPushRequestHash(proof, rows)
	require.NoError(t, err)
	body, err := json.Marshal(CompactPushRequest{
		SourceBundleID:       1,
		CanonicalRequestHash: hash,
		InitializationID:     initializationID,
		Rows:                 groups,
	})
	require.NoError(t, err)
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	push := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(body))
		request.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
		request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
		request = request.WithContext(ContextWithActor(request.Context(), fixture.writer))
		recorder := httptest.NewRecorder()
		handlers.HandlePush(recorder, request)
		return recorder
	}

	var held []func()
	for range cap(fixture.svc.pushBodies.decodePermits) {
		release, admitted := fixture.svc.pushBodies.tryAcquireDecode()
		require.True(t, admitted)
		held = append(held, release)
	}
	refused := push()
	require.Equal(t, http.StatusServiceUnavailable, refused.Code)
	require.Equal(t, "1", refused.Header().Get("Retry-After"))
	require.Contains(t, refused.Body.String(), `"error":"history_projection_capacity_exceeded"`)
	require.Contains(t, refused.Body.String(), `"retryable":true`)

	for _, release := range held {
		release()
	}
	// A body that is admitted and then fails to parse gives its admission back.
	malformed := httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(`{"source_bundle_id":`))
	malformed.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	malformed.Header.Set("Content-Type", "application/json")
	malformed.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
	malformed.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
	malformed = malformed.WithContext(ContextWithActor(malformed.Context(), fixture.writer))
	malformedRecorder := httptest.NewRecorder()
	handlers.HandlePush(malformedRecorder, malformed)
	require.Equal(t, http.StatusBadRequest, malformedRecorder.Code, malformedRecorder.Body.String())
	require.Empty(t, fixture.svc.pushBodies.decodePermits)
	require.Zero(t, fixture.svc.pushBodies.chargedBytes.Load())

	committed := push()
	require.Equal(t, http.StatusOK, committed.Code, committed.Body.String())
	replayed := push()
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	require.Equal(t, committed.Body.String(), replayed.Body.String())
	require.Empty(t, fixture.svc.pushBodies.decodePermits)
	require.Zero(t, fixture.svc.pushBodies.chargedBytes.Load())
}
