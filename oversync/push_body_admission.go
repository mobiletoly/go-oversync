// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// pushBodyInitialBufferBytes is the first buffer taken for a push body whose
// length the request does not declare.
const pushBodyInitialBufferBytes int64 = 32 << 10

var errPushBodyBudgetExhausted = errors.New("push body budget exhausted")

// pushBodyAdmission bounds the memory that push request bodies can hold, so
// that it depends on configured limits and not on the number of connections.
//
// A push body is small on the wire and large once decoded, so admission has
// two stages. While a body is being received, its buffer is charged to a
// shared byte budget. Before the body is decompressed and parsed, the request
// must take one of a fixed number of decode permits, which it keeps until its
// handler returns. Neither stage waits: a request that cannot be charged or
// admitted is refused with the retryable capacity error, before any
// decompression or parsing has been done for it.
type pushBodyAdmission struct {
	decodePermits chan struct{}
	budgetBytes   int64
	chargedBytes  atomic.Int64
}

// newPushBodyAdmission admits maxConcurrent decoded bodies at a time and lets
// bodies still being received hold as many bytes as maxConcurrent bodies of
// the largest readable size.
func newPushBodyAdmission(maxConcurrent int, maxBodyBytes int64) *pushBodyAdmission {
	return &pushBodyAdmission{
		decodePermits: make(chan struct{}, maxConcurrent),
		budgetBytes:   int64(maxConcurrent) * (maxBodyBytes + 1),
	}
}

func pushBodyCapacityError() error {
	return projectionError(HistoryProjectionCapacityExceeded, "push_body_admission", "history projection capacity exceeded")
}

func (a *pushBodyAdmission) tryCharge(bytes int64) bool {
	if a == nil || bytes < 0 {
		return false
	}
	for {
		charged := a.chargedBytes.Load()
		if bytes > a.budgetBytes-charged {
			return false
		}
		if a.chargedBytes.CompareAndSwap(charged, charged+bytes) {
			return true
		}
	}
}

func (a *pushBodyAdmission) refund(bytes int64) {
	if a != nil && bytes > 0 {
		a.chargedBytes.Add(-bytes)
	}
}

func (a *pushBodyAdmission) tryAcquireDecode() (func(), bool) {
	if a == nil {
		return nil, false
	}
	select {
	case a.decodePermits <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-a.decodePermits }) }, true
	default:
		return nil, false
	}
}

// readEncoded buffers at most limit+1 bytes of an encoded body and charges the
// buffer's capacity to the byte budget before each allocation. sizeHint is the
// declared length, or negative when unknown; it sizes the first buffer and is
// not trusted beyond that. The returned charge stays held until the caller
// refunds it, also when an error is returned.
func (a *pushBodyAdmission) readEncoded(body io.Reader, sizeHint, limit int64) (encoded []byte, charged int64, err error) {
	maxBuffer := limit + 1
	capacity := pushBodyInitialBufferBytes
	if sizeHint >= 0 {
		capacity = sizeHint + 1
	}
	capacity = min(capacity, maxBuffer)
	for {
		if grow := capacity - int64(cap(encoded)); grow > 0 {
			if !a.tryCharge(grow) {
				return nil, charged, errPushBodyBudgetExhausted
			}
			charged += grow
			grown := make([]byte, len(encoded), capacity)
			copy(grown, encoded)
			encoded = grown
		}
		n, readErr := body.Read(encoded[len(encoded):cap(encoded)])
		encoded = encoded[:len(encoded)+n]
		switch {
		case readErr == io.EOF:
			return encoded, charged, nil
		case readErr != nil:
			return nil, charged, readErr
		case int64(len(encoded)) >= maxBuffer:
			return encoded, charged, nil
		case len(encoded) == cap(encoded):
			capacity = min(2*int64(cap(encoded)), maxBuffer)
		}
	}
}

// admitPushBody reads and decodes one push request body under admission. On
// success the caller owns the decoded bytes and must call release when the
// request no longer needs them. Header and declared-length failures are
// reported before any capacity is consumed; capacity refusal is reported
// before any decompression or parsing.
func (s *SyncService) admitPushBody(r *http.Request, decodedLimit int64) (decoded []byte, release func(), err error) {
	if decodedLimit < 1 {
		return nil, nil, &boundedBodyError{code: "decoded_body_too_large", err: errors.New("decoded body limit is invalid")}
	}
	encoding, err := validateRequestContentEncoding(r)
	if err != nil {
		return nil, nil, err
	}
	if r.ContentLength > decodedLimit {
		return nil, nil, encodedBodyTooLargeError()
	}
	admission := s.pushBodies
	encoded, charged, err := admission.readEncoded(r.Body, r.ContentLength, decodedLimit)
	if errors.Is(err, errPushBodyBudgetExhausted) {
		admission.refund(charged)
		// Receive the rest without retaining it, so that the refusal is
		// delivered on a connection the client can keep using.
		_, _ = io.CopyN(io.Discard, r.Body, decodedLimit+1)
		return nil, nil, s.refusePushBody()
	}
	if err != nil {
		admission.refund(charged)
		return nil, nil, err
	}
	if int64(len(encoded)) > decodedLimit {
		admission.refund(charged)
		return nil, nil, encodedBodyTooLargeError()
	}
	release, admitted := admission.tryAcquireDecode()
	// From here the body is accounted for by the decode permit or dropped.
	admission.refund(charged)
	if !admitted {
		return nil, nil, s.refusePushBody()
	}
	decoded, err = decodeBoundedBody(encoding, encoded, decodedLimit)
	if err != nil {
		release()
		return nil, nil, err
	}
	return decoded, release, nil
}

func (s *SyncService) refusePushBody() error {
	s.historyProjectionMetrics.capacityFailures.Add(1)
	return pushBodyCapacityError()
}
