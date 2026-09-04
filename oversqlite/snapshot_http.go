package oversqlite

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	gzipBodyThreshold               = 2048
	snapshotControlBodyLimit  int64 = 64 << 10
	snapshotBodyEnvelopeBytes int64 = 64 << 10
	snapshotRetirementTimeout       = 5 * time.Second
)

// SnapshotResponseBodyTooLargeError reports that a decoded response exceeded
// the operation-specific memory boundary.
type SnapshotResponseBodyTooLargeError struct {
	Operation string
	Limit     int64
}

// EncodedResponseBodyTooLargeError reports a raw/native response that exceeds
// its encoded transport ceiling before decompression.
type EncodedResponseBodyTooLargeError struct {
	Operation string
	Limit     int64
}

func (e *EncodedResponseBodyTooLargeError) Error() string {
	return fmt.Sprintf("%s encoded response body exceeds %d bytes", e.Operation, e.Limit)
}

func (e *SnapshotResponseBodyTooLargeError) Error() string {
	return fmt.Sprintf("%s decoded response body exceeds %d bytes", e.Operation, e.Limit)
}

// SnapshotUnsupportedContentEncodingError reports a response encoding that
// the bounded reader cannot safely decode.
type SnapshotUnsupportedContentEncodingError struct {
	Operation string
	Encoding  string
}

func (e *SnapshotUnsupportedContentEncodingError) Error() string {
	return fmt.Sprintf("%s response uses unsupported content encoding", e.Operation)
}

type boundedHTTPFailureStage string

const (
	boundedHTTPFailureTransport boundedHTTPFailureStage = "transport"
	boundedHTTPFailureDecode    boundedHTTPFailureStage = "response_decode"
	boundedHTTPFailureRead      boundedHTTPFailureStage = "response_read"
	boundedHTTPFailureClose     boundedHTTPFailureStage = "response_close"
)

// boundedHTTPFailureError preserves only local classification state. It must
// never retain or unwrap the transport/body error supplied by the peer.
type boundedHTTPFailureError struct {
	operation string
	stage     boundedHTTPFailureStage
	retryable bool
}

func (e *boundedHTTPFailureError) Error() string {
	if e == nil {
		return "oversqlite HTTP operation failed"
	}
	return fmt.Sprintf("%s HTTP %s failed", e.operation, e.stage)
}

func newBoundedHTTPFailureError(ctx context.Context, operation string, stage boundedHTTPFailureStage, err error) error {
	return &boundedHTTPFailureError{
		operation: operation,
		stage:     stage,
		retryable: isRetryableOperationError(ctx, err),
	}
}

// Only the local growth callback may create this marker. Reader/peer failures
// still pass through the bounded sanitizer, even if they mimic a resource type.
type responseAdmissionFailure struct{ cause error }

func (e *responseAdmissionFailure) Error() string { return "local response admission failed" }

func responseReadFailure(ctx context.Context, operation string, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	var local *responseAdmissionFailure
	if errors.As(err, &local) {
		return local.cause
	}
	return newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureRead, err)
}

type boundedResponsePayload struct {
	body             []byte
	statusCode       int
	decodedBodyBytes int64
	retryAfter       string
}

type snapshotCapacityHTTPError struct {
	errorCode  string
	retryAfter time.Duration
}

func (e *snapshotCapacityHTTPError) Error() string {
	return "snapshot admission capacity is temporarily unavailable"
}

type exactlyOnceReadCloser struct {
	body     io.ReadCloser
	once     sync.Once
	closeErr error
}

func (c *exactlyOnceReadCloser) Read(p []byte) (int, error) {
	return c.body.Read(p)
}

func (c *exactlyOnceReadCloser) Close() error {
	c.once.Do(func() {
		c.closeErr = c.body.Close()
	})
	return c.closeErr
}

func checkedAddInt64(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("integer overflow adding %d and %d", a, b)
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("integer overflow adding %d and %d", a, b)
	}
	return a + b, nil
}

func checkedSnapshotChunkBodyLimit(maxBytes int64, maxRows int) (int64, error) {
	if maxBytes <= 0 || maxRows <= 0 {
		return 0, fmt.Errorf("snapshot chunk body budget requires positive row and byte limits")
	}
	limit, err := checkedAddInt64(maxBytes, int64(maxRows))
	if err != nil {
		return 0, fmt.Errorf("snapshot chunk body budget overflow: %w", err)
	}
	limit, err = checkedAddInt64(limit, snapshotBodyEnvelopeBytes)
	if err != nil {
		return 0, fmt.Errorf("snapshot chunk body budget overflow: %w", err)
	}
	return limit, nil
}

func readDecodedBodyBounded(ctx context.Context, operation string, resp *http.Response, limit int64) ([]byte, error) {
	if limit < 0 || limit == math.MaxInt64 {
		return nil, fmt.Errorf("%s response body limit is invalid", operation)
	}
	if resp == nil || resp.Body == nil {
		return nil, fmt.Errorf("%s response body is missing", operation)
	}
	if _, ok := resp.Body.(*exactlyOnceReadCloser); !ok {
		resp.Body = &exactlyOnceReadCloser{body: resp.Body}
	}

	closeDone := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() { defer close(closeDone); _ = resp.Body.Close() })
	defer func() {
		if !stopClose() {
			<-closeDone
		}
	}()

	encodedBody, err := readReservedResponse(ctx, resp.Body, limit+1, 0)
	if err != nil {
		return nil, responseReadFailure(ctx, operation, err)
	}
	if int64(len(encodedBody)) > limit {
		return nil, &EncodedResponseBodyTooLargeError{Operation: operation, Limit: limit}
	}
	encodingValues := responseHeaderValues(resp.Header, "Content-Encoding")
	if len(encodingValues) > 1 {
		return nil, &SnapshotUnsupportedContentEncodingError{Operation: operation, Encoding: "multiple"}
	}
	encoding := ""
	if len(encodingValues) == 1 {
		encoding = strings.ToLower(strings.TrimSpace(encodingValues[0]))
	}
	encodedReader := bytes.NewReader(encodedBody)
	var reader io.Reader = encodedReader
	var gzipReader *gzip.Reader
	switch encoding {
	case "", "identity":
	case "gzip":
		gzipReader, err = gzip.NewReader(encodedReader)
		if err != nil {
			return nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureDecode, err)
		}
		gzipReader.Multistream(false)
		reader = gzipReader
	default:
		return nil, &SnapshotUnsupportedContentEncodingError{Operation: operation, Encoding: encoding}
	}

	readLimit := limit + 1
	body, err := readReservedResponse(ctx, reader, readLimit, int64(len(encodedBody)))
	if err != nil {
		return nil, responseReadFailure(ctx, operation, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if int64(len(body)) > limit {
		return nil, &SnapshotResponseBodyTooLargeError{Operation: operation, Limit: limit}
	}
	if encoding == "gzip" {
		if err := gzipReader.Close(); err != nil {
			return nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureDecode, err)
		}
		if encodedReader.Len() != 0 {
			return nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureDecode, fmt.Errorf("gzip response has trailing encoded data"))
		}
	}
	return body, nil
}

func responseHeaderValues(header http.Header, name string) []string {
	var values []string
	for key, entries := range header {
		if strings.EqualFold(key, name) {
			values = append(values, entries...)
		}
	}
	return values
}

func gzipRequestBody(body []byte) ([]byte, bool, error) {
	if len(body) < gzipBodyThreshold {
		return body, false, nil
	}
	var encoded bytes.Buffer
	writer := gzip.NewWriter(&encoded)
	if _, err := writer.Write(body); err != nil {
		return nil, false, err
	}
	if err := writer.Close(); err != nil {
		return nil, false, err
	}
	if encoded.Len() >= len(body) {
		return body, false, nil
	}
	return encoded.Bytes(), true, nil
}

func (c *Client) doAuthenticatedBoundedRequest(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	return c.doAuthenticatedBoundedRequestAsSource(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, c.sourceID, onDecodedBodyRead)
}

func (c *Client) doAuthenticatedBoundedRequestAsSource(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	sourceID string,
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	encodedRequestBody := requestBody
	compressedRequest := false
	var err error
	if len(requestBody) > 0 {
		encodedRequestBody, compressedRequest, err = gzipRequestBody(requestBody)
		if err != nil {
			return boundedResponsePayload{}, fmt.Errorf("gzip request body: %w", err)
		}
	}
	var bodyReader io.Reader
	if len(encodedRequestBody) > 0 {
		bodyReader = bytes.NewReader(encodedRequestBody)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return boundedResponsePayload{}, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return boundedResponsePayload{}, err
	}
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if compressedRequest {
		req.Header.Set("Content-Encoding", "gzip")
	}
	// Setting this explicitly disables net/http's transparent unbounded gzip
	// decoding. The bounded reader performs decoding below.
	req.Header.Set("Accept-Encoding", "gzip")
	if err := applyAuthenticatedSyncHeadersWithSourceID(req, token, sourceID, c.schemaGenerationProof); err != nil {
		return boundedResponsePayload{}, err
	}

	return c.executeBoundedRequest(ctx, operation, req, successLimit, errorLimit, onDecodedBodyRead)
}

func (c *Client) executeBoundedRequest(ctx context.Context, operation string, req *http.Request, successLimit, errorLimit int64, onDecodedBodyRead func(int64)) (boundedResponsePayload, error) {
	resp, err := c.doHTTP(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return boundedResponsePayload{}, ctxErr
		}
		return boundedResponsePayload{}, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureTransport, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()

	limit := errorLimit
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		limit = successLimit
	}
	body, readErr := readDecodedBodyBounded(ctx, operation, resp, limit)
	if readErr != nil {
		return boundedResponsePayload{}, readErr
	}
	if onDecodedBodyRead != nil {
		onDecodedBodyRead(int64(len(body)))
	}
	closeErr := resp.Body.Close()
	closed = true
	if closeErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return boundedResponsePayload{}, ctxErr
		}
		return boundedResponsePayload{}, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureClose, closeErr)
	}
	return boundedResponsePayload{
		body:             body,
		statusCode:       resp.StatusCode,
		decodedBodyBytes: int64(len(body)),
		retryAfter:       resp.Header.Get("Retry-After"),
	}, nil
}

// doAuthenticatedSessionDeleteAsSource preserves the retirement contract that
// successful response bodies are never read. Error bodies are decoded under
// the normal control-plane bound so structured session and generation errors
// can still be classified exactly.
func (c *Client) doAuthenticatedSessionDeleteAsSource(
	ctx context.Context,
	operation, endpoint, sourceID string,
) (statusCode int, body []byte, resultErr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return 0, nil, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept-Encoding", "gzip")
	if err := applyAuthenticatedSyncHeadersWithSourceID(req, token, sourceID, c.schemaGenerationProof); err != nil {
		return 0, nil, err
	}
	resp, err := c.doHTTP(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, ctxErr
		}
		return 0, nil, newBoundedHTTPFailureError(ctx, operation, boundedHTTPFailureTransport, err)
	}
	if resp.Body != nil {
		defer func() {
			if closeErr := resp.Body.Close(); resultErr == nil && closeErr != nil {
				resultErr = closeErr
			}
		}()
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, nil, nil
	}
	body, err = readDecodedBodyBounded(ctx, operation, resp, snapshotControlBodyLimit)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func (c *Client) doAuthenticatedBoundedRequestWithRetry(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	transientErrorCodes map[string]struct{},
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	return c.doAuthenticatedBoundedRequestWithRetryAsSource(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, c.sourceID, transientErrorCodes, onDecodedBodyRead)
}

func (c *Client) doAuthenticatedBoundedRequestWithRetryAsSource(
	ctx context.Context,
	operation, method, endpoint string,
	requestBody []byte,
	contentType string,
	successLimit, errorLimit int64,
	sourceID string,
	transientErrorCodes map[string]struct{},
	onDecodedBodyRead func(int64),
) (boundedResponsePayload, error) {
	capacityPolicy := c.normalizedSnapshotCapacityRetryPolicy()
	var capacityStarted time.Time
	for {
		result, err := withRetryValue(ctx, c.normalizedRetryPolicy(), operation, func() (boundedResponsePayload, error) {
			result, err := c.doAuthenticatedBoundedRequestAsSource(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, sourceID, onDecodedBodyRead)
			if err != nil {
				return boundedResponsePayload{}, err
			}
			retryable := result.statusCode == http.StatusBadGateway || result.statusCode == http.StatusServiceUnavailable || result.statusCode == http.StatusGatewayTimeout
			if result.statusCode == http.StatusTooManyRequests {
				if transientErrorCodes == nil {
					retryable = true
				} else if errorResp, ok := decodeServerErrorResponse(result.body); ok {
					if _, capacity := transientErrorCodes[errorResp.Error]; capacity {
						return boundedResponsePayload{}, &snapshotCapacityHTTPError{
							errorCode:  errorResp.Error,
							retryAfter: parseRetryAfterDeltaSeconds(result.retryAfter),
						}
					}
				}
			}
			if retryable {
				return boundedResponsePayload{}, &retryHTTPError{
					Operation: operation, StatusCode: result.statusCode,
				}
			}
			return result, nil
		})
		if err == nil {
			return result, nil
		}
		var capacityErr *snapshotCapacityHTTPError
		if !errors.As(err, &capacityErr) {
			return boundedResponsePayload{}, err
		}
		atomic.AddInt64(&c.snapshotStats.capacityResponses, 1)
		if capacityStarted.IsZero() {
			capacityStarted = time.Now()
		}
		waited := time.Since(capacityStarted)
		if !capacityPolicy.enabled {
			return boundedResponsePayload{}, &SnapshotCapacityRetryExhaustedError{
				Operation: operation, ErrorCode: capacityErr.errorCode, Waited: waited,
			}
		}
		delay := capacityErr.retryAfter
		if delay <= 0 {
			delay = capacityPolicy.fallbackDelay
		}
		delay = positiveJitterDuration(delay, capacityPolicy.jitterFraction)
		if delay <= 0 || waited >= capacityPolicy.maxWait || delay > capacityPolicy.maxWait-waited {
			return boundedResponsePayload{}, &SnapshotCapacityRetryExhaustedError{
				Operation: operation, ErrorCode: capacityErr.errorCode, Waited: waited,
			}
		}
		atomic.AddInt64(&c.snapshotStats.capacityRetries, 1)
		waitStarted := time.Now()
		if err := waitRetryBackoff(ctx, delay); err != nil {
			return boundedResponsePayload{}, err
		}
		atomic.AddInt64(&c.snapshotStats.capacityWaitNanos, int64(time.Since(waitStarted)))
	}
}

func parseRetryAfterDeltaSeconds(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func positiveJitterDuration(base time.Duration, fraction float64) time.Duration {
	if base <= 0 || fraction <= 0 {
		return base
	}
	span := float64(base) * min(fraction, 1)
	return base + time.Duration(math.Round(rand.Float64()*span))
}

// Read one bounded page at a time; admit every growth before allocating it.
// The scope belongs to the public push invocation and survives HTTP cleanup.
func readReservedResponse(ctx context.Context, reader io.Reader, limit, held int64) ([]byte, error) {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return io.ReadAll(io.LimitReader(reader, limit))
	}
	var pages [][]byte
	var size int64
	for size < limit {
		next := min(int64(64*1024), limit-size)
		if err := scope.client.growPushHTTPBuffers(ctx, held+size+next+64*1024, 64+int64(len(pages))); err != nil {
			return nil, &responseAdmissionFailure{cause: err}
		}
		page := make([]byte, next)
		n, err := io.ReadFull(reader, page)
		if n > 0 {
			pages = append(pages, page[:n])
			size += int64(n)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if err := scope.client.growPushHTTPBuffers(ctx, held+2*size+64*1024, 64+int64(len(pages))); err != nil {
		return nil, &responseAdmissionFailure{cause: err}
	}
	body := make([]byte, size)
	offset := 0
	for _, page := range pages {
		offset += copy(body[offset:], page)
	}
	return body, nil
}

// Count parser/container allocations without constructing a JSON tree. Strict
// wire validation still owns syntax and semantic rejection after admission.
func responseAllocationShape(ctx context.Context, body []byte) (int64, error) {
	var depth, peak, containers int64
	quoted, escaped := false, false
	for i, b := range body {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		if quoted {
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		switch b {
		case '"':
			quoted = true
		case '[', '{':
			depth++
			peak = max(peak, depth)
			containers++
		case ']', '}':
			depth--
		}
	}
	if containers > math.MaxInt64/32 {
		return 0, fmt.Errorf("response allocation overflow")
	}
	return 64 + peak + 32*containers, nil
}
