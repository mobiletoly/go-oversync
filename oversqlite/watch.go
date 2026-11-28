package oversqlite

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) startDownloaderLoop(ctx context.Context) {
	useWatch, err := c.shouldUseBundleChangeWatch(ctx)
	if err != nil {
		if c.stopForRetiredGeneration(err) {
			return
		}
		if isProtocolVersionMismatch(err) {
			c.logger.Error("automatic downloads stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
			return
		}
	}
	if useWatch {
		c.watchAwareDownloaderLoop(ctx)
		return
	}
	c.downloaderLoop(ctx)
}

func (c *Client) shouldUseBundleChangeWatch(ctx context.Context) (bool, error) {
	if c == nil || c.config == nil || c.config.BundleChangeWatchMode != BundleChangeWatchAuto {
		return false, nil
	}
	if err := c.beginWatchSetupSyncOperation(ctx); err != nil {
		return false, err
	}
	connectErr := c.ensureConnectedSessionLocked(ctx, "Start() bundle change watch")
	c.writeMu.Unlock()
	if connectErr != nil {
		return false, connectErr
	}
	return c.serverSupportsBundleChangeWatch(ctx)
}

func (c *Client) beginWatchSetupSyncOperation(ctx context.Context) error {
	for {
		err := c.tryBeginSyncOperation()
		if err == nil {
			return nil
		}
		if !IsExpectedSyncContention(err) {
			return err
		}
		if err := sleepWatchInterval(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
}

func (c *Client) serverSupportsBundleChangeWatch(ctx context.Context) (bool, error) {
	if c == nil || c.discovery == nil {
		return false, fmt.Errorf("ready client has no admitted discovery contract")
	}
	return c.discovery.WatchEnabled, nil
}

func (c *Client) normalizedWatchFallbackInterval() time.Duration {
	if c != nil && c.config != nil {
		if c.config.WatchFallbackInterval > 0 {
			return c.config.WatchFallbackInterval
		}
		if c.config.BackoffMax > 0 {
			return c.config.BackoffMax
		}
	}
	return 60 * time.Second
}

func (c *Client) watchAwareDownloaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	if backoff <= 0 {
		backoff = time.Second
	}
	fallbackInterval := c.normalizedWatchFallbackInterval()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomicDownloadPaused(c) {
			if err := sleepWatchInterval(ctx, backoff); err != nil {
				return
			}
			continue
		}

		afterBundleSeq, err := c.LastBundleSeqSeen(ctx)
		if err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				if err := sleepWatchInterval(ctx, backoff); err != nil {
					return
				}
				continue
			}
			if err := sleepWatchInterval(ctx, backoff); err != nil {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		if err := c.watchRemoteChanges(ctx, afterBundleSeq, fallbackInterval); err != nil {
			var retry bool
			backoff, retry = c.recoverBundleChangeWatch(ctx, err, backoff)
			if !retry {
				return
			}
			continue
		}
		backoff = c.config.BackoffMin
	}
}

func atomicDownloadPaused(c *Client) bool {
	return c != nil && atomic.LoadInt32(&c.downloadPaused) == 1
}

type bundleChangeWatchPullError struct {
	err error
}

func (e *bundleChangeWatchPullError) Error() string {
	if e == nil || e.err == nil {
		return "bundle change watch pull failed"
	}
	return e.err.Error()
}

func (e *bundleChangeWatchPullError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

const (
	bundleChangeWatchMaxLineBytes  = 1 << 20
	bundleChangeWatchMaxEventBytes = 1 << 20
)

var (
	errBundleChangeWatchForbidden     = errors.New("bundle change watch forbidden")
	errBundleChangeWatchLineTooLarge  = errors.New("bundle change watch line exceeds byte limit")
	errBundleChangeWatchEventTooLarge = errors.New("bundle change watch event exceeds byte limit")
)

func isBundleChangeWatchForbidden(err error) bool {
	return errors.Is(err, errBundleChangeWatchForbidden)
}

func sleepWatchInterval(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) watchRemoteChanges(ctx context.Context, afterBundleSeq int64, fallbackInterval time.Duration) error {
	watchCtx, cancel := context.WithCancel(ctx)
	resp, err := c.openBundleChangeWatch(watchCtx, afterBundleSeq)
	if err != nil {
		cancel()
		return err
	}

	eventCh := make(chan oversync.BundleChangeEvent, 1)
	parserDone := make(chan struct{})
	var parserErr error
	go func() {
		defer close(parserDone)
		parserErr = parseBundleChangeWatchStream(watchCtx, resp.Body, eventCh)
		close(eventCh)
	}()
	defer func() {
		cancel()
		_ = resp.Body.Close()
		<-parserDone
	}()

	fallback := time.NewTicker(fallbackInterval)
	defer fallback.Stop()

	for {
		select {
		case <-watchCtx.Done():
			return watchCtx.Err()
		case event, ok := <-eventCh:
			if !ok {
				if parserErr != nil {
					return parserErr
				}
				return io.EOF
			}
			if event.BundleSeq <= 0 {
				return fmt.Errorf("bundle watch event missing bundle_seq")
			}
			if _, err := c.PullToStable(watchCtx); err != nil {
				return &bundleChangeWatchPullError{err: err}
			}
		case <-fallback.C:
			if _, err := c.PullToStable(watchCtx); err != nil {
				return &bundleChangeWatchPullError{err: err}
			}
		}
	}
}

func (c *Client) openBundleChangeWatch(ctx context.Context, afterBundleSeq int64) (*http.Response, error) {
	if afterBundleSeq < 0 {
		return nil, fmt.Errorf("after_bundle_seq must be >= 0")
	}
	sourceID, err := c.refreshCurrentSourceIDForWatch(ctx)
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/sync/watch?after_bundle_seq=" + strconv.FormatInt(afterBundleSeq, 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, err
	}
	if err := applyAuthenticatedSyncHeadersWithSourceID(req, token, sourceID, c.schemaGenerationProof); err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set(oversync.ProtocolVersionHeader, requiredProtocolVersion)

	resp, err := c.doHTTP(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, readErr := readDecodedBodyBounded(ctx, "bundle_change_watch", resp, snapshotControlBodyLimit)
		if readErr != nil {
			return nil, readErr
		}
		if generationErr := decodeSchemaGenerationRequestError(resp.StatusCode, body, c.schemaGenerationProof); generationErr != nil {
			return nil, generationErr
		}
		if resp.StatusCode == http.StatusForbidden {
			if errorResp, ok := decodeServerErrorResponse(body); ok && errorResp.Error == "bundle_change_watch_forbidden" {
				return nil, errBundleChangeWatchForbidden
			}
		}
		return nil, &retryHTTPError{
			Operation:  "bundle_change_watch",
			StatusCode: resp.StatusCode,
		}
	}
	return resp, nil
}

func (c *Client) refreshCurrentSourceIDForWatch(ctx context.Context) (string, error) {
	if c == nil {
		return "", nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		if validateErr := validateOptionalSourceID(c.sourceID); validateErr != nil {
			return "", validateErr
		}
		return c.sourceID, nil
	}
	defer c.writeMu.Unlock()
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return "", err
	}
	if attachment.CurrentSourceID != "" {
		c.sourceID = attachment.CurrentSourceID
	}
	if err := validateOptionalSourceID(c.sourceID); err != nil {
		return "", err
	}
	return c.sourceID, nil
}

func parseBundleChangeWatchStream(ctx context.Context, r io.Reader, events chan oversync.BundleChangeEvent) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024), bundleChangeWatchMaxLineBytes+2)

	var eventName string
	var data strings.Builder
	eventBytes := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > bundleChangeWatchMaxLineBytes {
			return errBundleChangeWatchLineTooLarge
		}
		if line == "" {
			if eventName == "bundle" {
				event, err := decodeBundleChangeEvent([]byte(data.String()))
				if err != nil {
					return fmt.Errorf("decode bundle watch event: %w", err)
				}
				select {
				case events <- event:
				default:
					select {
					case <-events:
					default:
					}
					select {
					case events <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			eventName = ""
			data.Reset()
			eventBytes = 0
			continue
		}
		lineBytes := len(line) + 1
		if lineBytes > bundleChangeWatchMaxEventBytes-eventBytes {
			return errBundleChangeWatchEventTooLarge
		}
		eventBytes += lineBytes
		if strings.HasPrefix(line, ":") {
			continue
		}
		if value, ok := strings.CutPrefix(line, "event:"); ok {
			eventName = strings.TrimPrefix(value, " ")
			continue
		}
		if value, ok := strings.CutPrefix(line, "data:"); ok {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(value, " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func decodeBundleChangeEvent(data []byte) (oversync.BundleChangeEvent, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return oversync.BundleChangeEvent{}, err
	}
	value, present := fields["bundle_seq"]
	if !present || len(fields) != 1 {
		return oversync.BundleChangeEvent{}, fmt.Errorf("bundle event must contain exactly bundle_seq")
	}
	var bundleSeq int64
	if err := json.Unmarshal(value, &bundleSeq); err != nil || bundleSeq <= 0 {
		return oversync.BundleChangeEvent{}, fmt.Errorf("bundle_seq must be a positive integer")
	}
	return oversync.BundleChangeEvent{BundleSeq: bundleSeq}, nil
}

// recoverBundleChangeWatch owns reconnect/fallback policy after a stream ends.
// A retired proof stops before fallback or backoff; ordinary transient behavior
// retains the existing lifecycle and protocol-error precedence.
func (c *Client) recoverBundleChangeWatch(ctx context.Context, err error, backoff time.Duration) (time.Duration, bool) {

	if ctx.Err() != nil {
		return backoff, false
	}
	if c.stopForRetiredGeneration(err) {
		return backoff, false
	}
	if isProtocolVersionMismatch(err) {
		c.logger.Error("bundle change watch loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
		return backoff, false
	}
	if isTerminalProtocolRepresentationError(err) {
		c.logger.Error("bundle change watch stopped by terminal protocol representation error", "error_type", fmt.Sprintf("%T", err))
		return backoff, false
	}
	if isBundleChangeWatchForbidden(err) {
		c.logger.Warn("bundle change watch disabled for client", "error", err)
		c.downloaderLoop(ctx)
		return backoff, false
	}
	c.logger.Warn("bundle change watch stream stopped", "error_type", fmt.Sprintf("%T", err))
	backoffErr := err
	var streamPullErr *bundleChangeWatchPullError
	if !errors.As(err, &streamPullErr) {
		if _, pullErr := c.PullToStable(ctx); pullErr != nil {
			if ctx.Err() != nil {
				return backoff, false
			}
			if c.stopForRetiredGeneration(pullErr) {
				return backoff, false
			}
			if isProtocolVersionMismatch(pullErr) {
				c.logger.Error("bundle change watch loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", pullErr))
				return backoff, false
			}
			if isTerminalProtocolRepresentationError(pullErr) {
				c.logger.Error("bundle change watch fallback stopped by terminal protocol representation error", "error_type", fmt.Sprintf("%T", pullErr))
				return backoff, false
			}
			backoffErr = pullErr
			if IsLifecyclePreconditionError(pullErr) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", pullErr)
			} else {
				c.logger.Warn("bundle change watch fallback pull failed", "error", pullErr)
			}
		}
	}
	if err := sleepWatchInterval(ctx, backoff); err != nil {
		return backoff, false
	}
	return nextSyncLoopBackoffAfterError(backoffErr, backoff, c.config.BackoffMin, c.config.BackoffMax), true
}
