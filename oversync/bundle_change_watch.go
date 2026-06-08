// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// BundleChangeEvent is a metadata-only wake-up hint for a newer committed bundle.
type BundleChangeEvent struct {
	BundleSeq      int64  `json:"bundle_seq"`
	SourceID       string `json:"source_id,omitempty"`
	SourceBundleID int64  `json:"source_bundle_id,omitempty"`
}

var errBundleChangeWatchDisabled = errors.New("bundle change watch is disabled")

type bundleChangeNotification struct {
	UserPK         int64  `json:"user_pk"`
	BundleSeq      int64  `json:"bundle_seq"`
	SourceID       string `json:"source_id,omitempty"`
	SourceBundleID int64  `json:"source_bundle_id,omitempty"`
}

type bundleChangeHub struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[int64]map[uint64]*bundleChangeSubscriber
}

type bundleChangeSubscriber struct {
	id      uint64
	userPK  int64
	ch      chan BundleChangeEvent
	lastSeq int64
}

func newBundleChangeHub() *bundleChangeHub {
	return &bundleChangeHub{
		subscribers: make(map[int64]map[uint64]*bundleChangeSubscriber),
	}
}

func quotePostgresNotificationChannel(channel string) (string, error) {
	if channel == "" {
		return "", fmt.Errorf("bundle change watch notify channel is required")
	}
	if strings.ContainsRune(channel, '\x00') {
		return "", fmt.Errorf("bundle change watch notify channel cannot contain NUL")
	}
	if len(channel) > 63 {
		return "", fmt.Errorf("bundle change watch notify channel must be 63 bytes or shorter")
	}
	return pgx.Identifier{channel}.Sanitize(), nil
}

func (h *bundleChangeHub) subscribe(ctx context.Context, userPK int64, afterBundleSeq int64) (<-chan BundleChangeEvent, func()) {
	if h == nil {
		h = newBundleChangeHub()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sub := &bundleChangeSubscriber{
		userPK:  userPK,
		ch:      make(chan BundleChangeEvent, 1),
		lastSeq: afterBundleSeq,
	}

	h.mu.Lock()
	h.nextID++
	sub.id = h.nextID
	byUser := h.subscribers[userPK]
	if byUser == nil {
		byUser = make(map[uint64]*bundleChangeSubscriber)
		h.subscribers[userPK] = byUser
	}
	byUser[sub.id] = sub
	h.mu.Unlock()

	var once sync.Once
	done := make(chan struct{})
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			defer close(done)
			byUser := h.subscribers[userPK]
			if byUser == nil {
				return
			}
			if _, ok := byUser[sub.id]; !ok {
				return
			}
			delete(byUser, sub.id)
			if len(byUser) == 0 {
				delete(h.subscribers, userPK)
			}
			close(sub.ch)
		})
	}

	go func() {
		select {
		case <-ctx.Done():
			unsubscribe()
		case <-done:
		}
	}()

	return sub.ch, unsubscribe
}

func (h *bundleChangeHub) publish(userPK int64, event BundleChangeEvent) {
	if h == nil || event.BundleSeq <= 0 {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range h.subscribers[userPK] {
		if event.BundleSeq <= sub.lastSeq {
			continue
		}
		select {
		case sub.ch <- event:
			sub.lastSeq = event.BundleSeq
		default:
			select {
			case <-sub.ch:
			default:
			}
			select {
			case sub.ch <- event:
				sub.lastSeq = event.BundleSeq
			default:
			}
		}
	}
}

func (h *bundleChangeHub) subscriberCount(userPK int64) int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subscribers[userPK])
}

func (h *bundleChangeHub) activeUserPKs() []int64 {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	userPKs := make([]int64, 0, len(h.subscribers))
	for userPK := range h.subscribers {
		userPKs = append(userPKs, userPK)
	}
	return userPKs
}

func (s *SyncService) ensureBundleChangeHub() *bundleChangeHub {
	if s == nil {
		return newBundleChangeHub()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundleChangeHub == nil {
		s.bundleChangeHub = newBundleChangeHub()
	}
	return s.bundleChangeHub
}

func (s *SyncService) emitBundleChangeNotify(ctx context.Context, tx pgx.Tx, userPK int64, event BundleChangeEvent) error {
	if !s.bundleChangeWatchEnabled() {
		return nil
	}
	cfg := s.effectiveBundleChangeWatchConfig()
	payload, err := json.Marshal(bundleChangeNotification{
		UserPK:         userPK,
		BundleSeq:      event.BundleSeq,
		SourceID:       event.SourceID,
		SourceBundleID: event.SourceBundleID,
	})
	if err != nil {
		return fmt.Errorf("marshal bundle change notification: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, cfg.NotifyChannel, string(payload)); err != nil {
		return fmt.Errorf("emit bundle change notification: %w", err)
	}
	return nil
}

// RunBundleChangeListener listens for committed bundle notifications and fans them out locally.
func (s *SyncService) RunBundleChangeListener(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.bundleChangeWatchEnabled() {
		return errBundleChangeWatchDisabled
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("bundle change listener requires a database pool")
	}

	listenerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.serviceClosedChannel():
			cancel()
		case <-listenerCtx.Done():
		}
	}()

	backoff := 100 * time.Millisecond
	for {
		if err := s.runBundleChangeListenerSession(listenerCtx); err != nil {
			if listenerCtx.Err() != nil {
				return listenerCtx.Err()
			}
			s.logger.Warn("Bundle change listener session failed", "error", err)
			if err := sleepWithContext(listenerCtx, backoff); err != nil {
				return err
			}
			backoff = nextBundleChangeListenerBackoff(backoff)
			continue
		}
		return nil
	}
}

func nextBundleChangeListenerBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return 100 * time.Millisecond
	}
	next := current * 2
	if next > 2*time.Second {
		return 2 * time.Second
	}
	return next
}

func (s *SyncService) runBundleChangeListenerSession(ctx context.Context) error {
	cfg := s.effectiveBundleChangeWatchConfig()
	quotedChannel, err := quotePostgresNotificationChannel(cfg.NotifyChannel)
	if err != nil {
		return err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire bundle change listener connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+quotedChannel); err != nil {
		return fmt.Errorf("listen for bundle changes: %w", err)
	}
	if err := s.catchUpActiveBundleChangeSubscribers(ctx); err != nil {
		return err
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wait for bundle change notification: %w", err)
		}
		if notification == nil {
			continue
		}
		if notification.Channel != cfg.NotifyChannel {
			continue
		}
		if err := s.publishBundleChangeNotification(notification.Payload); err != nil {
			s.logger.Warn("Ignored invalid bundle change notification", "error", err)
		}
	}
}

func (s *SyncService) publishBundleChangeNotification(payload string) error {
	var notification bundleChangeNotification
	if err := json.Unmarshal([]byte(payload), &notification); err != nil {
		return fmt.Errorf("decode bundle change notification: %w", err)
	}
	if notification.UserPK <= 0 || notification.BundleSeq <= 0 {
		return fmt.Errorf("bundle change notification requires positive user_pk and bundle_seq")
	}
	s.ensureBundleChangeHub().publish(notification.UserPK, BundleChangeEvent{
		BundleSeq:      notification.BundleSeq,
		SourceID:       notification.SourceID,
		SourceBundleID: notification.SourceBundleID,
	})
	return nil
}

func (s *SyncService) catchUpActiveBundleChangeSubscribers(ctx context.Context) error {
	hub := s.ensureBundleChangeHub()
	userPKs := hub.activeUserPKs()
	if len(userPKs) == 0 {
		return nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT user_pk, next_bundle_seq - 1
		FROM sync.user_state
		WHERE user_pk = ANY($1::bigint[])
	`, userPKs)
	if err != nil {
		return fmt.Errorf("catch up bundle change subscribers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userPK, latestBundleSeq int64
		if err := rows.Scan(&userPK, &latestBundleSeq); err != nil {
			return fmt.Errorf("scan bundle change catch-up row: %w", err)
		}
		hub.publish(userPK, BundleChangeEvent{BundleSeq: latestBundleSeq})
	}
	if rows.Err() != nil {
		return fmt.Errorf("iterate bundle change catch-up rows: %w", rows.Err())
	}
	return nil
}

// SubscribeBundleChanges subscribes to metadata-only wakeups for one initialized actor scope.
func (s *SyncService) SubscribeBundleChanges(
	ctx context.Context,
	actor Actor,
	afterBundleSeq int64,
) (<-chan BundleChangeEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.bundleChangeWatchEnabled() {
		return nil, errBundleChangeWatchDisabled
	}
	if err := actor.validate(false); err != nil {
		return nil, err
	}
	if afterBundleSeq < 0 {
		return nil, fmt.Errorf("after_bundle_seq must be >= 0")
	}

	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()

	var userPK int64
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := requireScopeInitializedQuerier(ctx, tx, actor.UserID); err != nil {
			return err
		}
		var txErr error
		userPK, txErr = lookupUserPK(ctx, tx, actor.UserID)
		return txErr
	})
	if err != nil {
		return nil, err
	}

	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.serviceClosedChannel():
			cancel()
		case <-subCtx.Done():
		}
	}()
	ch, unsubscribe := s.ensureBundleChangeHub().subscribe(subCtx, userPK, afterBundleSeq)
	latestBundleSeq, err := latestBundleSeqForUserPK(ctx, s.pool, userPK)
	if err != nil {
		cancel()
		unsubscribe()
		return nil, err
	}
	if latestBundleSeq > afterBundleSeq {
		s.ensureBundleChangeHub().publish(userPK, BundleChangeEvent{BundleSeq: latestBundleSeq})
	}

	return ch, nil
}

func latestBundleSeqForUserPK(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userPK int64) (int64, error) {
	var latestBundleSeq int64
	if err := q.QueryRow(ctx, `
		SELECT next_bundle_seq - 1
		FROM sync.user_state
		WHERE user_pk = $1
	`, userPK).Scan(&latestBundleSeq); err != nil {
		return 0, fmt.Errorf("query latest bundle seq for user_pk %d: %w", userPK, err)
	}
	return latestBundleSeq, nil
}

func (s *SyncService) bundleChangeSubscriberCount(userPK int64) int {
	return s.ensureBundleChangeHub().subscriberCount(userPK)
}

// BundleChangeSubscriberCountForTest returns the current process-local subscriber count for a
// scope. It is intended for example/test diagnostics; production correctness must not depend on it.
func (s *SyncService) BundleChangeSubscriberCountForTest(ctx context.Context, userID string) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return 0, fmt.Errorf("user_id is required")
	}
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("sync service is not initialized")
	}
	var userPK int64
	if err := s.pool.QueryRow(ctx, `
		SELECT user_pk
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&userPK); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("lookup user_pk for %s: %w", userID, err)
	}
	return s.bundleChangeSubscriberCount(userPK), nil
}
