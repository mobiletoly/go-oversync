// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type userStateExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type userStateQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func ensureUserStateExistsWithExec(ctx context.Context, exec userStateExecer, userID string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.user_state (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return fmt.Errorf("ensure user_state row: %w", err)
	}
	return nil
}

func (s *SyncService) ensureUserStateExists(ctx context.Context, userID string) error {
	return ensureUserStateExistsWithExec(ctx, s.pool, userID)
}

func lookupUserPK(ctx context.Context, q userStateQuerier, userID string) (int64, error) {
	var userPK int64
	if err := q.QueryRow(ctx, `
		SELECT user_pk
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&userPK); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("missing user_state row for %q", userID)
		}
		return 0, fmt.Errorf("lookup user_pk for %q: %w", userID, err)
	}
	return userPK, nil
}

type bundleTxContext struct {
	UserID           string
	UserPK           int64
	SourceID         string
	SourceBundleID   int64
	InitializationID string
}

func setBundleTxContext(ctx context.Context, tx pgx.Tx, bundle bundleTxContext) error {
	settings := []struct {
		name  string
		value string
	}{
		{name: "oversync.bundle_user_id", value: bundle.UserID},
		{name: "oversync.bundle_user_pk", value: strconv.FormatInt(bundle.UserPK, 10)},
		{name: "oversync.bundle_source_id", value: bundle.SourceID},
		{name: "oversync.bundle_source_bundle_id", value: strconv.FormatInt(bundle.SourceBundleID, 10)},
		{name: "oversync.bundle_initialization_id", value: bundle.InitializationID},
	}
	for _, setting := range settings {
		var existing string
		if err := tx.QueryRow(ctx, `SELECT COALESCE(current_setting($1, true), '')`, setting.name).Scan(&existing); err != nil {
			return fmt.Errorf("read existing bundle tx setting %s: %w", setting.name, err)
		}
		if existing != "" && existing != setting.value {
			return fmt.Errorf("transaction already carries conflicting sync bundle context for %s", setting.name)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config($1, $2, true)`, setting.name, setting.value); err != nil {
			return fmt.Errorf("set bundle tx setting %s: %w", setting.name, err)
		}
	}
	return nil
}

// acquireUserUploadConn pins push processing for one user to one connection and acquires
// a session-level advisory lock before the bundle transaction begins.
func (s *SyncService) acquireUserUploadConn(ctx context.Context, userID string) (*pgxpool.Conn, func(), error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire push connection: %w", err)
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, userID); err != nil {
		conn.Release()
		return nil, nil, fmt.Errorf("acquire user advisory lock: %w", err)
	}

	release := func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, userID); err != nil {
			s.logger.Error("Failed to release user advisory lock", "user_id", userID, "error", err)
		}
		conn.Release()
	}
	return conn, release, nil
}

func ensureUserStatePresent(ctx context.Context, tx pgx.Tx, userID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&exists); err != nil {
		return fmt.Errorf("check user_state row: %w", err)
	}
	if !exists {
		return fmt.Errorf("missing user_state row for %q", userID)
	}
	return nil
}

func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeCanonicalJSON(&buf, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonicalJSON(buf *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		enc, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(enc)
	case float64:
		enc, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(enc)
	case []any:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			encKey, err := json.Marshal(key)
			if err != nil {
				return err
			}
			buf.Write(encKey)
			buf.WriteByte(':')
			if err := writeCanonicalJSON(buf, v[key]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		enc, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(enc)
	}
	return nil
}
