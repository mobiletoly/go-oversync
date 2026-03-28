// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type userStateExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func ensureUserStateExistsWithExec(ctx context.Context, exec userStateExecer, userID string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.user_state (user_id, updated_at)
		VALUES ($1, now())
		ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return fmt.Errorf("ensure user_state row: %w", err)
	}
	return nil
}

func (s *SyncService) ensureUserStateExists(ctx context.Context, userID string) error {
	return ensureUserStateExistsWithExec(ctx, s.pool, userID)
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
