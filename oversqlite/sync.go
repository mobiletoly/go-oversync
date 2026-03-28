// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

type preparedUploadChange struct {
	Table        string
	Op           string
	LocalPK      string
	LocalPayload sql.NullString
}

func nextSyncLoopBackoffAfterError(err error, current, min, max time.Duration) time.Duration {
	if IsLifecyclePreconditionError(err) {
		return min
	}
	next := current * 2
	if next < min {
		return min
	}
	if next > max {
		return max
	}
	return next
}

// uploaderLoop runs the push loop with backoff.
func (c *Client) uploaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomic.LoadInt32(&c.uploadPaused) == 1 {
			time.Sleep(backoff)
			continue
		}

		if err := c.PushPending(ctx); err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("upload loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				time.Sleep(backoff)
				continue
			}
			time.Sleep(backoff)
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		backoff = c.config.BackoffMin
		time.Sleep(backoff)
	}
}

// downloaderLoop runs the pull loop with backoff.
func (c *Client) downloaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomic.LoadInt32(&c.downloadPaused) == 1 {
			continue
		}

		before, err := c.LastBundleSeqSeen(ctx)
		if err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				time.Sleep(backoff)
				continue
			}
			time.Sleep(backoff)
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		if err := c.PullToStable(ctx); err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				time.Sleep(backoff)
				continue
			}
			time.Sleep(backoff)
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		backoff = c.config.BackoffMin
		after, seqErr := c.LastBundleSeqSeen(ctx)
		if seqErr != nil || after == before {
			time.Sleep(backoff)
		}
	}
}

func (c *Client) serializeExistingRowInTx(ctx context.Context, tx *sql.Tx, tableName, pk string) (json.RawMessage, bool, error) {
	payload, err := c.serializeRowInTx(ctx, tx, tableName, pk)
	if err != nil {
		if strings.Contains(err.Error(), "row not found") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to serialize current row for %s.%s: %w", tableName, pk, err)
	}
	return payload, true, nil
}

func liveMatchesUploadedIntent(change preparedUploadChange, livePayload json.RawMessage, liveExists bool) (bool, error) {
	if change.Op == oversync.OpDelete {
		return !liveExists, nil
	}
	if !liveExists {
		return false, nil
	}
	return canonicalPayloadEqual(
		sql.NullString{String: string(livePayload), Valid: true},
		change.LocalPayload,
	)
}

func reconcileAppliedUploadState(change preparedUploadChange, pendingExists bool, pendingMatches bool, livePayload json.RawMessage, liveExists bool, liveMatches bool) (string, sql.NullString, bool, bool) {
	switch change.Op {
	case oversync.OpDelete:
		if liveExists {
			return oversync.OpInsert, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		return "", sql.NullString{}, true, false
	default:
		if !liveExists {
			return oversync.OpDelete, sql.NullString{}, true, true
		}
		if pendingExists && !pendingMatches {
			return oversync.OpUpdate, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		if !liveMatches {
			return oversync.OpUpdate, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		return "", sql.NullString{}, false, false
	}
}

func canonicalPayloadEqual(left sql.NullString, right sql.NullString) (bool, error) {
	if left.Valid != right.Valid {
		return false, nil
	}
	if !left.Valid {
		return true, nil
	}
	leftNorm, err := canonicalizeJSON(left.String)
	if err != nil {
		return false, err
	}
	rightNorm, err := canonicalizeJSON(right.String)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftNorm, rightNorm), nil
}

func canonicalizeJSON(raw string) ([]byte, error) {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("failed to normalize JSON payload: %w", err)
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal normalized JSON payload: %w", err)
	}
	return normalized, nil
}

func nullStringValue(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

// processPayloadForUpload converts locally stored JSON payloads into the wire format expected by
// bundle-shaped push requests. BLOB values are converted from hex to base64 to match JSON transport.
func (c *Client) processPayloadForUpload(tableName, payloadStr string) (json.RawMessage, error) {
	var payloadData map[string]any
	if err := json.Unmarshal([]byte(payloadStr), &payloadData); err != nil {
		return nil, fmt.Errorf("failed to parse payload JSON: %w", err)
	}

	tableInfo, err := c.getTableInfo(strings.ToLower(tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	pkColLower := ""
	if tableInfo.PrimaryKey != nil {
		pkColLower = strings.ToLower(tableInfo.PrimaryKey.Name)
	}
	for _, col := range tableInfo.Columns {
		if !col.IsBlob() {
			continue
		}
		colNameLower := strings.ToLower(col.Name)
		hexValue, ok := payloadData[colNameLower].(string)
		if !ok || hexValue == "" {
			continue
		}

		if tableInfo.PrimaryKeyIsBlob && pkColLower != "" && colNameLower == pkColLower {
			uuidBytes, err := decodeUUIDBytesFromString(hexValue)
			if err != nil {
				return nil, fmt.Errorf("failed to decode blob UUID pk for column %s: %w", col.Name, err)
			}
			id, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID bytes for column %s: %w", col.Name, err)
			}
			payloadData[colNameLower] = id.String()
			continue
		}
		if tableInfo.IsBlobReferenceColumn(col.Name) {
			uuidBytes, err := decodeUUIDBytesFromString(hexValue)
			if err != nil {
				return nil, fmt.Errorf("failed to decode blob UUID reference for column %s: %w", col.Name, err)
			}
			id, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID bytes for reference column %s: %w", col.Name, err)
			}
			payloadData[colNameLower] = id.String()
			continue
		}

		hexBytes, err := hex.DecodeString(hexValue)
		if err != nil {
			return nil, fmt.Errorf("failed to decode hex for column %s: %w", col.Name, err)
		}
		payloadData[colNameLower] = base64.StdEncoding.EncodeToString(hexBytes)
	}

	processedBytes, err := json.Marshal(payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal processed payload: %w", err)
	}
	return processedBytes, nil
}
