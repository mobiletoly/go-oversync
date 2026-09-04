// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) createSnapshotSession(ctx context.Context, req *oversync.SnapshotSessionCreateRequest) (*oversync.SnapshotSession, error) {
	responseSourceID := c.sourceID
	if req != nil && req.SourceReplacement != nil {
		responseSourceID = req.SourceReplacement.NewSourceID
	}
	var reqBody []byte
	var err error
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal snapshot session request: %w", err)
		}
		// The portable wire contract fixes the complete request bytes, including
		// the JSON record terminator.
		reqBody = append(reqBody, '\n')
	}
	contentType := ""
	if len(reqBody) > 0 {
		contentType = "application/json"
	}
	result, err := c.doAuthenticatedBoundedRequestWithRetry(
		ctx, "snapshot_session_create", http.MethodPost, buildSnapshotSessionCreateURL(c.BaseURL),
		reqBody, contentType, snapshotControlBodyLimit, snapshotControlBodyLimit,
		map[string]struct{}{"snapshot_build_capacity": {}},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send snapshot session request: %w", err)
	}
	if result.statusCode != http.StatusOK {
		if generationErr := decodeSchemaGenerationRequestError(result.statusCode, result.body, c.schemaGenerationProof); generationErr != nil {
			return nil, generationErr
		}
		if result.statusCode == http.StatusConflict {
			if limitErr := decodeSnapshotSessionLimitExceededError(result.body); limitErr != nil {
				return nil, limitErr
			}
			if retiredResp, ok := decodeSourceRetiredResponse(result.body, c.sourceID); ok {
				if req != nil && req.SourceReplacement != nil {
					if retiredResp.ReplacedBySourceID != "" && retiredResp.ReplacedBySourceID != req.SourceReplacement.NewSourceID {
						return nil, &SourceReplacementDivergedError{}
					}
				}
				return nil, &SourceRecoveryRequiredError{Code: SourceRecoveryRetired}
			}
			if errorResp, ok := decodeServerErrorResponse(result.body); ok && errorResp.Error == "source_retired" {
				return nil, errors.New("invalid source_retired response")
			}
		}
		return nil, fmt.Errorf("server returned status %d: %s", result.statusCode, decodeServerErrorBody(result.body))
	}

	var wire struct {
		SnapshotID        string `json:"snapshot_id"`
		SnapshotBundleSeq int64  `json:"snapshot_bundle_seq"`
		RowCount          int64  `json:"row_count"`
		LogicalByteCount  *int64 `json:"logical_byte_count"`
		ExpiresAt         string `json:"expires_at"`
	}
	if err := json.Unmarshal(result.body, &wire); err != nil {
		if strings.TrimSpace(wire.SnapshotID) != "" {
			c.deleteSnapshotSessionBestEffortAsSource(wire.SnapshotID, responseSourceID)
		}
		return nil, fmt.Errorf("failed to decode snapshot session response: %w", err)
	}
	if err := validateSnapshotSessionResponseFieldPresence(result.body); err != nil {
		if strings.TrimSpace(wire.SnapshotID) != "" {
			c.deleteSnapshotSessionBestEffortAsSource(wire.SnapshotID, responseSourceID)
		}
		return nil, err
	}
	if wire.LogicalByteCount == nil {
		if strings.TrimSpace(wire.SnapshotID) != "" {
			c.deleteSnapshotSessionBestEffortAsSource(wire.SnapshotID, responseSourceID)
		}
		return nil, fmt.Errorf("snapshot session response missing required logical_byte_count")
	}
	session := oversync.SnapshotSession{
		SnapshotID: wire.SnapshotID, SnapshotBundleSeq: wire.SnapshotBundleSeq,
		RowCount: wire.RowCount, ByteCount: *wire.LogicalByteCount, ExpiresAt: wire.ExpiresAt,
	}
	if err := validateSnapshotSession(&session); err != nil {
		c.deleteSnapshotSessionBestEffortAsSource(session.SnapshotID, responseSourceID)
		return nil, err
	}
	atomic.AddInt64(&c.snapshotStats.sessionsCreated, 1)
	return &session, nil
}

// SnapshotChunkTooSmallError reports a server row that cannot fit the explicit
// client budget. The budget is never increased implicitly.
type SnapshotChunkTooSmallError struct {
	ConfiguredBytes int64
	RequiredBytes   int64
}

type snapshotSessionResponseError struct {
	StatusCode int
	Code       string
}

func (e *snapshotSessionResponseError) Error() string {
	if e == nil {
		return "snapshot session request failed"
	}
	return fmt.Sprintf("server returned status %d: %s", e.StatusCode, e.Code)
}

func (e *SnapshotChunkTooSmallError) Error() string {
	return fmt.Sprintf("snapshot chunk byte budget %d is too small for the next row requiring %d bytes; increase Config.SnapshotChunkBytes", e.ConfiguredBytes, e.RequiredBytes)
}

func (c *Client) fetchSnapshotChunk(ctx context.Context, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64, maxRows int, maxBytes int64) (*oversync.SnapshotChunkResponse, int64, error) {
	return c.fetchSnapshotChunkAsSource(ctx, snapshotID, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes, c.sourceID)
}

func (c *Client) fetchSnapshotChunkAsSource(ctx context.Context, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64, maxRows int, maxBytes int64, sourceID string) (*oversync.SnapshotChunkResponse, int64, error) {
	bodyLimit, err := checkedSnapshotChunkBodyLimit(maxBytes, maxRows)
	if err != nil {
		return nil, 0, err
	}
	result, err := c.doAuthenticatedBoundedRequestWithRetryAsSource(
		ctx, "snapshot_chunk_fetch", http.MethodGet,
		buildSnapshotChunkURL(c.BaseURL, snapshotID, afterRowOrdinal, maxRows, maxBytes),
		nil, "", bodyLimit, snapshotControlBodyLimit,
		sourceID, map[string]struct{}{"snapshot_chunk_capacity": {}},
		func(decodedBodyBytes int64) {
			atomicRecordMax(&c.snapshotStats.maxChunkDecodedBodyBytes, decodedBodyBytes)
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to send snapshot chunk request: %w", err)
	}
	if result.statusCode != http.StatusOK {
		if generationErr := decodeSchemaGenerationRequestError(result.statusCode, result.body, c.schemaGenerationProof); generationErr != nil {
			return nil, 0, generationErr
		}
		if errorResp, ok := decodeServerErrorResponse(result.body); ok && errorResp.Error == "snapshot_chunk_too_small" {
			if errorResp.RequiredDecodedBodyBytes <= maxBytes {
				return nil, 0, fmt.Errorf("snapshot_chunk_too_small response has invalid required_decoded_body_bytes %d for requested max_bytes %d", errorResp.RequiredDecodedBodyBytes, maxBytes)
			}
			return nil, 0, &SnapshotChunkTooSmallError{ConfiguredBytes: maxBytes, RequiredBytes: errorResp.RequiredDecodedBodyBytes}
		}
		return nil, 0, &snapshotSessionResponseError{StatusCode: result.statusCode, Code: decodeServerErrorBody(result.body)}
	}
	var wire struct {
		Rows             []oversync.CompactRowGroup `json:"rows"`
		NextRowOrdinal   int64                      `json:"next_row_ordinal"`
		HasMore          bool                       `json:"has_more"`
		LogicalByteCount *int64                     `json:"logical_byte_count"`
	}
	if err := json.Unmarshal(result.body, &wire); err != nil {
		return nil, 0, fmt.Errorf("failed to decode snapshot chunk response: %w", err)
	}
	if err := validateSnapshotChunkResponseFieldPresence(result.body); err != nil {
		return nil, 0, err
	}
	if wire.LogicalByteCount == nil {
		return nil, 0, fmt.Errorf("snapshot chunk response missing required logical_byte_count")
	}
	if c.discovery == nil {
		return nil, 0, fmt.Errorf("snapshot chunk requires an admitted discovery contract")
	}
	groups := append([]oversync.CompactRowGroup(nil), wire.Rows...)
	if len(groups) == 0 {
		chunk := oversync.SnapshotChunkResponse{
			SnapshotID: snapshotID, SnapshotBundleSeq: snapshotBundleSeq,
			Rows: []oversync.SnapshotRow{}, NextRowOrdinal: wire.NextRowOrdinal, HasMore: wire.HasMore,
			ByteCount: *wire.LogicalByteCount,
		}
		if err := validateSnapshotChunkResponseWithExpectedByteCount(&chunk, snapshotID, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes, wire.LogicalByteCount); err != nil {
			return nil, 0, err
		}
		return &chunk, result.decodedBodyBytes, nil
	}
	firstOrdinal := groups[0].FirstRowOrdinal
	for index := range groups {
		groups[index].FirstRowOrdinal -= firstOrdinal
	}
	rows, err := oversync.ExpandCompactPushRows(c.discovery.SchemaGeneration, groups)
	if err != nil {
		return nil, 0, fmt.Errorf("expand compact snapshot rows: %w", err)
	}
	if err := c.validateAdmittedResponseRows("snapshot response", rows); err != nil {
		return nil, 0, err
	}
	snapshotRows := make([]oversync.SnapshotRow, 0, len(rows))
	for _, row := range rows {
		snapshotRows = append(snapshotRows, oversync.SnapshotRow{Schema: row.Schema, Table: row.Table, Key: row.Key, RowVersion: row.BaseRowVersion, Payload: row.Payload})
	}
	_, logicalByteCount, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, snapshotRows, firstOrdinal)
	if err != nil {
		return nil, 0, fmt.Errorf("encode compact snapshot rows: %w", err)
	}
	if logicalByteCount != *wire.LogicalByteCount {
		return nil, 0, fmt.Errorf("snapshot chunk logical_byte_count %d does not match computed %d", *wire.LogicalByteCount, logicalByteCount)
	}
	chunk := oversync.SnapshotChunkResponse{
		SnapshotID: snapshotID, SnapshotBundleSeq: snapshotBundleSeq,
		Rows: snapshotRows, NextRowOrdinal: wire.NextRowOrdinal, HasMore: wire.HasMore,
		ByteCount: *wire.LogicalByteCount,
	}
	if err := validateSnapshotChunkResponseWithExpectedByteCount(&chunk, snapshotID, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes, wire.LogicalByteCount); err != nil {
		return nil, 0, err
	}
	atomic.AddInt64(&c.snapshotStats.chunksFetched, 1)
	atomicRecordMax(&c.snapshotStats.maxChunkRows, int64(len(chunk.Rows)))
	atomicRecordMax(&c.snapshotStats.maxChunkWireBytes, chunk.ByteCount)
	return &chunk, result.decodedBodyBytes, nil
}

func (c *Client) deleteSnapshotSessionBestEffort(snapshotID string) {
	c.deleteSnapshotSessionBestEffortAsSource(snapshotID, c.sourceID)
}

func (c *Client) deleteSnapshotSessionBestEffortAsSource(snapshotID string, sourceID string) {
	if strings.TrimSpace(snapshotID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotRetirementTimeout)
	defer cancel()

	_ = c.deleteSnapshotSessionAsSource(ctx, snapshotID, sourceID)
}

func (c *Client) deleteSnapshotSessionAsSource(ctx context.Context, snapshotID string, sourceID string) error {
	statusCode, body, err := c.doAuthenticatedSessionDeleteAsSource(
		ctx, "snapshot_session_delete", buildSnapshotSessionDeleteURL(c.BaseURL, snapshotID), sourceID,
	)
	if err != nil {
		return err
	}
	if statusCode == http.StatusNoContent {
		return nil
	}
	if generationErr := decodeSchemaGenerationRequestError(statusCode, body, c.schemaGenerationProof); generationErr != nil {
		return generationErr
	}
	if response, ok := decodeServerErrorResponse(body); ok {
		if (statusCode == http.StatusNotFound && response.Error == "snapshot_session_not_found") ||
			(statusCode == http.StatusGone && response.Error == "snapshot_session_expired") {
			return nil
		}
	}
	return &retryHTTPError{Operation: "snapshot_session_delete", StatusCode: statusCode}
}

type snapshotTransferTotals struct {
	rows  int64
	bytes int64
}

func (c *Client) downloadSnapshotSession(ctx context.Context, session *oversync.SnapshotSession, limits snapshotNegotiation) (snapshotTransferTotals, error) {
	return c.downloadSnapshotSessionAsSource(ctx, session, limits, c.sourceID)
}

func (c *Client) downloadSnapshotSessionAsSource(ctx context.Context, session *oversync.SnapshotSession, limits snapshotNegotiation, sourceID string) (snapshotTransferTotals, error) {
	var totals snapshotTransferTotals
	afterRowOrdinal := int64(0)
	for {
		chunk, _, err := c.fetchSnapshotChunkAsSource(ctx, session.SnapshotID, session.SnapshotBundleSeq, afterRowOrdinal, limits.maxRows, limits.maxBytes, sourceID)
		if err != nil {
			return snapshotTransferTotals{}, err
		}
		if err := c.stageSnapshotChunk(ctx, chunk, afterRowOrdinal); err != nil {
			return snapshotTransferTotals{}, err
		}
		totals.rows, err = checkedAddInt64(totals.rows, int64(len(chunk.Rows)))
		if err != nil {
			return snapshotTransferTotals{}, fmt.Errorf("snapshot accumulated row count overflow: %w", err)
		}
		totals.bytes, err = checkedAddInt64(totals.bytes, chunk.ByteCount)
		if err != nil {
			return snapshotTransferTotals{}, fmt.Errorf("snapshot accumulated byte count overflow: %w", err)
		}
		if totals.rows > session.RowCount || totals.bytes > session.ByteCount {
			return snapshotTransferTotals{}, fmt.Errorf("snapshot chunk totals exceed declared session totals")
		}
		if chunk.HasMore {
			if totals.rows == session.RowCount || totals.bytes == session.ByteCount {
				return snapshotTransferTotals{}, fmt.Errorf("snapshot chunk reports has_more after reaching a declared session total")
			}
			afterRowOrdinal = chunk.NextRowOrdinal
			continue
		}
		if totals.rows != session.RowCount {
			return snapshotTransferTotals{}, fmt.Errorf("snapshot final row total %d does not match session row_count %d", totals.rows, session.RowCount)
		}
		if totals.bytes != session.ByteCount {
			return snapshotTransferTotals{}, fmt.Errorf("snapshot final byte total %d does not match session byte_count %d", totals.bytes, session.ByteCount)
		}
		return totals, nil
	}
}

func buildSnapshotSessionCreateURL(base string) string {
	return fmt.Sprintf("%s/sync/snapshot-sessions", base)
}

func encodeOpaquePathSegment(value string) string {
	const hex = "0123456789ABCDEF"
	var encoded strings.Builder
	encoded.Grow(len(value))
	for i := 0; i < len(value); i++ {
		b := value[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '~' {
			encoded.WriteByte(b)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hex[b>>4])
		encoded.WriteByte(hex[b&0x0f])
	}
	return encoded.String()
}

func buildSnapshotChunkURL(base, snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) string {
	values := url.Values{}
	values.Set("after_row_ordinal", strconv.FormatInt(afterRowOrdinal, 10))
	if maxRows > 0 {
		values.Set("max_rows", strconv.Itoa(maxRows))
	}
	if maxBytes > 0 {
		values.Set("max_bytes", strconv.FormatInt(maxBytes, 10))
	}
	return fmt.Sprintf("%s/sync/snapshot-sessions/%s?%s", base, encodeOpaquePathSegment(snapshotID), values.Encode())
}

func buildSnapshotSessionDeleteURL(base, snapshotID string) string {
	return fmt.Sprintf("%s/sync/snapshot-sessions/%s", base, encodeOpaquePathSegment(snapshotID))
}
