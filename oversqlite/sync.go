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
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

// uploaderLoop runs the uploader in a loop with backoff
func (c *Client) uploaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Respect pause switch for uploads in background loop
		if atomic.LoadInt32(&c.uploadPaused) == 1 {
			time.Sleep(backoff)
			continue
		}

		err := c.uploadBatch(ctx)
		if err != nil {
			// Exponential backoff on error
			time.Sleep(backoff)
			backoff = backoff * 2
			if backoff > c.config.BackoffMax {
				backoff = c.config.BackoffMax
			}
		} else {
			// Reset backoff on success
			backoff = c.config.BackoffMin
			time.Sleep(backoff) // Still wait a bit between successful uploads
		}
	}
}

// uploadBatch uploads a batch of pending changes
func (c *Client) uploadBatch(ctx context.Context) error {

	// Get client info
	var sourceID string
	var nextChangeID int64
	var lastServerSeq int64
	err := c.DB.QueryRowContext(ctx, `
		SELECT source_id, next_change_id, last_server_seq_seen
		FROM _sync_client_info WHERE user_id = ?
	`, c.UserID).Scan(&sourceID, &nextChangeID, &lastServerSeq)
	if err != nil {
		return fmt.Errorf("failed to get client info: %w", err)
	}

	// Get pending changes (up to limit) - now including payload and change_id
	rows, err := c.DB.QueryContext(ctx, `
		SELECT table_name, pk_uuid, op, base_version, payload, change_id
		FROM _sync_pending
		ORDER BY queued_at
		LIMIT ?
	`, c.config.UploadLimit)
	if err != nil {
		return fmt.Errorf("failed to query pending changes: %w", err)
	}
	defer rows.Close()

	// First, collect all pending changes with their stored payloads and change IDs
	type pendingChange struct {
		tableName   string
		pkUUID      string
		op          string
		baseVersion int64
		payload     sql.NullString // Stored payload from trigger
		changeID    sql.NullInt64  // Stored change ID for idempotency
		needsUpdate bool           // True if change_id was assigned and needs to be persisted
	}

	var pendingChanges []pendingChange
	changeIDStart := nextChangeID

	for rows.Next() {
		var tableName, pkUUID, op string
		var baseVersion int64
		var payload sql.NullString
		var changeID sql.NullInt64

		if err := rows.Scan(&tableName, &pkUUID, &op, &baseVersion, &payload, &changeID); err != nil {
			return fmt.Errorf("failed to scan pending change: %w", err)
		}

		// If change_id is null (old data), assign a new one
		actualChangeID := nextChangeID
		needsChangeIDUpdate := false
		if changeID.Valid {
			actualChangeID = changeID.Int64
		} else {
			// Assign new change_id and increment counter
			needsChangeIDUpdate = true
			nextChangeID++
		}

		pendingChanges = append(pendingChanges, pendingChange{
			tableName:   tableName,
			pkUUID:      pkUUID,
			op:          op,
			baseVersion: baseVersion,
			payload:     payload,
			changeID:    sql.NullInt64{Int64: actualChangeID, Valid: true},
			needsUpdate: needsChangeIDUpdate,
		})
	}

	// Close the rows before making additional queries
	rows.Close()

	if len(pendingChanges) == 0 {
		return nil // Nothing to upload
	}

	// Update change_id for any pending changes that didn't have one (critical for idempotency)
	// This ensures retries use the same change_id, preventing duplicate processing on server
	for _, pending := range pendingChanges {
		if pending.needsUpdate {
			_, err := c.DB.ExecContext(ctx, `
				UPDATE _sync_pending SET change_id = ? WHERE table_name = ? AND pk_uuid = ?
			`, pending.changeID.Int64, pending.tableName, pending.pkUUID)
			if err != nil {
				return fmt.Errorf("failed to update change_id for %s.%s: %w", pending.tableName, pending.pkUUID, err)
			}
		}
	}

	// Now create changes using stored payloads
	var changes []oversync.ChangeUpload
	for _, pending := range pendingChanges {
		// Debug logging for schema
		//fmt.Printf("🔍 DEBUG: oversqlite creating change - schema: %q, table: %q, op: %q, pk: %q\n",
		//	c.config.Schema, pending.tableName, pending.op, pending.pkUUID)

		change := oversync.ChangeUpload{
			SourceChangeID: pending.changeID.Int64,
			Schema:         c.config.Schema, // Use schema from config
			Table:          pending.tableName,
			Op:             pending.op,
			PK:             pending.pkUUID,
			ServerVersion:  pending.baseVersion,
		}

		// Use stored payload from trigger (for INSERT/UPDATE/DELETE)
		if pending.payload.Valid {
			// Process payload to convert hex-encoded BLOB data to base64 for server
			processedPayload, err := c.processPayloadForUpload(pending.tableName, pending.payload.String)
			if err != nil {
				return fmt.Errorf("failed to process payload for %s.%s: %w", pending.tableName, pending.pkUUID, err)
			}
			change.Payload = processedPayload
		} else if pending.op != "DELETE" {
			// Fallback to SerializeRow only if payload is missing and it's not DELETE
			// This should not happen with the new triggers, but provides safety
			payload, err := c.SerializeRow(ctx, pending.tableName, pending.pkUUID)
			if err != nil {
				return fmt.Errorf("failed to serialize row %s.%s (missing stored payload): %w", pending.tableName, pending.pkUUID, err)
			}
			processedPayload, err := c.processPayloadForUpload(pending.tableName, string(payload))
			if err != nil {
				return fmt.Errorf("failed to process serialized payload for %s.%s: %w", pending.tableName, pending.pkUUID, err)
			}
			change.Payload = processedPayload
		}
		// For DELETE operations, payload can be nil (server doesn't need it)

		changes = append(changes, change)
	}

	if len(changes) == 0 {
		return nil // Nothing to upload
	}

	return c.uploadChangesAdaptive(ctx, changes, lastServerSeq, changeIDStart)
}

// downloaderLoop runs the downloader in a loop
func (c *Client) downloaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Respect pause switch for downloads in background loop
		if atomic.LoadInt32(&c.downloadPaused) == 1 {
			continue
		}

		applied, _, err := c.downloadBatch(ctx, c.config.DownloadLimit, false)
		if err != nil {
			// Exponential backoff on error
			time.Sleep(backoff)
			backoff = backoff * 2
			if backoff > c.config.BackoffMax {
				backoff = c.config.BackoffMax
			}
		} else {
			// Reset backoff on success
			backoff = c.config.BackoffMin
			if applied == 0 {
				time.Sleep(backoff) // Wait longer if no changes
			}
		}
	}
}

// downloadBatch downloads and applies a batch of changes
func (c *Client) downloadBatch(ctx context.Context, limit int, includeSelf bool) (applied int, nextAfter int64, err error) {
	{
		// 1) Read current cursor WITHOUT a transaction
		var lastServerSeq int64
		if err := c.DB.QueryRowContext(ctx, `
	        SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	    `, c.UserID).Scan(&lastServerSeq); err != nil {
			return 0, 0, fmt.Errorf("failed to get last server seq: %w", err)
		}

		// 2) Fetch one page from server (network outside of any SQLite tx)
		downloadResponse, err := c.sendDownloadRequest(ctx, lastServerSeq, limit, includeSelf)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to download changes: %w", err)
		}

		// If server has advanced our cursor but provided no changes (edge-case), persist cursor and return
		if len(downloadResponse.Changes) == 0 {
			if downloadResponse.NextAfter > lastServerSeq {
				// Persist the cursor atomically
				tx, err := c.DB.BeginTx(ctx, nil)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to begin tx to persist cursor: %w", err)
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(ctx, `
	                UPDATE _sync_client_info SET last_server_seq_seen = ? WHERE user_id = ?
	            `, downloadResponse.NextAfter, c.UserID); err != nil {
					return 0, 0, fmt.Errorf("failed to update last_server_seq_seen: %w", err)
				}
				if err := tx.Commit(); err != nil {
					return 0, 0, fmt.Errorf("failed to commit cursor persist tx: %w", err)
				}
			}
			return 0, downloadResponse.NextAfter, nil
		}

		// 3) Apply the page ATOMICALLY in a single transaction
		tx, err := c.DB.BeginTx(ctx, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to begin transaction: %w", err)
		}
		committed := false
		defer func() {
			// Ensure rollback if not committed
			if !committed {
				_ = tx.Rollback()
				// Best-effort reset apply_mode outside the tx if we failed partway
				_, _ = c.DB.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, c.UserID)
			}
		}()

		// Defer FK validation until COMMIT for this batch (SQLite 3.20+)
		if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
			return 0, 0, fmt.Errorf("failed to enable deferred FK checks: %w", err)
		}

		// Suppress local triggers while we materialize server state
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 1 WHERE user_id = ?`, c.UserID); err != nil {
			return 0, 0, fmt.Errorf("failed to set apply_mode: %w", err)
		}

		// Apply each change with optimization to skip superseded DELETE operations
		applied = 0
		for i := range downloadResponse.Changes {
			ch := &downloadResponse.Changes[i]
			if !includeSelf && ch.SourceID == c.SourceID {
				continue
			}

			if ch.Op == "DELETE" {
				hasLaterInsertOrUpdate := false
				for j := i + 1; j < len(downloadResponse.Changes); j++ {
					laterCh := &downloadResponse.Changes[j]
					if laterCh.PK == ch.PK && laterCh.TableName == ch.TableName &&
						(laterCh.Op == "INSERT" || laterCh.Op == "UPDATE") &&
						laterCh.ServerVersion > ch.ServerVersion {
						hasLaterInsertOrUpdate = true
						break
					}
				}

				if hasLaterInsertOrUpdate {
					c.logger.Debug("SKIPPING DELETE - superseded by later operation",
						"table", ch.TableName, "pk", ch.PK, "version", ch.ServerVersion)
					continue
				}
			}

			if err := c.applyServerChangeInTx(ctx, tx, ch); err != nil {
				return applied, 0, fmt.Errorf("failed to apply server change: %w", err)
			}
			applied++
		}

		// Advance the cursor to the server-provided watermark inside the same transaction
		if downloadResponse.NextAfter > lastServerSeq {
			if _, err := tx.ExecContext(ctx, `
	            UPDATE _sync_client_info SET last_server_seq_seen = ? WHERE user_id = ?
	        `, downloadResponse.NextAfter, c.UserID); err != nil {
				return applied, 0, fmt.Errorf("failed to update last_server_seq_seen: %w", err)
			}
		}

		// Reset apply_mode before committing
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, c.UserID); err != nil {
			return applied, 0, fmt.Errorf("failed to reset apply_mode: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return applied, 0, fmt.Errorf("failed to commit transaction: %w", err)
		}
		committed = true

		return applied, downloadResponse.NextAfter, nil
	}
}

// downloadBatchInTx downloads and applies a batch of changes within an existing transaction
func (c *Client) downloadBatchInTx(ctx context.Context, tx *sql.Tx, limit int, includeSelf bool) (applied int, nextAfter int64, err error) {
	// Get current server seq
	var lastServerSeq int64
	err = tx.QueryRowContext(ctx, `
		SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	`, c.UserID).Scan(&lastServerSeq)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get last server seq: %w", err)
	}

	// Download changes from server (outside transaction)
	downloadResponse, err := c.sendDownloadRequest(ctx, lastServerSeq, limit, includeSelf)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to download changes: %w", err)
	}

	// If we have changes to apply, set apply_mode to suppress sync triggers for the entire batch
	if len(downloadResponse.Changes) > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 1 WHERE user_id = ?`, c.UserID)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to set apply_mode: %w", err)
		}

		// Apply changes locally within transaction with optimization
		for i, change := range downloadResponse.Changes {
			if !includeSelf && change.SourceID == c.SourceID {
				continue
			}

			// CRITICAL OPTIMIZATION: Skip DELETE operations superseded by later INSERT/UPDATE
			// This prevents record resurrection and improves efficiency when processing batches like DELETE→INSERT
			if change.Op == "DELETE" {
				hasLaterInsertOrUpdate := false
				for j := i + 1; j < len(downloadResponse.Changes); j++ {
					laterCh := &downloadResponse.Changes[j]
					if laterCh.PK == change.PK && laterCh.TableName == change.TableName &&
						(laterCh.Op == "INSERT" || laterCh.Op == "UPDATE") &&
						laterCh.ServerVersion > change.ServerVersion {
						hasLaterInsertOrUpdate = true
						break
					}
				}

				if hasLaterInsertOrUpdate {
					c.logger.Debug("SKIPPING DELETE - superseded by later operation",
						"table", change.TableName, "pk", change.PK, "version", change.ServerVersion)
					continue
				}
			}

			err = c.applyServerChangeInTx(ctx, tx, &change)
			if err != nil {
				return applied, 0, fmt.Errorf("failed to apply server change: %w", err)
			}
			applied++
		}

		// Reset apply_mode within transaction
		_, err = tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, c.UserID)
		if err != nil {
			return applied, 0, fmt.Errorf("failed to reset apply_mode: %w", err)
		}

		// Update last server seq within transaction
		if len(downloadResponse.Changes) > 0 {
			nextAfter = downloadResponse.NextAfter
			_, err = tx.ExecContext(ctx, `
				UPDATE _sync_client_info SET last_server_seq_seen = ? WHERE user_id = ?
			`, nextAfter, c.UserID)
			if err != nil {
				return applied, 0, fmt.Errorf("failed to update last server seq: %w", err)
			}

			// Fix for post-hydration change ID collision: Update next_change_id to prevent reuse
			// When hydrating (includeSelf=true), we need to ensure next_change_id accounts for
			// any changes that were uploaded by this client before hydration
			if includeSelf && applied > 0 {
				// Find the highest source_change_id from our own changes in the downloaded batch
				var maxSourceChangeID int64 = 0
				for _, change := range downloadResponse.Changes {
					if change.SourceID == c.SourceID && change.SourceChangeID > maxSourceChangeID {
						maxSourceChangeID = change.SourceChangeID
					}
				}

				// Only update if we found changes from this client
				if maxSourceChangeID > 0 {
					// Get current next_change_id
					var currentNextChangeID int64
					err = tx.QueryRowContext(ctx, `
						SELECT next_change_id FROM _sync_client_info WHERE user_id = ?
					`, c.UserID).Scan(&currentNextChangeID)
					if err != nil {
						return applied, 0, fmt.Errorf("failed to get current next_change_id: %w", err)
					}

					// Update next_change_id to be higher than any uploaded change
					newNextChangeID := maxSourceChangeID + 1
					if newNextChangeID > currentNextChangeID {
						_, err = tx.ExecContext(ctx, `
							UPDATE _sync_client_info SET next_change_id = ? WHERE user_id = ?
						`, newNextChangeID, c.UserID)
						if err != nil {
							return applied, 0, fmt.Errorf("failed to update next_change_id: %w", err)
						}

					}
				}
			}
		}
	}

	return applied, nextAfter, nil
}

// UploadResult contains the results of an upload operation
type UploadResult struct {
	ChangesUploaded  int              `json:"changes_uploaded"`
	Accepted         bool             `json:"accepted"`
	Applied          int              `json:"applied"`
	Conflicts        int              `json:"conflicts"`
	HighestServerSeq int64            `json:"highest_server_seq"`
	ConflictDetails  []ConflictDetail `json:"conflict_details,omitempty"`
}

// DownloadResult contains the results of a download operation
type DownloadResult struct {
	ChangesDownloaded int   `json:"changes_downloaded"`
	Applied           int   `json:"applied"`
	Skipped           int   `json:"skipped"`
	HasMore           bool  `json:"has_more"`
	NextAfter         int64 `json:"next_after"`
}

// ConflictDetail contains details about a conflict
type ConflictDetail struct {
	SourceChangeID int64           `json:"source_change_id"`
	ServerRow      json.RawMessage `json:"server_row,omitempty"`
}

// sendUploadRequest sends an upload request to the server
func (c *Client) sendUploadRequest(ctx context.Context, req *oversync.UploadRequest) (*oversync.UploadResponse, error) {
	// Convert local PK representations to server/wire format:
	// - For BLOB UUID PK tables, local metadata stores PK as hex; server expects UUID string.
	wireReq := *req
	wireReq.Changes = make([]oversync.ChangeUpload, len(req.Changes))
	for i := range req.Changes {
		ch := req.Changes[i]
		pk, err := c.normalizePKForServer(ch.Table, ch.PK)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize PK for upload (%s.%s): %w", ch.Table, ch.PK, err)
		}
		ch.PK = pk
		wireReq.Changes[i] = ch
	}

	jsonData, err := json.Marshal(&wireReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal upload request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+"/sync/upload", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Get JWT token
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp oversync.UploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("failed to decode upload response: %w", err)
	}

	return &uploadResp, nil
}

// sendDownloadRequest sends a download request to the server
func (c *Client) sendDownloadRequest(ctx context.Context, after int64, limit int, includeSelf bool) (*oversync.DownloadResponse, error) {
	return c.sendDownloadRequestWithUntil(ctx, after, limit, includeSelf, 0)
}

// sendDownloadRequestWithUntil adds an optional frozen upper bound (until) for windowed paging
func (c *Client) sendDownloadRequestWithUntil(ctx context.Context, after int64, limit int, includeSelf bool, until int64) (*oversync.DownloadResponse, error) {
	url := fmt.Sprintf("%s/sync/download?after=%d&limit=%d&schema=%s", c.BaseURL, after, limit, c.config.Schema)
	if includeSelf {
		url += "&include_self=true"
	}
	if until > 0 {
		url += fmt.Sprintf("&until=%d", until)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Get JWT token
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var downloadResp oversync.DownloadResponse
	if err := json.NewDecoder(resp.Body).Decode(&downloadResp); err != nil {
		return nil, fmt.Errorf("failed to decode download response: %w", err)
	}

	return &downloadResp, nil
}

// processUploadResponse processes the server response and updates local state
func (c *Client) processUploadResponse(ctx context.Context, response *oversync.UploadResponse, changeIDStart int64, changes []oversync.ChangeUpload) error {

	// Validate response structure
	if len(response.Statuses) != len(changes) {
		return fmt.Errorf("status count mismatch: sent %d changes, got %d statuses", len(changes), len(response.Statuses))
	}

	// Start transaction for atomic upload response processing
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even after commit

	// Track latest known server watermark from uploads without advancing the download cursor.
	// The download cursor (`last_server_seq_seen`) must only advance when we actually download/apply
	// server changes, otherwise we can permanently skip peer changes when server_id gaps are large.
	_, err = tx.ExecContext(ctx, `
		UPDATE _sync_client_info
		SET current_window_until = CASE
			WHEN current_window_until < ? THEN ?
			ELSE current_window_until
		END
		WHERE user_id = ?
	`, response.HighestServerSeq, response.HighestServerSeq, c.UserID)
	if err != nil {
		return fmt.Errorf("failed to update client info: %w", err)
	}

	// Process each status
	appliedCount := 0
	for i, status := range response.Statuses {
		change := changes[i]

		//fmt.Printf("🔍 DEBUG: Processing status %d/%d - change_id: %d, status: %s, table: %s, pk: %s\n",
		//	i+1, len(response.Statuses), status.SourceChangeID, status.Status, change.Table, change.PK)

		switch status.Status {
		case "applied":
			appliedCount++
			// Remove from pending queue
			_, err = tx.ExecContext(ctx, `
				DELETE FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?
			`, change.Table, change.PK)
			if err != nil {
				return fmt.Errorf("failed to remove from pending: %w", err)
			}

			// Update row metadata with new server version
			if status.NewServerVersion != nil {
				err = c.updateRowMetaInTx(ctx, tx, change.PK, change.Table, *status.NewServerVersion, change.Op == "DELETE")
				if err != nil {
					return fmt.Errorf("failed to update row meta: %w", err)
				}
			}

		case "conflict":
			// Handle conflict using resolver
			if status.ServerRow != nil && c.Resolver != nil {
				// Get local row
				localPayload, err := c.SerializeRow(ctx, change.Table, change.PK)
				if err != nil {
					// Row might not exist locally, accept server version
					// Parse server row to extract payload and version
					var serverRowData map[string]interface{}
					if err := json.Unmarshal(status.ServerRow, &serverRowData); err != nil {
						return fmt.Errorf("failed to parse server row for missing local row: %w", err)
					}

					serverVersion, ok := serverRowData["server_version"].(float64)
					if !ok {
						return fmt.Errorf("server row missing server_version field for missing local row")
					}

					// Extract the business payload from the server row
					payloadData, ok := serverRowData["payload"]
					if !ok {
						return fmt.Errorf("server row missing payload field for missing local row")
					}

					payloadBytes, err := json.Marshal(payloadData)
					if err != nil {
						return fmt.Errorf("failed to marshal payload for missing local row: %w", err)
					}

					err = c.applyServerChangeInTx(ctx, tx, &oversync.ChangeDownloadResponse{
						TableName:     change.Table,
						Op:            "INSERT", // Assume INSERT for missing row
						PK:            change.PK,
						Payload:       payloadBytes,
						ServerVersion: int64(serverVersion),
						SourceID:      "", // Not our change
					})
					if err != nil {
						return fmt.Errorf("failed to apply server change for conflict: %w", err)
					}

					// Remove from pending since we accepted server version
					_, err = tx.ExecContext(ctx, `
						DELETE FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?
					`, change.Table, change.PK)
					if err != nil {
						return fmt.Errorf("failed to remove from pending after conflict: %w", err)
					}
				} else {
					// Resolve conflict
					merged, keepLocal, err := c.Resolver.Merge(change.Table, change.PK, status.ServerRow, localPayload)
					if err != nil {
						return fmt.Errorf("failed to resolve conflict: %w", err)
					}

					if !keepLocal {
						// Accept server version - but only if we have server row data
						if status.ServerRow != nil {
							// Parse server row to extract payload and version
							var serverRowData map[string]interface{}
							if err := json.Unmarshal(status.ServerRow, &serverRowData); err != nil {
								return fmt.Errorf("failed to parse server row: %w", err)
							}

							serverVersion, ok := serverRowData["server_version"].(float64)
							if !ok {
								return fmt.Errorf("server row missing server_version field")
							}

							// Extract the business payload from the server row
							payloadData, ok := serverRowData["payload"]
							if !ok {
								return fmt.Errorf("server row missing payload field")
							}

							payloadBytes, err := json.Marshal(payloadData)
							if err != nil {
								return fmt.Errorf("failed to marshal payload: %w", err)
							}

							err = c.applyServerChangeInTx(ctx, tx, &oversync.ChangeDownloadResponse{
								TableName:     change.Table,
								Op:            "UPDATE",
								PK:            change.PK,
								Payload:       payloadBytes,
								ServerVersion: int64(serverVersion),
								SourceID:      "", // Not our change
							})
							if err != nil {
								return fmt.Errorf("failed to apply server change: %w", err)
							}
						}

						// Remove from pending
						_, err = tx.ExecContext(ctx, `
							DELETE FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?
						`, change.Table, change.PK)
						if err != nil {
							return fmt.Errorf("failed to remove from pending: %w", err)
						}
					} else {
						// Keep local, update pending with merged data
						// Apply merged data locally first
						var mergedMap map[string]interface{}
						if err := json.Unmarshal(merged, &mergedMap); err != nil {
							return fmt.Errorf("failed to unmarshal merged data: %w", err)
						}

						err = c.upsertRowInTx(ctx, tx, change.Table, change.PK, mergedMap)
						if err != nil {
							return fmt.Errorf("failed to apply merged data: %w", err)
						}

						// Update pending with new base version from server row
						var serverRowData map[string]interface{}
						if err := json.Unmarshal(status.ServerRow, &serverRowData); err != nil {
							return fmt.Errorf("failed to parse server row for base version update: %w", err)
						}

						serverVersion, ok := serverRowData["server_version"].(float64)
						if !ok {
							return fmt.Errorf("server row missing server_version field for base version update")
						}

						_, err = tx.ExecContext(ctx, `
							UPDATE _sync_pending
							SET base_version = ?, queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
							WHERE table_name = ? AND pk_uuid = ?
						`, int64(serverVersion), change.Table, change.PK)
						if err != nil {
							return fmt.Errorf("failed to update pending: %w", err)
						}
					}
				}
			}

		case "invalid":
			// Only drop clearly non-recoverable invalids (bad payload or unregistered table).
			if shouldDropInvalid(status) {
				_, err = tx.ExecContext(ctx, `
					DELETE FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?
				`, change.Table, change.PK)
				if err != nil {
					return fmt.Errorf("failed to remove invalid from pending: %w", err)
				}
			} else {
				// Keep retryable invalids (FK missing, batch too large, transient errors)
				c.logger.Warn("Retaining pending change after invalid status",
					"table", change.Table, "pk", change.PK, "reason", getInvalidReason(status),
					"message", status.Message)
			}

		case "materialize_error":
			// print error
			c.logger.Error("ERROR: Materialized error", "table", change.Table,
				"pk", change.PK, "error", status.Message)

		default:
			c.logger.Warn("Unknown status", "table", change.Table,
				"pk", change.PK, "status", status.Status)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// uploadChangesAdaptive retries with smaller chunks when the server signals batch_too_large.
func (c *Client) uploadChangesAdaptive(ctx context.Context, changes []oversync.ChangeUpload, lastServerSeq int64, changeIDStart int64) error {
	if len(changes) == 0 {
		return nil
	}

	chunkSize := len(changes)
	if c.config.UploadLimit > 0 && c.config.UploadLimit < chunkSize {
		chunkSize = c.config.UploadLimit
	}
	if chunkSize <= 0 {
		chunkSize = len(changes)
	}

	for start := 0; start < len(changes); {
		if chunkSize <= 0 {
			chunkSize = 1
		}
		if chunkSize > len(changes)-start {
			chunkSize = len(changes) - start
		}
		chunk := changes[start : start+chunkSize]

		uploadReq := &oversync.UploadRequest{
			LastServerSeqSeen: lastServerSeq,
			Changes:           chunk,
		}

		response, err := c.sendUploadRequest(ctx, uploadReq)
		if err != nil {
			return fmt.Errorf("failed to send upload request: %w", err)
		}

		// If server says batch too large, shrink and retry this window.
		if !response.Accepted && containsBatchTooLarge(response) && chunkSize > 1 {
			newSize := chunkSize / 2
			if newSize < 1 {
				newSize = 1
			}
			c.logger.Warn("Server rejected batch as too large; reducing chunk size",
				"from", chunkSize, "to", newSize, "pending", len(changes)-start)
			chunkSize = newSize
			continue
		}

		if err := c.processUploadResponse(ctx, response, changeIDStart, chunk); err != nil {
			return fmt.Errorf("failed to process upload response: %w", err)
		}

		// Advance window and update lastServerSeq with server watermark
		lastServerSeq = response.HighestServerSeq
		start += chunkSize
		// Optionally increase chunk size back up slowly? Keep current to avoid thrash.
	}
	return nil
}

func containsBatchTooLarge(resp *oversync.UploadResponse) bool {
	for _, st := range resp.Statuses {
		if st.Invalid != nil {
			if reason, ok := st.Invalid["reason"].(string); ok && reason == oversync.ReasonBatchTooLarge {
				return true
			}
		}
	}
	return false
}

// isRetryableFKError checks if an invalid status represents a retryable FK constraint violation
func isRetryableFKError(status oversync.ChangeUploadStatus) bool {
	if status.Status != "invalid" {
		return false
	}

	// Check if the invalid reason is FK missing
	if status.Invalid != nil {
		if reason, ok := status.Invalid["reason"].(string); ok {
			return reason == "fk_missing"
		}
	}

	return false
}

// shouldDropInvalid returns true only for non-recoverable invalid reasons.
func shouldDropInvalid(status oversync.ChangeUploadStatus) bool {
	reason := getInvalidReason(status)
	switch reason {
	case oversync.ReasonBadPayload, oversync.ReasonUnregisteredTable:
		return true
	default:
		return false
	}
}

func getInvalidReason(status oversync.ChangeUploadStatus) string {
	if status.Invalid == nil {
		return ""
	}
	if reason, ok := status.Invalid["reason"].(string); ok {
		return reason
	}
	return ""
}

// processPayloadForUpload converts hex-encoded BLOB data from triggers to base64 for server communication
// This follows the KMP approach: triggers use hex(), but server expects base64
func (c *Client) processPayloadForUpload(tableName, payloadStr string) (json.RawMessage, error) {
	// Parse the payload
	var payloadData map[string]interface{}
	if err := json.Unmarshal([]byte(payloadStr), &payloadData); err != nil {
		return nil, fmt.Errorf("failed to parse payload JSON: %w", err)
	}

	// Get table information to identify BLOB columns
	tableInfo, err := GetTableInfo(c.DB, strings.ToLower(tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// Convert hex-encoded BLOB data to base64
	for _, col := range tableInfo.Columns {
		if col.IsBlob() {
			colNameLower := strings.ToLower(col.Name)
			if hexValue, ok := payloadData[colNameLower].(string); ok && hexValue != "" {
				// Convert hex to bytes
				hexBytes, err := hex.DecodeString(hexValue)
				if err != nil {
					return nil, fmt.Errorf("failed to decode hex for column %s: %w", col.Name, err)
				}
				// Encode as base64 for server
				payloadData[colNameLower] = base64.StdEncoding.EncodeToString(hexBytes)
			}
		}
	}

	// Re-marshal the processed payload
	processedBytes, err := json.Marshal(payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal processed payload: %w", err)
	}

	return processedBytes, nil
}
