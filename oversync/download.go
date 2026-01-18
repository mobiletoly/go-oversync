// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ProcessDownloadWindowed handles a download request (user and schema-aware) with an
// optional frozen upper bound (until)
//
// includeSelf: if true, includes changes from the same sourceID (useful for client recovery)
// ProcessDownload handles a download request (user and schema-aware)
// includeSelf: if true, includes changes from the same sourceID (useful for client recovery)
// until: optional frozen upper bound for server_id window. If 0, we compute a snapshot for this call.
//
//	For multi-page hydration, the client SHOULD pass the same 'until' across calls.
//
// ProcessDownloadWindowed handles a download request
func (s *SyncService) ProcessDownloadWindowed(
	ctx context.Context,
	userID, sourceID string,
	after int64,
	limit int,
	schemaFilter string,
	includeSelf bool,
	until int64,
) (resp *DownloadResponse, err error) {
	totalStart := s.stageStart()
	defer func() {
		changeCount := 0
		if resp != nil {
			changeCount = len(resp.Changes)
		}
		s.observeStage(ctx, MetricsOpDownload, MetricsStageTotal, totalStart, changeCount, 0, err != nil)
	}()

	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	if schemaFilter != "" && !isValidSchemaName(schemaFilter) {
		return nil, fmt.Errorf("invalid schema filter: %s", schemaFilter)
	}

	// Freeze the window upper bound if not provided by the caller.
	if until <= 0 {
		wmStart := s.stageStart()
		until = s.getUserHighestServerSeq(ctx, userID)
		s.observeStage(ctx, MetricsOpDownload, MetricsStageDownloadWatermark, wmStart, 1, 0, false)
	}
	// Capture the frozen window for this response.
	windowUntil := until
	if after >= windowUntil {
		// Nothing to page
		return &DownloadResponse{
			Changes:     []ChangeDownloadResponse{},
			HasMore:     false,
			NextAfter:   after,
			WindowUntil: windowUntil,
		}, nil
	}

	// Prepare schema arg: NULL means no filter
	var schemaArg *string
	if strings.TrimSpace(schemaFilter) != "" {
		sf := schemaFilter
		schemaArg = &sf
	} else {
		schemaArg = nil
	}

	// Avoid joining sync_row_meta on the download hot path. All information needed to apply a
	// change is already present in server_change_log (including server_version), and `deleted`
	// is derivable from the operation type.
	//
	// Also avoid OR/CASE predicates so Postgres can use the best index for each mode (e.g.
	// scl_user_schema_seq_idx when schemaFilter is set).
	baseSelect := `
SELECT
  l.server_id,
  l.schema_name AS schema,
  l.table_name  AS "table",
  l.op,
  l.pk_uuid::text AS pk,
  l.payload,
  l.server_version,
  (l.op = 'DELETE') AS deleted,
  l.source_id,
  l.source_change_id,
  l.ts
FROM sync.server_change_log AS l
WHERE l.user_id   = $1
  AND l.server_id > $2
  AND l.server_id <= $3`

	q := baseSelect
	args := []any{userID, after, until}

	if schemaArg != nil {
		q += ` AND l.schema_name = $4`
		args = append(args, *schemaArg)
		if !includeSelf {
			q += ` AND l.source_id <> $5 ORDER BY l.server_id LIMIT ($6 + 1);`
			args = append(args, sourceID, limit)
		} else {
			q += ` ORDER BY l.server_id LIMIT ($5 + 1);`
			args = append(args, limit)
		}
	} else {
		if !includeSelf {
			q += ` AND l.source_id <> $4 ORDER BY l.server_id LIMIT ($5 + 1);`
			args = append(args, sourceID, limit)
		} else {
			q += ` ORDER BY l.server_id LIMIT ($4 + 1);`
			args = append(args, limit)
		}
	}

	fetchCount := 0
	fetchHadError := false
	fetchStart := s.stageStart()
	defer func() {
		s.observeStage(ctx, MetricsOpDownload, MetricsStageDownloadFetch, fetchStart, fetchCount, 0, fetchHadError)
	}()

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		fetchHadError = true
		return nil, fmt.Errorf("failed to fetch download page: %w", err)
	}
	defer rows.Close()

	changes := make([]ChangeDownloadResponse, 0, limit)
	hasMore := false
	for rows.Next() {
		var c ChangeDownloadResponse
		if scanErr := rows.Scan(
			&c.ServerID,
			&c.Schema,
			&c.TableName,
			&c.Op,
			&c.PK,
			&c.Payload,
			&c.ServerVersion,
			&c.Deleted,
			&c.SourceID,
			&c.SourceChangeID,
			&c.Timestamp,
		); scanErr != nil {
			fetchHadError = true
			return nil, fmt.Errorf("failed to scan download row: %w", scanErr)
		}
		changes = append(changes, c)
		fetchCount++
	}
	if rows.Err() != nil {
		fetchHadError = true
		return nil, fmt.Errorf("failed to fetch download page rows: %w", rows.Err())
	}

	if len(changes) > limit {
		hasMore = true
		changes = changes[:limit]
	}

	nextAfter := after
	if len(changes) > 0 {
		nextAfter = changes[len(changes)-1].ServerID
	}

	//s.logger.Debug("Processed download page",
	//	"user_id", userID, "source_id", sourceID,
	//	"after", after, "until", until, "window_until", windowUntil, "limit", limit,
	//	"include_self", includeSelf, "schema", schemaFilter,
	//	"changes_count", len(changes), "next_after", nextAfter, "has_more", hasMore,
	//)

	return &DownloadResponse{
		Changes:     changes,
		HasMore:     hasMore,
		NextAfter:   nextAfter,
		WindowUntil: windowUntil,
	}, nil
}

// getHighestServerSeq returns the highest server_id across all users
func (s *SyncService) getHighestServerSeq(ctx context.Context) int64 {
	if s.pool == nil {
		return 0
	}
	var maxSeq int64
	// This query doesn't need named args since it has no parameters
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(server_id), 0) FROM sync.server_change_log`).Scan(&maxSeq)
	if err != nil {
		s.logger.Error("Failed to get highest server seq", "error", err)
		return 0
	}
	return maxSeq
}

// getUserHighestServerSeq returns the highest server_id for a specific user
// This provides a more accurate watermark for per-user downloads
func (s *SyncService) getUserHighestServerSeq(ctx context.Context, userID string) int64 {
	if s.pool == nil {
		return 0
	}
	var maxSeq int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(server_id), 0) FROM sync.server_change_log WHERE user_id = @user_id`,
		pgx.NamedArgs{"user_id": userID},
	).Scan(&maxSeq)
	if err != nil {
		s.logger.Error("Failed to get user highest server seq", "error", err, "user_id", userID)
		return 0
	}
	return maxSeq
}
