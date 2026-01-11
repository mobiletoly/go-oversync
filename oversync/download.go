// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
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
) (*DownloadResponse, error) {
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
		until = s.getUserHighestServerSeq(ctx, userID)
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

	const q = `
WITH page_raw AS (
  SELECT
    l.server_id,
    l.schema_name AS schema,
    l.table_name  AS "table",
    l.op,
    l.pk_uuid::text AS pk,
    l.payload,
    l.server_version,
    COALESCE(m.deleted, false)    AS deleted,
    l.source_id,
    l.source_change_id,
    l.ts
  FROM sync.server_change_log AS l
  LEFT JOIN sync.sync_row_meta AS m
    ON m.user_id     = l.user_id
   AND m.schema_name = l.schema_name
   AND m.table_name  = l.table_name
   AND m.pk_uuid     = l.pk_uuid
  WHERE l.user_id   = $1
    AND l.server_id > $2
    AND l.server_id <= $7
    AND ($3::text IS NULL OR l.schema_name = $3)
    AND (CASE WHEN $4::bool THEN TRUE ELSE l.source_id <> $5 END)
  ORDER BY l.server_id
  LIMIT ($6 + 1)
),
page_limited AS (
  SELECT * FROM page_raw ORDER BY server_id LIMIT $6
),
agg AS (
  SELECT
    COALESCE(json_agg(to_jsonb(page_limited) ORDER BY page_limited.server_id), '[]'::json) AS changes,
    COALESCE(MAX(page_limited.server_id), $2) AS next_after,
    (SELECT COUNT(*) > $6 FROM page_raw) AS has_more
  FROM page_limited
)
SELECT changes, next_after, has_more FROM agg;`

	var (
		changesJSON []byte
		nextAfter   int64
		hasMore     bool
	)
	// Use positional args; pgx will handle NULL for $3 when schemaArg == nil.
	if err := s.pool.QueryRow(ctx, q,
		userID,      // $1
		after,       // $2
		schemaArg,   // $3
		includeSelf, // $4
		sourceID,    // $5
		limit,       // $6
		until,       // $7 (upper bound)
	).Scan(&changesJSON, &nextAfter, &hasMore); err != nil {
		return nil, fmt.Errorf("failed to fetch download page: %w", err)
	}

	var changes []ChangeDownloadResponse
	if err := json.Unmarshal(changesJSON, &changes); err != nil {
		return nil, fmt.Errorf("failed to decode page JSON: %w", err)
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
