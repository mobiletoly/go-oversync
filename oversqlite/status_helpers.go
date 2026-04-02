package oversqlite

import (
	"context"
	"fmt"
	"strings"
)

func (c *Client) syncStatusLocked(ctx context.Context) (SyncStatus, error) {
	if err := c.ensureConnectedSessionLocked(ctx, "SyncStatus()"); err != nil {
		return SyncStatus{}, err
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return SyncStatus{}, err
	}
	pending, err := c.pendingSyncStatusLocked(ctx)
	if err != nil {
		return SyncStatus{}, err
	}
	liveStructuredRows, err := c.countLiveStructuredRows(ctx)
	if err != nil {
		return SyncStatus{}, err
	}
	authority := AuthorityStatusAuthoritativeMaterialized
	switch {
	case strings.TrimSpace(attachment.PendingInitializationID) != "":
		authority = AuthorityStatusPendingLocalSeed
	case liveStructuredRows == 0:
		authority = AuthorityStatusAuthoritativeEmpty
	}
	return SyncStatus{
		Authority:         authority,
		Pending:           pending,
		LastBundleSeqSeen: attachment.LastBundleSeqSeen,
	}, nil
}

func (c *Client) countLiveStructuredRows(ctx context.Context) (int64, error) {
	var count int64
	if err := c.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM _sync_row_state
		WHERE deleted = 0
	`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count live structured rows: %w", err)
	}
	return count, nil
}
