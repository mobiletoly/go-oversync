package oversqlite

import (
	"context"
	"fmt"
)

func (c *Client) syncStatusLocked(ctx context.Context) (SyncStatus, error) {
	if c.sourceID == "" {
		return SyncStatus{}, &OpenRequiredError{Operation: "SyncStatus()"}
	}
	attachment, err := loadAttachmentState(ctx, c.db)
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
	case attachment.BindingState == attachmentBindingAnonymous:
		authority = AuthorityStatusLocalOnly
	case attachment.PendingInitializationID != "":
		authority = AuthorityStatusPendingLocalSeed
	case liveStructuredRows == 0:
		authority = AuthorityStatusAuthoritativeEmpty
	}
	conflict, err := readPendingConflict(ctx, c.db)
	if err != nil {
		return SyncStatus{}, err
	}
	rejection, err := readPendingPushRejection(ctx, c.db)
	if err != nil {
		return SyncStatus{}, err
	}
	action, err := c.ApplicationActionRequired(ctx)
	if err != nil {
		return SyncStatus{}, err
	}
	return SyncStatus{
		ContractConfirmed: c.localGeneration != nil,
		NeedsResolution:   conflict != nil || rejection != nil,
		Authority:         authority,
		Pending:           pending,
		LastBundleSeqSeen: attachment.LastBundleSeqSeen,
		ActionRequired:    action,
	}, nil
}

func (c *Client) countLiveStructuredRows(ctx context.Context) (int64, error) {
	var count int64
	if err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM _sync_row_state
		WHERE server_kind = 'live'
	`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count live structured rows: %w", err)
	}
	return count, nil
}
