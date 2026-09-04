package oversqlite

import (
	"context"
	"fmt"
)

// Inspect under the removal transaction's writer reservation, including metadata
// from an older runtime where optional tables have not yet been installed.
func (c *Client) requireNoUnresolvedWork(ctx context.Context, tx sqliteTransaction) error {
	if err := c.requireNoPendingConflict(ctx, tx); err != nil {
		return err
	}
	table, err := unresolvedWorkTable(ctx, tx)
	if err != nil {
		return err
	}
	if table != "" {
		return fmt.Errorf("cannot uninstall sync with unresolved work in %s", table)
	}
	return nil
}

func unresolvedWorkTable(ctx context.Context, tx sqliteTransaction) (string, error) {
	checks := []struct{ table, predicate string }{
		{"_sync_dirty_rows", "1"}, {"_sync_outbox_bundle", "state != 'none'"}, {"_sync_outbox_rows", "1"},
		{"_sync_push_stage", "1"}, {"_sync_snapshot_stage", "1"},
		{"_sync_attachment_state", "pending_initialization_id != '' OR rebuild_required != 0"},
		{"_sync_operation_state", "kind != 'none' OR reason != '' OR staged_snapshot_id != '' OR replacement_source_id != ''"},
		{"_sync_upgrade_journal", "phase != 'ready'"},
		{"_sync_upgrade_identity", "ready != 1"},
	}
	for _, check := range checks {
		exists, err := sqliteTableExists(ctx, tx, check.table)
		if err != nil {
			return "", err
		}
		if !exists {
			continue
		}
		var pending bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+quoteIdent(check.table)+" WHERE "+check.predicate+")").Scan(&pending); err != nil {
			return "", err
		}
		if pending {
			return check.table, nil
		}
	}
	return "", nil
}
