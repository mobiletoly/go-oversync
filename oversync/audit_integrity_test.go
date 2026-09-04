//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func requireAuditMetadataIntegrity(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	checks := []struct {
		name  string
		query string
	}{
		{
			name: "bundle row counts and ordinals",
			query: `
				SELECT COUNT(*)
				FROM sync.bundle_log AS log
				LEFT JOIN LATERAL (
					SELECT COUNT(*) AS actual_count,
						COALESCE(MIN(row_ordinal), 0) AS min_ordinal,
						COALESCE(MAX(row_ordinal), -1) AS max_ordinal
					FROM sync.bundle_rows AS row
					WHERE row.user_pk = log.user_pk
					  AND row.bundle_seq = log.bundle_seq
				) AS rows ON TRUE
				WHERE rows.actual_count <> log.row_count
				   OR rows.min_ordinal <> 1
				   OR rows.max_ordinal <> log.row_count
			`,
		},
		{
			name: "bundle and row versions stay below next sequence",
			query: `
				SELECT COUNT(*)
				FROM sync.user_state AS users
				WHERE EXISTS (
					SELECT 1 FROM sync.bundle_log AS log
					WHERE log.user_pk = users.user_pk
					  AND log.bundle_seq >= users.next_bundle_seq
				) OR EXISTS (
					SELECT 1 FROM sync.row_state AS state
					WHERE state.user_pk = users.user_pk
					  AND state.bundle_seq >= users.next_bundle_seq
				)
			`,
		},
		{
			name: "source watermarks cover retained source history",
			query: `
				SELECT COUNT(*)
				FROM sync.bundle_log AS log
				LEFT JOIN sync.source_state AS source
				  ON source.user_pk = log.user_pk
				 AND source.source_id = log.source_id
				WHERE source.user_pk IS NULL
				   OR source.max_committed_source_bundle_id < log.source_bundle_id
			`,
		},
		{
			name: "snapshot materialized counts",
			query: `
				SELECT COUNT(*)
				FROM sync.snapshot_sessions AS session
				LEFT JOIN LATERAL (
					SELECT COUNT(*) AS actual_count
					FROM sync.snapshot_session_rows AS row
					WHERE row.snapshot_id = session.snapshot_id
				) AS rows ON TRUE
				WHERE rows.actual_count <> session.row_count
			`,
		},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			var violations int64
			require.NoError(t, pool.QueryRow(ctx, check.query).Scan(&violations))
			require.Zero(t, violations, fmt.Sprintf("metadata integrity check failed: %s", check.name))
		})
	}
}

func TestAuditIntegrityOracle_AfterDirectCommittedWork(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{})

	committedID := auditUUID(1)
	_, err := pushRowsOneShot(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(committedID, "Committed"),
	})
	require.NoError(t, err)

	_, err = pushRowsOneShot(t, ctx, fixture.svc, fixture.writer, 2, []PushRequestRow{
		fixture.userRow(auditUUID(2), "Second"),
	})
	require.NoError(t, err)

	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}

func auditUUID(value int) uuid.UUID {
	var id uuid.UUID
	id[15] = byte(value)
	return id
}
