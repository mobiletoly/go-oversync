// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite_e2e

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const cyclicTeamsDDL = `CREATE TABLE teams (
	id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, captain_member_id TEXT,
	FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
)`

const cyclicMembersDDL = `CREATE TABLE team_members (
	id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, team_id TEXT NOT NULL,
	FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
)`

func e2ePendingCounts(t *testing.T, db *sql.DB) (dirty, outbound int, nextSource int64) {
	t.Helper()
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outbound))
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state LIMIT 1`).Scan(&nextSource))
	return
}

func TestEndToEnd_CyclicDeletesCrossSoftTargetsInOneServerPush(t *testing.T) {
	for _, target := range []string{"rows", "bytes"} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			schema := "e2e_cyclic_delete_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			server := newExampleServer(t, schema, "teams", "team_members")
			config := defaultOversqliteConfig(schema, syncTables("teams", "team_members"))
			if target == "rows" {
				config.UploadLimit = 1
			} else {
				config.UploadByteLimit = 1
			}
			userID := "e2e-cyclic-delete-" + uuid.NewString()
			writer, writerDB := newSQLiteClient(t, server, userID, "writer", config, cyclicTeamsDDL, cyclicMembersDDL)
			reader, readerDB := newSQLiteClient(t, server, userID, "reader",
				defaultOversqliteConfig(schema, syncTables("teams", "team_members")), cyclicTeamsDDL, cyclicMembersDDL)
			teamID, memberID := uuid.NewString(), uuid.NewString()
			tx, err := writerDB.Begin()
			require.NoError(t, err)
			_, err = tx.Exec(`INSERT INTO teams VALUES (?, 'Core', ?)`, teamID, memberID)
			require.NoError(t, err)
			_, err = tx.Exec(`INSERT INTO team_members VALUES (?, 'Ada', ?)`, memberID, teamID)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			for i := 0; i < 2; i++ {
				dirty, _, _ := e2ePendingCounts(t, writerDB)
				if dirty > 0 {
					mustPushPendingE2E(t, writer, ctx)
				}
			}
			mustPullToStableE2E(t, reader, ctx)
			var initial int
			require.NoError(t, readerDB.QueryRow(`SELECT count(*) FROM teams`).Scan(&initial))
			require.Equal(t, 1, initial)

			tx, err = writerDB.Begin()
			require.NoError(t, err)
			_, err = tx.Exec(`DELETE FROM teams WHERE id=?`, teamID)
			require.NoError(t, err)
			_, err = tx.Exec(`DELETE FROM team_members WHERE id=?`, memberID)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			dirty, outbound, before := e2ePendingCounts(t, writerDB)
			require.Equal(t, 2, dirty)
			require.Zero(t, outbound)
			mustPushPendingE2E(t, writer, ctx)
			dirty, outbound, after := e2ePendingCounts(t, writerDB)
			require.Zero(t, dirty)
			require.Zero(t, outbound)
			require.Equal(t, before+1, after)
			mustPullToStableE2E(t, reader, ctx)
			require.NoError(t, readerDB.QueryRow(`SELECT count(*) FROM teams`).Scan(&initial))
			require.Zero(t, initial)
			require.NoError(t, readerDB.QueryRow(`SELECT count(*) FROM team_members`).Scan(&initial))
			require.Zero(t, initial)
		})
	}
}

func TestEndToEnd_UpdateThenDeleteUsesServerBaseFKAtSoftBoundary(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_base_fk_delete_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema, "teams", "team_members")
	config := defaultOversqliteConfig(schema, syncTables("teams", "team_members"))
	config.UploadLimit = 1
	userID := "e2e-base-fk-delete-" + uuid.NewString()
	writer, writerDB := newSQLiteClient(t, server, userID, "writer", config, cyclicTeamsDDL, cyclicMembersDDL)
	reader, readerDB := newSQLiteClient(t, server, userID, "reader",
		defaultOversqliteConfig(schema, syncTables("teams", "team_members")), cyclicTeamsDDL, cyclicMembersDDL)
	teamID, memberID := uuid.NewString(), uuid.NewString()
	tx, err := writerDB.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO teams VALUES (?, 'Core', ?)`, teamID, memberID)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO team_members VALUES (?, 'Ada', ?)`, memberID, teamID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	mustPushPendingE2E(t, writer, ctx)
	mustPullToStableE2E(t, reader, ctx)
	_, err = writerDB.Exec(`UPDATE team_members SET name='Ada updated' WHERE id=?`, memberID)
	require.NoError(t, err)
	_, err = writerDB.Exec(`UPDATE teams SET captain_member_id=NULL WHERE id=?`, teamID)
	require.NoError(t, err)
	_, err = writerDB.Exec(`DELETE FROM team_members WHERE id=?`, memberID)
	require.NoError(t, err)
	_, err = writerDB.Exec(`DELETE FROM teams WHERE id=?`, teamID)
	require.NoError(t, err)
	dirty, outbound, before := e2ePendingCounts(t, writerDB)
	require.Equal(t, 2, dirty)
	require.Zero(t, outbound)
	mustPushPendingE2E(t, writer, ctx)
	dirty, outbound, after := e2ePendingCounts(t, writerDB)
	require.Zero(t, dirty)
	require.Zero(t, outbound)
	require.Equal(t, before+1, after)
	mustPullToStableE2E(t, reader, ctx)
	var count int
	require.NoError(t, readerDB.QueryRow(`SELECT count(*) FROM teams`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, readerDB.QueryRow(`SELECT count(*) FROM team_members`).Scan(&count))
	require.Zero(t, count)
}
