package oversqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// A snapshot installed over existing rows is charged the unique-cycle working set (the cycle closure
// over unique transfer edges) only when unique values move between rows; without such a move the
// restore must not be refused for it.
const uniqueCycleChargeRows = 100

// The restore holds about 2 entries per row plus 2 for the stage and plan (202), and needs about 232
// more for fixed metadata, about 434 in all; the unique-cycle working set adds 4 entries per row (400),
// about 602. The limit lies between.
const uniqueCycleChargeEntryLimit = 520

func rebuildUsersUnder(t *testing.T, workspaceEntries int64, swapUniqueEmails bool) (error, map[string]string) {
	t.Helper()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	for i := 0; i < uniqueCycleChargeRows; i++ {
		seedSyncedUserRow(t, c, db, fmt.Sprintf("u%03d", i), fmt.Sprintf("User%03d", i), 1)
	}
	_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; CREATE UNIQUE INDEX users_unique_email ON users(email)`)
	require.NoError(t, err)

	rows := make([]oversync.SnapshotRow, 0, uniqueCycleChargeRows)
	for i := 0; i < uniqueCycleChargeRows; i++ {
		id := fmt.Sprintf("u%03d", i)
		name, email := fmt.Sprintf("User%03d", i), fmt.Sprintf("user%03d@example.com", i)
		if i == 5 {
			name = "Renamed"
		}
		if swapUniqueEmails && i < 2 {
			email = fmt.Sprintf("user%03d@example.com", 1-i)
		}
		rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: "users", Key: mustBundleKey(id), RowVersion: 9, Payload: mustBundlePayload(t, id, name, email)})
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/snapshot-sessions":
			return snapshotFixtureResponse(t, c, r, "unique-charge", 9, rows), nil
		case "/sync/snapshot-sessions/unique-charge":
			if r.Method == http.MethodGet {
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "unique-charge", SnapshotBundleSeq: 9, Rows: rows, NextRowOrdinal: int64(len(rows))}), nil
			}
			return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return nil, io.EOF
	})}
	c.config.MaxSyncWorkspaceEntries = workspaceEntries
	_, err = c.Rebuild(context.Background())
	emails := map[string]string{}
	for _, id := range []string{"u000", "u001", "u005"} {
		var name, email string
		require.NoError(t, db.QueryRow(`SELECT name,email FROM users WHERE id=?`, id).Scan(&name, &email))
		emails[id] = name + " " + email
	}
	return err, emails
}

func TestSnapshotUniqueCycleCharge_RebuildWithoutUniqueMovesIsNotCharged(t *testing.T) {
	err, rows := rebuildUsersUnder(t, uniqueCycleChargeEntryLimit, false)
	require.NoError(t, err, "rebuild must fit the workspace")
	require.Equal(t, "Renamed user005@example.com", rows["u005"])
}

func TestSnapshotUniqueCycleCharge_RebuildThatSwapsUniqueValuesIsStillCharged(t *testing.T) {
	err, _ := rebuildUsersUnder(t, uniqueCycleChargeEntryLimit, true)
	var limit *SyncWorkspaceLimitError
	require.True(t, errors.As(err, &limit), "want a workspace refusal, got %v", err)
	require.Equal(t, "entries", limit.Dimension)
}

func TestSnapshotUniqueCycleCharge_RebuildThatSwapsUniqueValuesCompletesUnderAmpleLimits(t *testing.T) {
	err, rows := rebuildUsersUnder(t, 1_000_000, true)
	require.NoError(t, err)
	require.Equal(t, "User000 user001@example.com", rows["u000"])
	require.Equal(t, "User001 user000@example.com", rows["u001"])
}
