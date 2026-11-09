// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func queryStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	return values
}

// Stored generated columns are derived on insert; copy only the columns SQLite persists.
func storedColumns(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	columns := queryStrings(t, db, `SELECT name FROM pragma_table_xinfo(?) WHERE hidden=0 ORDER BY cid`, table)
	require.NotEmpty(t, columns, table)
	for i, column := range columns {
		columns[i] = quoteIdent(column)
	}
	return strings.Join(columns, ",")
}

// captureDurableSyncState copies every sync control table: the durable state a lost
// WAL commit falls back to. The fixture database uses exactly one connection.
func captureDurableSyncState(t *testing.T, db *sql.DB) []string {
	t.Helper()
	tables := queryStrings(t, db, `SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '\_sync\_%' ESCAPE '\' ORDER BY name`)
	require.NotEmpty(t, tables)
	for _, table := range tables {
		_, err := db.Exec(`CREATE TEMP TABLE ` + quoteIdent("durable_"+table) + ` AS SELECT ` + storedColumns(t, db, table) + ` FROM ` + quoteIdent(table))
		require.NoError(t, err, table)
	}
	return tables
}

// loseSyncCommitsSince discards every sync-state commit made after the capture, exactly
// as a power failure before the WAL reached disk would. Only tables whose content
// changed are rewritten, so revision triggers do not disturb untouched tables; tables
// without triggers are reconciled once more so no trigger side effect survives.
func loseSyncCommitsSince(t *testing.T, db *sql.DB, tables []string) {
	t.Helper()
	triggered := map[string]bool{}
	for _, table := range queryStrings(t, db, `SELECT DISTINCT tbl_name FROM sqlite_schema WHERE type='trigger'`) {
		triggered[table] = true
	}
	// The fixture owns one connection: read every column list before the writer holds it.
	columnsByTable := map[string]string{}
	for _, table := range tables {
		columnsByTable[table] = storedColumns(t, db, table)
	}
	_, err := db.Exec(`PRAGMA foreign_keys = OFF`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	restore := func(table string) {
		columns, current, durable := columnsByTable[table], quoteIdent(table), `temp.`+quoteIdent("durable_"+table)
		var changed bool
		require.NoError(t, tx.QueryRow(`SELECT EXISTS(SELECT `+columns+` FROM `+current+` EXCEPT SELECT `+columns+` FROM `+durable+`) OR EXISTS(SELECT `+columns+` FROM `+durable+` EXCEPT SELECT `+columns+` FROM `+current+`)`).Scan(&changed), table)
		if !changed {
			return
		}
		_, err := tx.Exec(`DELETE FROM ` + current)
		require.NoError(t, err, table)
		_, err = tx.Exec(`INSERT INTO ` + current + `(` + columns + `) SELECT ` + columns + ` FROM ` + durable)
		require.NoError(t, err, table)
	}
	for _, table := range tables {
		restore(table)
	}
	for _, table := range tables {
		if !triggered[table] {
			restore(table)
		}
	}
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)
	require.Empty(t, queryStrings(t, db, `SELECT "table" FROM pragma_foreign_key_check`))
	for _, table := range tables {
		_, err = db.Exec(`DROP TABLE temp.` + quoteIdent("durable_"+table))
		require.NoError(t, err, table)
	}
}

func pushedRowIdentities(t *testing.T, client *Client, request oversync.CompactPushRequest) []string {
	t.Helper()
	var identities []string
	for _, row := range expandCapturedPushRequest(t, client, request) {
		identities = append(identities, fmt.Sprintf("%s:%s:%v:%d", row.Table, row.Op, row.Key["id"], row.BaseRowVersion))
	}
	sort.Strings(identities)
	return identities
}

// A freeze commit that never reached disk leaves the server holding a terminal outcome
// the client has no local evidence for. The client re-freezes different content under
// the same source sequence, receives push_outcome_mismatch, and must converge through
// the sequence-only retained lookup with no lost row and no stuck upload.
func TestLostFreeze_RetainedOutcomeRecovery(t *testing.T) {
	for _, status := range []string{"committed", "rejected", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			c, db, s := recoveryJournalClient(t)
			exec := func(statement string) {
				t.Helper()
				_, err := db.Exec(statement)
				require.NoError(t, err)
			}
			_, err := c.PushPending(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))

			// Local intent that the lost freeze uploads: one new row and one update of a synced row.
			exec(`INSERT INTO users(id,name,email) VALUES('ada','Ada','ada@example.invalid')`)
			exec(`UPDATE users SET name='item v2' WHERE id='item'`)
			durable := journalProcessVector(t, db)
			durableTables := captureDurableSyncState(t, db)

			lose := true
			c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := s.RoundTrip(r)
				if err == nil && lose {
					lose = false
					response.Body.Close()
					return nil, errors.New("terminal answer lost")
				}
				return response, err
			})}
			switch status {
			case "rejected":
				s.pushHook = func(p *mockDirectPush) *http.Response {
					return s.retainOutcome(oversync.PushOutcome{SourceID: p.SourceID, GenerationID: p.Generation.ID, GenerationFingerprint: p.Generation.Fingerprint, SourceBundleID: p.SourceBundleID, CanonicalRequestHash: p.CanonicalRequestHash, Status: "rejected", Rejection: &oversync.PushOutcomeRejection{Code: "application_rejected"}}, p.Generation)
				}
			case "cancelled":
				// The request never reaches the server; the resumed attempt then resolves and
				// the server durably cancels the sequence.
				c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
				_, err = c.PushPending(ctx)
				require.Error(t, err)
			}
			_, err = c.PushPending(ctx)
			require.Error(t, err)
			require.False(t, lose, "the terminal answer must have been produced and lost")
			s.pushHook = nil
			require.Equal(t, status, s.terminalOutcomes[2].Status)
			require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.Len(t, s.ackRequests, 1)

			// Power loss: the freeze commit (and everything after it) never reached disk.
			require.NoError(t, c.Close())
			loseSyncCommitsSince(t, db, durableTables)
			require.Equal(t, durable, journalProcessVector(t, db), "restore must reproduce the pre-freeze durable state")
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))

			// Later edits make the re-frozen request differ from the one the server answered.
			exec(`UPDATE users SET name='Ada v2' WHERE id='ada'`)
			exec(`INSERT INTO users(id,name,email) VALUES('bob','Bob','bob@example.invalid')`)

			reopened, err := newAdmittedFixtureClient(db, c.BaseURL, c.Token, c.config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			attachTestClient(t, reopened, c.UserID, "bundle-device")
			reopened.HTTP = &http.Client{Transport: s}
			pushes, resolves := len(s.pushRequests), len(s.resolveRequests)

			report, err := reopened.PushPending(ctx)
			require.NoError(t, err)
			require.Equal(t, []oversync.PushOutcomeRetainedRequest{{SourceBundleID: 2}}, s.retainedRequests)
			require.Len(t, s.pushRequests, pushes+1)
			require.EqualValues(t, 2, s.pushRequests[pushes].SourceBundleID)
			require.Len(t, s.ackRequests, 2)
			require.EqualValues(t, 2, s.ackRequests[1].SourceBundleID)
			require.Equal(t, s.terminalOutcomes[2].CanonicalRequestHash, s.ackRequests[1].CanonicalRequestHash, "ack must use the retained binding")
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
			var adaOp string
			var adaBase int64
			require.NoError(t, db.QueryRow(`SELECT op,base_row_version FROM _sync_dirty_rows WHERE key_json='{"id":"ada"}'`).Scan(&adaOp, &adaBase))
			if status == "committed" {
				// The committed effect became the base; the satisfied update was consumed.
				require.Equal(t, PushOutcomeCommitted, report.Outcome)
				require.Equal(t, "UPDATE", adaOp)
				require.EqualValues(t, 2, adaBase)
				require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
				require.EqualValues(t, 2, countA3(t, db, `SELECT row_version FROM _sync_row_state WHERE key_json='{"id":"item"}'`))
			} else {
				// No business effect was committed: every frozen row keeps its original base.
				require.Equal(t, PushOutcomeNoChange, report.Outcome)
				require.Equal(t, "INSERT", adaOp)
				require.Zero(t, adaBase)
				require.EqualValues(t, 3, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
				require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_push_rejection_state WHERE pending!=''`), "rejection evidence of the lost request must not bind the kept intent")
			}

			_, err = reopened.PushPending(ctx)
			require.NoError(t, err)
			require.Len(t, s.pushRequests, pushes+2)
			require.EqualValues(t, 3, s.pushRequests[pushes+1].SourceBundleID)
			require.Len(t, s.resolveRequests, resolves, "recovery never resolves or cancels")
			require.Len(t, s.retainedRequests, 1)
			require.Len(t, s.ackRequests, 3)
			require.EqualValues(t, 3, s.ackRequests[2].SourceBundleID)
			successor := pushedRowIdentities(t, reopened, s.pushRequests[pushes+1])
			if status == "committed" {
				require.Equal(t, []string{"users:INSERT:bob:0", "users:UPDATE:ada:2"}, successor)
			} else {
				require.Equal(t, []string{"users:INSERT:ada:0", "users:INSERT:bob:0", "users:UPDATE:item:1"}, successor)
			}
			require.Equal(t, "committed", s.terminalOutcomes[3].Status)

			// No lost row: every value the application wrote is local and was uploaded.
			for id, name := range map[string]string{"ada": "Ada v2", "item": "item v2", "bob": "Bob"} {
				var local string
				require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, id).Scan(&local))
				require.Equal(t, name, local, id)
			}
			uploaded := map[string]string{}
			for _, sequence := range []int64{2, 3} {
				bundle, ok := s.committedBySource[sequence]
				if !ok {
					continue
				}
				for _, row := range bundle.Rows {
					uploaded[fmt.Sprint(row.Key["id"])] = string(row.Payload)
				}
			}
			require.Contains(t, uploaded["ada"], "Ada v2")
			require.Contains(t, uploaded["item"], "item v2")
			require.Contains(t, uploaded["bob"], "Bob")

			// No stuck upload: nothing dirty, frozen or awaiting acknowledgment.
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
			require.EqualValues(t, 4, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
			outbox, err := loadOutboxBundle(ctx, db)
			require.NoError(t, err)
			require.Equal(t, outboxStateNone, outbox.State)
			final, err := reopened.PushPending(ctx)
			require.NoError(t, err)
			require.Equal(t, PushOutcomeNoChange, final.Outcome)
			require.Len(t, s.pushRequests, pushes+2)
		})
	}
}

// A failure after the journal was durably rebound must resume from the stored retained
// outcome: no second lookup, no re-dispatch of the frozen request and no lost intent.
func TestLostFreeze_InterruptedRecoveryResumesFromStoredOutcome(t *testing.T) {
	ctx := context.Background()
	c, db, s := recoveryJournalClient(t)
	exec := func(statement string) {
		t.Helper()
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	_, err := c.PushPending(ctx)
	require.NoError(t, err)
	exec(`INSERT INTO users(id,name,email) VALUES('ada','Ada','ada@example.invalid')`)
	exec(`UPDATE users SET name='item v2' WHERE id='item'`)
	durableTables := captureDurableSyncState(t, db)
	c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := s.RoundTrip(r)
		if err == nil {
			response.Body.Close()
			return nil, errors.New("committed answer lost")
		}
		return response, err
	})}
	_, err = c.PushPending(ctx)
	require.Error(t, err)
	require.Equal(t, "committed", s.terminalOutcomes[2].Status)
	require.NoError(t, c.Close())
	loseSyncCommitsSince(t, db, durableTables)
	exec(`UPDATE users SET name='Ada v2' WHERE id='ada'`)

	reopen := func() *Client {
		t.Helper()
		client, err := newAdmittedFixtureClient(db, c.BaseURL, c.Token, c.config)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		attachTestClient(t, client, c.UserID, "bundle-device")
		client.HTTP = &http.Client{Transport: s}
		return client
	}
	interrupted := reopen()
	exec(`CREATE TEMP TRIGGER fail_settlement BEFORE UPDATE ON users BEGIN SELECT RAISE(ABORT,'synthetic settlement failure'); END`)
	_, err = interrupted.PushPending(ctx)
	require.Error(t, err)

	// The retained evidence is durable under the retained binding; the frozen rows and
	// every local value are intact and nothing was acknowledged.
	var phase, boundHash string
	var outcomeBytes int64
	require.NoError(t, db.QueryRow(`SELECT phase,length(outcome_body),json_extract(binding,'$.operation.canonical_request_hash') FROM _sync_upload_journal`).Scan(&phase, &outcomeBytes, &boundHash))
	require.Equal(t, "frozen", phase)
	require.Positive(t, outcomeBytes)
	require.Equal(t, s.terminalOutcomes[2].CanonicalRequestHash, boundHash)
	outbox := requireOutboxBundle(t, db)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.NotEqual(t, boundHash, outbox.CanonicalRequestHash)
	require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Len(t, s.retainedRequests, 1)
	require.Len(t, s.ackRequests, 1)
	require.EqualValues(t, 2, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='ada' AND name='Ada v2'`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='item' AND name='item v2'`))

	exec(`DROP TRIGGER temp.fail_settlement`)
	require.NoError(t, interrupted.Close())
	resumed := reopen()
	pushes, resolves := len(s.pushRequests), len(s.resolveRequests)
	report, err := resumed.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.Len(t, s.retainedRequests, 1, "resume must settle from the stored outcome")
	require.Len(t, s.pushRequests, pushes, "the frozen request must not be dispatched again")
	require.Len(t, s.resolveRequests, resolves)
	require.Len(t, s.ackRequests, 2)
	require.EqualValues(t, 2, s.ackRequests[1].SourceBundleID)
	require.Equal(t, boundHash, s.ackRequests[1].CanonicalRequestHash)

	_, err = resumed.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"users:UPDATE:ada:2"}, pushedRowIdentities(t, resumed, s.pushRequests[len(s.pushRequests)-1]))
	require.EqualValues(t, 3, s.pushRequests[len(s.pushRequests)-1].SourceBundleID)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	require.EqualValues(t, 4, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='ada' AND name='Ada v2'`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='item' AND name='item v2'`))
}

// A retained lookup that finds nothing proves nothing: the frozen request stays intact
// as a blocker and is neither re-dispatched under a new sequence nor discarded.
func TestLostFreeze_AbsentRetainedOutcomeKeepsFrozenRequest(t *testing.T) {
	ctx := context.Background()
	c, db, s := recoveryJournalClient(t)
	answered := false
	c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push" && !answered {
			answered = true
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{Error: "push_outcome_mismatch"}), nil
		}
		return s.RoundTrip(r)
	})}
	_, err := c.PushPending(ctx)
	var integrity *UploadJournalIntegrityError
	require.ErrorAs(t, err, &integrity)
	require.Contains(t, integrity.Reason, "push_outcome_absent")
	require.Equal(t, []oversync.PushOutcomeRetainedRequest{{SourceBundleID: 1}}, s.retainedRequests)
	require.Empty(t, s.ackRequests)
	var phase string
	var outcomeBytes int64
	require.NoError(t, db.QueryRow(`SELECT phase,length(outcome_body) FROM _sync_upload_journal`).Scan(&phase, &outcomeBytes))
	require.Equal(t, "frozen", phase)
	require.Zero(t, outcomeBytes)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id='item'`).Scan(&name))
	require.Equal(t, "frozen", name)
}
