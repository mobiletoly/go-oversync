package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"

	activityfixture "github.com/mobiletoly/go-oversync/oversqlite/testdata"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func newActivityRecoveryClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	generation, registered, err := activityfixture.Definition("main")
	require.NoError(t, err)
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, ok := registry.Generation(1)
	require.True(t, ok)
	proof := oversync.SchemaGenerationProof{ID: 1, Fingerprint: compiled.Fingerprint()}
	var tables []SyncTable
	var columns []LocalTableColumns
	for _, table := range generation.Tables {
		var keys []string
		for _, registered := range registered {
			if registered.Table == table.Table {
				keys = registered.SyncKeyColumns
			}
		}
		tables = append(tables, SyncTable{TableName: table.Table, SyncKeyColumns: keys})
		local := LocalTableColumns{Schema: "main", Table: table.Table}
		for _, column := range table.Columns {
			codec := map[oversync.GenerationWireType]LocalColumnCodec{oversync.GenerationWireTypeText: LocalCodecText, oversync.GenerationWireTypeBoolean: LocalCodecBooleanInteger, oversync.GenerationWireTypeInt64: LocalCodecInt64Integer, oversync.GenerationWireTypeFloat64: LocalCodecFloat64Real}[column.WireType]
			require.NotEmpty(t, codec)
			local.Columns = append(local.Columns, LocalColumnContract{Name: column.Name, Codec: codec})
		}
		columns = append(columns, local)
	}
	contract := &LocalGenerationContract{Proof: proof, RegisteredTables: registered, Generation: generation, Tables: tables, Columns: columns}
	config := admittedFixtureConfig("main", tables, proof)
	config.localContract = contract
	config.LocalOnlyTables = []string{"app_settings"}
	c, db := newBundleClientWithConfig(t, config, activityfixture.SQLiteSchema)
	_, err = db.Exec(activityfixture.SeedSQLite)
	require.NoError(t, err)
	rows := activitySnapshotRows(t, c, db, 1)
	tx, err := db.Begin()
	require.NoError(t, err)
	for _, row := range rows {
		key, _, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		require.NoError(t, err)
		require.NoError(t, c.seedFixtureRowStateInTx(context.Background(), tx, "main", row.Table, key, 1, false))
	}
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows; UPDATE _sync_attachment_state SET last_bundle_seq_seen=1`)
	require.NoError(t, err)
	return c, db
}

func activitySnapshotRows(t *testing.T, c *Client, db *sql.DB, version int64) []oversync.SnapshotRow {
	t.Helper()
	var result []oversync.SnapshotRow
	for _, table := range c.config.Tables {
		name := table.TableName
		pk := c.pkByTable[name]
		rows, err := db.Query(`SELECT ` + quoteIdent(pk) + ` FROM ` + quoteIdent(name) + ` ORDER BY ` + quoteIdent(pk))
		require.NoError(t, err)
		var keys []string
		for rows.Next() {
			var key string
			require.NoError(t, rows.Scan(&key))
			keys = append(keys, key)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		for _, key := range keys {
			payload, err := c.SerializeRow(context.Background(), name, key)
			require.NoError(t, err)
			payload, err = c.processPayloadForUpload(name, string(payload))
			require.NoError(t, err)
			result = append(result, oversync.SnapshotRow{Schema: "main", Table: name, Key: oversync.SyncKey{pk: key}, RowVersion: version, Payload: payload})
		}
	}
	return result
}

func editActivityRows(t *testing.T, rows []oversync.SnapshotRow, table string, keys map[string]map[string]any) {
	t.Helper()
	for i := range rows {
		row := &rows[i]
		if row.Table != table {
			continue
		}
		var key string
		for _, value := range row.Key {
			key = value.(string)
		}
		changes, ok := keys[key]
		if !ok {
			continue
		}
		var payload map[string]any
		require.NoError(t, json.Unmarshal(row.Payload, &payload))
		for name, value := range changes {
			payload[name] = value
		}
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		row.Payload = encoded
	}
}

func activityVector(t *testing.T, db *sql.DB) string {
	t.Helper()
	generation, _, err := activityfixture.Definition("main")
	require.NoError(t, err)
	names := []string{"app_settings", "_sync_row_state", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_upload_journal", "_sync_attachment_state", "_sync_source_state"}
	for _, table := range generation.Tables {
		names = append(names, table.Table)
	}
	sort.Strings(names)
	return recoveryVector(t, db, names...)
}

func TestFinalStateRecovery_ActivitySchema(t *testing.T) {
	for _, scenario := range []string{"DT01", "DT02", "DT03", "DT04", "DT05", "DT06", "DT07"} {
		t.Run(scenario, func(t *testing.T) {
			if scenario == "DT07" {
				activityPendingRecovery(t)
				return
			}
			c, db := newActivityRecoveryClient(t)
			rows := activitySnapshotRows(t, c, db, 2)
			local := recoveryVector(t, db, "app_settings")
			switch scenario {
			case "DT01":
				require.Len(t, c.config.Tables, 14)
				var value map[string]any
				for _, row := range rows {
					if row.Table == "activity" && row.Key["id"] == "A" {
						require.NoError(t, json.Unmarshal(row.Payload, &value))
						require.Equal(t, true, value["delete_when_expired"])
						require.Equal(t, false, value["days_confirm_required"])
					}
					if row.Table == "value_registry" && row.Key["key"] == "body-weight" {
						require.NoError(t, json.Unmarshal(row.Payload, &value))
						require.Equal(t, "73.25", value["numeric_value"])
						require.Equal(t, "2026-10-01T00:00:00.123456789Z", value["updated_at"])
					}
				}
			case "DT02":
				editActivityRows(t, rows, "profile", map[string]map[string]any{"profile-name": {"body_weight": "74", "unlock_codes": "[\"synthetic\"]"}})
			case "DT03", "DT04":
				editActivityRows(t, rows, "activity", map[string]map[string]any{"A": {"sched_write_ref_start_at": "date-b", "sched_write_ref_repeat_after_days": "repeat-b", "sched_write_ref_days_duration": "duration-b"}, "B": {"sched_write_ref_start_at": "date-a", "sched_write_ref_repeat_after_days": "repeat-a", "sched_write_ref_days_duration": "duration-a"}})
				editActivityRows(t, rows, "program_item_input_goal", map[string]map[string]any{"G": {"input_key": "reps"}, "H": {"input_key": "weight"}})
				editActivityRows(t, rows, "program_item_input_goal_revision", map[string]map[string]any{"R": {"revision": "2"}, "S": {"revision": "1"}})
			case "DT05":
				editActivityRows(t, rows, "daily_log", map[string]map[string]any{"L1": {"parent_daily_log_id": "L2"}, "L2": {"parent_daily_log_id": nil, "notes": "new parent"}})
			case "DT06":
				editActivityRows(t, rows, "program_item_input_goal_revision", map[string]map[string]any{"R": {"effective_day": "2026-1-01"}})
			}
			session := stageRecoverySnapshot(t, c, "activity-"+scenario, 2, rows)
			before := activityVector(t, db)
			err := c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{})
			if scenario == "DT06" {
				require.ErrorContains(t, err, "CHECK constraint")
				require.Equal(t, before, activityVector(t, db))
				return
			}
			require.NoError(t, err)
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
			require.Equal(t, local, recoveryVector(t, db, "app_settings"))
			actual := activitySnapshotRows(t, c, db, 2)
			require.Len(t, actual, len(rows))
			for i := range rows {
				require.Equal(t, rows[i].Table, actual[i].Table)
				require.Equal(t, rows[i].Key, actual[i].Key)
				var want, got any
				require.NoError(t, json.Unmarshal(rows[i].Payload, &want))
				require.NoError(t, json.Unmarshal(actual[i].Payload, &got))
				require.Equal(t, want, got, rows[i].Table)
			}
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
			if scenario == "DT03" {
				duplicate := activitySnapshotRows(t, c, db, 3)
				editActivityRows(t, duplicate, "activity", map[string]map[string]any{"A": {"sched_write_ref_start_at": "same"}, "B": {"sched_write_ref_start_at": "same"}})
				session = stageRecoverySnapshot(t, c, "activity-duplicate", 3, duplicate)
				before = activityVector(t, db)
				err = c.applyStagedSnapshotLocked(context.Background(), session, snapshotApplyOptions{})
				var blocked *FinalStateApplyBlockedError
				require.ErrorAs(t, err, &blocked)
				require.Equal(t, "conflicting_desired_state", blocked.Reason)
				require.Equal(t, before, activityVector(t, db))
			}
		})
	}
}

func activityPendingRecovery(t *testing.T) {
	for _, pending := range []string{"dirty", "prepared", "unsafe_committed", "ack_pending"} {
		t.Run(pending, func(t *testing.T) {
			c, db := newActivityRecoveryClient(t)
			c.config.RetryPolicy = &RetryPolicy{MaxAttempts: 1}
			server := newMockPushServer(t)
			_, err := db.Exec(`UPDATE daily_log SET notes='synthetic offline edit' WHERE id='L1'`)
			require.NoError(t, err)
			c.HTTP = &http.Client{Transport: server}
			switch pending {
			case "dirty":
				c.PauseUploads()
			case "prepared":
				c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
			case "unsafe_committed":
				server.pushHook = func(p *mockDirectPush) *http.Response {
					for i, row := range p.Rows {
						if row.Table == "daily_log" {
							var payload map[string]any
							require.NoError(t, json.Unmarshal(row.Payload, &payload))
							payload["notes"] = "server normalized"
							p.Rows[i].Payload = mustJSONPayload(t, payload)
						}
					}
					return server.defaultPushResponse(p)
				}
				c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
			case "ack_pending":
				c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/sync/push-outcomes/ack" {
						return nil, context.Canceled
					}
					return server.RoundTrip(r)
				})}
			}
			if pending != "dirty" {
				_, err = c.PushPending(context.Background())
				require.Error(t, err)
			}
			if pending == "unsafe_committed" {
				c.SetBeforePushReplayHook(nil)
				_, err = db.Exec(`CREATE TRIGGER unsafe_daily_log BEFORE UPDATE ON daily_log BEGIN SELECT RAISE(ABORT,'unsafe committed target');END`)
				require.NoError(t, err)
			}
			snapshots := 0
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.Contains(r.URL.Path, "snapshot-sessions") {
					snapshots++
				}
				return nil, context.Canceled
			})}
			before := activityVector(t, db)
			acks := len(server.ackRequests)
			require.NoError(t, c.RetryRecovery(context.Background()))
			_, err = c.Sync(context.Background())
			require.Error(t, err)
			require.Equal(t, before, activityVector(t, db))
			require.Zero(t, snapshots)
			require.Len(t, server.ackRequests, acks)
			if pending == "unsafe_committed" {
				require.NotEmpty(t, baselineText(t, db, `SELECT hex(outcome_body) FROM _sync_upload_journal`))
				require.Empty(t, server.resolveRequests)
			}
		})
	}
}
