package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type replayTableFingerprint struct {
	Table  string `json:"table"`
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

func replayStateTables(business string) []string {
	return []string{business, "_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state"}
}

func replayHashRow(h hash.Hash, row map[string]any) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(data)
	return nil
}

// This reproduces qualificationState's exact projections and row order without
// retaining a complete table. JSON decode/re-encode preserves its map semantics.
func replayStateFingerprint(db *sql.DB, business string) ([]replayTableFingerprint, error) {
	result := make([]replayTableFingerprint, 0, 10)
	for _, name := range replayStateTables(business) {
		columns, err := replayFingerprintColumns(db, name)
		if err != nil {
			return nil, err
		}
		var projections, ordering []string
		for _, column := range columns {
			q := quoteIdent(column)
			ordering = append(ordering, q)
			projections = append(projections, fmt.Sprintf(`'%s',json_object('storage',typeof(%s),'value',CASE WHEN typeof(%s)='blob' THEN lower(hex(%s)) WHEN %s IS NULL THEN NULL ELSE CAST(%s AS TEXT) END)`, strings.ReplaceAll(column, "'", "''"), q, q, q, q, q))
		}
		query := `SELECT json_object(` + strings.Join(projections, ",") + `) FROM ` + quoteIdent(name) + ` ORDER BY ` + strings.Join(ordering, ",")
		fingerprint, err := replayFingerprintTable(db, name, query)
		if err != nil {
			return nil, err
		}
		result = append(result, fingerprint)
	}
	return result, nil
}

func replayFingerprintColumns(db *sql.DB, name string) (columns []string, resultErr error) {
	rows, err := db.Query(`PRAGMA table_info(` + quoteIdent(name) + `)`)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var id, notnull, pk int
		var column, kind string
		var defaultValue any
		if err := rows.Scan(&id, &column, &kind, &notnull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("missing columns for replay state table %s", name)
	}
	return columns, nil
}

func replayFingerprintTable(db *sql.DB, name, query string) (result replayTableFingerprint, resultErr error) {
	rows, err := db.Query(query)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	h := sha256.New()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return result, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			return result, err
		}
		if err := replayHashRow(h, row); err != nil {
			return result, err
		}
		result.Rows++
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	result.Table = name
	result.SHA256 = hex.EncodeToString(h.Sum(nil))
	return result, nil
}

type replayMemorySnapshot struct {
	HeapAlloc           uint64 `json:"heap_alloc_bytes"`
	HeapInuse           uint64 `json:"heap_inuse_bytes"`
	TotalAlloc          uint64 `json:"total_alloc_bytes"`
	NextGC              uint64 `json:"next_gc_bytes"`
	NumGC               uint32 `json:"num_gc"`
	ProcessRSSHighWater uint64 `json:"process_cumulative_rss_high_water_bytes"`
}

func replayHeapSnapshot() replayMemorySnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return replayMemorySnapshot{HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, TotalAlloc: m.TotalAlloc, NextGC: m.NextGC, NumGC: m.NumGC, ProcessRSSHighWater: currentProcessPeakRSSBytes()}
}

type replayMemoryMeasurement struct {
	Version                 int                   `json:"version"`
	Mode                    string                `json:"mode"`
	ForcedGCBeforeCall      bool                  `json:"forced_gc_before_call"`
	Before                  replayMemorySnapshot  `json:"before_call"`
	After                   replayMemorySnapshot  `json:"after_call"`
	AllocatedBytes          uint64                `json:"allocated_bytes"`
	Peak                    map[string]int64      `json:"sampled_peak"`
	NativeObserverAvailable bool                  `json:"native_observer_available"`
	RetainedClientAlive     *replayMemorySnapshot `json:"after_gc_client_db_alive,omitempty"`
	GCPolicy                map[string]string     `json:"gc_environment"`
	sampler                 *replayMemorySampler
	finished                bool
}

func replayMemoryMode(enabled bool, mode string) (string, error) {
	if !enabled {
		return "", nil
	}
	if mode == "" {
		return "isolated_sdk", nil
	}
	if mode != "isolated_sdk" && mode != "whole_process" {
		return "", fmt.Errorf("unknown replay memory mode %q", mode)
	}
	return mode, nil
}

func prepareReplayMemoryMeasurement(mode string) *replayMemoryMeasurement {
	if mode == "" {
		return nil
	}
	measurement := &replayMemoryMeasurement{Version: 2, Mode: mode, ForcedGCBeforeCall: mode == "isolated_sdk", GCPolicy: map[string]string{}}
	for _, key := range []string{"GOGC", "GOMEMLIMIT", "GODEBUG"} {
		value, found := os.LookupEnv(key)
		if !found {
			value = "unset"
		}
		measurement.GCPolicy[key] = value
	}
	if measurement.ForcedGCBeforeCall {
		runtime.GC()
	}
	return measurement
}

func (measurement *replayMemoryMeasurement) start() {
	measurement.Before = replayHeapSnapshot()
	measurement.sampler = startReplayMemorySampler()
}

// Stop before observer aggregation, assertions and any retained-memory GC.
func (measurement *replayMemoryMeasurement) finish() {
	if measurement.finished {
		return
	}
	measurement.Peak = measurement.sampler.stop()
	measurement.After = replayHeapSnapshot()
	measurement.AllocatedBytes = measurement.After.TotalAlloc - measurement.Before.TotalAlloc
	measurement.NativeObserverAvailable = measurement.sampler.nativeObserved
	measurement.finished = true
}

func (measurement *replayMemoryMeasurement) captureRetained(c *Client, db *sql.DB) {
	measurement.finish()
	runtime.GC()
	snapshot := replayHeapSnapshot()
	measurement.RetainedClientAlive = &snapshot
	runtime.KeepAlive(c)
	runtime.KeepAlive(db)
}

func replayFingerprintLegacy(t *testing.T, vector qualificationVector, business string) []replayTableFingerprint {
	t.Helper()
	result := make([]replayTableFingerprint, 0, 10)
	for _, name := range replayStateTables(business) {
		h := sha256.New()
		for _, row := range vector[name] {
			require.NoError(t, replayHashRow(h, row))
		}
		result = append(result, replayTableFingerprint{Table: name, Rows: int64(len(vector[name])), SHA256: hex.EncodeToString(h.Sum(nil))})
	}
	return result
}

func newReplayFingerprintFixture(t *testing.T) (*sql.DB, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "fingerprint.sqlite")
	db, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, name := range replayStateTables("business") {
		_, err := db.Exec(`CREATE TABLE ` + quoteIdent(name) + `(id TEXT, payload BLOB, note TEXT, scalar, nullable, provenance TEXT, ordinal INTEGER, timestamp TEXT)`)
		require.NoError(t, err)
	}
	return db, file
}

func TestReplayAllocation_StreamingOracle(t *testing.T) {
	db, file := newReplayFingerprintFixture(t)
	assertSame := func() []replayTableFingerprint {
		actual, err := replayStateFingerprint(db, "business")
		require.NoError(t, err)
		require.Equal(t, replayFingerprintLegacy(t, qualificationState(t, db, "business"), "business"), actual)
		return actual
	}
	empty := assertSame()
	for _, name := range replayStateTables("business") {
		_, err := db.Exec(`INSERT INTO `+quoteIdent(name)+` VALUES(?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?)`, "b", []byte{0, 255}, "雪😀\"", int64(1), nil, `{"source":"exact"}`, 2, "2026-10-06T00:00:00Z", "a", []byte{}, "", "1", "null", "", 1, "")
		require.NoError(t, err)
	}
	populated := assertSame()
	require.NotEqual(t, empty, populated)
	require.NoError(t, db.Close())
	reopened, err := sql.Open("sqlite3", file)
	require.NoError(t, err)
	defer reopened.Close()
	actual, err := replayStateFingerprint(reopened, "business")
	require.NoError(t, err)
	require.Equal(t, populated, actual)
}

func TestReplayAllocation_StreamingOracleMutations(t *testing.T) {
	db, _ := newReplayFingerprintFixture(t)
	for _, name := range replayStateTables("business") {
		_, err := db.Exec(`INSERT INTO ` + quoteIdent(name) + ` VALUES('base',x'01','',1,NULL,'original',1,'original')`)
		require.NoError(t, err)
	}
	base, err := replayStateFingerprint(db, "business")
	require.NoError(t, err)
	mutations := []string{`UPDATE %s SET id='changed'`, `UPDATE %s SET payload=x'02'`, `UPDATE %s SET note='changed'`, `UPDATE %s SET scalar='1'`, `UPDATE %s SET nullable=''`, `UPDATE %s SET provenance='changed'`, `UPDATE %s SET ordinal=2`, `UPDATE %s SET timestamp='changed'`, `DELETE FROM %s`, `INSERT INTO %s SELECT * FROM %s`}
	for _, name := range replayStateTables("business") {
		for i, mutation := range mutations {
			t.Run(fmt.Sprintf("%s/%d", name, i), func(t *testing.T) {
				quoted := quoteIdent(name)
				var query string
				if i == len(mutations)-1 {
					query = fmt.Sprintf(mutation, quoted, quoted)
				} else {
					query = fmt.Sprintf(mutation, quoted)
				}
				_, err := db.Exec(`BEGIN`)
				require.NoError(t, err)
				defer func() { _, err := db.Exec(`ROLLBACK`); require.NoError(t, err) }()
				_, err = db.Exec(query)
				require.NoError(t, err)
				changed, err := replayStateFingerprint(db, "business")
				require.NoError(t, err)
				require.NotEqual(t, base, changed)
				require.Equal(t, replayFingerprintLegacy(t, qualificationState(t, db, "business"), "business"), changed)
			})
		}
	}
}

func TestReplayAllocation_StreamingOracleManagedReopen(t *testing.T) {
	c, db, path := qualificationDurableClient(t)
	replayDiagnosticBacklog(t, c, db, 3, 16)
	expected, err := replayStateFingerprint(db, "nodes")
	require.NoError(t, err)
	require.Equal(t, replayFingerprintLegacy(t, qualificationState(t, db, "nodes"), "nodes"), expected)
	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
	reopened, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	defer reopened.Close()
	actual, err := replayStateFingerprint(reopened, "nodes")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, replayFingerprintLegacy(t, qualificationState(t, reopened, "nodes"), "nodes"), actual)
}

func TestReplayAllocation_StreamingOracleErrors(t *testing.T) {
	db, _ := newReplayFingerprintFixture(t)
	_, err := db.Exec(`DROP TABLE _sync_conflict_state`)
	require.NoError(t, err)
	_, err = replayStateFingerprint(db, "business")
	require.ErrorContains(t, err, "missing columns")
	_, err = replayFingerprintTable(db, "business", `SELECT '{'`)
	require.Error(t, err)
	// The failed cursor has closed: a one-connection pool remains usable.
	var one int
	require.NoError(t, db.QueryRow(`SELECT 1`).Scan(&one))
	require.NoError(t, db.Close())
	_, err = replayStateFingerprint(db, "business")
	require.Error(t, err)
}

func TestReplayAllocation_MeasurementModes(t *testing.T) {
	for _, tc := range []struct {
		enabled    bool
		mode, want string
		wantErr    bool
	}{
		{false, "invalid", "", false}, {true, "", "isolated_sdk", false}, {true, "isolated_sdk", "isolated_sdk", false}, {true, "whole_process", "whole_process", false}, {true, "invalid", "", true},
	} {
		t.Run(fmt.Sprintf("%t/%s", tc.enabled, tc.mode), func(t *testing.T) {
			mode, err := replayMemoryMode(tc.enabled, tc.mode)
			require.Equal(t, tc.want, mode)
			require.Equal(t, tc.wantErr, err != nil)
		})
	}
	require.Nil(t, prepareReplayMemoryMeasurement(""))
	before := replayHeapSnapshot()
	isolated := prepareReplayMemoryMeasurement("isolated_sdk")
	after := replayHeapSnapshot()
	require.True(t, isolated.ForcedGCBeforeCall)
	require.Greater(t, after.NumGC, before.NumGC)
	require.False(t, prepareReplayMemoryMeasurement("whole_process").ForcedGCBeforeCall)
}

func TestReplayAllocation_MeasurementLifecycle(t *testing.T) {
	for _, cause := range []error{nil, errors.New("body failure"), context.Canceled} {
		t.Run(fmt.Sprint(cause), func(t *testing.T) {
			measurement := prepareReplayMemoryMeasurement("isolated_sdk")
			measurement.start()
			body := func() error { defer measurement.finish(); return cause }
			require.ErrorIs(t, body(), cause)
			select {
			case <-measurement.sampler.stopped:
			default:
				t.Fatal("sampler remains active")
			}
			peak := measurement.Peak["go_heap_alloc"]
			largeVerification := make([]byte, 8<<20)
			for i := range largeVerification {
				largeVerification[i] = 1
			}
			measurement.finish()
			require.Equal(t, peak, measurement.Peak["go_heap_alloc"], "verification must not update a stopped interval")
			runtime.KeepAlive(largeVerification)
			require.GreaterOrEqual(t, measurement.After.TotalAlloc, measurement.Before.TotalAlloc)
		})
	}
}

func TestReplayAllocation_SamplerCleanup(t *testing.T) {
	sampler := startReplayMemorySampler()
	first := sampler.stop()
	first["go_heap_alloc"] = -1
	second := sampler.stop()
	require.NotEqualValues(t, -1, second["go_heap_alloc"], "reports must not alias mutable sampler state")
	require.NotContains(t, second, "native_statements", "MEMUSED is bytes, not statement count")
}

func TestReplayAllocation_CanonicalPayloadComparison(t *testing.T) {
	type sample struct {
		name, left, right           string
		leftValid, rightValid, want bool
		errorText                   string
	}
	cases := []sample{
		{"absent", "invalid", "invalid", false, false, true, ""},
		{"presence", "invalid", "invalid", true, false, false, ""},
		{"identical", `{"a":"x"}`, `{"a":"x"}`, true, true, true, ""},
		{"reordered", `{"a":1,"b":2}`, ` {"b":2,"a":1.0} `, true, true, true, ""},
		{"different", `{"a":"x"}`, `{"a":"y"}`, true, true, false, ""},
		{"array_order", `[1,2]`, `[2,1]`, true, true, false, ""},
		{"exact_numeric_strings", `{"n":"9007199254740992"}`, `{"n":"9007199254740993"}`, true, true, false, ""},
		{"unicode", `{"雪":"😀"}`, `{"\u96ea":"\ud83d\ude00"}`, true, true, true, ""},
		{"identical_invalid", `{`, `{`, true, true, false, "failed to normalize"},
		{"invalid_utf8", string([]byte{0xff}), string([]byte{0xff}), true, true, false, "UTF-8"},
		{"duplicates", `{"a":1,"a":2}`, `{"a":1,"a":2}`, true, true, false, "duplicate"},
		{"trailing", `{} {}`, `{} {}`, true, true, false, "more than one"},
		{"surrogate", `{"a":"\ud800"}`, `{"a":"\ud800"}`, true, true, false, "surrogate"},
		{"excess_depth", strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66), "{}", true, true, false, "depth"},
		{"nonfinite_number", `{"n":1e400}`, `{"n":1e400}`, true, true, false, "finite"},
		{"left_error_first", `{"a":1,"a":2}`, string([]byte{0xff}), true, true, false, "duplicate"},
		{"right_error", `{}`, `{"a":1,"a":2}`, true, true, false, "duplicate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalPayloadEqual(sql.NullString{String: tc.left, Valid: tc.leftValid}, sql.NullString{String: tc.right, Valid: tc.rightValid})
			require.Equal(t, tc.want, got)
			if tc.errorText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errorText)
			}
		})
	}
}

func BenchmarkReplayAllocation_PayloadComparison(b *testing.B) {
	for _, width := range []int{16, 1024} {
		left := fmt.Sprintf(`{"a":"%s","b":1}`, strings.Repeat("x", width))
		for _, kind := range []string{"identical", "equivalent", "different"} {
			right := left
			if kind == "equivalent" {
				right = fmt.Sprintf(`{"b":1.0,"a":"%s"}`, strings.Repeat("x", width))
			}
			if kind == "different" {
				right = fmt.Sprintf(`{"a":"%s","b":2}`, strings.Repeat("x", width))
			}
			b.Run(fmt.Sprintf("%d/%s", width, kind), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(left) + len(right)))
				for i := 0; i < b.N; i++ {
					equal, err := canonicalPayloadEqual(sql.NullString{String: left, Valid: true}, sql.NullString{String: right, Valid: true})
					if err != nil || equal != (kind != "different") {
						b.Fatalf("comparison result: %t/%v", equal, err)
					}
				}
			})
		}
	}
}
