package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"testing"

	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestAuthorityModesDeclareExactAdditiveGraphWithoutOmission(t *testing.T) {
	for _, mode := range []authorityMode{modeAdditive, modeSparse} {
		t.Run(string(mode), func(t *testing.T) {
			config, err := newFixtureConfig(mode)
			require.NoError(t, err)
			require.NoError(t, validateFixtureOnly(config))
			proofs, err := generationProofs(config)
			require.NoError(t, err)
			t.Logf("%s generation proofs: %v", mode, proofs)
			require.Len(t, config.generations, 2)

			one := config.generations[0]
			target := config.generations[1]
			require.Equal(t, oversync.SchemaGenerationID(1), one.ID)
			if mode == modeSparse {
				require.Equal(t, oversync.SchemaGenerationID(3), target.ID)
				require.NotContains(t, generationIDs(config.generations), oversync.SchemaGenerationID(2))
			} else {
				require.Equal(t, oversync.SchemaGenerationID(2), target.ID)
			}
			require.Equal(t, []string{"notes", "people"}, tableNames(one))
			require.Equal(t, []string{"attachments", "notes", "people", "projects"}, tableNames(target))
			for _, retained := range []string{"notes", "people"} {
				require.Contains(t, tableNames(target), retained)
			}

			people := tableByName(t, target, "people")
			require.Equal(t, []string{"details", "id", "name", "project_id"}, columnNames(people))
			require.True(t, columnByName(t, people, "details").Nullable)
			require.True(t, columnByName(t, people, "project_id").Nullable)
			require.Equal(t, []oversync.GenerationForeignKey{{
				Columns: []string{"project_id"}, ReferencedSchema: schemaName, ReferencedTable: "projects",
				ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeySetNull,
				OnUpdate: oversync.GenerationForeignKeyNoAction,
			}}, people.ForeignKeys)

			attachments := tableByName(t, target, "attachments")
			require.True(t, columnByName(t, attachments, "person_id").Nullable)
			require.Equal(t, oversync.GenerationForeignKeyCascade, attachments.ForeignKeys[0].OnDelete)
			require.Equal(t, "people", attachments.ForeignKeys[0].ReferencedTable)
		})
	}
}

func TestSparseModeHasNoGenerationTwoDescriptorOrProof(t *testing.T) {
	config, err := newFixtureConfig(modeSparse)
	require.NoError(t, err)
	proofs, err := generationProofs(config)
	require.NoError(t, err)
	require.Equal(t, []any{oversync.SchemaGenerationID(1), oversync.SchemaGenerationID(3)}, []any{proofs[0]["id"], proofs[1]["id"]})
}

func TestRetiredModeUsesImmutableLedgerAndExactOldProof(t *testing.T) {
	config, err := newFixtureConfig(modeRetired)
	require.NoError(t, err)
	require.Len(t, config.generations, 1)
	require.Equal(t, oversync.SchemaGenerationID(2), config.generations[0].ID)
	require.NotNil(t, config.readiness)
	ledger := config.readiness.Expected.GenerationLedger
	require.NotNil(t, ledger)
	require.Equal(t, int64(3), ledger.Revision)
	require.Equal(t, "retired", ledger.Entries[0].State)
	require.Equal(t, oversync.SchemaGenerationID(1), ledger.Entries[0].Proof.ID)
	require.Len(t, ledger.Entries[0].Proof.Fingerprint, 64)
	require.Equal(t, "supported", ledger.Entries[1].State)
	require.Equal(t, oversync.SchemaGenerationID(2), ledger.Entries[1].Proof.ID)
	proofs, err := generationProofs(config)
	require.NoError(t, err)
	require.Equal(t, "retired", proofs[0]["state"])
	require.Equal(t, ledger.Entries[0].Proof.Fingerprint, proofs[0]["fingerprint"])
	require.NoError(t, requireImmutableReadinessWrite(1))
	require.EqualError(
		t,
		requireImmutableReadinessWrite(0),
		"schema-evolution readiness evidence is immutable and does not match the stored ledger",
	)
}

func TestTargetPauseRejectsOnlyTargetAndLeavesOldWriterAvailable(t *testing.T) {
	config, err := newFixtureConfig(modeAdditive)
	require.NoError(t, err)
	gate := &targetGate{target: config.targetGeneration}
	gate.paused.Store(true)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := gate.middleware(next)

	oldRequest := httptest.NewRequest(http.MethodPost, "/sync/push", nil)
	oldRequest.Header.Set(oversync.SchemaGenerationIDHeader, "1")
	old := httptest.NewRecorder()
	handler.ServeHTTP(old, oldRequest)
	require.Equal(t, http.StatusNoContent, old.Code)

	targetRequest := httptest.NewRequest(http.MethodPost, "/sync/push", nil)
	targetRequest.Header.Set(oversync.SchemaGenerationIDHeader, strconv.FormatInt(int64(config.targetGeneration), 10))
	target := httptest.NewRecorder()
	handler.ServeHTTP(target, targetRequest)
	require.Equal(t, http.StatusServiceUnavailable, target.Code)
	require.Contains(t, target.Body.String(), `"retryable":true`)

	discovery := httptest.NewRecorder()
	handler.ServeHTTP(discovery, httptest.NewRequest(http.MethodGet, "/sync/generations/2", nil))
	require.Equal(t, http.StatusServiceUnavailable, discovery.Code)
}

func TestControlAndFaultSurfacesRequireAuthenticationAndBounds(t *testing.T) {
	config, err := newFixtureConfig(modeAdditive)
	require.NoError(t, err)
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	components := &server.ServerComponents{Handler: fallback}
	gate := &targetGate{target: config.targetGeneration}
	faults := &faultController{}
	handler := authorityHandler(components, config, "control-secret", gate, faults, newRequestCounters())
	statusUnauthorized := httptest.NewRecorder()
	handler.ServeHTTP(statusUnauthorized, httptest.NewRequest(http.MethodGet, "/schema-evolution/status", nil))
	require.Equal(t, http.StatusUnauthorized, statusUnauthorized.Code)
	statusWrongToken := httptest.NewRequest(http.MethodGet, "/schema-evolution/status", nil)
	statusWrongToken.Header.Set("X-Schema-Evolution-Control-Token", "wrong-secret")
	statusWrong := httptest.NewRecorder()
	handler.ServeHTTP(statusWrong, statusWrongToken)
	require.Equal(t, http.StatusUnauthorized, statusWrong.Code)

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/schema-evolution/control", bytes.NewBufferString(`{"action":"pause_target"}`)))
	require.Equal(t, http.StatusUnauthorized, unauthorized.Code)

	pauseRequest := httptest.NewRequest(http.MethodPost, "/schema-evolution/control", bytes.NewBufferString(`{"action":"pause_target"}`))
	pauseRequest.Header.Set("X-Schema-Evolution-Control-Token", "control-secret")
	paused := httptest.NewRecorder()
	handler.ServeHTTP(paused, pauseRequest)
	require.Equal(t, http.StatusOK, paused.Code)
	require.True(t, gate.paused.Load())

	faultRequest := httptest.NewRequest(http.MethodPost, "/schema-evolution/fault", bytes.NewBufferString(`{"kind":"lost_reply","count":9}`))
	faultRequest.Header.Set("X-Schema-Evolution-Control-Token", "control-secret")
	fault := httptest.NewRecorder()
	handler.ServeHTTP(fault, faultRequest)
	require.Equal(t, http.StatusBadRequest, fault.Code)
	require.False(t, faults.consumeLostReply())
}

func TestControlTokenMiddlewareForwardsAuthorizedStatus(t *testing.T) {
	called := false
	protected := requireControlToken("control-secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/schema-evolution/status", r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/schema-evolution/status", nil)
	request.Header.Set("X-Schema-Evolution-Control-Token", "control-secret")
	response := httptest.NewRecorder()
	protected.ServeHTTP(response, request)
	require.True(t, called)
	require.Equal(t, http.StatusNoContent, response.Code)
}

func TestRequestCountersKeepScopesAndProofsSeparate(t *testing.T) {
	counters := newRequestCounters()
	first := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
	first.Header.Set(oversync.SchemaGenerationIDHeader, "1")
	first.Header.Set(oversync.SchemaGenerationFingerprintHeader, "aaa")
	second := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
	second.Header.Set(oversync.SchemaGenerationIDHeader, "2")
	second.Header.Set(oversync.SchemaGenerationFingerprintHeader, "bbb")
	counters.record(first)
	counters.record(second)
	snapshot := counters.snapshot()
	require.Equal(t, int64(2), snapshot["paths"].(map[string]int64)["POST /sync/connect"])
	require.Equal(t, int64(1), snapshot["proofs"].(map[string]int64)["1/aaa"])
	require.Equal(t, int64(1), snapshot["proofs"].(map[string]int64)["2/bbb"])
}

func TestLostReplyRuleIsBoundedAndConsumedExactlyOnce(t *testing.T) {
	faults := &faultController{}
	require.NoError(t, faults.setCommitRejection(http.StatusUnprocessableEntity, 1))
	status, rejected := faults.consumeCommitRejection()
	require.True(t, rejected)
	require.Equal(t, http.StatusUnprocessableEntity, status)
	_, rejected = faults.consumeCommitRejection()
	require.False(t, rejected)
	require.Error(t, faults.setCommitRejection(http.StatusOK, 1))
	require.Error(t, faults.setCommitRejection(http.StatusServiceUnavailable, 9))
	require.NoError(t, faults.setLostReply(1))
	require.True(t, faults.consumeLostReply())
	require.False(t, faults.consumeLostReply())
	require.Error(t, faults.setLostReply(-1))
	require.Error(t, faults.setLostReply(9))
}

func generationIDs(generations []oversync.SchemaGeneration) []oversync.SchemaGenerationID {
	result := make([]oversync.SchemaGenerationID, 0, len(generations))
	for _, generation := range generations {
		result = append(result, generation.ID)
	}
	return result
}

func tableNames(generation oversync.SchemaGeneration) []string {
	result := make([]string, 0, len(generation.Tables))
	for _, table := range generation.Tables {
		result = append(result, table.Table)
	}
	sort.Strings(result)
	return result
}

func tableByName(t *testing.T, generation oversync.SchemaGeneration, name string) oversync.GenerationTable {
	t.Helper()
	for _, table := range generation.Tables {
		if table.Table == name {
			return table
		}
	}
	t.Fatalf("table %s not found", name)
	return oversync.GenerationTable{}
}

func columnNames(table oversync.GenerationTable) []string {
	result := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		result = append(result, column.Name)
	}
	sort.Strings(result)
	return result
}

func columnByName(t *testing.T, table oversync.GenerationTable, name string) oversync.GenerationColumn {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			return column
		}
	}
	t.Fatalf("column %s not found", name)
	return oversync.GenerationColumn{}
}
