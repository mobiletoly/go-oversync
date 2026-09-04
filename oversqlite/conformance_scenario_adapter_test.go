package oversqlite

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type portableUpgradeScenario struct {
	ID                string           `json:"id"`
	Family            string           `json:"family"`
	InitialState      map[string]any   `json:"initial_state"`
	Actions           []map[string]any `json:"actions"`
	ProtocolExchanges []map[string]any `json:"protocol_exchanges"`
	Faults            []map[string]any `json:"faults"`
	Expected          map[string]any   `json:"expected"`
	Preserved         []string         `json:"preserved"`
	Invariants        []string         `json:"invariants"`
	tracker           *portableExecutionTracker
}

type portableUpgradeScenarioFile struct {
	Scenarios []portableUpgradeScenario `json:"scenarios"`
}

type portableUpgradeObservation struct {
	ID                string         `json:"id"`
	FixtureSHA256     string         `json:"fixture_sha256"`
	Observed          map[string]any `json:"observed"`
	Preserved         []string       `json:"preserved"`
	ActionCount       int            `json:"action_count"`
	ExchangeCount     int            `json:"exchange_count"`
	FaultCount        int            `json:"fault_count"`
	ConsumedActions   []string       `json:"consumed_actions"`
	ConsumedExchanges []string       `json:"consumed_exchanges"`
	ConsumedFaults    []string       `json:"consumed_faults"`
}

type portableExecutionTracker struct {
	actions, exchanges, faults                         []string
	consumedActions, consumedExchanges, consumedFaults []string
}

func portableActionIdentity(index int, action map[string]any) string {
	return fmt.Sprintf("%d:%s", index, action["kind"])
}

func portableExchangeIdentity(index int, exchange map[string]any) string {
	return fmt.Sprintf("%d:%s->%s", index, exchange["request"], exchange["response"])
}

func portableFaultIdentity(index int, fault map[string]any) string {
	return fmt.Sprintf("%d:%s:%s", index, fault["at"], fault["effect"])
}

func newPortableExecutionTracker(scenario portableUpgradeScenario) *portableExecutionTracker {
	tracker := &portableExecutionTracker{}
	for index, action := range scenario.Actions {
		tracker.actions = append(tracker.actions, portableActionIdentity(index, action))
	}
	for index, exchange := range scenario.ProtocolExchanges {
		tracker.exchanges = append(tracker.exchanges, portableExchangeIdentity(index, exchange))
	}
	for index, fault := range scenario.Faults {
		tracker.faults = append(tracker.faults, portableFaultIdentity(index, fault))
	}
	return tracker
}

func consumePortableIdentity(t *testing.T, expected []string, consumed *[]string, identity, kind string) {
	t.Helper()
	require.Less(t, len(*consumed), len(expected), "duplicate or undeclared %s consumption %s", kind, identity)
	require.Equal(t, expected[len(*consumed)], identity, "%s consumption is out of order", kind)
	*consumed = append(*consumed, identity)
}

func (s portableUpgradeScenario) consumeAction(t *testing.T, kind string) {
	t.Helper()
	index := len(s.tracker.consumedActions)
	consumePortableIdentity(t, s.tracker.actions, &s.tracker.consumedActions, fmt.Sprintf("%d:%s", index, kind), "action")
}

func (s portableUpgradeScenario) consumeExchange(t *testing.T) {
	t.Helper()
	index := len(s.tracker.consumedExchanges)
	require.Less(t, index, len(s.ProtocolExchanges))
	consumePortableIdentity(t, s.tracker.exchanges, &s.tracker.consumedExchanges, portableExchangeIdentity(index, s.ProtocolExchanges[index]), "exchange")
}

func (s portableUpgradeScenario) consumeFault(t *testing.T, at, effect string) {
	t.Helper()
	index := len(s.tracker.consumedFaults)
	consumePortableIdentity(t, s.tracker.faults, &s.tracker.consumedFaults, fmt.Sprintf("%d:%s:%s", index, at, effect), "fault")
}

func (t *portableExecutionTracker) completionError() error {
	if !slices.Equal(t.actions, t.consumedActions) {
		return fmt.Errorf("actions consumed %v; want %v", t.consumedActions, t.actions)
	}
	if !slices.Equal(t.exchanges, t.consumedExchanges) {
		return fmt.Errorf("exchanges consumed %v; want %v", t.consumedExchanges, t.exchanges)
	}
	if !slices.Equal(t.faults, t.consumedFaults) {
		return fmt.Errorf("faults consumed %v; want %v", t.consumedFaults, t.faults)
	}
	return nil
}

func TestClientUpgradeConformance_TrackerRejectsDeclaredButUnconsumedItems(t *testing.T) {
	scenario := portableUpgradeScenario{
		Actions:           []map[string]any{{"kind": "preview"}},
		ProtocolExchanges: []map[string]any{{"request": "request", "response": "response"}},
		Faults:            []map[string]any{{"at": "reply", "effect": "lost"}},
	}
	tests := []struct {
		name    string
		consume func(*portableExecutionTracker)
	}{
		{name: "action", consume: func(tracker *portableExecutionTracker) {
			tracker.consumedExchanges = append([]string(nil), tracker.exchanges...)
			tracker.consumedFaults = append([]string(nil), tracker.faults...)
		}},
		{name: "exchange", consume: func(tracker *portableExecutionTracker) {
			tracker.consumedActions = append([]string(nil), tracker.actions...)
			tracker.consumedFaults = append([]string(nil), tracker.faults...)
		}},
		{name: "fault", consume: func(tracker *portableExecutionTracker) {
			tracker.consumedActions = append([]string(nil), tracker.actions...)
			tracker.consumedExchanges = append([]string(nil), tracker.exchanges...)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newPortableExecutionTracker(scenario)
			test.consume(tracker)
			require.Error(t, tracker.completionError())
		})
	}
}

func TestClientUpgradeConformance_ExecutePortableScenario(t *testing.T) {
	// Historical fixture input only: this v1 corpus specifies the removed
	// capabilities and upload-session routes. P0000 direct-push behavior is
	// covered by the active compact push/replay and snapshot suites; it must not
	// be made to emulate these retired exchanges.
	t.Skip("historical client-upgrade v1 fixture input uses retired protocol routes")

	fixturePath := os.Getenv("OVERSYNC_CLIENT_UPGRADE_SCENARIO")
	if fixturePath != "" {
		body, err := os.ReadFile(fixturePath)
		require.NoError(t, err)
		var scenario portableUpgradeScenario
		require.NoError(t, json.Unmarshal(body, &scenario))
		observation := executePortableUpgradeScenario(t, scenario, body)
		encoded, err := json.Marshal(observation)
		require.NoError(t, err)
		outputPath := os.Getenv("OVERSYNC_CLIENT_UPGRADE_OBSERVATION")
		require.NotEmpty(t, outputPath)
		require.NoError(t, os.WriteFile(outputPath, encoded, 0o600))
		return
	}

	root := portableUpgradeCorpusRoot(t)
	seen := 0
	for _, relative := range []string{"scenarios/core.json", "scenarios/boundaries.json"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		require.NoError(t, err)
		var file portableUpgradeScenarioFile
		require.NoError(t, json.Unmarshal(body, &file))
		for _, scenario := range file.Scenarios {
			scenario := scenario
			t.Run(scenario.ID, func(t *testing.T) {
				canonical, err := json.Marshal(scenario)
				require.NoError(t, err)
				observation := executePortableUpgradeScenario(t, scenario, canonical)
				require.Equal(t, scenario.Expected, observation.Observed)
				require.Equal(t, scenario.Preserved, observation.Preserved)
			})
			seen++
		}
	}
	require.Equal(t, 24, seen)
}

func executePortableUpgradeScenario(t *testing.T, scenario portableUpgradeScenario, fixture []byte) portableUpgradeObservation {
	t.Helper()
	require.NotEmpty(t, scenario.ID)
	require.NotEmpty(t, scenario.InitialState)
	require.NotEmpty(t, scenario.Actions)
	require.NotEmpty(t, scenario.Expected)
	require.NotEmpty(t, scenario.Invariants)

	knownActions := map[string]bool{
		"preview": true, "resolve_old_intent": true, "pause": true, "discard_recreated_session": true,
		"old_keep_source_snapshot": true, "migrate": true, "create_replacement_snapshot": true,
		"stage_snapshot": true, "apply_and_publish": true, "create_final_schema": true,
		"offline_local_only_migration": true, "upgrade_device_a_same_file": true, "sync_device_b_old": true,
		"freeze_and_start": true, "enter_old_resolution": true, "record_drain_witness": true,
		"target_preview_and_recheck": true, "application_migration": true, "clear_and_reserve_source": true,
		"replacement_snapshot_create": true, "stage_verified_chunk": true, "apply_verify_publish_ready": true,
		"preview_unknown_generation": true, "preview_mismatched_fingerprint": true,
		"resolve_conflict_retry": true, "cancel_each_boundary": true,
	}
	for _, action := range scenario.Actions {
		kind, ok := action["kind"].(string)
		require.True(t, ok && knownActions[kind], "unconsumed action in %s: %v", scenario.ID, action)
	}
	for _, fault := range scenario.Faults {
		require.NotEmpty(t, fault["at"])
		require.Contains(t, []any{"lost", "process_death", "rollback", "expired", "cancelled", "ready"}, fault["effect"])
	}
	root := portableUpgradeCorpusRoot(t)
	for _, exchange := range scenario.ProtocolExchanges {
		for _, side := range []string{"request", "response"} {
			relative, ok := exchange[side].(string)
			require.True(t, ok)
			require.NotContains(t, relative, "..")
			_, err := os.ReadFile(filepath.Join(root, "wire", filepath.FromSlash(strings.TrimPrefix(relative, "wire/"))))
			require.NoError(t, err)
		}
	}
	validatePortableScenarioExecutionInputs(t, scenario)
	scenario.tracker = newPortableExecutionTracker(scenario)

	if phase, ok := scenario.InitialState["phase"].(string); ok {
		require.Contains(t, []string{
			string(UpgradePhaseReady), string(UpgradePhaseStarted), string(UpgradePhaseResolvingOldIntent),
			string(UpgradePhaseMigratingLocal), string(UpgradePhaseLocalMigrated),
			string(UpgradePhaseReattachPending), string(UpgradePhaseRebuildRunning),
		}, phase)
	}
	observed, preserved := observePortableUpgradeScenario(t, scenario)
	require.NoError(t, scenario.tracker.completionError())

	sum := sha256.Sum256(fixture)
	return portableUpgradeObservation{
		ID: scenario.ID, FixtureSHA256: fmt.Sprintf("%x", sum),
		Observed: observed, Preserved: preserved,
		ActionCount: len(scenario.tracker.consumedActions), ExchangeCount: len(scenario.tracker.consumedExchanges), FaultCount: len(scenario.tracker.consumedFaults),
		ConsumedActions:   append([]string(nil), scenario.tracker.consumedActions...),
		ConsumedExchanges: append([]string(nil), scenario.tracker.consumedExchanges...),
		ConsumedFaults:    append([]string(nil), scenario.tracker.consumedFaults...),
	}
}

func validatePortableScenarioExecutionInputs(t *testing.T, scenario portableUpgradeScenario) {
	t.Helper()
	wantActions := map[string][]string{
		"CU-N01": {"preview"}, "CU-N02": {"preview"}, "CU-D01": {"resolve_old_intent"},
		"CU-D02": {"pause", "discard_recreated_session"}, "CU-P01": {"old_keep_source_snapshot"},
		"CU-M01": {"migrate"}, "CU-R01": {"create_replacement_snapshot"},
		"CU-S01": {"stage_snapshot", "apply_and_publish"}, "CU-F01": {"create_final_schema"},
		"CU-L01": {"offline_local_only_migration"}, "CU-X01": {"upgrade_device_a_same_file", "sync_device_b_old"},
		"CU-U0": {"freeze_and_start"}, "CU-U1": {"enter_old_resolution"}, "CU-U2": {"record_drain_witness"},
		"CU-U3": {"target_preview_and_recheck"}, "CU-U4": {"application_migration"},
		"CU-U5": {"clear_and_reserve_source"}, "CU-U6": {"replacement_snapshot_create"},
		"CU-U7": {"stage_verified_chunk"}, "CU-U8": {"apply_verify_publish_ready"},
		"CU-N03": {"preview_unknown_generation"}, "CU-N04": {"preview_mismatched_fingerprint"},
		"CU-D03": {"resolve_conflict_retry"}, "CU-C01": {"cancel_each_boundary"},
	}
	wantFaults := map[string][]string{
		"CU-N01": {}, "CU-N02": {}, "CU-D01": {"commit_reply:lost"}, "CU-D02": {}, "CU-P01": {},
		"CU-M01": {"before_U4_commit:process_death", "after_U4_commit:process_death"},
		"CU-R01": {"reply:lost"},
		"CU-S01": {"session:expired", "before_U8_commit:process_death", "after_U8_commit:process_death"},
		"CU-F01": {}, "CU-L01": {"first_attempt:rollback"},
		"CU-X01": {"after_U6_remote_reply_before_local_receipt:process_death"},
		"CU-U0":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-U1":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-U2":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-U3":  {"preview_reply:lost"},
		"CU-U4":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-U5":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-U6":  {"remote_commit_before_local_receipt:process_death", "after_local_commit:process_death"},
		"CU-U7":  {"before_chunk_commit:process_death", "after_chunk_commit:process_death", "session:expired"},
		"CU-U8":  {"before_commit:process_death", "after_commit:process_death"},
		"CU-N03": {}, "CU-N04": {}, "CU-D03": {},
		"CU-C01": {
			"before_U0_commit:cancelled", "after_U0_commit:cancelled", "before_U1_commit:cancelled", "after_U1_commit:cancelled",
			"before_U2_commit:cancelled", "after_U2_commit:cancelled", "before_U3_commit:cancelled", "after_U3_commit:cancelled",
			"before_U4_commit:cancelled", "after_U4_commit:cancelled", "before_U5_commit:cancelled", "after_U5_commit:cancelled",
			"before_U6_commit:cancelled", "after_U6_commit:cancelled", "before_U7_commit:cancelled", "after_U7_commit:cancelled",
			"before_U8_commit:cancelled", "after_U8_commit:ready",
		},
	}
	actions, ok := wantActions[scenario.ID]
	require.True(t, ok, "scenario %s has no action contract", scenario.ID)
	gotActions := make([]string, 0, len(scenario.Actions))
	for _, action := range scenario.Actions {
		gotActions = append(gotActions, action["kind"].(string))
	}
	require.Equal(t, actions, gotActions, "scenario actions must select the production execution")
	faults, ok := wantFaults[scenario.ID]
	require.True(t, ok, "scenario %s has no fault contract", scenario.ID)
	gotFaults := make([]string, 0, len(scenario.Faults))
	for _, fault := range scenario.Faults {
		gotFaults = append(gotFaults, fmt.Sprint(fault["at"])+":"+fmt.Sprint(fault["effect"]))
	}
	require.Equal(t, faults, gotFaults, "scenario faults must select the production execution")

	if scenario.ID == "CU-N01" {
		require.Equal(t, "3", fmt.Sprint(scenario.Actions[0]["target_generation"]))
	}
	if scenario.ID == "CU-N02" {
		require.Equal(t, "2", fmt.Sprint(scenario.Actions[0]["target_generation"]))
	}
	if scenario.ID == "CU-M01" || scenario.ID == "CU-U4" {
		target := scenario.Actions[0]["to"]
		if scenario.ID == "CU-U4" {
			target = scenario.Actions[0]["target_version"]
		}
		require.Equal(t, "2", fmt.Sprint(target))
	}
}

func observePortableUpgradeScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	switch scenario.ID {
	case "CU-N01":
		return observePortableNegotiationScenario(t, scenario)
	case "CU-N02", "CU-N03", "CU-N04":
		return observePortableNegotiationScenario(t, scenario)
	case "CU-D01":
		return observePortableOldIntentReplyLoss(t, scenario)
	case "CU-D02":
		return observePortableExplicitResolution(t, scenario)
	case "CU-D03":
		return observePortableConflictRetry(t, scenario)
	case "CU-C01":
		return observePortableCancellationMatrix(t, scenario)
	case "CU-P01":
		return observePortablePrunedRecovery(t, scenario)
	case "CU-M01":
		return observePortableMigration(t, scenario)
	case "CU-R01":
		return observePortableReplacement(t, scenario)
	case "CU-S01":
		return observePortableTargetRebuild(t, scenario)
	case "CU-F01":
		return observePortableFreshScenario(t, scenario)
	case "CU-L01":
		return observePortableLocalOnlyScenario(t, scenario)
	case "CU-X01":
		return observePortableMobileScenario(t, scenario)
	case "CU-U0", "CU-U1", "CU-U2", "CU-U3", "CU-U4", "CU-U5", "CU-U6", "CU-U7", "CU-U8":
		return observePortableBoundaryScenario(t, scenario)
	default:
		t.Fatalf("portable scenario %s has no semantic executor", scenario.ID)
		return nil, nil
	}
}

func semanticPortablePhase(phase UpgradePhase) string {
	if phase == UpgradePhaseReady {
		return "old_ready"
	}
	return string(phase)
}

func portableUpgradeCorpusRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(source), "..", "conformance", "client-upgrade", "v1")
}
