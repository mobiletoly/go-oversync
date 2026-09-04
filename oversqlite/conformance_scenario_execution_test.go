package oversqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type portableBoundaryState struct {
	path          string
	phase         UpgradePhase
	drainComplete bool
	version       int64
	ready         bool
	newSourceID   string
	sourceID      string
	generation    int64
	fingerprint   string
	checkpoint    int64
	stagedRows    int64
	localNote     string
	localDigest   string
	oldSourceID   string
	reason        string
	snapshotID    string
	columns       string
	contractValid bool
}

type portableE2EObservation struct {
	PushOutboxAfterDeath string `json:"push_outbox_after_death"`
	PushLocatorPreserved bool   `json:"push_locator_preserved"`
	FirstRequestCommits  int    `json:"first_request_commits"`
	NewerIntentPreserved bool   `json:"newer_intent_preserved"`
	ReplacementPhase     string `json:"replacement_phase"`
	ReplacementExact     bool   `json:"replacement_exact"`
	FinalOutcome         string `json:"final_outcome"`
	OldSource            string `json:"old_source"`
	NewSource            string `json:"new_source"`
}

type portableWireExchangeCorpus struct {
	Exchanges []struct {
		Method         string            `json:"method"`
		Path           string            `json:"path"`
		RequestBody    string            `json:"request_body"`
		RequestHeaders map[string]string `json:"request_headers"`
		Status         string            `json:"status"`
	} `json:"exchanges"`
}

func portableWireExchange(t *testing.T, scenario portableUpgradeScenario) (string, string, map[string]string, int) {
	t.Helper()
	require.Len(t, scenario.ProtocolExchanges, 1)
	wanted := strings.TrimPrefix(scenario.ProtocolExchanges[0]["request"].(string), "wire/")
	body, err := os.ReadFile(filepath.Join(portableUpgradeCorpusRoot(t), "wire", "exchanges.json"))
	require.NoError(t, err)
	var corpus portableWireExchangeCorpus
	require.NoError(t, json.Unmarshal(body, &corpus))
	for _, exchange := range corpus.Exchanges {
		if exchange.RequestBody == wanted {
			status, err := strconv.Atoi(exchange.Status)
			require.NoError(t, err)
			return exchange.Method, exchange.Path, exchange.RequestHeaders, status
		}
	}
	t.Fatalf("no portable wire exchange uses %s", wanted)
	return "", "", nil, 0
}

func portableRequestBody(t *testing.T, request *http.Request) []byte {
	t.Helper()
	if request.Body == nil {
		return []byte{}
	}
	body, err := io.ReadAll(request.Body)
	require.NoError(t, err)
	return body
}

var (
	portableE2EOnce sync.Once
	portableE2EData portableE2EObservation
	portableE2EErr  error
)

func productionE2EObservation(t *testing.T) portableE2EObservation {
	t.Helper()
	portableE2EOnce.Do(func() {
		repositoryRoot := filepath.Clean(filepath.Join(portableUpgradeCorpusRoot(t), "..", "..", ".."))
		evidence, err := os.CreateTemp("", "upgrade-e2e-observation-*.json")
		if err != nil {
			portableE2EErr = err
			return
		}
		path := evidence.Name()
		_ = evidence.Close()
		defer os.Remove(path)
		command := exec.Command("go", "test", "./oversqlite_e2e", "-run", "^TestUpgrade_ProductionRemoteCommitProcessDeath$", "-count=1", "-timeout=3m30s")
		command.Dir = repositoryRoot
		command.Env = append(os.Environ(), "GOTOOLCHAIN=go1.25.12", "OVERSYNC_UPGRADE_E2E_OBSERVATION="+path)
		output, err := command.CombinedOutput()
		if err != nil {
			portableE2EErr = fmt.Errorf("production e2e scenario: %w: %s", err, output)
			return
		}
		body, err := os.ReadFile(path)
		if err != nil {
			portableE2EErr = err
			return
		}
		portableE2EErr = json.Unmarshal(body, &portableE2EData)
	})
	require.NoError(t, portableE2EErr)
	return portableE2EData
}

func observePortableOldIntentReplyLoss(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	actual := productionE2EObservation(t)
	waiting := actual.PushOutboxAfterDeath == outboxStatePrepared && actual.PushLocatorPreserved
	scenario.consumeAction(t, "resolve_old_intent")
	if waiting && actual.FirstRequestCommits == 1 {
		scenario.consumeFault(t, "commit_reply", "lost")
	}
	return map[string]any{
		"outcome":      map[bool]string{true: "waiting_for_network", false: "invalid"}[waiting],
		"retry":        map[bool]string{true: "same_q39_request", false: "different_request"}[actual.FirstRequestCommits == 1],
		"newer_intent": map[bool]string{true: "preserved", false: "lost"}[actual.NewerIntentPreserved],
	}, preservedWhen(waiting && actual.FirstRequestCommits == 1, "outbox", "receipt", "source_sequence")
}

func observePortableReplacement(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	root := portableUpgradeCorpusRoot(t)
	requestFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["request"].(string))))
	require.NoError(t, err)
	responseFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["response"].(string))))
	require.NoError(t, err)
	fixtureMethod, fixturePath, fixtureHeaders, fixtureStatus := portableWireExchange(t, scenario)
	path := filepath.Join(t.TempDir(), "portable-replacement-retry.sqlite")
	plan := prepareU6ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	var requests [][]byte
	wireMatches := []bool{}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-replacement.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				body, readErr := io.ReadAll(request.Body)
				require.NoError(t, readErr)
				requests = append(requests, body)
				headersMatch := true
				for name, value := range fixtureHeaders {
					if name != "Authorization" && request.Header.Get(name) != value {
						headersMatch = false
					}
				}
				wireMatches = append(wireMatches, request.Method == fixtureMethod && request.URL.Path == fixturePath && headersMatch)
				if len(requests) == 1 {
					return nil, io.ErrUnexpectedEOF
				}
				return rawJSONResponse(http.StatusOK, string(responseFixture)), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/snapshot-1":
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected replacement retry request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	exact := result.Status.Outcome == UpgradeOutcomeReady && len(requests) >= 2 && len(wireMatches) >= 2 && wireMatches[0] && wireMatches[1] && fixtureStatus == http.StatusOK
	for _, request := range requests {
		exact = exact && bytes.Equal(request, requestFixture)
	}

	path = filepath.Join(t.TempDir(), "portable-replacement-divergence.sqlite")
	plan = prepareU6ProcessTestDatabase(t, path)
	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	coordinator, err = newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-replacement.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				return errorJSONResponse(http.StatusConflict, oversync.SourceRetiredResponse{
					Error: "source_retired", SourceID: "source-old", ReplacedBySourceID: "different-source",
				}), nil
			default:
				return nil, fmt.Errorf("unexpected replacement divergence request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, runErr := coordinator.Advance(context.Background())
	require.Error(t, runErr)
	diverged := result.Status.Outcome == UpgradeOutcomeReplacementDiverged
	scenario.consumeAction(t, "create_replacement_snapshot")
	if exact {
		scenario.consumeExchange(t)
		scenario.consumeFault(t, "reply", "lost")
	}
	return map[string]any{
		"retry":      map[bool]string{true: "exact_tuple", false: "different_tuple"}[exact],
		"divergence": map[bool]string{true: "replacement_diverged", false: "accepted"}[diverged],
	}, preservedWhen(exact && diverged, "tuple", "old_source_evidence")
}

func observePortableExplicitResolution(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-resolution.sqlite")
	plan := prepareNeedsResolutionProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	var pushBodies [][]byte
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-resolution.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/push":
				pushBodies = append(pushBodies, portableRequestBody(t, request))
				if len(pushBodies) == 1 {
					return nil, io.ErrUnexpectedEOF
				}
				return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{
					"error": "transition_input_invalid", "message": "discard requires a definitive rejection",
				}), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				return jsonResponse(oversync.SnapshotSession{SnapshotID: "portable-resolution-snapshot", ExpiresAt: "2030-01-01T00:00:00Z"}), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/portable-resolution-snapshot":
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "portable-resolution-snapshot"}), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/portable-resolution-snapshot":
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected explicit-resolution request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeNeedsResolution, rec.Outcome)
	initialOutcome := rec.Outcome
	scenario.consumeAction(t, "pause")
	initialOutbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	firstErr := coordinator.discardPendingIntent(context.Background(), rec)
	require.Error(t, firstErr)
	midOutbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, initialOutbox.CanonicalRequestHash, midOutbox.CanonicalRequestHash)
	require.Equal(t, outboxStatePrepared, midOutbox.State)
	require.NoError(t, coordinator.discardPendingIntent(context.Background(), rec))
	scenario.consumeAction(t, "discard_recreated_session")
	finalOutbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	drained, reason, err := inspectOldDrain(context.Background(), db)
	require.NoError(t, err)
	exact := false
	if len(pushBodies) == 2 {
		var first, second oversync.CompactPushRequest
		require.NoError(t, json.Unmarshal(pushBodies[0], &first))
		require.NoError(t, json.Unmarshal(pushBodies[1], &second))
		exact = bytes.Equal(pushBodies[0], pushBodies[1]) && first.CanonicalRequestHash == initialOutbox.CanonicalRequestHash &&
			second.CanonicalRequestHash == initialOutbox.CanonicalRequestHash
	}
	complete := drained && reason == "" && finalOutbox.State == outboxStateNone
	normalization, extraEffects, emptyProgress := observePortableAuthoritativeSettlement(t)
	return map[string]any{
		"outcome":                         string(initialOutcome),
		"normalization_and_extra_effects": map[bool]string{true: "verified", false: "mismatch"}[normalization && extraEffects],
		"empty_progress":                  map[bool]string{true: "contiguous", false: "incomplete"}[emptyProgress],
	}, preservedWhen(exact && complete && normalization && extraEffects && emptyProgress, "frozen_request_until_decision")
}

func observePortableConflictRetry(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.Resolver = &ClientWinsResolver{}
	const userID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	seedSyncedUserRow(t, client, db, userID, "Ada", 1)
	_, err := db.Exec(`UPDATE users SET name='Ada Local' WHERE id=?`, userID)
	require.NoError(t, err)

	root := portableUpgradeCorpusRoot(t)
	requestFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["request"].(string))))
	require.NoError(t, err)
	responseFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["response"].(string))))
	require.NoError(t, err)
	fixtureMethod, fixturePath, fixtureHeaders, fixtureStatus := portableWireExchange(t, scenario)
	server := newMockPushServer(t)
	successHook := newFixedVersionPushHook(t, server, 3)
	var sourceBundleIDs []int64
	var pushBodies [][]byte
	wireExchangeMatched := false
	attempt := 0
	transport := rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			body, readErr := io.ReadAll(request.Body)
			require.NoError(t, readErr)
			pushBodies = append(pushBodies, body)
			request.Body = io.NopCloser(bytes.NewReader(body))
			headersMatch := true
			for name, value := range fixtureHeaders {
				if name != "Authorization" && request.Header.Get(name) != value {
					headersMatch = false
				}
			}
			wireExchangeMatched = request.Method == fixtureMethod && request.URL.Path == fixturePath &&
				bytes.Equal(body, requestFixture) && fixtureStatus == http.StatusConflict && headersMatch
		}
		return server.RoundTrip(request)
	})
	var retryPayload []byte
	server.pushHook = func(session *mockDirectPush) *http.Response {
		attempt++
		sourceBundleIDs = append(sourceBundleIDs, session.SourceBundleID)
		if attempt == 1 {
			return rawJSONResponse(http.StatusConflict, string(responseFixture))
		}
		require.Equal(t, int64(2), session.Rows[0].BaseRowVersion)
		retryPayload = append([]byte(nil), session.Rows[0].Payload...)
		return successHook(session)
	}
	client.HTTP = &http.Client{Transport: transport}
	report, err := client.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, PushOutcomeCommitted, report.Outcome)
	require.Len(t, pushBodies, 2)
	require.True(t, wireExchangeMatched)
	var retry oversync.CompactPushRequest
	require.NoError(t, json.Unmarshal(pushBodies[1], &retry))
	require.Equal(t, int64(1), retry.SourceBundleID)
	require.Len(t, server.pushRequests, 2)
	require.JSONEq(t, `{"id":"`+userID+`","name":"Ada Local","email":"ada@example.com"}`, string(retryPayload))
	next, _ := requireClientBundleState(t, db, client.UserID)
	exact := sourceBundleIDs[0] == 1 && sourceBundleIDs[1] == 1 && next == 2
	scenario.consumeAction(t, "resolve_conflict_retry")
	if exact && wireExchangeMatched {
		scenario.consumeExchange(t)
	}
	return map[string]any{
		"outcome":         "committed",
		"requeued_intent": map[bool]string{true: "exact", false: "changed"}[exact],
		"source_sequence": map[bool]string{true: "advanced_once", false: "invalid"}[next == 2],
	}, preservedWhen(exact, "resolved_request_binding")
}

func observePortableCancellationMatrix(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	wantPhases := map[string]UpgradePhase{
		"before_U0_commit": UpgradePhaseReady, "after_U0_commit": UpgradePhaseStarted,
		"before_U1_commit": UpgradePhaseStarted, "after_U1_commit": UpgradePhaseResolvingOldIntent,
		"before_U2_commit": UpgradePhaseResolvingOldIntent, "after_U2_commit": UpgradePhaseResolvingOldIntent,
		"before_U3_commit": UpgradePhaseResolvingOldIntent, "after_U3_commit": UpgradePhaseMigratingLocal,
		"before_U4_commit": UpgradePhaseMigratingLocal, "after_U4_commit": UpgradePhaseLocalMigrated,
		"before_U5_commit": UpgradePhaseLocalMigrated, "after_U5_commit": UpgradePhaseReattachPending,
		"before_U6_commit": UpgradePhaseReattachPending, "after_U6_commit": UpgradePhaseRebuildRunning,
		"before_U7_commit": UpgradePhaseRebuildRunning, "after_U7_commit": UpgradePhaseRebuildRunning,
		"before_U8_commit": UpgradePhaseRebuildRunning, "after_U8_commit": UpgradePhaseReady,
	}
	matrix := make(map[string]any, len(scenario.Faults))
	preserved := true
	for _, fault := range scenario.Faults {
		name := fault["at"].(string)
		state, outcome, intact := runPortableBoundaryCancellation(t, name)
		require.Equal(t, wantPhases[name], state.phase, "cancellation phase at %s", name)
		wantOutcome := UpgradeOutcomeCancelled
		if name == "after_U8_commit" {
			wantOutcome = UpgradeOutcomeReady
		}
		require.Equal(t, wantOutcome, outcome, "cancellation outcome at %s", name)
		matrix[name] = map[string]any{"phase": string(state.phase), "outcome": string(outcome)}
		preserved = preserved && intact
		scenario.consumeFault(t, name, string(wantOutcome))
	}
	scenario.consumeAction(t, "cancel_each_boundary")
	return map[string]any{
		"outcome":      "boundary_specific",
		"matrix":       matrix,
		"preservation": map[bool]string{true: "verified", false: "failed"}[preserved],
	}, preservedWhen(preserved, "durable_phase", "installed_identity", "local_only", "intent_or_snapshot_evidence")
}

func runPortableBoundaryCancellation(t *testing.T, name string) (portableBoundaryState, UpgradeOutcome, bool) {
	t.Helper()
	boundary := strings.TrimSuffix(strings.TrimPrefix(name, "before_"), "_commit")
	if strings.HasPrefix(name, "after_") {
		boundary = strings.TrimSuffix(strings.TrimPrefix(name, "after_"), "_commit")
	}
	path := filepath.Join(t.TempDir(), "portable-cancel.sqlite")
	var plan UpgradePlan
	switch boundary {
	case "U0":
		plan = prepareGenerationReadyProcessTestDatabase(t, path)
	case "U1":
		plan = prepareGenerationBoundaryPhase(t, path, UpgradePhaseStarted)
	case "U2":
		plan = prepareGenerationBoundaryPhase(t, path, UpgradePhaseResolvingOldIntent)
	case "U3":
		plan = prepareGenerationDrainedProcessTestDatabase(t, path)
	case "U4":
		preparePortableGenerationU4(t, path)
		plan = portableGenerationU4Plan(t)
	case "U5":
		plan = prepareLocalMigratedProcessTestDatabase(t, path)
	case "U6":
		plan = prepareU6ProcessTestDatabase(t, path)
	case "U7":
		plan = prepareU7ProcessTestDatabase(t, path)
	case "U8":
		plan = prepareU8ProcessTestDatabase(t, path)
	default:
		t.Fatalf("unknown cancellation boundary %s", boundary)
	}
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	beforeJournal, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	expectedReplacement := replacementTupleFromJournal(beforeJournal)
	requireReplacement := boundary == "U5" || boundary == "U6" || boundary == "U7" || boundary == "U8"
	beforeNote, beforeLocalDigest := portableCancellationLocalState(t, db, beforeJournal, plan)
	ctx, cancel := context.WithCancel(context.Background())
	targetHook := name
	row := processSnapshotRow()
	config := &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-cancel.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
					return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
				}
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
				return jsonResponse(oversync.PullResponse{StableBundleSeq: 0}), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				return jsonResponse(oversync.SnapshotSession{SnapshotID: "boundary-snapshot", SnapshotBundleSeq: 7, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/process-snapshot":
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "process-snapshot", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{row}, NextRowOrdinal: 1, ByteCount: testSnapshotRowsByteCount([]oversync.SnapshotRow{row})}), nil
			case request.Method == http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected cancellation request %s %s", request.Method, request.URL.Path)
			}
		})},
		testBoundary: func(reached string) {
			if reached == targetHook {
				if reached == "after_U5_commit" {
					committed, loadErr := loadUpgradeJournal(context.Background(), db)
					require.NoError(t, loadErr)
					expectedReplacement = replacementTupleFromJournal(committed)
				}
				cancel()
			}
		},
	}
	if boundary == "U4" {
		portable := portableGenerationU4Config(t, db)
		config.HTTP = portable.HTTP
	}
	coordinator, err := newTestOpening(db, config)
	require.NoError(t, err)
	result, runErr := coordinator.Advance(ctx)
	if name != "after_U8_commit" {
		require.Error(t, runErr)
	}
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())

	inspect, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	inspect.SetMaxOpenConns(1)
	defer inspect.Close()
	rec, err := loadUpgradeJournal(context.Background(), inspect)
	require.NoError(t, err)
	identity, err := loadUpgradeIdentity(context.Background(), inspect)
	require.NoError(t, err)
	state := portableBoundaryState{path: path, phase: rec.Phase, newSourceID: rec.NewSourceID, oldSourceID: rec.OldSourceID, reason: rec.ReplacementReason, snapshotID: rec.SnapshotID}
	_ = inspect.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&state.localNote)
	intact := portableCancellationEvidenceIntact(t, inspect, plan, rec, identity, beforeNote, beforeLocalDigest, requireReplacement, expectedReplacement)
	return state, result.Status.Outcome, intact
}

type portableReplacementTuple struct {
	oldSourceID                 string
	newSourceID                 string
	reason                      string
	targetGenerationID          int64
	targetGenerationFingerprint string
}

func replacementTupleFromJournal(rec *upgradeJournalRecord) portableReplacementTuple {
	if rec == nil {
		return portableReplacementTuple{}
	}
	return portableReplacementTuple{
		oldSourceID:                 rec.OldSourceID,
		newSourceID:                 rec.NewSourceID,
		reason:                      rec.ReplacementReason,
		targetGenerationID:          rec.TargetGenerationID,
		targetGenerationFingerprint: rec.TargetGenerationFingerprint,
	}
}

func replacementTupleMatchesJournal(want portableReplacementTuple, rec *upgradeJournalRecord) bool {
	return rec != nil && want == replacementTupleFromJournal(rec)
}

func portableCancellationLocalState(t *testing.T, db *sql.DB, rec *upgradeJournalRecord, plan UpgradePlan) (string, string) {
	t.Helper()
	var note string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
	var (
		digest string
		err    error
	)
	if rec != nil && rec.Phase != UpgradePhaseReady {
		digest, err = localOnlyDigestWithUpgradeGuards(context.Background(), db, plan.LocalOnlyTables)
	} else {
		digest, err = localOnlyDigest(context.Background(), db, plan.LocalOnlyTables)
	}
	require.NoError(t, err)
	return note, digest
}

func portableCancellationEvidenceIntact(
	t *testing.T,
	db *sql.DB,
	plan UpgradePlan,
	rec *upgradeJournalRecord,
	identity *upgradeIdentityRecord,
	beforeNote string,
	beforeLocalDigest string,
	requireReplacement bool,
	expectedReplacement portableReplacementTuple,
) bool {
	t.Helper()
	require.NotNil(t, rec)
	require.NotNil(t, identity)

	completedTarget := rec.Phase == UpgradePhaseReady && rec.PlanID == plan.PlanID
	wantGeneration := plan.testInstalled.Proof
	wantVersion := plan.InstalledVersion
	if completedTarget {
		wantGeneration = plan.testTarget.Proof
		wantVersion = plan.TargetVersion
	} else {
		switch rec.Phase {
		case UpgradePhaseLocalMigrated, UpgradePhaseReattachPending, UpgradePhaseRebuildRunning:
			wantVersion = plan.TargetVersion
		}
	}
	require.Equal(t, plan.DatabaseID, identity.DatabaseID)
	require.Equal(t, plan.ServiceID, identity.ServiceID)
	require.Equal(t, plan.UserID, identity.UserID)
	require.Equal(t, int64(wantGeneration.ID), identity.GenerationID)
	require.Equal(t, wantGeneration.Fingerprint, identity.GenerationFingerprint)
	require.Equal(t, wantVersion, identity.LocalVersion)
	require.Equal(t, rec.Phase == UpgradePhaseReady, identity.Ready)
	if rec.Phase != UpgradePhaseReady || completedTarget {
		require.Equal(t, plan.PlanID, rec.PlanID)
		require.Equal(t, plan.DatabaseID, rec.DatabaseID)
		require.Equal(t, plan.ServiceID, rec.ServiceID)
		require.Equal(t, plan.UserID, rec.UserID)
		require.Equal(t, int64(plan.testInstalled.Proof.ID), rec.OldGenerationID)
		require.Equal(t, plan.testInstalled.Proof.Fingerprint, rec.OldGenerationFingerprint)
		require.Equal(t, plan.InstalledVersion, rec.OldVersion)
		require.Equal(t, int64(plan.testTarget.Proof.ID), rec.TargetGenerationID)
		require.Equal(t, plan.testTarget.Proof.Fingerprint, rec.TargetGenerationFingerprint)
		require.Equal(t, plan.TargetVersion, rec.TargetVersion)
	}
	if requireReplacement {
		require.True(t, replacementTupleMatchesJournal(expectedReplacement, rec), "cancellation must preserve the exact source-replacement tuple and target proof")
	}

	afterNote, afterLocalDigest := portableCancellationLocalState(t, db, rec, plan)
	require.Equal(t, beforeNote, afterNote)
	require.Equal(t, beforeLocalDigest, afterLocalDigest)

	outbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	var outboxRows, dirtyRows, pushStageRows int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&pushStageRows))
	require.Equal(t, outbox.RowCount, outboxRows)
	if outbox.State == outboxStateNone {
		require.Empty(t, outbox.CanonicalRequestHash)
		require.Zero(t, outboxRows)
		require.Zero(t, pushStageRows)
	} else {
		require.NotEmpty(t, outbox.CanonicalRequestHash)
		require.NotEmpty(t, outbox.SourceID)
	}
	if rec.PendingRequestHash != "" {
		require.Equal(t, rec.PendingRequestHash, outbox.CanonicalRequestHash)
	}
	require.Zero(t, dirtyRows, "cancellation fixture must retain its established complete old-intent drain")

	if completedTarget {
		matched, err := portableSnapshotStageEvidenceMatches(context.Background(), db, rec)
		require.NoError(t, err)
		require.True(t, matched)
		attachment, err := loadAttachmentState(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, rec.NewSourceID, attachment.CurrentSourceID)
		require.Equal(t, rec.SnapshotBundleSeq, attachment.LastBundleSeqSeen)
		row := processSnapshotRow()
		var name string
		require.NoError(t, db.QueryRow(`SELECT name FROM people WHERE id=?`, row.Key["id"]).Scan(&name))
		require.Equal(t, "after restart", name)
		return true
	}

	matched, err := portableSnapshotStageEvidenceMatches(context.Background(), db, rec)
	require.NoError(t, err)
	require.True(t, matched)
	if rec.Phase == UpgradePhaseReattachPending || rec.Phase == UpgradePhaseRebuildRunning {
		require.Equal(t, "source-old", rec.OldSourceID)
		require.NotEmpty(t, rec.NewSourceID)
		require.Equal(t, "schema_generation_upgrade", rec.ReplacementReason)
	}
	if rec.Phase == UpgradePhaseRebuildRunning {
		require.True(t, rec.ReplacementConfirmed)
		require.NotEmpty(t, rec.SnapshotID)
	}
	return true
}

func portableSnapshotStageEvidenceMatches(ctx context.Context, db *sql.DB, rec *upgradeJournalRecord) (bool, error) {
	var count int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count); err != nil {
		return false, err
	}
	if rec.Phase == UpgradePhaseReady {
		return count == 0, nil
	}
	if count != rec.SnapshotRowsStaged {
		return false, nil
	}
	if count == 0 {
		return true, nil
	}
	if count != 1 {
		return false, nil
	}
	row := processSnapshotRow()
	var snapshotID, schemaName, tableName, keyJSON, payload string
	var ordinal, rowVersion int64
	if err := db.QueryRowContext(ctx, `
		SELECT snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload
		FROM _sync_snapshot_stage
	`).Scan(&snapshotID, &ordinal, &schemaName, &tableName, &keyJSON, &rowVersion, &payload); err != nil {
		return false, err
	}
	return snapshotID == rec.SnapshotID && ordinal == 1 && schemaName == row.Schema && tableName == row.Table &&
		keyJSON == `{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}` && rowVersion == row.RowVersion && payload == string(row.Payload), nil
}

func TestClientUpgradeConformance_CancellationEvidenceRejectsMissingSnapshotStage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portable-cancel-missing-stage.sqlite")
	prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	matched, err := portableSnapshotStageEvidenceMatches(context.Background(), db, rec)
	require.NoError(t, err)
	require.True(t, matched)
	_, err = db.Exec(`DELETE FROM _sync_snapshot_stage`)
	require.NoError(t, err)
	matched, err = portableSnapshotStageEvidenceMatches(context.Background(), db, rec)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestClientUpgradeConformance_CancellationEvidenceRejectsReplacementTupleSubstitution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portable-cancel-tuple.sqlite")
	prepareU6ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	want := replacementTupleFromJournal(rec)
	require.True(t, replacementTupleMatchesJournal(want, rec))

	for _, test := range []struct {
		name   string
		mutate func(*upgradeJournalRecord)
	}{
		{name: "old-source", mutate: func(got *upgradeJournalRecord) { got.OldSourceID = "substituted-old" }},
		{name: "new-source", mutate: func(got *upgradeJournalRecord) { got.NewSourceID = "substituted-new" }},
		{name: "reason", mutate: func(got *upgradeJournalRecord) { got.ReplacementReason = "substituted-reason" }},
		{name: "target-generation", mutate: func(got *upgradeJournalRecord) { got.TargetGenerationID++ }},
		{name: "target-fingerprint", mutate: func(got *upgradeJournalRecord) { got.TargetGenerationFingerprint = strings.Repeat("f", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			altered := *rec
			test.mutate(&altered)
			require.False(t, replacementTupleMatchesJournal(want, &altered))
		})
	}
}

func observePortableAuthoritativeSettlement(t *testing.T) (normalization, extraEffects, emptyProgress bool) {
	t.Helper()
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	const uploadedID = "00000000-0000-4000-8000-000000000031"
	const extraID = "00000000-0000-4000-8000-000000000032"
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES(?,?,?)`, uploadedID, "client", "client@example.com")
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := []oversync.BundleRow{
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": uploadedID}, Op: oversync.OpInsert, RowVersion: 1,
				Payload: mustJSONPayload(t, map[string]any{"id": uploadedID, "name": "normalized", "email": "client@example.com"})},
			{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": extraID}, Op: oversync.OpInsert, RowVersion: 1,
				Payload: mustJSONPayload(t, map[string]any{"id": extraID, "name": "extra", "email": "extra@example.com"})},
		}
		committed := &mockCommittedBundle{
			BundleSeq: 1, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID,
			PlannedRowCount: session.RequestRowCount, Rows: rows,
			BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash,
		}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[1] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	client.HTTP = &http.Client{Transport: server}
	report := mustPushPending(t, client, ctx)
	var uploadedName, extraName string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, uploadedID).Scan(&uploadedName))
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id=?`, extraID).Scan(&extraName))
	normalization = report.Outcome == PushOutcomeCommitted && uploadedName == "normalized"
	extraEffects = extraName == "extra"

	emptyHash := mustBundleHash(t, nil)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/sync/pull" {
			return nil, fmt.Errorf("unexpected empty-progress request %s %s", request.Method, request.URL.Path)
		}
		return jsonResponse(oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{{
			BundleSeq: 2, SourceID: "server", SourceBundleID: 1,
			RowCount: 0, ByteCount: 2, BundleHash: emptyHash, Rows: []oversync.BundleRow{},
		}}}), nil
	})}
	pull, err := client.pullToStableLocked(ctx)
	require.NoError(t, err)
	var checkpoint int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	emptyProgress = pull.Outcome == RemoteSyncOutcomeAppliedIncremental && checkpoint == 2
	return normalization, extraEffects, emptyProgress
}

type portablePrunedFixture struct {
	client  *Client
	db      *sql.DB
	options snapshotApplyOptions
	session *oversync.SnapshotSession
}

func newPortablePrunedFixture(t *testing.T, suffix string) portablePrunedFixture {
	t.Helper()
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	require.NoError(t, updateSourceNextBundleID(ctx, db, client.sourceID, 4))
	_, err = db.Exec(`INSERT INTO users VALUES('committed-intent','Intent','intent@example.com')`)
	require.NoError(t, err)
	outbound, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, client.markCheckpointRecoveryRequiredLocked(ctx, "history_pruned"))
	require.NotZero(t, outbound.SourceBundleID)
	options := snapshotApplyOptions{PreserveOutbox: true}
	options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(t, err)
	session := stageUserSnapshotForGate(t, client, "portable-pruned-"+suffix, "remote", 8,
		mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}))
	return portablePrunedFixture{client: client, db: db, options: options, session: session}
}

func observePortablePrunedRecovery(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	ctx := context.Background()

	success := newPortablePrunedFixture(t, "success")
	oldSource := success.client.sourceID
	oldProof := success.client.schemaGenerationProof
	successErr := success.client.applyStagedSnapshotLocked(ctx, success.session, success.options)
	require.NoError(t, successErr)
	sourcePreserved := success.client.sourceID == oldSource
	proofPreserved := success.client.schemaGenerationProof == oldProof

	missing := newPortablePrunedFixture(t, "missing")
	_, missingErr := missing.client.pinSnapshotApplyGuard(ctx, snapshotApplyOptions{})
	require.Error(t, missingErr)

	newer := newPortablePrunedFixture(t, "newer")
	_, err := newer.db.Exec(`INSERT INTO users VALUES('newer','Newer','newer@example.com')`)
	require.NoError(t, err)
	newerErr := newer.client.applyStagedSnapshotLocked(ctx, newer.session, newer.options)
	require.Error(t, newerErr)

	corrupt := newPortablePrunedFixture(t, "corrupt")
	_, err = corrupt.db.Exec(`UPDATE _sync_outbox_rows SET wire_payload='{"email":"intent@example.com","id":"committed-intent","name":"Tampered"}'`)
	require.NoError(t, err)
	corruptErr := corrupt.client.applyStagedSnapshotLocked(ctx, corrupt.session, corrupt.options)
	require.Error(t, corruptErr)
	scenario.consumeAction(t, "old_keep_source_snapshot")

	return map[string]any{
		"outcome":          map[bool]string{true: "ready_to_migrate", false: "blocked"}[successErr == nil],
		"missing_receipt":  rejectSemantic(missingErr),
		"newer_intent":     rejectSemantic(newerErr),
		"corrupt_evidence": rejectSemantic(corruptErr),
	}, preservedWhen(sourcePreserved && proofPreserved, "old_source", "old_proof")
}

func observePortableMigration(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-migration-success.sqlite")
	preparePortableGenerationU4(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	remoteCalls := 0
	config := portableGenerationU4Config(t, db)
	config.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		remoteCalls++
		return nil, fmt.Errorf("remote I/O during portable migration")
	})}
	coordinator, err := newTestOpening(db, config)
	require.NoError(t, err)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.NoError(t, coordinator.runLocalMigration(context.Background(), rec))
	successPhase := rec.Phase
	var note string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))

	failure := runPortableMigrationFailure(t, "callback", func(context.Context, LocalMigrationTx, LocalMigrationStep) error {
		return fmt.Errorf("portable migration failure")
	})
	falseVersion := runPortableMigrationFailure(t, "false-version", func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
			ALTER TABLE people RENAME TO people_old;
			CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
			INSERT INTO people SELECT id,legacy_name FROM people_old;
			DROP TABLE people_old;
		`)
		return err
	})
	schemaMismatch := runPortableMigrationFailure(t, "schema-mismatch", func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
			DROP TABLE people;
			CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY);
			PRAGMA user_version=2;
		`)
		return err
	})
	before := runPortableBoundaryDeath(t, "U4", "before")
	after := runPortableBoundaryDeath(t, "U4", "after")
	atomic := before.version == 1 && after.version == 2 && before.phase == UpgradePhaseMigratingLocal && after.phase == UpgradePhaseLocalMigrated
	scenario.consumeAction(t, "migrate")
	if before.version == 1 && before.phase == UpgradePhaseMigratingLocal {
		scenario.consumeFault(t, "before_U4_commit", "process_death")
	}
	if after.version == 2 && after.phase == UpgradePhaseLocalMigrated {
		scenario.consumeFault(t, "after_U4_commit", "process_death")
	}
	return map[string]any{
		"success":         string(successPhase),
		"failure":         string(failure),
		"false_version":   string(falseVersion),
		"remote_io":       map[bool]string{true: "forbidden", false: "performed"}[remoteCalls == 0],
		"schema_mismatch": string(schemaMismatch),
	}, preservedWhen(atomic && note == "preserved", "atomic_old_or_target_schema", "local_only")
}

func runPortableMigrationFailure(t *testing.T, suffix string, migrate func(context.Context, LocalMigrationTx, LocalMigrationStep) error) UpgradeOutcome {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-migration-"+suffix+".sqlite")
	preparePortableGenerationU4(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	config := portableGenerationU4Config(t, db)
	config.Plan.Migrator = testLocalMigrator{migrate: migrate}
	coordinator, err := newTestOpening(db, config)
	require.NoError(t, err)
	result, runErr := coordinator.Advance(context.Background())
	require.Error(t, runErr)
	return result.Status.Outcome
}

func observePortableTargetRebuild(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	staged := runPortableBoundaryDeath(t, "U7", "after")
	beforeReady := runPortableBoundaryDeath(t, "U8", "before")
	ready := runPortableBoundaryDeath(t, "U8", "after")
	expired := observePortableExpiredSnapshot(t)
	if expired == "new_target_snapshot" {
		expired = "fresh_target_snapshot"
	}

	path := filepath.Join(t.TempDir(), "portable-target-trigger.sqlite")
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	_, err = db.Exec(`
		CREATE TRIGGER corrupt_portable_target_snapshot
		AFTER INSERT ON people
		WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1),0)=1
		BEGIN UPDATE people SET name='corrupted' WHERE id=NEW.id; END
	`)
	require.NoError(t, err)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-target.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected target-rebuild request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, triggerErr := coordinator.Advance(context.Background())
	require.Error(t, triggerErr)
	rejected := result.Status.Phase == UpgradePhaseRebuildRunning
	checkpoint := ready.checkpoint == 7
	if staged.stagedRows > 0 {
		scenario.consumeAction(t, "stage_snapshot")
	}
	if expired == "fresh_target_snapshot" {
		scenario.consumeFault(t, "session", "expired")
	}
	if beforeReady.phase == UpgradePhaseRebuildRunning {
		scenario.consumeFault(t, "before_U8_commit", "process_death")
	}
	if ready.ready && ready.phase == UpgradePhaseReady {
		scenario.consumeFault(t, "after_U8_commit", "process_death")
		scenario.consumeAction(t, "apply_and_publish")
	}
	return map[string]any{
		"outcome":              map[bool]string{true: "ready", false: "not_ready"}[ready.ready && ready.phase == UpgradePhaseReady],
		"expired":              expired,
		"trigger_side_effects": map[bool]string{true: "reject", false: "accepted"}[rejected],
		"checkpoint":           map[bool]string{true: "snapshot_bundle_seq", false: "other"}[checkpoint],
	}, preservedWhen(ready.localNote == "preserved", "local_only")
}

func observePortableNegotiationScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	targetID, err := strconv.ParseInt(fmt.Sprint(scenario.Actions[0]["target_generation"]), 10, 64)
	require.NoError(t, err)
	target := testUpgradeContract(t, oversync.SchemaGenerationID(targetID), []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	db := openUpgradeTestDB(t)
	require.NoError(t, initializeDatabase(db))
	require.NoError(t, persistUpgradeIdentity(context.Background(), db, upgradeIdentityRecord{
		DatabaseID: "portable-negotiation", ServiceID: "service", GenerationID: 1,
		GenerationFingerprint: strings.Repeat("a", 64), LocalVersion: 1, Ready: true,
	}))
	require.NoError(t, func() error {
		_, err := db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES ('portable-source',1)`)
		return err
	}())
	_, err = db.Exec(`
		UPDATE _sync_attachment_state
		SET current_source_id='portable-source',binding_state='attached',attached_user_id='portable-user',schema_name='main'
		WHERE singleton_key=1
	`)
	require.NoError(t, err)
	beforeIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	var beforeSchemaVersion int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&beforeSchemaVersion))

	status := http.StatusOK
	response := []byte{}
	discoveryResponse := []byte{}
	requestFixture := []byte{}
	if len(scenario.ProtocolExchanges) > 0 {
		requestPath := scenario.ProtocolExchanges[0]["request"].(string)
		requestFixture, err = os.ReadFile(filepath.Join(portableUpgradeCorpusRoot(t), filepath.FromSlash(requestPath)))
		require.NoError(t, err)
		responsePath := scenario.ProtocolExchanges[0]["response"].(string)
		response, err = os.ReadFile(filepath.Join(portableUpgradeCorpusRoot(t), filepath.FromSlash(responsePath)))
		require.NoError(t, err)
		discoveryResponse = append([]byte(nil), response...)
	}
	switch scenario.ID {
	case "CU-N01":
		status = http.StatusOK
		response, err = json.Marshal(oversync.GenerationDiscoveryResponse{
			ProtocolVersion:  requiredProtocolVersion,
			SchemaGeneration: upgradeCapabilitiesValue(target).SchemaGeneration,
			Limits:           testWireLimits(),
		})
		require.NoError(t, err)
	case "CU-N03":
		status = http.StatusConflict
		response, err = json.Marshal(map[string]any{
			"error": "unknown_schema_generation", "message": "schema generation is unknown",
			"retryable": false, "remediation": "upgrade_or_reconfigure_client",
			"schema_generation_id": strconv.FormatInt(targetID, 10),
		})
		require.NoError(t, err)
	case "CU-N04":
		status = http.StatusOK
		response, err = json.Marshal(oversync.GenerationDiscoveryResponse{
			ProtocolVersion:  requiredProtocolVersion,
			SchemaGeneration: upgradeCapabilitiesValue(target).SchemaGeneration,
			Limits:           testWireLimits(),
		})
		require.NoError(t, err)
	default:
		status = http.StatusGone
		response = []byte(`{"error":"unsupported_schema_generation","message":"schema generation is no longer supported","retryable":false,"remediation":"upgrade_or_reconfigure_client"}`)
	}
	requestCount := 0
	wireMatched := false
	fixtureMethod, fixturePath := http.MethodGet, "/sync/generations/"+fmt.Sprint(targetID)
	fixtureHeaders := map[string]string{oversync.SchemaGenerationIDHeader: fmt.Sprint(targetID)}
	if len(scenario.ProtocolExchanges) > 0 {
		fixtureMethod, _, fixtureHeaders, _ = portableWireExchange(t, scenario)
	}
	delete(fixtureHeaders, oversync.SchemaGenerationIDHeader)
	delete(fixtureHeaders, oversync.SchemaGenerationFingerprintHeader)
	coordinator := &Opening{db: db, config: openingRuntimeConfig{
		Plan: UpgradePlan{TargetGenerationID: target.Proof.ID, testTarget: target, UserID: "portable-user"}, BaseURL: "http://portable.invalid", Token: tokenProviderForTests,
		TargetClientConfig: DefaultConfig(target.Proof.ID, target.Tables),
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
				if scenario.ID != "CU-N01" && scenario.ID != "CU-N04" {
					t.Fatalf("scenario %s unexpectedly reached generation discovery confirmation", scenario.ID)
				}
				if len(discoveryResponse) == 0 {
					return jsonResponse(upgradeCapabilities(t, target)), nil
				}
				discoveryStatus := http.StatusOK
				if scenario.ID == "CU-N04" {
					discoveryStatus = http.StatusConflict
				}
				return &http.Response{StatusCode: discoveryStatus, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(discoveryResponse))}, nil
			}
			requestCount++
			require.Equal(t, fixtureMethod, request.Method)
			require.Equal(t, fixturePath, request.URL.Path)
			requestBody := portableRequestBody(t, request)
			for name, value := range fixtureHeaders {
				if name != "Authorization" {
					require.Equal(t, value, request.Header.Get(name), "portable request header %s", name)
				}
			}
			wireMatched = bytes.Equal(requestBody, requestFixture)
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(response))}, nil
		})},
	}}
	previewErr := coordinator.previewTarget(context.Background())
	var afterSchemaVersion int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&afterSchemaVersion))
	afterIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, 1, requestCount)
	mutationFree := beforeSchemaVersion == afterSchemaVersion && *beforeIdentity == *afterIdentity
	scenario.consumeAction(t, scenario.Actions[0]["kind"].(string))
	if len(scenario.ProtocolExchanges) > 0 && requestCount == 1 && wireMatched {
		scenario.consumeExchange(t)
	}

	if scenario.ID == "CU-N01" {
		require.NoError(t, previewErr)
		return map[string]any{
			"outcome":        string(UpgradeOutcomeReady),
			"sparse_upgrade": targetID > beforeIdentity.GenerationID+1,
		}, preservedWhen(mutationFree, "old_identity_until_U8")
	}
	var unavailable *UpgradeError
	require.ErrorAs(t, previewErr, &unavailable)
	if scenario.ID == "CU-N03" || scenario.ID == "CU-N04" {
		return map[string]any{
			"outcome":  string(unavailable.Outcome),
			"mutation": !mutationFree,
		}, preservedWhen(mutationFree, "installed_identity")
	}
	downgradeTarget := testUpgradeContract(t, 1, []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}, {Name: "name", WireType: oversync.GenerationWireTypeText}})
	downgradePlan := UpgradePlan{
		PlanID: "portable-downgrade", Mode: UpgradeModeGeneration, DatabaseID: "db", ServiceID: "service", UserID: "user",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: target.Proof.ID, testInstalled: target,
		TargetGenerationID: downgradeTarget.Proof.ID, testTarget: downgradeTarget,
		Migrator: testLocalMigrator{},
	}
	_, downgradeErr := validateUpgradePlan(downgradePlan)
	changedPlan := downgradePlan
	changedPlan.testTarget = target
	changedPlan.TargetVersion = 1
	changedPlan.PlanID = " portable-plan "
	_, planChangeErr := validateUpgradePlan(changedPlan)
	return map[string]any{
		"outcome":     string(unavailable.Outcome),
		"mutation":    !mutationFree,
		"downgrade":   rejectionSemantic(downgradeErr),
		"plan_change": rejectionSemantic(planChangeErr),
	}, preservedWhen(mutationFree, "installed_identity")
}

func observePortableFreshScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	require.Equal(t, "create_final_schema", scenario.Actions[0]["kind"])
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	remoteCalls := 0
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: UpgradePlan{
			PlanID: "portable-fresh", Mode: UpgradeModeFresh, DatabaseID: "portable-fresh-db", ServiceID: "service",
			TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
			Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
				_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); PRAGMA user_version=1`)
				return err
			}},
		},
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
			remoteCalls++
			return nil, fmt.Errorf("fresh mode attempted remote I/O")
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	scenario.consumeAction(t, "create_final_schema")
	identity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	return map[string]any{
		"outcome":      string(result.Status.Outcome),
		"remote_calls": strconv.Itoa(remoteCalls),
	}, preservedWhen(identity != nil && identity.UserID == "" && identity.Ready, "anonymous_identity")
}

func observePortableLocalOnlyScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	require.Equal(t, "offline_local_only_migration", scenario.Actions[0]["kind"])
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "portable-local-fresh", Mode: UpgradeModeFresh, DatabaseID: "portable-local-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
				CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL);
				INSERT INTO local_notes VALUES ('sentinel','before');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8','preserved'); UPDATE _sync_apply_state SET apply_mode=0`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO people VALUES ('550e8400-e29b-41d4-a716-446655440000','pending')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES ('portable-source',7)`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET current_source_id='portable-source',last_bundle_seq_seen=11 WHERE singleton_key=1`)
	require.NoError(t, err)
	beforeDigest, err := localOnlyDigest(context.Background(), db, syncPreservationTableNames(contract))
	require.NoError(t, err)
	var beforeSource string
	var beforeCheckpoint int64
	var beforePending int64
	require.NoError(t, db.QueryRow(`SELECT current_source_id,last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&beforeSource, &beforeCheckpoint))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&beforePending))
	require.Positive(t, beforePending)
	attempt := 0
	plan := UpgradePlan{
		PlanID: "portable-local-v2", Mode: UpgradeModeLocalOnly, DatabaseID: "portable-local-db", ServiceID: "service",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			attempt++
			if _, err := tx.ExecContext(ctx, `UPDATE local_notes SET note='after'; PRAGMA user_version=2`); err != nil {
				return err
			}
			if attempt == 1 {
				return fmt.Errorf("portable first-attempt rollback")
			}
			return nil
		}},
	}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	first, firstErr := coordinator.Advance(context.Background())
	require.Error(t, firstErr)
	require.Equal(t, UpgradeOutcomeLocalMigrationFailed, first.Status.Outcome)
	scenario.consumeFault(t, "first_attempt", "rollback")
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	scenario.consumeAction(t, "offline_local_only_migration")
	afterDigest, err := localOnlyDigest(context.Background(), db, syncPreservationTableNames(contract))
	require.NoError(t, err)
	var afterSource string
	var afterCheckpoint int64
	var afterPending int64
	require.NoError(t, db.QueryRow(`SELECT current_source_id,last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&afterSource, &afterCheckpoint))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&afterPending))
	var note string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
	preserved := []string{}
	if beforeDigest == afterDigest {
		preserved = append(preserved, "synchronized_state")
	}
	if note == "after" {
		preserved = append(preserved, "local_only")
	}
	return map[string]any{
		"outcome":    string(result.Status.Outcome),
		"source":     equalitySemantic(beforeSource, afterSource),
		"checkpoint": equalitySemantic(beforeCheckpoint, afterCheckpoint),
		"pending_intent": equalitySemantic(
			fmt.Sprintf("%s:%d", beforeDigest, beforePending),
			fmt.Sprintf("%s:%d", afterDigest, afterPending),
		),
	}, preserved
}

func observePortableBoundaryScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	boundary := strings.TrimPrefix(scenario.ID, "CU-")
	if boundary == "U3" {
		return observePortableU3(t, scenario)
	}
	before := runPortableBoundaryDeath(t, boundary, "before")
	after := runPortableBoundaryDeath(t, boundary, "after")
	scenario.consumeAction(t, scenario.Actions[0]["kind"].(string))
	if boundary == "U6" {
		scenario.consumeFault(t, "remote_commit_before_local_receipt", "process_death")
		scenario.consumeFault(t, "after_local_commit", "process_death")
	} else if boundary == "U7" {
		scenario.consumeFault(t, "before_chunk_commit", "process_death")
		scenario.consumeFault(t, "after_chunk_commit", "process_death")
	} else {
		scenario.consumeFault(t, "before_commit", "process_death")
		scenario.consumeFault(t, "after_commit", "process_death")
	}
	switch boundary {
	case "U0":
		writesFrozen, unchanged := portableU0WritesAreGuarded(t, after.path)
		return map[string]any{"before": semanticPortablePhase(before.phase), "after": string(after.phase), "writes": map[bool]string{true: "frozen", false: "available"}[writesFrozen]}, preservedWhen(unchanged, "installed_identity", "old_intent")
	case "U1":
		preserved := before.sourceID != "" && before.sourceID == after.sourceID &&
			before.generation == after.generation && before.fingerprint == after.fingerprint
		return map[string]any{"before": string(before.phase), "after": string(after.phase)}, preservedWhen(preserved, "old_source", "old_proof")
	case "U2":
		blocked, evidence := portableUnresolvedIntentBlocks(t)
		return map[string]any{"before": string(before.phase), "after": string(after.phase), "drain_complete": after.drainComplete, "unresolved_intent": blockSemantic(blocked)}, preservedWhen(evidence, "frozen_request_evidence")
	case "U4":
		return map[string]any{
			"before": map[string]any{"phase": string(before.phase), "version": strconv.FormatInt(before.version, 10), "schema": map[bool]string{true: "old", false: "invalid"}[before.columns == "id,legacy_name" && before.contractValid]},
			"after":  map[string]any{"phase": string(after.phase), "version": strconv.FormatInt(after.version, 10), "schema": map[bool]string{true: "target", false: "invalid"}[after.columns == "id,name" && after.contractValid]},
		}, preservedWhen(before.localDigest != "" && before.localDigest == after.localDigest, "local_only")
	case "U5":
		allocations := 0
		if before.newSourceID == "" && after.newSourceID != "" {
			allocations = 1
		}
		tupleStable := after.oldSourceID != "" && after.newSourceID != "" && after.reason == "schema_generation_upgrade" && portableReplacementTupleSurvivesReopen(t, after)
		return map[string]any{"before": string(before.phase), "after": string(after.phase), "new_source_allocations": strconv.Itoa(allocations)}, preservedWhen(before.localDigest != "" && before.localDigest == after.localDigest && tupleStable, "local_only", "replacement_tuple")
	case "U6":
		tuplePreserved := before.oldSourceID == after.oldSourceID && before.newSourceID == after.newSourceID && before.reason == after.reason && before.newSourceID != ""
		exchangeMatched := portableReplacementExchangeMatches(t, scenario)
		if tuplePreserved && exchangeMatched {
			scenario.consumeExchange(t)
		}
		return map[string]any{"before_local_receipt": replacementReceiptSemantic(before.phase, before.newSourceID), "after": string(after.phase)}, preservedWhen(tuplePreserved, "tuple", "old_source_evidence")
	case "U7":
		expired := observePortableExpiredSnapshot(t)
		if expired == "new_target_snapshot" {
			scenario.consumeFault(t, "session", "expired")
		}
		targetPreserved := before.sourceID == after.sourceID && before.generation == after.generation && before.fingerprint == after.fingerprint && before.snapshotID == after.snapshotID
		return map[string]any{"before": prefixSemantic(before.stagedRows), "after": prefixSemantic(after.stagedRows), "expired": expired}, preservedWhen(targetPreserved, "target_source", "target_proof")
	case "U8":
		return map[string]any{"before": readyPublicationSemantic(before), "after": readyPublicationSemantic(after)}, preservedWhen(before.localNote == after.localNote, "local_only")
	default:
		t.Fatalf("unsupported portable boundary %s", boundary)
		return nil, nil
	}
}

func runPortableBoundaryDeath(t *testing.T, boundary, side string) portableBoundaryState {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-boundary.sqlite")
	caseName := "GENERATION_BOUNDARY"
	prepare := func() {
		switch boundary {
		case "U0":
			prepareGenerationReadyProcessTestDatabase(t, path)
		case "U1":
			prepareGenerationBoundaryPhase(t, path, UpgradePhaseStarted)
		case "U2":
			prepareGenerationBoundaryPhase(t, path, UpgradePhaseResolvingOldIntent)
		case "U3":
			prepareGenerationDrainedProcessTestDatabase(t, path)
		case "U4":
			preparePortableGenerationU4(t, path)
			caseName = "PORTABLE_U4"
		case "U5":
			prepareLocalMigratedProcessTestDatabase(t, path)
		case "U6":
			prepareU6ProcessTestDatabase(t, path)
		case "U7":
			prepareU7ProcessTestDatabase(t, path)
		case "U8":
			prepareU8ProcessTestDatabase(t, path)
			caseName = "U8"
		default:
			t.Fatalf("unsupported boundary %s", boundary)
		}
	}
	prepare()
	productionBoundary := side + "_" + boundary + "_commit"
	command := exec.Command(os.Args[0], "-test.run=^TestUpgradeProcessHelper$")
	command.Env = append(os.Environ(),
		upgradeProcessHelperEnv+"=1", upgradeProcessCaseEnv+"="+caseName,
		upgradeProcessDBEnv+"="+path, upgradeProcessBoundaryEnv+"="+productionBoundary,
	)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	require.Equal(t, 93, exit.ExitCode(), string(output))
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	state := portableBoundaryState{path: path, phase: rec.Phase, drainComplete: rec.DrainComplete, newSourceID: rec.NewSourceID, oldSourceID: rec.OldSourceID, reason: rec.ReplacementReason, snapshotID: rec.SnapshotID, stagedRows: rec.SnapshotRowsStaged}
	var ready int
	require.NoError(t, db.QueryRow(`SELECT ready FROM _sync_upgrade_identity WHERE singleton_key=1`).Scan(&ready))
	state.ready = ready == 1
	require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&state.version))
	_ = db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&state.checkpoint)
	_ = db.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&state.sourceID)
	_ = db.QueryRow(`SELECT generation_id,generation_fingerprint FROM _sync_upgrade_identity WHERE singleton_key=1`).Scan(&state.generation, &state.fingerprint)
	_ = db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&state.localNote)
	state.localDigest, _ = localOnlyDigestWithUpgradeGuards(context.Background(), db, []string{"local_notes"})
	state.columns = portablePhysicalColumns(t, db, "people")
	if boundary == "U4" {
		plan := portableGenerationU4Plan(t)
		contract := plan.testInstalled
		if state.version == plan.TargetVersion {
			contract = plan.testTarget
		}
		state.contractValid = validatePhysicalLocalContract(context.Background(), db, contract, plan.LocalOnlyTables) == nil
	}
	return state
}

func portablePhysicalColumns(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_xinfo(` + quoteIdent(table) + `)`)
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notNull, pk, hidden int
		var name, declared string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &declared, &notNull, &defaultValue, &pk, &hidden))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	return strings.Join(columns, ",")
}

func portableU0WritesAreGuarded(t *testing.T, path string) (bool, bool) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var dirtyBefore int
	var noteBefore string
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyBefore))
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&noteBefore))
	_, syncErr := db.Exec(`INSERT INTO people(id,name) VALUES ('550e8400-e29b-41d4-a716-446655440000','blocked')`)
	_, localErr := db.Exec(`UPDATE local_notes SET note='changed' WHERE id='sentinel'`)
	var dirtyAfter int
	var noteAfter string
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyAfter))
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&noteAfter))
	return syncErr != nil && localErr != nil, dirtyBefore == dirtyAfter && noteBefore == noteAfter
}

func portableReplacementTupleSurvivesReopen(t *testing.T, state portableBoundaryState) bool {
	t.Helper()
	db, err := sql.Open("sqlite3", state.path)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	return rec.NewSourceID == state.newSourceID && rec.OldSourceID == state.oldSourceID && rec.ReplacementReason == state.reason
}

func portableReplacementExchangeMatches(t *testing.T, scenario portableUpgradeScenario) bool {
	t.Helper()
	root := portableUpgradeCorpusRoot(t)
	requestFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["request"].(string))))
	require.NoError(t, err)
	responseFixture, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenario.ProtocolExchanges[0]["response"].(string))))
	require.NoError(t, err)
	method, requestPath, headers, status := portableWireExchange(t, scenario)
	path := filepath.Join(t.TempDir(), "portable-u6-wire.sqlite")
	plan := prepareU6ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	matched := false
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-u6-wire.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				body, readErr := io.ReadAll(request.Body)
				require.NoError(t, readErr)
				headersMatch := true
				for name, value := range headers {
					if name != "Authorization" && request.Header.Get(name) != value {
						headersMatch = false
					}
				}
				matched = request.Method == method && request.URL.Path == requestPath && status == http.StatusOK &&
					headersMatch && bytes.Equal(body, requestFixture)
				return rawJSONResponse(status, string(responseFixture)), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/snapshot-1":
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-1", SnapshotBundleSeq: 42}), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/snapshot-1":
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, fmt.Errorf("unexpected portable U6 wire request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NoError(t, coordinator.Close())
	return matched
}

func portableUnresolvedIntentBlocks(t *testing.T) (blocked, evidence bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-unresolved.sqlite")
	prepareNeedsResolutionProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	outbox, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	drained, _, err := inspectOldDrain(context.Background(), db)
	require.NoError(t, err)
	blocked = rec.Phase == UpgradePhaseResolvingOldIntent && rec.Outcome == UpgradeOutcomeNeedsResolution && !drained
	evidence = rec.PendingRequestHash != "" && rec.PendingRequestHash == outbox.CanonicalRequestHash && outbox.State == outboxStatePrepared
	return blocked, evidence
}

func portableGenerationU4Plan(t *testing.T) UpgradePlan {
	t.Helper()
	oldContract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "legacy_name", WireType: oversync.GenerationWireTypeText},
	})
	targetContract := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	return UpgradePlan{
		PlanID: "portable-generation-u4", Mode: UpgradeModeGeneration, DatabaseID: "portable-u4-db", ServiceID: "service", UserID: "user-1",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: oldContract.Proof.ID, TargetGenerationID: targetContract.Proof.ID, testInstalled: oldContract, testTarget: targetContract, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				ALTER TABLE people RENAME TO people_old;
				CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
				INSERT INTO people SELECT id,legacy_name FROM people_old;
				DROP TABLE people_old;
				PRAGMA user_version=2;
			`)
			return err
		}},
	}
}

func preparePortableGenerationU4(t *testing.T, path string) {
	t.Helper()
	plan := portableGenerationU4Plan(t)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "portable-u4-fresh", Mode: UpgradeModeFresh, DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID,
		TargetVersion: 1, TargetGenerationID: plan.testInstalled.Proof.ID, testTarget: plan.testInstalled, LocalOnlyTables: plan.LocalOnlyTables,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,legacy_name TEXT NOT NULL);
				CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL);
				INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8','Ada');
				INSERT INTO local_notes VALUES ('sentinel','preserved');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET current_source_id='source-old',binding_state='attached',attached_user_id='user-1',schema_name='main' WHERE singleton_key=1`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_upgrade_identity SET user_id='user-1' WHERE singleton_key=1`)
	require.NoError(t, err)
	coordinator, err := newTestOpening(db, portableGenerationU4Config(t, db))
	require.NoError(t, err)
	rec, err := coordinator.loadOrStart(context.Background())
	require.NoError(t, err)
	require.NoError(t, coordinator.transitionPhase(context.Background(), rec, UpgradePhaseResolvingOldIntent))
	require.NoError(t, coordinator.resolveOldIntent(context.Background(), rec))
	require.Equal(t, UpgradePhaseResolvingOldIntent, rec.Phase)
	require.True(t, rec.DrainComplete)
	require.NoError(t, coordinator.resolveOldIntent(context.Background(), rec))
	require.Equal(t, UpgradePhaseMigratingLocal, rec.Phase)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func portableGenerationU4Config(t *testing.T, _ *sql.DB) *openingRuntimeConfig {
	t.Helper()
	plan := portableGenerationU4Plan(t)
	return &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-u4.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
					return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
				}
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
				return jsonResponse(oversync.PullResponse{StableBundleSeq: 0}), nil
			default:
				return nil, fmt.Errorf("unexpected portable U4 request %s %s", request.Method, request.URL.Path)
			}
		})},
	}
}

func observePortableU3(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	requestFixture, err := os.ReadFile(filepath.Join(portableUpgradeCorpusRoot(t), filepath.FromSlash(scenario.ProtocolExchanges[0]["request"].(string))))
	require.NoError(t, err)
	responseFixture, err := os.ReadFile(filepath.Join(portableUpgradeCorpusRoot(t), filepath.FromSlash(scenario.ProtocolExchanges[0]["response"].(string))))
	require.NoError(t, err)
	fixtureMethod, fixturePath, fixtureHeaders, fixtureStatus := portableWireExchange(t, scenario)
	path := filepath.Join(t.TempDir(), "portable-u3.sqlite")
	plan := prepareGenerationDrainedProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	allowTarget := false
	wireMatched := false
	config := &openingRuntimeConfig{Plan: plan, BaseURL: "http://portable-u3.invalid", Token: tokenProviderForTests}
	config.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
			requestBody := portableRequestBody(t, request)
			if !allowTarget && request.Header.Get(oversync.SchemaGenerationIDHeader) == fmt.Sprint(plan.testTarget.Proof.ID) {
				require.Equal(t, requestFixture, requestBody)
				return nil, io.ErrUnexpectedEOF
			}
			if request.Header.Get(oversync.SchemaGenerationIDHeader) == fmt.Sprint(plan.testInstalled.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
			}
			headersMatch := true
			for name, value := range fixtureHeaders {
				if name != "Authorization" && request.Header.Get(name) != value {
					headersMatch = false
				}
			}
			wireMatched = request.Method == fixtureMethod && request.URL.Path == fixturePath && fixtureStatus == http.StatusOK &&
				headersMatch && bytes.Equal(requestBody, requestFixture)
			return rawJSONResponse(fixtureStatus, string(responseFixture)), nil
		}
		if request.Method == http.MethodGet && request.URL.Path == "/sync/pull" {
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0}), nil
		}
		return nil, fmt.Errorf("unexpected portable U3 request %s %s", request.Method, request.URL.Path)
	})}
	coordinator, err := newTestOpening(db, config)
	require.NoError(t, err)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	beforeIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	previewErr := coordinator.resolveOldIntent(context.Background(), rec)
	require.Error(t, previewErr)
	scenario.consumeFault(t, "preview_reply", "lost")
	failedJournal, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, UpgradePhaseResolvingOldIntent, failedJournal.Phase)
	require.True(t, failedJournal.DrainComplete)
	allowTarget = true
	require.NoError(t, coordinator.resolveOldIntent(context.Background(), failedJournal))
	afterIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	afterJournal, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	scenario.consumeAction(t, "target_preview_and_recheck")
	if wireMatched {
		scenario.consumeExchange(t)
	}
	return map[string]any{
		"phase":                  string(afterJournal.Phase),
		"local_schema":           schemaSemantic(beforeIdentity.LocalVersion),
		"remote_identity_change": *beforeIdentity != *afterIdentity,
	}, preservedWhen(afterJournal.DrainComplete, "drain_witness")
}

func observePortableExpiredSnapshot(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "portable-expired.sqlite")
	plan := prepareU8ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	clearUpgradeSnapshotStageForTest(t, db)
	created := ""
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://portable-expired.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/process-snapshot":
				return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "snapshot_session_not_found"}), nil
			case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
				created = "portable-new-target"
				return jsonResponse(oversync.SnapshotSession{SnapshotID: created, SnapshotBundleSeq: 7, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
			case request.Method == http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: make(http.Header)}, nil
			default:
				return nil, fmt.Errorf("unexpected expired snapshot request %s %s", request.Method, request.URL.Path)
			}
		})},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	if result.Status.SnapshotID == created && created != "" {
		return "new_target_snapshot"
	}
	return "unchanged_snapshot"
}

func observePortableMobileScenario(t *testing.T, scenario portableUpgradeScenario) (map[string]any, []string) {
	t.Helper()
	root := portableUpgradeCorpusRoot(t)
	repositoryRoot := filepath.Clean(filepath.Join(root, "..", "..", ".."))
	evidence := filepath.Join(t.TempDir(), "mobile-observation.json")
	command := exec.Command("go", "test", "./simulator", "-run", "^TestMobileFlow_UpgradeProductionServerSameFileProcessDeathJourney$", "-count=1", "-timeout=3m30s")
	command.Dir = filepath.Join(repositoryRoot, "integration", "mobile_flow")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=go1.25.12", "OVERSYNC_UPGRADE_MOBILE_OBSERVATION="+evidence)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	var observed struct {
		DeviceA            string `json:"device_a"`
		DeviceB            string `json:"device_b"`
		IndependentSources bool   `json:"independent_sources"`
		SameFile           bool   `json:"same_file"`
		LocalOnly          bool   `json:"local_only"`
	}
	body, err := os.ReadFile(evidence)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &observed))
	if observed.DeviceA == "new_ready" {
		scenario.consumeAction(t, "upgrade_device_a_same_file")
		scenario.consumeFault(t, "after_U6_remote_reply_before_local_receipt", "process_death")
	}
	if observed.DeviceB == "old_ready" {
		scenario.consumeAction(t, "sync_device_b_old")
	}
	preserved := []string{}
	if observed.SameFile {
		preserved = append(preserved, "same_file_identity")
	}
	if observed.LocalOnly {
		preserved = append(preserved, "local_only_sentinel")
	}
	return map[string]any{
		"device_a":            observed.DeviceA,
		"device_b":            observed.DeviceB,
		"independent_sources": observed.IndependentSources,
	}, preserved
}

func preservedWhen(condition bool, names ...string) []string {
	if !condition {
		return []string{}
	}
	return append([]string(nil), names...)
}

func rejectionSemantic(err error) string {
	if err != nil {
		return "rejected"
	}
	return "accepted"
}

func rejectSemantic(err error) string {
	if err != nil {
		return "reject"
	}
	return "accept"
}

func equalitySemantic[T comparable](left, right T) string {
	if left == right {
		return "unchanged"
	}
	return "changed"
}

func freezeSemantic(identityReady bool) string {
	if !identityReady {
		return "frozen"
	}
	return "available"
}

func blockSemantic(blocked bool) string {
	if blocked {
		return "blocks"
	}
	return "unverified"
}

func schemaSemantic(version int64) string {
	if version >= 2 {
		return "target"
	}
	return "old"
}

func prefixSemantic(rows int64) string {
	if rows == 0 {
		return "prior_prefix"
	}
	return "extended_prefix"
}

func replacementReceiptSemantic(phase UpgradePhase, sourceID string) string {
	if phase == UpgradePhaseReattachPending && sourceID != "" {
		return "reattach_pending_same_tuple"
	}
	return string(phase)
}

func readyPublicationSemantic(state portableBoundaryState) string {
	if state.phase == UpgradePhaseReady && state.ready && state.checkpoint > 0 {
		return "ready_target_identity_checkpoint"
	}
	if state.phase == UpgradePhaseRebuildRunning && state.stagedRows > 0 {
		return "rebuild_running_with_stage"
	}
	return string(state.phase)
}

func fixtureDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum)
}
