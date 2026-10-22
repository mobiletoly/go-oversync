// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const oversqliteContractsRootEnv = "OVERSQLITE_CONTRACTS_ROOT"

// requireManifestPinnedContracts is the shared-corpus test boundary. A test
// suite using cross-runtime fixtures must pass an explicit root; it never
// falls back to copied literals or a guessed sibling checkout.
func requireManifestPinnedContracts(t *testing.T) string {
	t.Helper()
	root := os.Getenv(oversqliteContractsRootEnv)
	if root == "" {
		t.Fatalf("%s is required for shared Oversqlite contract tests", oversqliteContractsRootEnv)
	}
	if err := validateContractManifest(root); err != nil {
		t.Fatalf("shared Oversqlite contract corpus is invalid: %v", err)
	}
	return root
}

func validateContractManifest(root string) error {
	manifestPath := filepath.Join(root, "MANIFEST.sha256")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	expected := map[string]string{}
	lines := strings.Split(strings.TrimSpace(string(manifest)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return fmt.Errorf("manifest is empty")
	}
	previousPath := ""
	for _, line := range lines {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 || len(parts[0]) != 64 || parts[1] == "" || filepath.IsAbs(parts[1]) || strings.Contains(parts[1], "\\") {
			return fmt.Errorf("invalid manifest record %q", line)
		}
		if decoded, decodeErr := hex.DecodeString(parts[0]); decodeErr != nil || len(decoded) != sha256.Size || parts[0] != strings.ToLower(parts[0]) {
			return fmt.Errorf("invalid manifest hash %q", parts[0])
		}
		if path.Clean(parts[1]) != parts[1] || parts[1] == "." || strings.HasPrefix(parts[1], "../") {
			return fmt.Errorf("manifest path is not normalized relative POSIX %q", parts[1])
		}
		if previousPath != "" && parts[1] <= previousPath {
			return fmt.Errorf("manifest paths are not strictly sorted: %q after %q", parts[1], previousPath)
		}
		previousPath = parts[1]
		if _, duplicate := expected[parts[1]]; duplicate {
			return fmt.Errorf("duplicate manifest path %q", parts[1])
		}
		expected[parts[1]] = parts[0]
	}
	actual := make([]string, 0, len(expected))
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel != "MANIFEST.sha256" {
			actual = append(actual, rel)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk corpus: %w", err)
	}
	sort.Strings(actual)
	if len(actual) != len(expected) {
		return fmt.Errorf("manifest has %d records for %d corpus files", len(expected), len(actual))
	}
	for _, rel := range actual {
		want, present := expected[rel]
		if !present {
			return fmt.Errorf("corpus file %q is absent from manifest", rel)
		}
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return err
		}
		actualHash := sha256.Sum256(body)
		if want != hex.EncodeToString(actualHash[:]) {
			return fmt.Errorf("manifest hash mismatch for %q", rel)
		}
	}
	return nil
}

func TestContractCorpus_ManifestPinnedRoot(t *testing.T) {
	requireManifestPinnedContracts(t)
}

func TestContractCorpus_ManifestFailsClosed(t *testing.T) {
	writeCorpus := func(t *testing.T, records []string, files map[string]string) string {
		t.Helper()
		root := t.TempDir()
		for name, body := range files {
			fullPath := filepath.Join(root, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fullPath, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "MANIFEST.sha256"), []byte(strings.Join(records, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return root
	}
	hash := func(body string) string {
		sum := sha256.Sum256([]byte(body))
		return hex.EncodeToString(sum[:])
	}
	valid := []string{hash("a") + "  a.json", hash("b") + "  b.json"}
	files := map[string]string{"a.json": "a", "b.json": "b"}
	if err := validateContractManifest(writeCorpus(t, valid, files)); err != nil {
		t.Fatalf("valid corpus rejected: %v", err)
	}
	for name, records := range map[string][]string{
		"unsorted":  {valid[1], valid[0]},
		"duplicate": {valid[0], valid[0]},
		"traversal": {hash("a") + "  ../a.json", valid[1]},
		"uppercase": {strings.ToUpper(hash("a")) + "  a.json", valid[1]},
		"stale":     {strings.Repeat("0", 64) + "  a.json", valid[1]},
		"missing":   {valid[0]},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateContractManifest(writeCorpus(t, records, files)); err == nil {
				t.Fatal("invalid corpus was accepted")
			}
		})
	}
	t.Run("extra", func(t *testing.T) {
		extraFiles := map[string]string{"a.json": "a", "b.json": "b", "c.json": "c"}
		if err := validateContractManifest(writeCorpus(t, valid, extraFiles)); err == nil {
			t.Fatal("extra corpus file was accepted")
		}
	})
}

func readContractFixture(t *testing.T, root, relativePath string, target any) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relativePath)))
	if err != nil {
		t.Fatalf("read shared contract fixture %s: %v", relativePath, err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode shared contract fixture %s: %v", relativePath, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("shared contract fixture %s has trailing JSON: %v", relativePath, err)
	}
}

func TestContractCorpus_CurrentWireFamiliesAreValidJSON(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	wireRoot := filepath.Join(root, "wire")
	entries, err := os.ReadDir(wireRoot)
	if err != nil {
		t.Fatalf("read current wire fixture directory: %v", err)
	}
	want := map[string]bool{
		"compact-rows.json": false, "compression.json": false, "errors.json": false,
		"faults.json": false, "limits.json": false, "request-counts.json": false,
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if _, required := want[entry.Name()]; !required {
			t.Errorf("current wire fixture family %s has no registered semantic consumer", entry.Name())
			continue
		}
		body, err := os.ReadFile(filepath.Join(wireRoot, entry.Name()))
		if err != nil || !json.Valid(body) {
			t.Fatalf("current wire fixture %s is not valid JSON: %v", entry.Name(), err)
		}
		want[entry.Name()] = true
	}
	for name, present := range want {
		if !present {
			t.Errorf("current wire fixture family %s is missing", name)
		}
	}
}

func TestContractCorpus_CurrentProtocolFixtureCoverage(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var classifications struct {
		Current []string `json:"currentProtocolFixtures"`
	}
	readContractFixture(t, root, "CLASSIFICATIONS.json", &classifications)
	registered := map[string]string{
		"local-apply/unique-final-state.json": "oversqlite SQLite runtime consumer",
		"local-apply/a1.json":                 "local apply limits and survivor reducer",
		"protocol-handshake/connect.json":     "handshake reducer",
		"protocol-http/requests.json":         "HTTP request builder reducer",
		"watch/basic.json":                    "SSE and watch conflation reducers",
		"wire/compact-rows.json":              "compact codec",
		"wire/compression.json":               "transport framing",
		"wire/limits.json":                    "wire limits",
		"recovery/protocol.json":              "exact outcomes, snapshots and workflow contracts",
	}
	currentSet := make(map[string]struct{}, len(classifications.Current))
	for _, current := range classifications.Current {
		if _, duplicate := currentSet[current]; duplicate {
			t.Errorf("active fixture path %q is classified more than once", current)
		}
		currentSet[current] = struct{}{}
		if _, registered := registered[current]; !registered {
			t.Errorf("active fixture path %q has no registered semantic consumer", current)
		}
	}
	if len(currentSet) != len(registered) {
		t.Errorf("current fixture classifications = %v, registered consumers = %v", classifications.Current, registered)
	}
	for expected := range registered {
		if _, present := currentSet[expected]; !present {
			t.Errorf("registered semantic consumer %q is missing from active fixture classifications", expected)
		}
	}

	var handshake struct {
		FormatVersion  int `json:"formatVersion"`
		DiscoveryCases []struct {
			Discovery map[string]any `json:"discovery"`
			Connect   map[string]any `json:"connect"`
			Expected  struct {
				Kind                        string         `json:"kind"`
				OrdinaryOperationPreflights int            `json:"ordinaryOperationPreflights"`
				TableIndices                map[string]int `json:"tableIndices"`
			} `json:"expected"`
		} `json:"discoveryCases"`
	}
	readContractFixture(t, root, "protocol-handshake/connect.json", &handshake)
	if handshake.FormatVersion != 2 {
		t.Fatalf("handshake formatVersion = %d, want 2", handshake.FormatVersion)
	}
	allowedDiscoveryFields := map[string]bool{"protocol_version": true, "schema_generation": true, "limits": true, "watch_enabled": true}
	requiredLimitFields := map[string]bool{
		"max_push_rows": true, "max_push_decoded_bytes": true, "max_pull_bundles": true,
		"max_pull_decoded_bytes": true, "max_snapshot_rows_per_chunk": true,
		"max_snapshot_decoded_bytes_per_chunk": true, "max_snapshot_rows": true,
		"max_snapshot_logical_bytes": true, "max_row_decoded_bytes": true, "max_push_outcome_decoded_bytes": true,
	}
	for _, test := range handshake.DiscoveryCases {
		limits, _ := test.Discovery["limits"].(map[string]any)
		valid := test.Discovery["protocol_version"] == "v1" && len(test.Discovery) == len(allowedDiscoveryFields) && len(limits) == len(requiredLimitFields)
		for name := range test.Discovery {
			valid = valid && allowedDiscoveryFields[name]
		}
		for name, value := range limits {
			number, ok := value.(float64)
			valid = valid && requiredLimitFields[name] && ok && number > 0
		}
		if (test.Expected.Kind == "ready") != valid {
			t.Errorf("handshake expected %q, reducer valid=%t", test.Expected.Kind, valid)
		}
		if valid {
			if test.Expected.OrdinaryOperationPreflights != 0 || len(test.Connect) != 1 || test.Connect["has_local_pending_rows"] == nil {
				t.Errorf("ready handshake does not reduce to one connect without operation preflights")
			}
			generation := test.Discovery["schema_generation"].(map[string]any)
			tables := generation["tables"].([]any)
			actualIndices := map[string]int{}
			for index, rawTable := range tables {
				table := rawTable.(map[string]any)
				actualIndices[table["schema"].(string)+"."+table["table"].(string)] = index
			}
			if !reflect.DeepEqual(actualIndices, test.Expected.TableIndices) {
				t.Errorf("handshake table indices = %v, want %v", actualIndices, test.Expected.TableIndices)
			}
		}
	}

	var requests struct {
		Cases                  []protocolHTTPRequestCase    `json:"cases"`
		Removed                []string                     `json:"removedOperations"`
		RequiredProtocolHeader struct{ Name, Value string } `json:"requiredProtocolHeader"`
	}
	readContractFixture(t, root, "protocol-http/requests.json", &requests)
	if requests.RequiredProtocolHeader.Name != ProtocolVersionHeader || requests.RequiredProtocolHeader.Value != SyncProtocolVersion {
		t.Fatal("required protocol header fixture mismatch")
	}
	var pushFixture struct {
		Cases []struct {
			PushRequest json.RawMessage `json:"pushRequest"`
		} `json:"cases"`
	}
	readContractFixture(t, root, "push/basic-insert.json", &pushFixture)
	expectedOperations := map[string]bool{
		"discovery":       false,
		"connect":         false,
		"push":            false,
		"pull":            false,
		"snapshotSession": false,
		"snapshotChunk":   false,
		"snapshotRelease": false,
		"watch":           false,
	}
	caseNames := make(map[string]struct{}, len(requests.Cases))
	for _, test := range requests.Cases {
		seen, known := expectedOperations[test.Operation]
		if !known {
			t.Errorf("HTTP request case %q uses unknown operation", test.Operation)
		} else if seen {
			t.Errorf("HTTP request operation %q is covered more than once", test.Operation)
		} else {
			expectedOperations[test.Operation] = true
		}
		if test.Name == "" {
			t.Errorf("HTTP request operation %q has no case name", test.Operation)
		} else if _, duplicate := caseNames[test.Name]; duplicate {
			t.Errorf("HTTP request case name %q is repeated", test.Name)
		} else {
			caseNames[test.Name] = struct{}{}
		}
		actual, err := reduceProtocolHTTPRequest(test, pushFixture.Cases[0].PushRequest)
		if err != nil {
			t.Errorf("HTTP request case %q: %v", test.Operation, err)
			continue
		}
		if actual.Method != test.Expected.Method || actual.Path != test.Expected.Path || actual.SourceHeader != test.Expected.SourceHeader || !reflect.DeepEqual(actual.Query, test.Expected.Query) || !jsonDocumentsEqual(actual.Body, test.Expected.Body) {
			t.Errorf("HTTP request case %q reduced to %#v, want %#v", test.Operation, actual, test.Expected)
		}
		for _, forbidden := range test.Expected.BodyMustNotContain {
			if bytes.Contains(actual.Body, []byte(forbidden)) {
				t.Errorf("HTTP request case %q body contains forbidden token %q", test.Operation, forbidden)
			}
		}
	}
	for operation, present := range expectedOperations {
		if !present {
			t.Errorf("HTTP request fixture is missing operation %q", operation)
		}
	}
	wantRemoved := []string{"capabilities", "pushSessionCreate", "pushSessionChunk", "pushSessionCommit", "committedRows"}
	sort.Strings(requests.Removed)
	sort.Strings(wantRemoved)
	if !reflect.DeepEqual(requests.Removed, wantRemoved) {
		t.Errorf("removed operations = %v, want %v", requests.Removed, wantRemoved)
	}

	var pullSnapshot struct {
		Cases []struct {
			AfterBundleSeq int64 `json:"afterBundleSeq"`
			PullResponse   struct {
				StableBundleSeq int64 `json:"stable_bundle_seq"`
				Bundles         []struct {
					BundleSeq int64           `json:"bundle_seq"`
					Rows      json.RawMessage `json:"rows"`
				} `json:"bundles"`
			} `json:"pullResponse"`
			SnapshotSession *struct {
				SnapshotBundleSeq int64 `json:"snapshot_bundle_seq"`
				RowCount          int64 `json:"row_count"`
				LogicalByteCount  int64 `json:"logical_byte_count"`
			} `json:"snapshotSession"`
			SnapshotChunk *struct {
				Rows             json.RawMessage `json:"rows"`
				NextRowOrdinal   int64           `json:"next_row_ordinal"`
				LogicalByteCount int64           `json:"logical_byte_count"`
				HasMore          bool            `json:"has_more"`
			} `json:"snapshotChunkResponse"`
			Expected struct {
				LastBundleSeqSeen int64 `json:"lastBundleSeqSeen"`
				UsersRowCount     int   `json:"usersRowCount"`
				RowStateDeleted   int   `json:"rowStateDeleted"`
			} `json:"expectedFinalState"`
		} `json:"cases"`
	}
	readContractFixture(t, root, "pull-snapshot/basic.json", &pullSnapshot)
	for _, test := range pullSnapshot.Cases {
		checkpoint := test.AfterBundleSeq
		users := map[string]bool{}
		deleted := 0
		for _, bundle := range test.PullResponse.Bundles {
			if bundle.BundleSeq != checkpoint+1 {
				t.Errorf("pull bundle sequence %d does not follow checkpoint %d", bundle.BundleSeq, checkpoint)
			}
			if err := applyCompactUserFixtureRows(bundle.Rows, users, &deleted, true); err != nil {
				t.Errorf("pull bundle %d: %v", bundle.BundleSeq, err)
			}
			checkpoint = bundle.BundleSeq
		}
		if test.SnapshotSession != nil {
			users = map[string]bool{}
			deleted = 0
			if test.SnapshotChunk == nil || test.SnapshotChunk.HasMore || test.SnapshotChunk.NextRowOrdinal != test.SnapshotSession.RowCount || test.SnapshotChunk.LogicalByteCount != test.SnapshotSession.LogicalByteCount {
				t.Errorf("snapshot fixture does not describe one complete bounded chunk")
			} else if err := applyCompactUserFixtureRows(test.SnapshotChunk.Rows, users, &deleted, false); err != nil {
				t.Errorf("snapshot rows: %v", err)
			}
			checkpoint = test.SnapshotSession.SnapshotBundleSeq
		}
		if checkpoint != test.Expected.LastBundleSeqSeen || len(users) != test.Expected.UsersRowCount || deleted != test.Expected.RowStateDeleted {
			t.Errorf("reduced pull/snapshot state = checkpoint %d users %d deleted %d, want %+v", checkpoint, len(users), deleted, test.Expected)
		}
	}

	var watch struct {
		SSECases []struct {
			Name           string   `json:"name"`
			Lines          []string `json:"lines"`
			ExpectedEvents []struct {
				BundleSeq int64 `json:"bundle_seq"`
			} `json:"expectedEvents"`
			ExpectError bool `json:"expectError"`
		} `json:"sseCases"`
		DiscoveryCases []struct {
			Response struct {
				Status int `json:"status"`
				Body   struct {
					WatchEnabled bool `json:"watch_enabled"`
				} `json:"body"`
			} `json:"response"`
			ExpectedWatchEnabled bool `json:"expectedWatchEnabled"`
		} `json:"discoveryCases"`
		ConflationCases []struct {
			Events                 int   `json:"events"`
			Checkpoint             int64 `json:"checkpoint"`
			HighWater              int64 `json:"highWater"`
			ExpectedActivePulls    int   `json:"expectedActivePulls"`
			ExpectedTrailingAtMost int   `json:"expectedTrailingPullsAtMost"`
			ExpectedCheckpoint     int64 `json:"expectedCheckpoint"`
		} `json:"conflationCases"`
	}
	readContractFixture(t, root, "watch/basic.json", &watch)
	for _, test := range watch.DiscoveryCases {
		if test.Response.Status != http.StatusOK || test.Response.Body.WatchEnabled != test.ExpectedWatchEnabled {
			t.Errorf("watch discovery did not preserve watch_enabled")
		}
	}
	for _, test := range watch.SSECases {
		actualEvents, err := reduceSSEBundleEvents(test.Lines)
		if test.ExpectError {
			if err == nil {
				t.Errorf("SSE case %q was accepted", test.Name)
			}
			continue
		}
		if err != nil {
			t.Errorf("SSE case %q: %v", test.Name, err)
			continue
		}
		wantEvents := make([]int64, len(test.ExpectedEvents))
		for index, event := range test.ExpectedEvents {
			wantEvents[index] = event.BundleSeq
		}
		if !reflect.DeepEqual(actualEvents, wantEvents) {
			t.Errorf("SSE case %q events = %v, want %v", test.Name, actualEvents, wantEvents)
		}
		if test.Name == "canonical-bundle-event" {
			var rendered bytes.Buffer
			if err := writeSSEBundleEvent(&rendered, BundleChangeEvent{BundleSeq: test.ExpectedEvents[0].BundleSeq}); err != nil {
				t.Fatal(err)
			}
			if rendered.String() != strings.Join(test.Lines, "\n")+"\n" {
				t.Errorf("canonical SSE = %q", rendered.String())
			}
		}
	}
	for _, test := range watch.ConflationCases {
		active, trailing, checkpoint := reduceWatchConflation(test.Events, test.Checkpoint, test.HighWater)
		wantCheckpoint := test.ExpectedCheckpoint
		if wantCheckpoint == 0 {
			wantCheckpoint = test.HighWater
		}
		if active != test.ExpectedActivePulls || trailing > test.ExpectedTrailingAtMost || checkpoint != wantCheckpoint {
			t.Errorf("watch reducer = active %d trailing %d checkpoint %d", active, trailing, checkpoint)
		}
	}
}

type protocolHTTPRequestCase struct {
	Name      string `json:"name"`
	Operation string `json:"operation"`
	SourceID  string `json:"sourceId"`
	Args      struct {
		GenerationID        string `json:"generationId"`
		HasLocalPendingRows bool   `json:"hasLocalPendingRows"`
		SourceBundleID      int64  `json:"sourceBundleId"`
		AfterBundleSeq      int64  `json:"afterBundleSeq"`
		MaxBundles          int    `json:"maxBundles"`
		TargetBundleSeq     int64  `json:"targetBundleSeq"`
		SnapshotID          string `json:"snapshotId"`
		AfterRowOrdinal     int64  `json:"afterRowOrdinal"`
		MaxRows             int    `json:"maxRows"`
		MaxBytes            int64  `json:"maxBytes"`
	} `json:"args"`
	Expected reducedProtocolHTTPRequest `json:"expected"`
}

type reducedProtocolHTTPRequest struct {
	Method             string            `json:"method"`
	Path               string            `json:"path"`
	Query              map[string]string `json:"query"`
	SourceHeader       string            `json:"sourceHeader"`
	Body               json.RawMessage   `json:"body"`
	BodyMustNotContain []string          `json:"bodyMustNotContain"`
}

func reduceProtocolHTTPRequest(test protocolHTTPRequestCase, pushBody json.RawMessage) (reducedProtocolHTTPRequest, error) {
	request := reducedProtocolHTTPRequest{Query: map[string]string{}, SourceHeader: test.SourceID, Body: json.RawMessage("null")}
	switch test.Operation {
	case "discovery":
		request.Method, request.Path = http.MethodGet, "/api/v1/sync/generations/"+test.Args.GenerationID
	case "connect":
		request.Method, request.Path = http.MethodPost, "/api/v1/sync/connect"
		request.Body = json.RawMessage(fmt.Sprintf(`{"has_local_pending_rows":%t}`, test.Args.HasLocalPendingRows))
	case "push":
		request.Method, request.Path = http.MethodPost, "/api/v1/sync/push"
		var body map[string]any
		if err := json.Unmarshal(pushBody, &body); err != nil {
			return reducedProtocolHTTPRequest{}, err
		}
		body["source_bundle_id"] = test.Args.SourceBundleID
		request.Body, _ = json.Marshal(body)
	case "pull":
		request.Method, request.Path = http.MethodGet, "/api/v1/sync/pull"
		request.Query = map[string]string{
			"after_bundle_seq":  strconv.FormatInt(test.Args.AfterBundleSeq, 10),
			"max_bundles":       strconv.Itoa(test.Args.MaxBundles),
			"target_bundle_seq": strconv.FormatInt(test.Args.TargetBundleSeq, 10),
		}
	case "snapshotSession":
		request.Method, request.Path = http.MethodPost, "/api/v1/sync/snapshot-sessions"
	case "snapshotChunk":
		request.Method, request.Path = http.MethodGet, "/api/v1/sync/snapshot-sessions/"+test.Args.SnapshotID
		request.Query = map[string]string{
			"after_row_ordinal": strconv.FormatInt(test.Args.AfterRowOrdinal, 10),
			"max_rows":          strconv.Itoa(test.Args.MaxRows),
			"max_bytes":         strconv.FormatInt(test.Args.MaxBytes, 10),
		}
	case "snapshotRelease":
		request.Method, request.Path = http.MethodDelete, "/api/v1/sync/snapshot-sessions/"+test.Args.SnapshotID
	case "watch":
		request.Method, request.Path = http.MethodGet, "/api/v1/sync/watch"
		request.Query = map[string]string{"after_bundle_seq": strconv.FormatInt(test.Args.AfterBundleSeq, 10)}
	default:
		return reducedProtocolHTTPRequest{}, fmt.Errorf("unknown protocol request operation %q", test.Operation)
	}
	return request, nil
}

func jsonDocumentsEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}

func applyCompactUserFixtureRows(raw json.RawMessage, users map[string]bool, deleted *int, allowDelete bool) error {
	if len(raw) == 0 {
		return nil
	}
	var groups []CompactRowGroup
	if err := json.Unmarshal(raw, &groups); err != nil {
		return err
	}
	for _, group := range groups {
		if group.TableIndex != 0 {
			return fmt.Errorf("unexpected fixture table index %d", group.TableIndex)
		}
		for _, row := range group.Rows {
			if len(row.KeyValues) != 1 {
				return errors.New("fixture user row must have one key")
			}
			key, ok := row.KeyValues[0].(string)
			if !ok || key == "" {
				return errors.New("fixture user key is invalid")
			}
			switch row.Op {
			case "i", "u":
				if len(row.Payload) == 0 {
					return errors.New("fixture upsert omits payload")
				}
				users[key] = true
			case "d":
				if !allowDelete {
					return errors.New("snapshot fixture contains delete")
				}
				delete(users, key)
				(*deleted)++
			default:
				return fmt.Errorf("unknown compact operation %q", row.Op)
			}
		}
	}
	return nil
}

func reduceSSEBundleEvents(lines []string) ([]int64, error) {
	events := []int64{}
	eventName, data := "", ""
	flush := func() error {
		defer func() { eventName, data = "", "" }()
		if eventName != "bundle" {
			return nil
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &body); err != nil || len(body) != 1 {
			return errors.New("bundle SSE data must contain exactly bundle_seq")
		}
		var sequence int64
		if err := json.Unmarshal(body["bundle_seq"], &sequence); err != nil || sequence < 1 {
			return errors.New("bundle SSE sequence is invalid")
		}
		events = append(events, sequence)
		return nil
	}
	for _, line := range lines {
		switch {
		case line == "":
			if err := flush(); err != nil {
				return nil, err
			}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event: "):
			eventName = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		default:
			return nil, fmt.Errorf("invalid SSE line %q", line)
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return events, nil
}

func reduceWatchConflation(events int, checkpoint, highWater int64) (active, trailing int, finalCheckpoint int64) {
	if events == 0 {
		return 0, 0, checkpoint
	}
	active = 1
	if events > 1 {
		trailing = 1
	}
	finalCheckpoint = int64(events)
	if highWater > finalCheckpoint {
		finalCheckpoint = highWater
	}
	return active, trailing, finalCheckpoint
}

type compactRowsCorpus struct {
	FormatVersion int `json:"formatVersion"`
	Generation    struct {
		ID          string `json:"id"`
		Fingerprint string `json:"fingerprint"`
		Tables      []struct {
			Schema         string   `json:"schema"`
			Table          string   `json:"table"`
			SyncKeyColumns []string `json:"sync_key_columns"`
			Columns        []string `json:"columns"`
		} `json:"tables"`
	} `json:"generation"`
	EquivalenceCases []struct {
		Name                        string          `json:"name"`
		CompactRows                 json.RawMessage `json:"compactRows"`
		Snapshot                    bool            `json:"snapshot"`
		ExpectedExpandedOperations  []string        `json:"expectedExpandedOperations"`
		ExpectedOrdinals            []int64         `json:"expectedOrdinals"`
		ExpectedExpandedRows        json.RawMessage `json:"expectedExpandedRows"`
		VerboseLogicalRows          json.RawMessage `json:"verboseLogicalRows"`
		ExpectedSnapshotLogicalRows json.RawMessage `json:"expectedSnapshotLogicalRows"`
		HashDomain                  string          `json:"hashDomain"`
		SnapshotLogicalHashDomain   string          `json:"snapshotLogicalHashDomain"`
	} `json:"equivalenceCases"`
	InvalidCases []struct {
		Name     string          `json:"name"`
		Rows     json.RawMessage `json:"rows"`
		Snapshot bool            `json:"snapshot"`
	} `json:"invalidCases"`
}

func validateSnapshotCorpusRows(generation SchemaGenerationCapabilities, rows []PushRequestRow) error {
	tables := make(map[string]SchemaGenerationTableCapabilities, len(generation.Tables))
	for _, table := range generation.Tables {
		tables[table.Schema+"\x00"+table.Table] = table
	}
	for _, row := range rows {
		if row.Op != OpInsert {
			return fmt.Errorf("snapshot operation %q is not insert", row.Op)
		}
		table := tables[row.Schema+"\x00"+row.Table]
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			return err
		}
		if len(payload) != len(table.Columns) {
			return fmt.Errorf("snapshot payload for %s.%s is not a full after-image", row.Schema, row.Table)
		}
		for _, column := range table.Columns {
			if _, ok := payload[column.Name]; !ok {
				return fmt.Errorf("snapshot payload for %s.%s is missing %s", row.Schema, row.Table, column.Name)
			}
		}
	}
	return validateGenerationPushRows(schemaGenerationContractFromCapabilities(generation), rows)
}

func expandedRowsMatchFixture(actual []PushRequestRow, raw json.RawMessage) bool {
	var expected []struct {
		Schema  string          `json:"schema"`
		Table   string          `json:"table"`
		Key     SyncKey         `json:"key"`
		Op      string          `json:"op"`
		Version int64           `json:"version"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &expected); err != nil || len(expected) != len(actual) {
		return false
	}
	for index, want := range expected {
		got := actual[index]
		if got.Schema != want.Schema || got.Table != want.Table || got.Op != want.Op || got.BaseRowVersion != want.Version || !reflect.DeepEqual(got.Key, want.Key) {
			return false
		}
		if len(want.Payload) != 0 && !bytes.Equal(bytes.TrimSpace(got.Payload), bytes.TrimSpace(want.Payload)) {
			return false
		}
	}
	return true
}

func (fixture compactRowsCorpus) capabilities() SchemaGenerationCapabilities {
	generation := SchemaGenerationCapabilities{ID: fixture.Generation.ID, Fingerprint: fixture.Generation.Fingerprint}
	for _, table := range fixture.Generation.Tables {
		projected := SchemaGenerationTableCapabilities{Schema: table.Schema, Table: table.Table, SyncKeyColumns: table.SyncKeyColumns}
		for _, column := range table.Columns {
			projected.Columns = append(projected.Columns, SchemaGenerationColumnCapabilities{Name: column, WireType: GenerationWireTypeText, Nullable: true})
		}
		generation.Tables = append(generation.Tables, projected)
	}
	return generation
}

func TestContractCorpus_CompactRowsExecuteAgainstCodec(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var fixture compactRowsCorpus
	readContractFixture(t, root, "wire/compact-rows.json", &fixture)
	if fixture.FormatVersion != 2 {
		t.Fatalf("compact rows fixture formatVersion = %d, want 2", fixture.FormatVersion)
	}
	generation := fixture.capabilities()
	for _, test := range fixture.EquivalenceCases {
		t.Run(test.Name, func(t *testing.T) {
			var groups []CompactRowGroup
			if err := json.Unmarshal(test.CompactRows, &groups); err != nil {
				t.Fatalf("decode compact rows: %v", err)
			}
			expanded, err := ExpandCompactPushRows(generation, groups)
			if err != nil {
				t.Fatalf("expand compact rows: %v", err)
			}
			if len(test.ExpectedExpandedOperations) != 0 {
				got := make([]string, len(expanded))
				ordinals := make([]int64, len(expanded))
				for index, row := range expanded {
					got[index] = row.Op
					ordinals[index] = int64(index)
				}
				if !reflect.DeepEqual(got, test.ExpectedExpandedOperations) || !reflect.DeepEqual(ordinals, test.ExpectedOrdinals) {
					t.Fatalf("expanded operations/ordinals = %v/%v, want %v/%v", got, ordinals, test.ExpectedExpandedOperations, test.ExpectedOrdinals)
				}
			}
			if len(test.ExpectedExpandedRows) != 0 {
				if !expandedRowsMatchFixture(expanded, test.ExpectedExpandedRows) {
					t.Fatalf("expanded compact rows do not match %s", test.ExpectedExpandedRows)
				}
			}
			if test.HashDomain != "" {
				const hashDomain = "oversync.generation-pinned-push-request.v1"
				if test.HashDomain != hashDomain {
					t.Fatalf("hash domain = %q, want %q", test.HashDomain, hashDomain)
				}
				if _, err := computeGenerationPinnedPushRequestHash(SchemaGenerationProof{ID: 1, Fingerprint: generation.Fingerprint}, expanded); err != nil {
					t.Fatalf("hash expanded compact rows: %v", err)
				}
			}
			if test.Snapshot {
				if err := validateSnapshotCorpusRows(generation, expanded); err != nil {
					t.Fatalf("snapshot compact rows are invalid: %v", err)
				}
			}
		})
	}
	for _, test := range fixture.InvalidCases {
		t.Run("invalid/"+test.Name, func(t *testing.T) {
			var groups []CompactRowGroup
			decodeErr := json.Unmarshal(test.Rows, &groups)
			if decodeErr != nil {
				return
			}
			expanded, expandErr := ExpandCompactPushRows(generation, groups)
			if expandErr != nil {
				return
			}
			if test.Snapshot {
				if err := validateSnapshotCorpusRows(generation, expanded); err != nil {
					return
				}
			} else if err := validateGenerationPushRows(schemaGenerationContractFromCapabilities(generation), expanded); err != nil {
				return
			}
			t.Fatalf("invalid compact fixture was accepted")
		})
	}
}

func TestContractCorpus_PushHashesExecuteAgainstNormativeAlgorithms(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var compact compactRowsCorpus
	readContractFixture(t, root, "wire/compact-rows.json", &compact)
	generation := compact.capabilities()
	contract := schemaGenerationContractFromCapabilities(generation)
	proof := SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}

	var fixture struct {
		FormatVersion int `json:"formatVersion"`
		Cases         []struct {
			Name         string              `json:"name"`
			PushRequest  CompactPushRequest  `json:"pushRequest"`
			PushResponse CompactPushResponse `json:"pushResponse"`
		} `json:"cases"`
	}
	readContractFixture(t, root, "push/basic-insert.json", &fixture)
	if fixture.FormatVersion != 2 {
		t.Fatalf("push fixture formatVersion = %d, want 2", fixture.FormatVersion)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Name, func(t *testing.T) {
			requestRows, err := ExpandCompactPushRows(generation, test.PushRequest.Rows)
			if err != nil {
				t.Fatalf("expand request: %v", err)
			}
			requestHash, err := computeGenerationPinnedPushRequestHash(proof, requestRows)
			if err != nil {
				t.Fatalf("hash request: %v", err)
			}
			if requestHash != test.PushRequest.CanonicalRequestHash {
				t.Fatalf("canonical_request_hash = %s, want %s", test.PushRequest.CanonicalRequestHash, requestHash)
			}

			responseRows, err := ExpandCompactPushRows(generation, test.PushResponse.Rows)
			if err != nil {
				t.Fatalf("expand response: %v", err)
			}
			bundleRows := make([]BundleRow, 0, len(responseRows))
			for _, row := range responseRows {
				bundleRows = append(bundleRows, BundleRow{
					Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
					RowVersion: row.BaseRowVersion, Payload: row.Payload,
				})
			}
			bundleHash, _, err := computeCommittedBundleHash(bundleRows)
			if err != nil {
				t.Fatalf("hash response bundle: %v", err)
			}
			wantBundleHash := hex.EncodeToString(bundleHash)
			if test.PushResponse.BundleHash != wantBundleHash {
				t.Fatalf("bundle_hash = %s, want %s", test.PushResponse.BundleHash, wantBundleHash)
			}
		})
	}
}

func schemaGenerationContractFromCapabilities(generation SchemaGenerationCapabilities) SchemaGenerationContract {
	tables := make([]GenerationTableContract, 0, len(generation.Tables))
	for _, table := range generation.Tables {
		columns := make([]GenerationColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, GenerationColumn{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
		}
		tables = append(tables, GenerationTableContract{Schema: table.Schema, Table: table.Table, SyncKeyColumns: table.SyncKeyColumns, Columns: columns})
	}
	id, err := strconv.ParseInt(generation.ID, 10, 32)
	if err != nil {
		return SchemaGenerationContract{}
	}
	return SchemaGenerationContract{id: SchemaGenerationID(id), fingerprint: generation.Fingerprint, tables: tables}
}

func TestContractCorpus_ErrorAndRequestTraceDecisions(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var errorsFixture struct {
		FormatVersion            int      `json:"formatVersion"`
		PushValidationPrecedence []string `json:"pushValidationPrecedence"`
		Cases                    []struct {
			Name                 string `json:"name"`
			CompactRowsCase      string `json:"compactRowsCase"`
			CanonicalRequestHash string `json:"canonicalRequestHash"`
			Status               int    `json:"status"`
			Error                string `json:"error"`
		} `json:"cases"`
	}
	readContractFixture(t, root, "wire/errors.json", &errorsFixture)
	wantPrecedence := []string{
		"method_and_authentication", "source_and_generation_headers", "media_type_and_content_encoding",
		"encoded_limit", "gzip_decode_and_decoded_limit", "strict_json_and_envelope",
		"compact_tuple_logical_validation_and_canonical_request_hash", "committed_history_replay",
		"source_lifecycle_sequence_initialization_conflict_application_commit",
	}
	if !reflect.DeepEqual(errorsFixture.PushValidationPrecedence, wantPrecedence) {
		t.Fatalf("push validation precedence = %v, want %v", errorsFixture.PushValidationPrecedence, wantPrecedence)
	}
	statusForCode := map[string]int{
		"method_not_allowed": 405, "authentication_failed": 401,
		"malformed_schema_generation_fingerprint": 400,
		"unsupported_media_type":                  415, "unsupported_content_encoding": 415,
		"encoded_body_too_large": 413, "decoded_body_too_large": 413,
		"invalid_gzip": 400, "invalid_request": 400, "push_invalid": 400,
		"push_replay_mismatch": 409, "push_conflict": 409,
	}
	seen := map[string]bool{}
	for _, test := range errorsFixture.Cases {
		wantStatus, known := statusForCode[test.Error]
		if !known || wantStatus != test.Status {
			t.Errorf("error case %q = %d %s", test.Name, test.Status, test.Error)
		}
		seen[test.Name] = true
		if test.Name == "hidden-column-after-valid-canonical-hash" {
			var compact compactRowsCorpus
			readContractFixture(t, root, "wire/compact-rows.json", &compact)
			var raw json.RawMessage
			for _, invalid := range compact.InvalidCases {
				if invalid.Name == test.CompactRowsCase {
					raw = invalid.Rows
				}
			}
			if raw == nil {
				t.Fatalf("hidden-column case %q is not present", test.CompactRowsCase)
			}
			var groups []CompactRowGroup
			if err := json.Unmarshal(raw, &groups); err != nil {
				t.Fatal(err)
			}
			rows, err := ExpandCompactPushRows(compact.capabilities(), groups)
			if err != nil {
				t.Fatalf("hidden-column compact rows must be structurally valid: %v", err)
			}
			proof := SchemaGenerationProof{ID: 1, Fingerprint: compact.Generation.Fingerprint}
			actualHash, err := computeGenerationPinnedPushRequestHash(proof, rows)
			if err != nil {
				t.Fatal(err)
			}
			if actualHash != test.CanonicalRequestHash {
				t.Fatalf("hidden-column canonical hash = %s, fixture = %s", actualHash, test.CanonicalRequestHash)
			}
			if err := validateGenerationPushRows(schemaGenerationContractFromCapabilities(compact.capabilities()), rows); err == nil {
				t.Fatal("hidden-column rows passed logical validation after their canonical hash was proved")
			}
		}
	}
	for _, name := range []string{
		"wrong-method-wins", "auth-wins-over-malformed-body", "generation-header-wins-over-unsupported-encoding",
		"empty-content-encoding", "duplicate-content-encoding", "encoding-wins-over-encoded-overflow",
		"connect-missing-has-local-pending-rows", "connect-null-has-local-pending-rows",
		"snapshot-source-replacement-null", "snapshot-source-replacement-missing-reason",
		"hidden-column-after-valid-canonical-hash", "replay-hash-mismatch-after-valid-hash",
	} {
		if !seen[name] {
			t.Errorf("wire/errors.json is missing semantic case %q", name)
		}
	}

	var traces struct {
		FormatVersion int `json:"formatVersion"`
		Traces        []struct {
			Name                    string         `json:"name"`
			Initial                 map[string]any `json:"initial"`
			Events                  []string       `json:"events"`
			Requests                []string       `json:"requests"`
			ForbiddenRequests       []string       `json:"forbiddenRequests"`
			SequentialExchangeCount int            `json:"sequentialExchangeCount"`
			ReplayIsExact           bool           `json:"replayIsExact"`
			ExpectedTerminal        map[string]any `json:"expectedTerminal"`
		} `json:"traces"`
		QueryCases []struct {
			Name      string              `json:"name"`
			Operation string              `json:"operation"`
			Query     map[string][]string `json:"query"`
			Expected  struct {
				Accepted       bool   `json:"accepted"`
				AfterBundleSeq string `json:"after_bundle_seq"`
				MaxBundles     string `json:"max_bundles"`
				TargetBundle   string `json:"target_bundle_seq"`
				AfterRow       string `json:"after_row_ordinal"`
				MaxRows        string `json:"max_rows"`
				MaxBytes       string `json:"max_bytes"`
			} `json:"expected"`
		} `json:"queryCases"`
	}
	readContractFixture(t, root, "wire/request-counts.json", &traces)
	if traces.FormatVersion != 3 {
		t.Fatalf("request trace formatVersion = %d, want 3", traces.FormatVersion)
	}
	wantTraces := map[string]bool{"opening": false, "admitted-no-change": false, "admitted-one-row": false, "lost-push-response-replay": false, "pagination": false, "watch-burst": false, "pull-defaults": false, "watch-default": false, "snapshot-chunk-defaults": false}
	for _, trace := range traces.Traces {
		if _, ok := wantTraces[trace.Name]; !ok {
			t.Errorf("unknown request trace %q", trace.Name)
		} else {
			wantTraces[trace.Name] = true
		}
		actualRequests, terminal, err := reduceRequestTrace(trace.Initial, trace.Events)
		if err != nil {
			t.Errorf("trace %q: %v", trace.Name, err)
			continue
		}
		requestSummaries := make([]string, len(actualRequests))
		for index, request := range actualRequests {
			requestSummaries[index] = request.Summary
		}
		if !reflect.DeepEqual(requestSummaries, trace.Requests) {
			t.Errorf("trace %q requests = %v, want %v", trace.Name, requestSummaries, trace.Requests)
		}
		for _, request := range requestSummaries {
			if strings.Contains(request, "capabilities") || strings.Contains(request, "push-sessions") || strings.Contains(request, "committed-bundles") {
				t.Errorf("trace %q actively requests removed route %q", trace.Name, request)
			}
		}
		for _, forbidden := range trace.ForbiddenRequests {
			for _, actual := range requestSummaries {
				if actual == forbidden {
					t.Errorf("trace %q emitted forbidden request %q", trace.Name, forbidden)
				}
			}
		}
		if trace.SequentialExchangeCount != 0 && trace.SequentialExchangeCount != len(actualRequests) {
			t.Errorf("trace %q sequentialExchangeCount = %d for %d requests", trace.Name, trace.SequentialExchangeCount, len(actualRequests))
		}
		if trace.Name == "lost-push-response-replay" && (!trace.ReplayIsExact || len(actualRequests) < 2 || actualRequests[0] != actualRequests[1] || actualRequests[0].SourceID == "" || actualRequests[0].Body == "") {
			t.Errorf("lost response replay trace is not exact")
		}
		for key, want := range trace.ExpectedTerminal {
			if !reflect.DeepEqual(terminal[key], want) {
				t.Errorf("trace %q terminal %s = %#v, want %#v", trace.Name, key, terminal[key], want)
			}
		}
	}
	for name, present := range wantTraces {
		if !present {
			t.Errorf("wire/request-counts.json is missing trace %q", name)
		}
	}
	for _, test := range traces.QueryCases {
		request := httptest.NewRequest(http.MethodGet, "/sync/test", nil)
		request.URL.RawQuery = encodeRawQuery(test.Query)
		names := map[string][]string{
			"pull":     {"after_bundle_seq", "max_bundles", "target_bundle_seq"},
			"watch":    {"after_bundle_seq"},
			"snapshot": {"after_row_ordinal", "max_rows", "max_bytes"},
		}[test.Operation]
		resolved := map[string]string{
			"after_bundle_seq": "0", "max_bundles": "server_default", "target_bundle_seq": "0",
			"after_row_ordinal": "0", "max_rows": "server_default", "max_bytes": "server_default",
		}
		accepted := len(names) != 0
		for _, name := range names {
			value, present, err := optionalScalarQueryParam(request, name)
			if err != nil {
				accepted = false
				break
			}
			if present {
				resolved[name] = value
			}
		}
		if accepted != test.Expected.Accepted {
			t.Errorf("query case %q accepted = %t, want %t", test.Name, accepted, test.Expected.Accepted)
			continue
		}
		if !accepted {
			continue
		}
		for name, want := range map[string]string{
			"after_bundle_seq": test.Expected.AfterBundleSeq, "max_bundles": test.Expected.MaxBundles,
			"target_bundle_seq": test.Expected.TargetBundle, "after_row_ordinal": test.Expected.AfterRow,
			"max_rows": test.Expected.MaxRows, "max_bytes": test.Expected.MaxBytes,
		} {
			if want != "" && resolved[name] != want {
				t.Errorf("query case %q %s = %q, want %q", test.Name, name, resolved[name], want)
			}
		}
	}
}

func encodeRawQuery(query map[string][]string) string {
	var parts []string
	for name, values := range query {
		for _, value := range values {
			parts = append(parts, name+"="+value)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

type contractTraceRequest struct {
	Summary        string
	SourceID       string `json:"sourceId"`
	SourceBundleID int64  `json:"sourceBundleId"`
	Body           string `json:"body"`
}

func contractTraceFrozenRequest(initial map[string]any, key, summary string) (contractTraceRequest, bool, error) {
	raw, present := initial[key]
	if !present {
		return contractTraceRequest{}, false, nil
	}
	body, err := json.Marshal(raw)
	if err != nil {
		return contractTraceRequest{}, true, err
	}
	request := contractTraceRequest{Summary: summary}
	if err := json.Unmarshal(body, &request); err != nil {
		return contractTraceRequest{}, true, err
	}
	if request.SourceID == "" || request.SourceBundleID < 1 || request.Body == "" || !json.Valid([]byte(request.Body)) {
		return contractTraceRequest{}, true, errors.New("frozen push request identity is incomplete")
	}
	return request, true, nil
}

func reduceRequestTrace(initial map[string]any, events []string) ([]contractTraceRequest, map[string]any, error) {
	requests := []contractTraceRequest{}
	checkpoint := initial["checkpoint"]
	if checkpoint == nil {
		checkpoint = float64(0)
	}
	terminal := map[string]any{"checkpoint": checkpoint, "running": true, "activePulls": float64(0), "trailingPulls": float64(0)}
	frozenPush, hasFrozenPush, err := contractTraceFrozenRequest(initial, "frozenPush", "POST /sync/push")
	if err != nil {
		return nil, nil, err
	}
	appendRequest := func(summary string) {
		requests = append(requests, contractTraceRequest{Summary: summary})
	}
	for _, event := range events {
		switch {
		case event == "discover":
			appendRequest("GET /sync/generations/{generation_id}")
		case event == "connect":
			appendRequest("POST /sync/connect")
		case event == "sync":
			if hasFrozenPush {
				requests = append(requests, frozenPush)
			} else {
				appendRequest("GET /sync/pull")
			}
		case event == "push_complete":
			appendRequest("GET /sync/pull")
		case event == "push_response_lost":
			// The frozen request remains authoritative until exact replay resolves it.
		case event == "retry_exact_push":
			if !hasFrozenPush {
				return nil, nil, errors.New("exact push retry has no frozen request")
			}
			requests = append(requests, frozenPush)
		case event == "pull_complete":
			if len(requests) != 0 && requests[len(requests)-1].Summary == "POST /sync/push" {
				appendRequest("GET /sync/pull")
			}
			terminal["running"] = false
		case strings.HasPrefix(event, "pull_has_more:"):
			appendRequest("GET /sync/pull")
		case strings.HasPrefix(event, "pull_complete:"):
			value, err := strconv.ParseFloat(strings.TrimPrefix(event, "pull_complete:"), 64)
			if err != nil {
				return nil, nil, err
			}
			terminal["checkpoint"] = value
			terminal["running"] = false
			terminal["activePulls"] = float64(0)
			terminal["trailingPulls"] = float64(0)
		case strings.HasPrefix(event, "watch_burst:"):
			eventCount, err := strconv.Atoi(strings.TrimPrefix(event, "watch_burst:"))
			if err != nil || eventCount < 0 {
				return nil, nil, fmt.Errorf("invalid watch burst %q", event)
			}
			if eventCount > 0 {
				appendRequest("GET /sync/pull")
				terminal["activePulls"] = float64(1)
			}
			if eventCount > 1 {
				appendRequest("GET /sync/pull")
				terminal["trailingPulls"] = float64(1)
			}
		case event == "pull":
			appendRequest("GET /sync/pull")
		case event == "watch":
			appendRequest("GET /sync/watch")
		case event == "snapshot_chunk":
			appendRequest("GET /sync/snapshot-sessions/{snapshot_id}")
		default:
			return nil, nil, fmt.Errorf("unexecuted request trace event %q", event)
		}
	}
	return requests, terminal, nil
}

func TestContractCorpus_CompressionExecutesBoundaryDecisions(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var fixture struct {
		FormatVersion         int `json:"formatVersion"`
		ThresholdDecodedBytes int `json:"thresholdDecodedBytes"`
		SelectionCases        []struct {
			Name                string `json:"name"`
			EncodedBodyBytes    int    `json:"encodedBodyBytes"`
			Payload             string `json:"payload"`
			SelectedEncoding    string `json:"selectedEncoding"`
			GzipStrictlySmaller bool   `json:"gzipStrictlySmaller"`
		} `json:"selectionCases"`
		ReceiverCases []struct {
			Name            string `json:"name"`
			ContentEncoding string `json:"contentEncoding"`
			EncodedBytes    int    `json:"encodedBytes"`
			EncodedLimit    int    `json:"encodedLimit"`
			DecodedBytes    int    `json:"decodedBytes"`
			DecodedLimit    int    `json:"decodedLimit"`
			ExpectedError   string `json:"expectedError"`
			Applicability   string `json:"applicability"`
		} `json:"receiverCases"`
	}
	readContractFixture(t, root, "wire/compression.json", &fixture)
	if fixture.FormatVersion != 2 || fixture.ThresholdDecodedBytes != 2048 {
		t.Fatalf("compression fixture header is invalid")
	}
	for _, test := range fixture.SelectionCases {
		body := deterministicCorpusBody(test.EncodedBodyBytes, test.Payload)
		request := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
		request.Header.Set("Accept-Encoding", "gzip")
		recorder := httptest.NewRecorder()
		(&HTTPSyncHandlers{}).writeJSONBytes(recorder, request, http.StatusOK, body)
		got := "identity"
		if recorder.Header().Get("Content-Encoding") == "gzip" {
			got = "gzip"
		}
		if got != test.SelectedEncoding {
			t.Errorf("compression case %q selected %s, want %s", test.Name, got, test.SelectedEncoding)
		}
	}
	for _, test := range fixture.ReceiverCases {
		if test.Applicability != "raw_transport" && test.Applicability != "all" {
			if test.Name != "fetch-server-wire-gzip" && test.Name != "fetch-decoded-overflow" {
				t.Errorf("unknown fetch receiver case %q", test.Name)
			}
			continue
		}
		body, headers, limit := corpusReceiverInput(t, test.Name, test.EncodedBytes, test.DecodedBytes, test.EncodedLimit, test.DecodedLimit)
		request := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(body))
		for _, value := range headers {
			request.Header.Add("Content-Encoding", value)
		}
		_, err := readBoundedRequestBody(request, limit)
		var bounded *boundedBodyError
		if !errors.As(err, &bounded) || bounded.code != test.ExpectedError {
			t.Errorf("receiver case %q error = %v, want %s", test.Name, err, test.ExpectedError)
		}
	}
}

func deterministicCorpusBody(size int, payload string) []byte {
	if size < 10 {
		return bytes.Repeat([]byte{'x'}, size)
	}
	body := make([]byte, size)
	copy(body, `{"pad":"`)
	copy(body[size-2:], `"}`)
	if payload == "repeat:A" {
		for index := 8; index < size-2; index++ {
			body[index] = 'A'
		}
		return body
	}
	state := uint64(0x6f76657273716c69)
	for index := 8; index < size-2; index++ {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		body[index] = byte((state * 2685821657736338717) >> 56)
	}
	return body
}

func gzipCorpusBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func corpusReceiverInput(t *testing.T, name string, encodedBytes, decodedBytes, encodedLimit, decodedLimit int) ([]byte, []string, int64) {
	t.Helper()
	limit := decodedLimit
	if limit == 0 {
		limit = encodedLimit
	}
	if limit == 0 {
		limit = 2 << 20
	}
	switch name {
	case "malformed-gzip":
		return []byte("not-a-gzip-member"), []string{"gzip"}, int64(limit)
	case "truncated-gzip":
		body := gzipCorpusBody(t, []byte("truncated"))
		return body[:len(body)-2], []string{"gzip"}, int64(limit)
	case "concatenated-gzip-members":
		first := gzipCorpusBody(t, []byte("first"))
		return append(first, gzipCorpusBody(t, []byte("second"))...), []string{"gzip"}, int64(limit)
	case "unsupported-content-encoding":
		return nil, []string{"br"}, 1
	case "multiple-content-encodings":
		return nil, []string{"gzip", "identity"}, 1
	case "encoded-overflow":
		return bytes.Repeat([]byte{'x'}, encodedBytes), []string{"gzip"}, int64(encodedLimit)
	case "decoded-overflow":
		return gzipCorpusBody(t, bytes.Repeat([]byte{'A'}, decodedBytes)), []string{"gzip"}, int64(decodedLimit)
	default:
		t.Fatalf("unexecuted raw receiver case %q", name)
		return nil, nil, 0
	}
}

func TestContractCorpus_LimitsAndFaultsExecuteInvariants(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var limits struct {
		Cases []map[string]any `json:"cases"`
	}
	readContractFixture(t, root, "wire/limits.json", &limits)
	var compact compactRowsCorpus
	readContractFixture(t, root, "wire/compact-rows.json", &compact)
	wantLimitCases := map[string]bool{
		"soft-target-first-row-progress": false, "row-exceeds-logical-limit": false,
		"first-row-exceeds-hard-request-limit": false, "envelope-exceeds-hard-limit": false,
		"indivisible-pull-fits-admitted-limit": false, "indivisible-pull-rejected-before-publish": false,
		"snapshot-row-rejected-before-session": false, "snapshot-one-row-retry": false,
		"snapshot-one-row-retry-over-hard-max": false, "snapshot-logical-total-pagination-independent": false,
	}
	for _, test := range limits.Cases {
		name, _ := test["name"].(string)
		if _, ok := wantLimitCases[name]; !ok {
			t.Errorf("unknown limit case %q", name)
			continue
		}
		wantLimitCases[name] = true
		number := func(key string) float64 { return test[key].(float64) }
		switch name {
		case "soft-target-first-row-progress":
			progress := number("firstRowRequestBytes") > number("localPushTargetBytes") && number("firstRowRequestBytes") <= number("maxPushDecodedBytes") && number("firstRowLogicalBytes") > 0
			if !progress || test["expected"] != "freeze_and_send_one_row" || test["sourceBundleIdConsumed"] != true {
				t.Errorf("soft-target exception does not guarantee progress")
			}
		case "row-exceeds-logical-limit":
			if number("firstRowLogicalBytes") <= number("maxRowDecodedBytes") || test["sourceBundleIdConsumed"] != false {
				t.Errorf("%s does not exceed its exact limit without consuming an ID", name)
			}
		case "first-row-exceeds-hard-request-limit":
			if number("firstRowRequestBytes") <= number("maxPushDecodedBytes") || test["sourceBundleIdConsumed"] != false {
				t.Errorf("%s does not exceed its exact limit without consuming an ID", name)
			}
		case "envelope-exceeds-hard-limit":
			if number("emptyEnvelopeBytes") <= number("maxPushDecodedBytes") || test["sourceBundleIdConsumed"] != false {
				t.Errorf("%s consumes a source bundle ID", name)
			}
		case "indivisible-pull-fits-admitted-limit":
			if number("oneBundleResponseBytes") != number("maxPullDecodedBytes") || test["expected"] != "returned" {
				t.Errorf("exact-limit pull is not returned")
			}
		case "indivisible-pull-rejected-before-publish":
			if number("oneBundleResponseBytes") <= number("maxPullDecodedBytes") || test["emptyHasMoreResponseForbidden"] != true {
				t.Errorf("indivisible pull case permits an empty loop")
			}
		case "snapshot-row-rejected-before-session":
			body, err := snapshotCorpusBodyAtSize(compact.capabilities(), int(number("oneRowChunkBytes")), false)
			if err != nil {
				t.Fatal(err)
			}
			if number("oneRowChunkBytes") <= number("maxSnapshotDecodedBytesPerChunk") || test["sessionPublished"] != false || test["dimension"] != "row_decoded_body_bytes" {
				t.Errorf("snapshot row limit case publishes invalid state")
			}
			if len(body) != int(number("oneRowChunkBytes")) {
				t.Errorf("snapshot row body size = %d, want %d", len(body), int(number("oneRowChunkBytes")))
			}
		case "snapshot-one-row-retry":
			body, err := snapshotCorpusBodyAtSize(compact.capabilities(), int(number("requiredDecodedBodyBytes")), false)
			if err != nil {
				t.Fatal(err)
			}
			if number("requiredDecodedBodyBytes") <= number("requestedMaxBytes") || number("requiredDecodedBodyBytes") > number("serverMaxBytes") || number("retryMaxRows") != 1 || number("retryMaxBytes") != number("serverMaxBytes") {
				t.Errorf("snapshot retry does not derive the exact one-row boundary")
			}
			if len(body) != int(number("requiredDecodedBodyBytes")) {
				t.Errorf("snapshot retry body size = %d, want %d", len(body), int(number("requiredDecodedBodyBytes")))
			}
		case "snapshot-one-row-retry-over-hard-max":
			body, err := snapshotCorpusBodyAtSize(compact.capabilities(), int(number("requiredDecodedBodyBytes")), false)
			if err != nil {
				t.Fatal(err)
			}
			if number("requiredDecodedBodyBytes") <= number("serverMaxBytes") || test["expected"] != "protocol_violation_checkpoint_unchanged" {
				t.Errorf("snapshot hard-max overflow is not terminal")
			}
			if len(body) != int(number("requiredDecodedBodyBytes")) {
				t.Errorf("snapshot hard-max body size = %d, want %d", len(body), int(number("requiredDecodedBodyBytes")))
			}
		case "snapshot-logical-total-pagination-independent":
			rows, err := snapshotRowsFromLogicalFixture(compact.capabilities(), test["rows"])
			if err != nil {
				t.Fatal(err)
			}
			plans := test["paginationPlans"].([]any)
			var totals []int64
			for _, rawPlan := range plans {
				plan := rawPlan.([]any)
				var total int64
				offset := 0
				for _, rawCount := range plan {
					count := int(rawCount.(float64))
					_, logicalBytes, err := CompactGroupsForSnapshotRowsAt(compact.capabilities(), rows[offset:offset+count], int64(offset+1))
					if err != nil {
						t.Fatal(err)
					}
					total += logicalBytes
					offset += count
				}
				totals = append(totals, total)
			}
			if len(totals) != 2 || totals[0] != totals[1] || totals[0] == 0 || test["expectedSameLogicalByteCount"] != true {
				t.Errorf("snapshot logical accounting depends on pagination")
			}
		}
	}
	for name, present := range wantLimitCases {
		if !present {
			t.Errorf("wire/limits.json is missing %q", name)
		}
	}

	var faults struct {
		FormatVersion int `json:"formatVersion"`
		Cases         []struct {
			Name      string         `json:"name"`
			Operation string         `json:"operation"`
			Initial   map[string]any `json:"initial"`
			Events    []string       `json:"events"`
			Expected  map[string]any `json:"expected"`
		} `json:"cases"`
	}
	readContractFixture(t, root, "wire/faults.json", &faults)
	if faults.FormatVersion != 3 {
		t.Fatalf("fault fixture formatVersion = %d, want 3", faults.FormatVersion)
	}
	wantFaults := map[string]bool{
		"death-before-send": false, "request-interruption-before-commit": false,
		"response-loss-after-commit": false, "process-death-after-commit-before-apply": false,
		"initialization-replay-after-restart": false, "replay-mismatch": false, "conflict": false,
		"application-rejection": false, "source-retirement": false, "projection-too-large": false,
		"committed-bundle-unrepresentable": false, "snapshot-session-limit": false,
	}
	for _, test := range faults.Cases {
		if _, ok := wantFaults[test.Name]; !ok {
			t.Errorf("unknown fault case %q", test.Name)
			continue
		}
		wantFaults[test.Name] = true
		actual, err := reduceFaultTrace(test.Operation, test.Initial, test.Events)
		if err != nil {
			t.Errorf("fault %q: %v", test.Name, err)
			continue
		}
		for key, want := range test.Expected {
			if !reflect.DeepEqual(actual[key], want) {
				t.Errorf("fault %q %s = %#v, want %#v", test.Name, key, actual[key], want)
			}
		}
	}
	for name, present := range wantFaults {
		if !present {
			t.Errorf("wire/faults.json is missing %q", name)
		}
	}
}

func snapshotCorpusBodyAtSize(generation SchemaGenerationCapabilities, target int, hasMore bool) ([]byte, error) {
	for padding := 0; padding <= target; padding++ {
		payload, err := json.Marshal(map[string]any{"id": "u1", "name": strings.Repeat("A", padding)})
		if err != nil {
			return nil, err
		}
		groups, logicalBytes, err := CompactGroupsForSnapshotRowsAt(generation, []SnapshotRow{{
			Schema: "main", Table: "users", Key: SyncKey{"id": "u1"}, RowVersion: 1, Payload: payload,
		}}, 1)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(CompactSnapshotChunkResponse{
			Rows: groups, NextRowOrdinal: 2, LogicalByteCount: logicalBytes, HasMore: hasMore,
		})
		if err != nil {
			return nil, err
		}
		if len(body) == target {
			return body, nil
		}
		if len(body) > target {
			return nil, fmt.Errorf("snapshot body size skipped target %d at %d bytes", target, len(body))
		}
	}
	return nil, fmt.Errorf("snapshot body did not reach target %d", target)
}

func snapshotRowsFromLogicalFixture(generation SchemaGenerationCapabilities, raw any) ([]SnapshotRow, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, errors.New("snapshot logical rows are not an array")
	}
	rows := make([]SnapshotRow, 0, len(values))
	for _, rawRow := range values {
		row := rawRow.([]any)
		tableIndex := int(row[1].(float64))
		table := generation.Tables[tableIndex]
		keyValues := row[2].([]any)
		key := SyncKey{}
		for index, column := range table.SyncKeyColumns {
			key[column] = keyValues[index]
		}
		payload, err := json.Marshal(row[4])
		if err != nil {
			return nil, err
		}
		rows = append(rows, SnapshotRow{Schema: table.Schema, Table: table.Table, Key: key, RowVersion: int64(row[3].(float64)), Payload: payload})
	}
	return rows, nil
}

func reduceFaultTrace(operation string, initial map[string]any, events []string) (map[string]any, error) {
	state := map[string]any{
		"committed": false, "requestCount": float64(0), "running": true,
		"exactReplay": false, "mutableEffects": float64(0),
	}
	for _, key := range []string{"checkpoint", "initializationId", "sessionPublished"} {
		if value, present := initial[key]; present {
			state[key] = value
		}
	}
	frozenRequest, hasFrozenRequest, err := contractTraceFrozenRequest(initial, "frozenRequest", "POST /sync/push")
	if err != nil {
		return nil, err
	}
	originalFrozenRequest := frozenRequest
	requestLog := []contractTraceRequest{}
	requestForOperation := func() (contractTraceRequest, error) {
		switch operation {
		case "push":
			if !hasFrozenRequest {
				return contractTraceRequest{}, errors.New("push fault has no frozen request")
			}
			return frozenRequest, nil
		case "pull":
			return contractTraceRequest{Summary: "GET /sync/pull"}, nil
		case "snapshot_create":
			return contractTraceRequest{Summary: "POST /sync/snapshot-sessions"}, nil
		default:
			return contractTraceRequest{}, fmt.Errorf("unknown fault operation %q", operation)
		}
	}
	recordRequest := func() error {
		request, err := requestForOperation()
		if err != nil {
			return err
		}
		requestLog = append(requestLog, request)
		state["requestCount"] = float64(len(requestLog))
		return nil
	}
	for _, event := range events {
		switch {
		case event == "process_death":
			state["running"] = false
		case event == "request_sent":
			if err := recordRequest(); err != nil {
				return nil, err
			}
		case event == "transport_interrupted", event == "response_lost", event == "replay_lookup", event == "replay_return":
			// These observations do not mutate durable request or checkpoint state.
		case event == "server_commit":
			state["committed"] = true
			state["mutableEffects"] = state["mutableEffects"].(float64) + 1
		case event == "retry_exact":
			if err := recordRequest(); err != nil {
				return nil, err
			}
			state["exactReplay"] = len(requestLog) >= 2 && requestLog[0] == requestLog[len(requestLog)-1]
		case event == "restart":
			state["running"] = true
		case strings.HasPrefix(event, "terminal:"):
			state["terminalError"] = strings.TrimPrefix(event, "terminal:")
			state["running"] = false
		case strings.HasPrefix(event, "recover:"):
			state["recovery"] = strings.TrimPrefix(event, "recover:")
			state["running"] = false
		case event == "retry_tick":
			if state["running"] == true {
				if err := recordRequest(); err != nil {
					return nil, err
				}
			}
		default:
			return nil, fmt.Errorf("unexecuted fault trace event %q", event)
		}
	}
	if hasFrozenRequest {
		state["frozenRequest"] = frozenRequest == originalFrozenRequest
	}
	return state, nil
}
