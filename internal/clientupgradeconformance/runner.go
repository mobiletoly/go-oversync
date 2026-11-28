// Package clientupgradeconformance verifies the implementation-independent
// client-upgrade corpus without importing either oversqlite or oversync.
package clientupgradeconformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type manifest struct {
	Format    string     `json:"format"`
	Revision  string     `json:"revision"`
	Artifacts []artifact `json:"artifacts"`
	Scenarios []string   `json:"scenarios"`
	Vectors   []string   `json:"vectors"`
}

type artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type scenarioFile struct {
	Format    string     `json:"format"`
	Scenarios []scenario `json:"scenarios"`
}

type scenario struct {
	ID                string           `json:"id"`
	Family            string           `json:"family"`
	InitialState      map[string]any   `json:"initial_state"`
	Actions           []map[string]any `json:"actions"`
	ProtocolExchanges []map[string]any `json:"protocol_exchanges"`
	Faults            []map[string]any `json:"faults"`
	Expected          map[string]any   `json:"expected"`
	Preserved         []string         `json:"preserved"`
	Invariants        []string         `json:"invariants"`
}

type vectorFile struct {
	Format  string   `json:"format"`
	Vectors []vector `json:"vectors"`
}

type vector struct {
	ID             string      `json:"id"`
	Semantic       *typedValue `json:"semantic,omitempty"`
	Codec          string      `json:"codec,omitempty"`
	SQLiteType     string      `json:"sqlite_type,omitempty"`
	SQLiteValue    any         `json:"sqlite_value,omitempty"`
	Expected       string      `json:"expected,omitempty"`
	LimitKind      string      `json:"limit_kind,omitempty"`
	LimitDecimal   string      `json:"limit_decimal,omitempty"`
	ActualDecimal  string      `json:"actual_decimal,omitempty"`
	WireHex        string      `json:"wire_hex,omitempty"`
	RepeatByteHex  string      `json:"repeat_byte_hex,omitempty"`
	RepeatLength   int         `json:"repeat_length,omitempty"`
	ExpectedSHA256 string      `json:"expected_sha256"`
}

type typedValue struct {
	Type     string `json:"type"`
	Value    any    `json:"value,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

type invariantsFile struct {
	Format     string      `json:"format"`
	Invariants []invariant `json:"invariants"`
}

type transitionsFile struct {
	Format      string       `json:"format"`
	Transitions []transition `json:"transitions"`
}

type transition struct {
	ID          string   `json:"id"`
	From        string   `json:"from"`
	To          string   `json:"to"`
	Requires    []string `json:"requires"`
	Transaction string   `json:"transaction"`
	Remote      []string `json:"remote"`
	Idempotent  string   `json:"idempotent"`
	Recovery    string   `json:"recovery"`
}

type exchangesFile struct {
	Format          string            `json:"format"`
	Exchanges       []wireExchange    `json:"exchanges"`
	ReferenceHashes map[string]string `json:"reference_hashes"`
}

type protocolHashesFile struct {
	Format  string               `json:"format"`
	Vectors []protocolHashVector `json:"vectors"`
}

type protocolHashVector struct {
	ID                    string          `json:"id"`
	Algorithm             string          `json:"algorithm"`
	GenerationID          string          `json:"generation_id,omitempty"`
	GenerationFingerprint string          `json:"generation_fingerprint,omitempty"`
	Rows                  json.RawMessage `json:"rows,omitempty"`
	CanonicalUTF8         string          `json:"canonical_utf8"`
	ExpectedByteCount     string          `json:"expected_byte_count"`
	ExpectedSHA256        string          `json:"expected_sha256"`
}

type wireExchange struct {
	ID              string            `json:"id"`
	Method          string            `json:"method"`
	Path            string            `json:"path"`
	RequestBody     string            `json:"request_body"`
	RequestHeaders  map[string]string `json:"request_headers"`
	Status          string            `json:"status"`
	ResponseHeaders map[string]string `json:"response_headers"`
	ResponseBody    string            `json:"response_body,omitempty"`
	ResponseUTF8    string            `json:"response_utf8,omitempty"`
	Expected        string            `json:"expected,omitempty"`
}

type invariant struct {
	ID                string   `json:"id"`
	PortableScenarios []string `json:"portable_scenarios"`
	GoTests           []string `json:"go_tests"`
	KMP               string   `json:"kmp"`
	Dart              string   `json:"dart"`
	Swift             string   `json:"swift"`
}

type goAdapterFile struct {
	Format      string              `json:"format"`
	GoScenarios []goScenarioAdapter `json:"go_scenarios"`
}

type goScenarioAdapter struct {
	ID              string   `json:"id"`
	GoTests         []string `json:"go_tests"`
	ExecutorPackage string   `json:"executor_package"`
	ExecutorTest    string   `json:"executor_test"`
}

type correctionsFile struct {
	Format              string `json:"format"`
	Revision            string `json:"revision"`
	PriorManifestSHA256 string `json:"prior_manifest_sha256"`
	Corrections         []struct {
		ID         string   `json:"id"`
		Rationale  string   `json:"rationale"`
		Invariants []string `json:"invariants"`
	} `json:"corrections"`
}

// Validate verifies manifest coverage and hashes, parses every semantic
// artifact, independently checks fixed vector hashes, and proves I01-I12 have
// both portable scenarios and named Go evidence.
func Validate(root string) error {
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return err
	}
	var m manifest
	if err := decodeExact(raw, &m); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if m.Format != "client-upgrade-conformance/v1" || m.Revision == "" {
		return fmt.Errorf("manifest format or revision is invalid")
	}
	seen := map[string]bool{}
	for _, entry := range m.Artifacts {
		if entry.Path == "" || filepath.IsAbs(entry.Path) || strings.Contains(entry.Path, "..") || seen[entry.Path] {
			return fmt.Errorf("invalid or duplicate artifact path %q", entry.Path)
		}
		seen[entry.Path] = true
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(entry.Path)))
		if err != nil {
			return fmt.Errorf("artifact %s: %w", entry.Path, err)
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			return fmt.Errorf("artifact %s SHA-256 mismatch", entry.Path)
		}
	}
	for _, required := range []string{"README.md", "schema.json", "transitions.json", "values.json", "limits.json", "invariants.json", "go-adapter.json", "corrections.json", "wire/protocol-hashes.json"} {
		if !seen[required] {
			return fmt.Errorf("manifest omits %s", required)
		}
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative != "manifest.json" && !seen[relative] {
			return fmt.Errorf("manifest omits artifact %s", relative)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := validateSchema(filepath.Join(root, "schema.json")); err != nil {
		return err
	}
	if err := validateSemanticArtifactsAgainstSchema(root, m); err != nil {
		return err
	}
	if err := validateTransitions(filepath.Join(root, "transitions.json"), m.Format); err != nil {
		return err
	}
	if err := validateExchanges(filepath.Join(root, "wire", "exchanges.json"), root, m.Format, seen); err != nil {
		return err
	}
	if err := validateProtocolHashes(filepath.Join(root, "wire", "protocol-hashes.json"), m.Format); err != nil {
		return err
	}
	scenarioIDs := map[string]bool{}
	for _, path := range m.Scenarios {
		if !seen[path] || !strings.HasPrefix(path, "scenarios/") {
			return fmt.Errorf("invalid scenario inventory entry %q", path)
		}
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return err
		}
		var file scenarioFile
		if err := decodeExact(body, &file); err != nil || file.Format != m.Format {
			return fmt.Errorf("scenario file %s is invalid: %w", path, err)
		}
		for _, item := range file.Scenarios {
			if item.ID == "" || item.Family == "" || item.InitialState == nil || item.Expected == nil || len(item.Actions) == 0 || len(item.Invariants) == 0 || scenarioIDs[item.ID] {
				return fmt.Errorf("scenario %q is incomplete or duplicate", item.ID)
			}
			for _, exchange := range item.ProtocolExchanges {
				for _, key := range []string{"request", "response"} {
					if referenced, ok := exchange[key].(string); ok && referenced != "" && !seen[referenced] {
						return fmt.Errorf("scenario %s references untracked wire artifact %s", item.ID, referenced)
					}
				}
			}
			scenarioIDs[item.ID] = true
		}
	}
	if err := requireExactIDs("scenario", scenarioIDs, []string{
		"CU-N01", "CU-N02", "CU-N03", "CU-N04", "CU-D01", "CU-D02", "CU-D03", "CU-C01", "CU-P01", "CU-M01", "CU-R01", "CU-S01", "CU-F01", "CU-L01", "CU-X01",
		"CU-U0", "CU-U1", "CU-U2", "CU-U3", "CU-U4", "CU-U5", "CU-U6", "CU-U7", "CU-U8",
	}); err != nil {
		return err
	}
	if err := validateGoAdapter(filepath.Join(root, "go-adapter.json"), m.Format, scenarioIDs); err != nil {
		return err
	}
	if err := validateCorrections(filepath.Join(root, "corrections.json"), m.Format, m.Revision); err != nil {
		return err
	}
	for _, path := range m.Vectors {
		if !seen[path] {
			return fmt.Errorf("invalid vector inventory entry %q", path)
		}
		if err := validateVectors(filepath.Join(root, filepath.FromSlash(path)), m.Format, path); err != nil {
			return err
		}
	}
	return validateInvariants(filepath.Join(root, "invariants.json"), m.Format, scenarioIDs)
}

func validateProtocolHashes(path, format string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file protocolHashesFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("protocol hashes: %w", err)
	}
	want := map[string]string{
		"q39-generation-pinned-request": "generation_pinned_push_request_v1",
		"generation-projected-effects":  "committed_bundle_v1",
		"complete-snapshot-chunk-frame": "snapshot_chunk_wire_json_v1",
	}
	seen := make(map[string]bool, len(want))
	for _, vector := range file.Vectors {
		if want[vector.ID] != vector.Algorithm || seen[vector.ID] || vector.CanonicalUTF8 == "" {
			return fmt.Errorf("protocol hash vector %q is invalid or duplicate", vector.ID)
		}
		byteCount, err := strconv.ParseInt(vector.ExpectedByteCount, 10, 64)
		if err != nil || byteCount != int64(len([]byte(vector.CanonicalUTF8))) {
			return fmt.Errorf("protocol hash vector %s byte count mismatch", vector.ID)
		}
		sum := sha256.Sum256([]byte(vector.CanonicalUTF8))
		if vector.ExpectedSHA256 != hex.EncodeToString(sum[:]) {
			return fmt.Errorf("protocol hash vector %s SHA-256 mismatch", vector.ID)
		}
		if vector.Algorithm != "snapshot_chunk_wire_json_v1" && len(vector.Rows) == 0 {
			return fmt.Errorf("protocol hash vector %s omits semantic rows", vector.ID)
		}
		seen[vector.ID] = true
	}
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	return requireExactIDs("protocol hash vector", seen, ids)
}

func validateTransitions(path, format string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file transitionsFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("transitions: %w", err)
	}
	want := map[string][2]string{
		"U0": {"ready", "upgrade_started"}, "U1": {"upgrade_started", "resolving_old_intent"},
		"U2": {"resolving_old_intent", "resolving_old_intent"}, "U3": {"resolving_old_intent", "migrating_local"},
		"U4": {"migrating_local", "local_migrated"}, "U5": {"local_migrated", "reattach_pending"},
		"U6": {"reattach_pending", "rebuild_running"}, "U7": {"rebuild_running", "rebuild_running"},
		"U8": {"rebuild_running", "ready"}, "FRESH": {"absent", "ready"}, "LOCAL_ONLY": {"ready", "ready"},
	}
	seen := map[string]bool{}
	for _, item := range file.Transitions {
		edge, ok := want[item.ID]
		if !ok || seen[item.ID] || item.From != edge[0] || item.To != edge[1] || len(item.Requires) == 0 || item.Transaction == "" || item.Idempotent == "" || item.Recovery == "" {
			return fmt.Errorf("transition %q does not match the normative edge", item.ID)
		}
		seen[item.ID] = true
	}
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	return requireExactIDs("transition", seen, ids)
}

func validateExchanges(path, root, format string, artifacts map[string]bool) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file exchangesFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("wire exchanges: %w", err)
	}
	want := map[string][2]string{
		"wire-capabilities-target":            {"200", ""},
		"wire-source-replacement":             {"200", ""},
		"wire-capabilities-unknown":           {"409", "target_generation_unavailable"},
		"wire-capabilities-mismatch":          {"409", "target_generation_unavailable"},
		"wire-push-conflict":                  {"409", "needs_resolution"},
		"wire-malformed-missing-byte-count":   {"200", "upgrade_integrity_failed"},
		"wire-malformed-divergent-retirement": {"409", "replacement_diverged"},
	}
	seen := map[string]bool{}
	for _, item := range file.Exchanges {
		expected, ok := want[item.ID]
		if !ok || seen[item.ID] || item.Method == "" || item.Path == "" || item.Status != expected[0] || item.Expected != expected[1] {
			return fmt.Errorf("wire exchange %q does not match its fixed expectation", item.ID)
		}
		if item.RequestHeaders["Oversync-Schema-Generation-ID"] == "" || item.RequestHeaders["Oversync-Schema-Generation-Fingerprint"] == "" {
			return fmt.Errorf("wire exchange %s omits generation proof headers", item.ID)
		}
		// The m-series upgrade corpus retains its pre-p0000 discovery exchange as
		// historical fixture input. It is not a route supported by this build.
		if item.Path != "/sync/capabilities" && item.RequestHeaders["Oversync-Source-ID"] == "" {
			return fmt.Errorf("wire exchange %s omits its pinned source header", item.ID)
		}
		for _, relative := range []string{item.RequestBody, item.ResponseBody} {
			if relative == "" {
				continue
			}
			artifactPath := filepath.ToSlash(filepath.Join("wire", relative))
			if !artifacts[artifactPath] {
				return fmt.Errorf("wire exchange %s references untracked body %s", item.ID, artifactPath)
			}
			if _, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(artifactPath))); err != nil {
				return err
			}
		}
		if (item.ResponseBody == "") == (item.ResponseUTF8 == "") {
			return fmt.Errorf("wire exchange %s must select exactly one response body representation", item.ID)
		}
		seen[item.ID] = true
	}
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	if err := requireExactIDs("wire exchange", seen, ids); err != nil {
		return err
	}
	empty := sha256.Sum256([]byte("[]"))
	if file.ReferenceHashes["empty_array_sha256"] != hex.EncodeToString(empty[:]) {
		return fmt.Errorf("wire empty-array reference hash mismatch")
	}
	replacement, err := os.ReadFile(filepath.Join(root, "wire", "replacement.request.json"))
	if err != nil {
		return err
	}
	replacementHash := sha256.Sum256(replacement)
	if file.ReferenceHashes["replacement_request_sha256"] != hex.EncodeToString(replacementHash[:]) {
		return fmt.Errorf("wire replacement-request reference hash mismatch")
	}
	return nil
}

func requireExactIDs(kind string, got map[string]bool, want []string) error {
	if len(got) != len(want) {
		return fmt.Errorf("%s inventory has %d entries; want %d", kind, len(got), len(want))
	}
	for _, id := range want {
		if !got[id] {
			return fmt.Errorf("%s inventory omits %s", kind, id)
		}
	}
	return nil
}

func decodeExact(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func validateSchema(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var schema map[string]any
	if err := json.Unmarshal(body, &schema); err != nil {
		return fmt.Errorf("schema.json: %w", err)
	}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" || schema["$id"] != "https://mobiletoly.github.io/go-oversync/conformance/client-upgrade/v1/schema.json" {
		return fmt.Errorf("schema.json identity is invalid")
	}
	return nil
}

func validateSemanticArtifactsAgainstSchema(root string, m manifest) error {
	schemaPath := filepath.Join(root, "schema.json")
	schemaBody, err := os.Open(schemaPath)
	if err != nil {
		return err
	}
	defer schemaBody.Close()
	document, err := jsonschema.UnmarshalJSON(schemaBody)
	if err != nil {
		return fmt.Errorf("decode conformance JSON Schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	const schemaURL = "https://mobiletoly.github.io/go-oversync/conformance/client-upgrade/v1/schema.json"
	if err := compiler.AddResource(schemaURL, document); err != nil {
		return fmt.Errorf("register conformance JSON Schema: %w", err)
	}
	compiled, err := compiler.Compile(schemaURL)
	if err != nil {
		return fmt.Errorf("compile conformance JSON Schema: %w", err)
	}
	paths := append([]string{"transitions.json", "invariants.json", "go-adapter.json"}, m.Scenarios...)
	paths = append(paths, m.Vectors...)
	for _, relative := range paths {
		body, err := os.Open(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return err
		}
		instance, decodeErr := jsonschema.UnmarshalJSON(body)
		closeErr := body.Close()
		if decodeErr != nil {
			return fmt.Errorf("decode %s for JSON Schema validation: %w", relative, decodeErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if err := compiled.Validate(instance); err != nil {
			return fmt.Errorf("%s does not satisfy schema.json: %w", relative, err)
		}
	}
	return nil
}

func validateGoAdapter(path, format string, scenarios map[string]bool) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file goAdapterFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("Go scenario adapter: %w", err)
	}
	seen := make(map[string]bool, len(file.GoScenarios))
	for _, item := range file.GoScenarios {
		if !scenarios[item.ID] || seen[item.ID] || len(item.GoTests) == 0 || item.ExecutorPackage == "" || !strings.HasPrefix(item.ExecutorTest, "Test") {
			return fmt.Errorf("Go scenario adapter %q is unknown, duplicate, or empty", item.ID)
		}
		tests := make(map[string]bool, len(item.GoTests))
		for _, test := range item.GoTests {
			if !strings.HasPrefix(test, "Test") || tests[test] {
				return fmt.Errorf("Go scenario adapter %s has invalid or duplicate test %q", item.ID, test)
			}
			tests[test] = true
		}
		seen[item.ID] = true
	}
	ids := make([]string, 0, len(scenarios))
	for id := range scenarios {
		ids = append(ids, id)
	}
	return requireExactIDs("Go scenario adapter", seen, ids)
}

func validateCorrections(path, format, revision string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file correctionsFile
	if err := decodeExact(body, &file); err != nil {
		return fmt.Errorf("conformance corrections: %w", err)
	}
	if file.Format != format || file.Revision != revision || len(file.PriorManifestSHA256) != 64 || len(file.Corrections) == 0 {
		return fmt.Errorf("conformance corrections identity or inventory is invalid")
	}
	seen := make(map[string]bool)
	for _, correction := range file.Corrections {
		if correction.ID == "" || correction.Rationale == "" || len(correction.Invariants) == 0 || seen[correction.ID] {
			return fmt.Errorf("conformance correction %q is incomplete or duplicate", correction.ID)
		}
		seen[correction.ID] = true
		for _, invariant := range correction.Invariants {
			if invariant < "I01" || invariant > "I12" {
				return fmt.Errorf("conformance correction %s references invalid invariant %s", correction.ID, invariant)
			}
		}
	}
	return nil
}

func validateVectors(path, format, kind string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file vectorFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("vector file %s is invalid: %w", path, err)
	}
	ids := map[string]bool{}
	for _, item := range file.Vectors {
		if item.ID == "" || ids[item.ID] || len(item.ExpectedSHA256) != 64 {
			return fmt.Errorf("invalid vector %q", item.ID)
		}
		if kind == "values.json" {
			if item.Semantic == nil || item.Semantic.Type == "" || item.Codec == "" || item.Expected == "" {
				return fmt.Errorf("typed vector %s omits semantic, codec, or expected result", item.ID)
			}
		} else if kind == "limits.json" {
			if item.LimitKind == "" || item.LimitDecimal == "" || item.ActualDecimal == "" || (item.Expected != "accepted" && item.Expected != "rejected") {
				return fmt.Errorf("limit vector %s omits its exact bound or result", item.ID)
			}
		}
		ids[item.ID] = true
		hasher := sha256.New()
		switch {
		case item.WireHex != "" && item.RepeatByteHex == "":
			var payload []byte
			payload, err = hex.DecodeString(item.WireHex)
			if err == nil {
				_, err = hasher.Write(payload)
			}
		case item.WireHex == "" && item.RepeatByteHex != "" && item.RepeatLength >= 0:
			one, decodeErr := hex.DecodeString(item.RepeatByteHex)
			if decodeErr != nil || len(one) != 1 {
				return fmt.Errorf("vector %s repeat byte is invalid", item.ID)
			}
			block := bytes.Repeat(one, 32<<10)
			remaining := item.RepeatLength
			for remaining > 0 {
				write := min(remaining, len(block))
				if _, err = hasher.Write(block[:write]); err != nil {
					break
				}
				remaining -= write
			}
		default:
			return fmt.Errorf("vector %s must select exactly one byte representation", item.ID)
		}
		if err != nil {
			return fmt.Errorf("vector %s hex: %w", item.ID, err)
		}
		if hex.EncodeToString(hasher.Sum(nil)) != item.ExpectedSHA256 {
			return fmt.Errorf("vector %s expected SHA-256 mismatch", item.ID)
		}
	}
	return nil
}

func validateInvariants(path, format string, scenarios map[string]bool) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var file invariantsFile
	if err := decodeExact(body, &file); err != nil || file.Format != format {
		return fmt.Errorf("invariants: %w", err)
	}
	got := make([]string, 0, len(file.Invariants))
	for _, item := range file.Invariants {
		got = append(got, item.ID)
		if len(item.PortableScenarios) == 0 || len(item.GoTests) == 0 || item.KMP != "pending_non_go" || item.Dart != "pending_non_go" || item.Swift != "pending_non_go" {
			return fmt.Errorf("invariant %s evidence is incomplete", item.ID)
		}
		for _, id := range item.PortableScenarios {
			if !scenarios[id] {
				return fmt.Errorf("invariant %s references unknown scenario %s", item.ID, id)
			}
		}
	}
	want := []string{"I01", "I02", "I03", "I04", "I05", "I06", "I07", "I08", "I09", "I10", "I11", "I12"}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("invariant inventory is not exactly I01-I12")
	}
	return nil
}
