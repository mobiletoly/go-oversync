package jcs

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// TestLegacyUniformNumericStringBridgeFixture keeps the pre-generation SQLite
// storage bridge fixture readable. It is not the active generation wire
// contract; generation exactness is covered by the wirevalue and Oversync
// generation tests.
func TestLegacyUniformNumericStringBridgeFixture(t *testing.T) {
	path := filepath.Join("..", "..", "..", "sqlitenow-kmp", "oversqlite-contracts", "canonical-json", "jcs-uniform-numeric-strings.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("sqlitenow-kmp sibling checkout is not present")
	}
	if err != nil {
		t.Fatal(err)
	}

	var fixture map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	requireObjectKeys(t, "fixture", fixture, []string{
		"contract",
		"contract_id",
		"fixture_schema_version",
		"required_protocol_version",
		"integer_cases",
		"decimal_cases",
		"float_cases",
		"boolean_cases",
		"replay_cases",
		"invalid_cases",
		"canonical_examples",
		"protocol",
	})
	if got := requireString(t, fixture, "contract_id"); got != "jcs_uniform_numeric_strings_v1" {
		t.Fatalf("contract_id = %q", got)
	}
	if got := requireInt(t, fixture, "fixture_schema_version"); got != 1 {
		t.Fatalf("fixture_schema_version = %d", got)
	}
	if got := requireString(t, fixture, "required_protocol_version"); got != "v1" {
		t.Fatalf("required_protocol_version = %q", got)
	}

	requireValueCases(t, fixture, "integer_cases", []string{
		"int64_min",
		"int64_max",
		"above_javascript_safe_integer",
		"int2_min",
		"int2_max",
		"int4_min",
		"int4_max",
	})
	requireValueCases(t, fixture, "decimal_cases", []string{
		"decimal_precision_and_scale",
		"decimal_exponent",
		"decimal_postgresql_authoritative_scale",
	})
	requireValueCases(t, fixture, "float_cases", []string{
		"binary64_min_subnormal",
		"binary64_max_finite",
		"binary64_negative_max_finite",
		"float4_rounding",
		"floating_negative_zero_normalizes",
		"floating_integer_spelling",
	})
	requireBooleanCases(t, fixture)
	requireReplayCases(t, fixture)
	requireInvalidCases(t, fixture)
	requireCanonicalExamples(t, fixture)
	requireProtocolExpectations(t, fixture)
}

func requireValueCases(t *testing.T, fixture map[string]json.RawMessage, section string, wantNames []string) {
	t.Helper()
	cases := requireObjects(t, fixture, section)
	requireNames(t, section, cases, wantNames)
	for _, item := range cases {
		name := requireString(t, item, "name")
		requireObjectKeys(t, name, item, []string{"name", "local_sqlite", "uploaded_wire", "postgres_destination", "committed_server"})
		local := requireObject(t, item, "local_sqlite")
		requireObjectKeys(t, name+".local_sqlite", local, []string{"storage_class", "value_text"})
		requireString(t, item, "uploaded_wire")
		requireString(t, item, "committed_server")
	}
}

func requireBooleanCases(t *testing.T, fixture map[string]json.RawMessage) {
	t.Helper()
	cases := requireObjects(t, fixture, "boolean_cases")
	requireNames(t, "boolean_cases", cases, []string{"sqlite_boolean_false", "sqlite_boolean_true"})
	wires := make(map[string]bool)
	committed := make(map[bool]bool)
	for _, item := range cases {
		wires[requireString(t, item, "uploaded_wire")] = true
		var value bool
		if err := json.Unmarshal(item["committed_server"], &value); err != nil {
			t.Fatalf("%s committed_server must be Boolean: %v", requireString(t, item, "name"), err)
		}
		committed[value] = true
		local := requireObject(t, item, "local_sqlite")
		if requireString(t, local, "storage_class") != "integer" || requireString(t, item, "postgres_destination") != "boolean" {
			t.Fatalf("%s does not pin the SQLite integer to PostgreSQL Boolean bridge", requireString(t, item, "name"))
		}
	}
	if !reflect.DeepEqual(wires, map[string]bool{"0": true, "1": true}) || !reflect.DeepEqual(committed, map[bool]bool{false: true, true: true}) {
		t.Fatalf("Boolean bridge values are incomplete")
	}
}

func requireReplayCases(t *testing.T, fixture map[string]json.RawMessage) {
	t.Helper()
	cases := requireObjects(t, fixture, "replay_cases")
	requireNames(t, "replay_cases", cases, []string{"committed_replay_unchanged_local_intent", "committed_replay_later_local_edit"})
	decisions := make(map[string]bool)
	for _, item := range cases {
		name := requireString(t, item, "name")
		requireObjectKeys(t, name, item, []string{"name", "frozen_local", "uploaded_wire", "committed_server", "live_local", "decision"})
		decisions[requireString(t, item, "decision")] = true
	}
	want := map[string]bool{"apply_committed_and_clear_dirty": true, "preserve_later_local_edit_and_requeue": true}
	if !reflect.DeepEqual(decisions, want) {
		t.Fatalf("replay decisions = %v, want %v", decisions, want)
	}
}

func requireInvalidCases(t *testing.T, fixture map[string]json.RawMessage) {
	t.Helper()
	cases := requireObjects(t, fixture, "invalid_cases")
	requireNames(t, "invalid_cases", cases, []string{
		"integer_leading_plus",
		"integer_leading_zero",
		"integer_negative_zero",
		"integer_fraction",
		"integer_exponent",
		"integer_out_of_range_high",
		"integer_out_of_range_low",
		"integer_legacy_json_number",
		"decimal_leading_plus",
		"decimal_leading_zero",
		"decimal_negative_zero",
		"decimal_malformed_fraction",
		"decimal_malformed_exponent",
		"decimal_nan",
		"decimal_legacy_json_number",
		"float_noncanonical_fraction",
		"float_noncanonical_exponent",
		"float_negative_zero",
		"float_malformed",
		"float_nan",
		"float_positive_infinity",
		"float_negative_infinity",
		"float_legacy_json_number",
		"boolean_string_true",
		"boolean_integer_two",
	})
	categories := map[string]map[string]bool{
		"integer": {"leading_plus": true, "leading_zero": true, "negative_zero": true, "fraction": true, "exponent": true, "out_of_range": true, "legacy_json_number": true},
		"decimal": {"leading_plus": true, "leading_zero": true, "negative_zero": true, "malformed": true, "non_finite": true, "legacy_json_number": true},
		"float":   {"noncanonical": true, "malformed": true, "non_finite": true, "legacy_json_number": true},
		"boolean": {"invalid_boolean_bridge": true},
	}
	for _, item := range cases {
		name := requireString(t, item, "name")
		requireObjectKeys(t, name, item, []string{"name", "family", "input", "input_json_type", "category", "outcome"})
		family := requireString(t, item, "family")
		category := requireString(t, item, "category")
		if !categories[family][category] {
			t.Fatalf("%s has invalid classification %s/%s", name, family, category)
		}
		if requireString(t, item, "outcome") != "reject_before_mutation" {
			t.Fatalf("%s does not reject before mutation", name)
		}
		inputType := requireString(t, item, "input_json_type")
		var input any
		if err := json.Unmarshal(item["input"], &input); err != nil {
			t.Fatalf("%s input: %v", name, err)
		}
		switch inputType {
		case "string":
			if _, ok := input.(string); !ok {
				t.Fatalf("%s input is not a string", name)
			}
		case "number":
			if _, ok := input.(float64); !ok || category != "legacy_json_number" {
				t.Fatalf("%s does not pin a legacy JSON number", name)
			}
		default:
			t.Fatalf("%s has unknown input_json_type %q", name, inputType)
		}
	}
}

func requireCanonicalExamples(t *testing.T, fixture map[string]json.RawMessage) {
	t.Helper()
	examples := requireObject(t, fixture, "canonical_examples")
	requireObjectKeys(t, "canonical_examples", examples, []string{"push_request", "committed_bundle", "pull_response", "snapshot_response", "conflict_response"})
	for name := range examples {
		item := requireObject(t, examples, name)
		requireObjectKeys(t, name, item, []string{"canonical_bytes", "utf8_base64", "sha256"})
		canonical := []byte(requireString(t, item, "canonical_bytes"))
		if got, want := base64.StdEncoding.EncodeToString(canonical), requireString(t, item, "utf8_base64"); got != want {
			t.Fatalf("%s Base64 = %s, want %s", name, got, want)
		}
		sum := sha256.Sum256(canonical)
		if got, want := hex.EncodeToString(sum[:]), requireString(t, item, "sha256"); got != want {
			t.Fatalf("%s SHA-256 = %s, want %s", name, got, want)
		}
		if !json.Valid(canonical) {
			t.Fatalf("%s canonical bytes are not JSON", name)
		}
	}
}

func requireProtocolExpectations(t *testing.T, fixture map[string]json.RawMessage) {
	t.Helper()
	protocol := requireObject(t, fixture, "protocol")
	capabilities := requireObject(t, protocol, "capabilities")
	if got := requireString(t, capabilities, "protocol_version"); got != "v1" {
		t.Fatalf("capabilities protocol_version = %q", got)
	}
	rejections := requireObjects(t, protocol, "updated_client_rejections")
	requireNames(t, "updated_client_rejections", rejections, []string{"reject_v0", "reject_empty_version", "reject_unknown_version"})
	actuals := make(map[string]bool)
	for _, item := range rejections {
		actuals[requireString(t, item, "actual")] = true
		if requireString(t, item, "category") != "protocol_version_mismatch" || requireString(t, item, "timing") != "before_connect_or_outbox_freeze" {
			t.Fatalf("%s has invalid protocol rejection classification", requireString(t, item, "name"))
		}
	}
	if !reflect.DeepEqual(actuals, map[string]bool{"v0": true, "": true, "v-next": true}) {
		t.Fatalf("protocol rejection values = %v", actuals)
	}
	reset := requireObject(t, protocol, "full_reset")
	if requireString(t, reset, "client_database") != "recreate" || requireString(t, reset, "server_database") != "recreate_including_business_data" || requireBool(t, reset, "preserve_frozen_outbox") {
		t.Fatalf("full reset expectations are incomplete")
	}
	incompatible := requireObject(t, protocol, "incompatible_development_build")
	if !requireBool(t, incompatible, "same_v1_may_be_incompatible") || requireString(t, incompatible, "mixed_versions") != "unsupported" {
		t.Fatalf("incompatible development build expectations are incomplete")
	}
}

func requireObject(t *testing.T, object map[string]json.RawMessage, key string) map[string]json.RawMessage {
	t.Helper()
	var value map[string]json.RawMessage
	if err := json.Unmarshal(object[key], &value); err != nil {
		t.Fatalf("%s must be an object: %v", key, err)
	}
	return value
}

func requireObjects(t *testing.T, object map[string]json.RawMessage, key string) []map[string]json.RawMessage {
	t.Helper()
	var value []map[string]json.RawMessage
	if err := json.Unmarshal(object[key], &value); err != nil {
		t.Fatalf("%s must be an array of objects: %v", key, err)
	}
	return value
}

func requireString(t *testing.T, object map[string]json.RawMessage, key string) string {
	t.Helper()
	var value string
	if err := json.Unmarshal(object[key], &value); err != nil {
		t.Fatalf("%s must be a string: %v", key, err)
	}
	return value
}

func requireInt(t *testing.T, object map[string]json.RawMessage, key string) int {
	t.Helper()
	var value int
	if err := json.Unmarshal(object[key], &value); err != nil {
		t.Fatalf("%s must be an integer: %v", key, err)
	}
	return value
}

func requireBool(t *testing.T, object map[string]json.RawMessage, key string) bool {
	t.Helper()
	var value bool
	if err := json.Unmarshal(object[key], &value); err != nil {
		t.Fatalf("%s must be a Boolean: %v", key, err)
	}
	return value
}

func requireObjectKeys(t *testing.T, name string, object map[string]json.RawMessage, want []string) {
	t.Helper()
	got := make([]string, 0, len(object))
	for key := range object {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s keys = %v, want %v", name, got, want)
	}
}

func requireNames(t *testing.T, section string, items []map[string]json.RawMessage, want []string) {
	t.Helper()
	got := make([]string, 0, len(items))
	for _, item := range items {
		got = append(got, requireString(t, item, "name"))
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s names = %v, want %v", section, got, want)
	}
}
