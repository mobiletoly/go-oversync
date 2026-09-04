// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSchemaGenerationPublicDocumentationAudit(t *testing.T) {
	repositoryRoot := filepath.Clean("..")
	command := exec.Command("git", "ls-files", "-z", "--", "README.md", "oversqlite/README.md", "docs/*.md", "docs/**/*.md", "examples/**/README.md")
	command.Dir = repositoryRoot
	output, err := command.Output()
	require.NoError(t, err)
	listed := bytes.Split(bytes.TrimSuffix(output, []byte{0}), []byte{0})
	require.NotEmpty(t, listed, "public documentation corpus must not be empty")
	sort.Slice(listed, func(i, j int) bool { return bytes.Compare(listed[i], listed[j]) < 0 })
	roots := map[string]int{"README.md": 0, "oversqlite/README.md": 0, "docs/": 0, "examples/": 0}
	var corpus strings.Builder
	for _, rawPath := range listed {
		path := string(rawPath)
		content, readErr := os.ReadFile(filepath.Join(repositoryRoot, filepath.FromSlash(path)))
		require.NoError(t, readErr, path)
		corpus.Write(content)
		corpus.WriteByte('\n')
		for root := range roots {
			if path == root || strings.HasPrefix(path, root) {
				roots[root]++
			}
		}
	}
	for root, count := range roots {
		require.Positive(t, count, "expected public documentation root %s is empty", root)
	}
	text := corpus.String()
	require.NotContains(t, text, "MaxSupportedSchemaVersion:")
	for _, required := range []string{"SchemaGenerations", "SchemaGenerationProof", SchemaGenerationIDHeader, SchemaGenerationFingerprintHeader, "source_generation_bindings", "canonical_request_hash"} {
		require.Contains(t, text, required, "public documentation must cover %q", required)
	}
}

func TestOversqliteReadmeRejectsRemovedPushSessionGuidance(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "oversqlite", "README.md"))
	require.NoError(t, err)
	text := string(content)
	for _, removed := range []string{
		"server session and acknowledged row ordinal",
		"staged prefixes",
		"uploads only the remaining frozen rows",
	} {
		require.NotContains(t, text, removed)
	}
	for _, required := range []string{
		"one complete atomic `POST /sync/push`",
		"freezes the request bytes",
		"retry the exact complete request",
	} {
		require.Contains(t, text, required)
	}
	require.Equal(t, 1, strings.Count(text, "_sync_push_progress"), "the removed table may appear only in the active absence assertion")
	require.Contains(t, text, "no server upload session, acknowledged ordinal, staged\n")
	require.Contains(t, text, "prefix, or `_sync_push_progress` state")
}

func TestSchemaGenerationGettingStartedUsesOnePhysicallyCompatibleSurface(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "docs", "getting-started.md"))
	require.NoError(t, err)
	text := string(content)
	require.NotContains(t, strings.ToUpper(text), "TIMESTAMPTZ")
	require.NotContains(t, strings.ToUpper(text), "TIMESTAMP DEFAULT")
	require.Contains(t, text, "created_at TEXT NOT NULL")
	require.GreaterOrEqual(t, strings.Count(text, "id TEXT NOT NULL PRIMARY KEY"), 2)
	require.Contains(t, text, `{Name: "created_at", WireType: oversync.GenerationWireTypeText}`)
	require.Contains(t, text, `{Name: "content", WireType: oversync.GenerationWireTypeText, Nullable: true}`)
	require.Contains(t, text, "CompileSchemaGenerationRegistry(registeredTables, schemaGenerations)")
	require.Contains(t, text, "GenerationID: 1")
	require.Contains(t, text, "The v1 generation contract deliberately excludes PostgreSQL date/time types.")
}

func TestGenerationDiscoveryDocumentationPreservesAdmissionAndMalformedRouting(t *testing.T) {
	for _, path := range []string{filepath.Join("..", "README.md"), filepath.Join("..", "docs", "getting-started.md"), filepath.Join("..", "docs", "documentation", "server-deployment-and-operations.md")} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		text := string(content)
		for _, required := range []string{"withSyncAdmission", "ReadinessGuard", "admittedSync", "GET /sync/generations/{generation_id}", "GET /sync/generations/", "http.HandlerFunc(func", "http.MethodGet", `r.URL.Path == "/sync/generations"`, `strings.HasPrefix(r.URL.Path, "/sync/generations/")`, `strings.HasPrefix(r.URL.Path, "/sync/")`, "ServeHTTP(w, r)", "bypasses only", "client policy", "HTTP 400"} {
			require.Contains(t, text, required, "%s must preserve %q", path, required)
		}
		require.Equal(t, 1, strings.Count(text, "withSyncAdmission(admissionTarget)"), path)
		require.NotContains(t, text, "withSyncAdmission(mux)", path)
	}
}

func TestSchemaGenerationOpenAPIGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "swagger", "two_way_sync.yaml"))
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, yaml.Unmarshal(data, &document))
	require.Equal(t, "3.1.0", document["openapi"])

	paths := document["paths"].(map[string]any)
	required := map[string][]string{
		"/sync/generations/{generation_id}":     {"get"},
		"/sync/connect":                         {"post"},
		"/sync/push":                            {"post"},
		"/sync/pull":                            {"get"},
		"/sync/watch":                           {"get"},
		"/sync/snapshot-sessions":               {"post"},
		"/sync/snapshot-sessions/{snapshot_id}": {"get", "delete"},
	}
	expectedStatuses := map[string][]string{
		"GET /sync/generations/{generation_id}":        {"200", "400", "401", "409", "426", "500", "503"},
		"POST /sync/connect":                           {"200", "400", "401", "409", "410", "413", "415", "426", "500", "503"},
		"POST /sync/push":                              {"200", "400", "401", "409", "410", "413", "415", "422", "426", "500", "503"},
		"GET /sync/pull":                               {"200", "400", "401", "409", "410", "413", "426", "500", "503"},
		"GET /sync/watch":                              {"200", "400", "401", "403", "409", "410", "426", "500", "503"},
		"POST /sync/snapshot-sessions":                 {"200", "400", "401", "409", "410", "413", "415", "426", "429", "500", "503"},
		"GET /sync/snapshot-sessions/{snapshot_id}":    {"200", "400", "401", "403", "404", "409", "410", "426", "429", "500", "503"},
		"DELETE /sync/snapshot-sessions/{snapshot_id}": {"204", "400", "401", "403", "404", "409", "410", "426", "500", "503"},
	}
	for path, methods := range required {
		item, present := paths[path].(map[string]any)
		require.True(t, present, "required data-plane path %s", path)
		for _, method := range methods {
			operation, present := item[method].(map[string]any)
			require.True(t, present, "%s %s", method, path)
			params := operation["parameters"].([]any)
			refs := map[string]bool{}
			for _, raw := range params {
				if ref, ok := raw.(map[string]any)["$ref"].(string); ok {
					refs[ref] = true
				}
			}
			require.True(t, refs["#/components/parameters/SourceIDHeader"], "%s %s source header", method, path)
			if path != "/sync/generations/{generation_id}" {
				require.True(t, refs["#/components/parameters/SchemaGenerationIDHeader"], "%s %s generation ID", method, path)
				require.True(t, refs["#/components/parameters/SchemaGenerationFingerprintHeader"], "%s %s generation fingerprint", method, path)
			}
			responses := operation["responses"].(map[string]any)
			actualStatuses := make([]string, 0, len(responses))
			for status := range responses {
				actualStatuses = append(actualStatuses, status)
			}
			require.ElementsMatch(t, expectedStatuses[strings.ToUpper(method)+" "+path], actualStatuses, "%s %s status matrix", method, path)
		}
	}
	for _, removed := range []string{"/sync/capabilities", "/sync/push-sessions", "/sync/push-sessions/{push_id}/chunks", "/sync/push-sessions/{push_id}/commit", "/sync/committed-bundles/{bundle_seq}/rows"} {
		require.NotContains(t, paths, removed)
	}

	components := document["components"].(map[string]any)
	parameters := components["parameters"].(map[string]any)
	require.Equal(t, SchemaGenerationIDHeader, parameters["SchemaGenerationIDHeader"].(map[string]any)["name"])
	require.Equal(t, SchemaGenerationFingerprintHeader, parameters["SchemaGenerationFingerprintHeader"].(map[string]any)["name"])
	for _, name := range []string{"AfterBundleSeq", "MaxBundles", "AfterRowOrdinal", "MaxRows", "MaxBytes", "TargetBundleSeq"} {
		require.Equal(t, false, parameters[name].(map[string]any)["required"], "%s must retain its runtime default", name)
	}
	schemas := components["schemas"].(map[string]any)
	discovery := schemas["DiscoveryResponse"].(map[string]any)
	require.ElementsMatch(t, []any{"protocol_version", "schema_generation", "limits", "watch_enabled"}, discovery["required"].([]any))
	require.Equal(t, false, discovery["additionalProperties"])
	limits := schemas["WireLimits"].(map[string]any)
	require.ElementsMatch(t, []any{"max_push_rows", "max_push_decoded_bytes", "max_pull_bundles", "max_pull_decoded_bytes", "max_snapshot_rows_per_chunk", "max_snapshot_decoded_bytes_per_chunk", "max_snapshot_rows", "max_snapshot_logical_bytes", "max_row_decoded_bytes"}, limits["required"].([]any))
	token := schemas["CanonicalInitializationToken"].(map[string]any)
	require.Equal(t, "uuid", token["format"])
	require.EqualValues(t, 36, token["minLength"])
	require.EqualValues(t, 36, token["maxLength"])
	connect := schemas["ConnectResponse"].(map[string]any)
	require.Equal(t, false, connect["additionalProperties"])
	require.Equal(t, "#/components/schemas/CanonicalInitializationToken", connect["properties"].(map[string]any)["initialization_id"].(map[string]any)["$ref"])
	snapshotCreate := schemas["SnapshotSessionCreateRequest"].(map[string]any)
	require.Equal(t, false, snapshotCreate["additionalProperties"])
	require.Equal(t, false, snapshotCreate["properties"].(map[string]any)["source_replacement"].(map[string]any)["additionalProperties"])
	push := schemas["PushRequest"].(map[string]any)
	require.ElementsMatch(t, []any{"source_bundle_id", "canonical_request_hash", "rows"}, push["required"].([]any))
	require.Equal(t, false, push["additionalProperties"])
	require.Equal(t, "#/components/schemas/CanonicalInitializationToken", push["properties"].(map[string]any)["initialization_id"].(map[string]any)["$ref"])
	require.Equal(t, "#/components/schemas/NonEmptyCompactRowGroups", push["properties"].(map[string]any)["rows"].(map[string]any)["$ref"])
	for _, removed := range []string{"planned_row_count", "push_id", "row_count", "byte_count", "features", "bundle_limits"} {
		require.NotContains(t, push["properties"].(map[string]any), removed)
	}
	for _, schemaName := range []string{"CompactRowGroup", "CompactRowTuple", "SnapshotCompactRowGroups", "PushResponse", "CompactBundle", "SnapshotChunkResponse"} {
		require.Contains(t, schemas, schemaName)
	}
	pushResponse := schemas["PushResponse"].(map[string]any)
	require.Equal(t, "#/components/schemas/NonEmptyCompactRowGroups", pushResponse["properties"].(map[string]any)["rows"].(map[string]any)["$ref"])
	compactBundle := schemas["CompactBundle"].(map[string]any)
	require.Equal(t, "#/components/schemas/CompactRowGroups", compactBundle["properties"].(map[string]any)["rows"].(map[string]any)["$ref"])
	compactGroup := schemas["CompactRowGroup"].(map[string]any)
	require.EqualValues(t, 3, compactGroup["minItems"])
	require.EqualValues(t, 3, compactGroup["maxItems"])
	require.Equal(t, false, compactGroup["items"])
	compactTuple := schemas["CompactRowTuple"].(map[string]any)
	require.Len(t, compactTuple["oneOf"].([]any), 2)
	snapshotGroups := schemas["SnapshotCompactRowGroups"].(map[string]any)
	require.NotContains(t, snapshotGroups, "minItems", "empty snapshot chunks are valid")
	snapshotTuple := schemas["SnapshotCompactRowTuple"].(map[string]any)
	require.EqualValues(t, 4, snapshotTuple["minItems"])
	require.EqualValues(t, 4, snapshotTuple["maxItems"])
	require.Equal(t, false, snapshotTuple["items"])
	errorAlternatives := schemas["ErrorResponse"].(map[string]any)["properties"].(map[string]any)["error"].(map[string]any)["oneOf"].([]any)
	errorValues := errorAlternatives[0].(map[string]any)["enum"].([]any)
	for _, code := range []string{"invalid_request", "push_invalid", "push_replay_mismatch", "invalid_gzip", "encoded_body_too_large", "decoded_body_too_large", "bundle_projection_too_large", "committed_bundle_unrepresentable", "snapshot_chunk_too_small"} {
		require.Contains(t, errorValues, code)
	}
}
