package oversync

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Independent local contract interpreter. This is not SQLite execution.
type a3Object map[string]any

func a3Read(t *testing.T, family string) a3Object {
	t.Helper()
	root := requireManifestPinnedContracts(t)
	raw, err := os.ReadFile(filepath.Join(root, "local-apply", family+".json"))
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var f a3Object
	require.NoError(t, decoder.Decode(&f))
	require.Equal(t, io.EOF, decoder.Decode(new(any)))
	require.Equal(t, int64(1), a3Int(t, f, "formatVersion"))
	require.Equal(t, "local-contract-runtime-pending", f["classification"])
	require.Equal(t, family, f["family"])
	require.Equal(t, false, f["runtimeExecuted"])
	var classification struct {
		LocalContractCandidates []struct{ Path, Contract, GoConsumer, JVMConsumer string }
	}
	readContractFixture(t, root, "CLASSIFICATIONS.json", &classification)
	var found int
	for _, c := range classification.LocalContractCandidates {
		if c.Contract != family {
			continue
		}
		found++
		require.Equal(t, "local-apply/"+family+".json", c.Path)
		title := strings.TrimPrefix(family, "a3-")
		title = strings.ToUpper(title[:1]) + title[1:]
		require.Equal(t, "oversync/contract_a3_test.go:TestContractCorpus_A3"+title, c.GoConsumer)
		require.Equal(t, "Shared"+title+"ContractTest.contract", c.JVMConsumer)
	}
	require.Equal(t, 1, found)
	return f
}

func a3Fields(t *testing.T, o a3Object, fields string) {
	t.Helper()
	names := strings.Fields(fields)
	require.Len(t, o, len(names), "unexpected or missing fields: %v", o)
	for _, field := range names {
		require.Contains(t, o, field)
	}
}
func a3Int(t *testing.T, o a3Object, field string) int64 {
	t.Helper()
	n, ok := o[field].(json.Number)
	require.True(t, ok, "%s is not an integer", field)
	value, err := n.Int64()
	require.NoError(t, err)
	require.GreaterOrEqual(t, value, int64(0))
	return value
}
func a3Obj(t *testing.T, v any) a3Object {
	t.Helper()
	o, ok := v.(map[string]any)
	require.True(t, ok)
	return o
}
func a3Array(t *testing.T, v any) []any {
	t.Helper()
	a, ok := v.([]any)
	require.True(t, ok)
	return a
}
func a3Compare(t *testing.T, expected any, actual a3Object) {
	t.Helper()
	e, err := json.Marshal(expected)
	require.NoError(t, err)
	a, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, string(e), string(a))
}
func a3Cases(t *testing.T, f a3Object, ids []string, execute func(*testing.T, string, a3Object) a3Object) {
	t.Helper()
	cases := a3Array(t, f["cases"])
	require.Len(t, cases, len(ids))
	seen := map[string]bool{}
	for index, value := range cases {
		c := a3Obj(t, value)
		a3Fields(t, c, "id kind input expected")
		id, ok := c["id"].(string)
		require.True(t, ok)
		require.Equal(t, ids[index], id)
		require.False(t, seen[id])
		seen[id] = true
		kind, ok := c["kind"].(string)
		require.True(t, ok)
		t.Run(id, func(t *testing.T) { a3Compare(t, c["expected"], execute(t, kind, a3Obj(t, c["input"]))) })
	}
}

func TestContractCorpus_A3Workspace(t *testing.T) {
	f := a3Read(t, "a3-workspace")
	a3Fields(t, f, "formatVersion classification family physicalInventory cases runtimeObligations runtimeExecuted")
	// Canonical JSON pins all physical fields, checks, indexes, charge formula and ownership.
	inventory, err := json.Marshal(f["physicalInventory"])
	require.NoError(t, err)
	require.Equal(t, "8580a0f75f18fc4eebd3604bc9c2142b866eb85af07f40b368752f27f45117b0", fmt.Sprintf("%x", sha256.Sum256(inventory)))
	require.Equal(t, []any{"second-supported-handle", "live-buffer-cancellation", "physical-owner-death-and-reacquisition", "old-partial-tampered-format-rejection", "paged-settlement-backlog", "trigger-live-deviation-preservation"}, f["runtimeObligations"])
	a3Cases(t, f, []string{"aggregate-exact", "aggregate-entries-first", "aggregate-bytes", "aggregate-below", "aggregate-overflow", "work-remaining", "work-intrinsic", "work-exact", "work-cancelled", "work-ready-after-validation", "work-after-overshoot", "work-saturates", "transfer-release-once", "page-row-cap", "page-byte-cap", "page-large-one", "guard-exact", "guard-changed-limits", "guard-changed-dirty", "guard-changed-baseline", "guard-changed-attachment", "guard-changed-input"}, a3WorkspaceResult)
}

func a3WorkspaceResult(t *testing.T, kind string, in a3Object) a3Object {
	switch kind {
	case "aggregate":
		return a3Aggregate(t, in)
	case "work":
		a3Fields(t, in, "limit used required cancelled firstReady")
		limit, used, requested := a3Int(t, in, "limit"), a3Int(t, in, "used"), a3Int(t, in, "required")
		cancelled, ok := in["cancelled"].(bool)
		require.True(t, ok)
		firstReady, ok := in["firstReady"].(bool)
		require.True(t, ok)
		require.Positive(t, limit)
		result := "fits"
		switch {
		case cancelled:
			result = "cancelled"
		case !firstReady && used >= limit:
			result = "sync_work_limit"
		}
		if result == "fits" {
			if requested > math.MaxInt64-used {
				used = math.MaxInt64
			} else {
				used += requested
			}
		}
		return a3Object{"result": result, "used": used, "reserved": requested, "ready": true, "businessChanged": result == "fits"}
	case "reservation":
		return a3Reservation(t, in)
	case "page":
		a3Fields(t, in, "rowBytes maxRows softBytes")
		maxRows, soft := a3Int(t, in, "maxRows"), a3Int(t, in, "softBytes")
		require.Equal(t, int64(128), maxRows)
		require.Equal(t, int64(65536), soft)
		var count, total int64
		for _, v := range a3Array(t, in["rowBytes"]) {
			size := a3Int(t, a3Object{"n": v}, "n")
			if count == maxRows || (count > 0 && (total >= soft || size > soft-total)) {
				break
			}
			require.LessOrEqual(t, size, int64(math.MaxInt64)-total)
			total += size
			count++
		}
		return a3Object{"rows": count, "bytes": total}
	case "guard":
		a3Fields(t, in, "saved current")
		_, ok := in["saved"].(string)
		require.True(t, ok)
		_, ok = in["current"].(string)
		require.True(t, ok)
		return a3Object{"resume": in["saved"] == in["current"], "originalInputRetained": true, "intentRetained": true}
	default:
		t.Fatalf("unsupported A3 workspace case kind: %s", kind)
		return nil
	}
}

func a3Aggregate(t *testing.T, in a3Object) a3Object {
	a3Fields(t, in, "owners requestedEntries requestedBytes maxEntries maxBytes")
	var entries, size int64
	overflow := false
	seen := map[string]bool{}
	for _, v := range a3Array(t, in["owners"]) {
		o := a3Obj(t, v)
		a3Fields(t, o, "owner entries bytes")
		owner, ok := o["owner"].(string)
		require.True(t, ok)
		require.Contains(t, []string{"planner", "apply", "push-stage", "snapshot-stage", "transient"}, owner)
		require.False(t, seen[owner])
		seen[owner] = true
		e, b := a3Int(t, o, "entries"), a3Int(t, o, "bytes")
		if e > math.MaxInt64-entries {
			overflow = true
		} else {
			entries += e
		}
		require.LessOrEqual(t, b, int64(math.MaxInt64)-size)
		size += b
	}
	e, b := a3Int(t, in, "requestedEntries"), a3Int(t, in, "requestedBytes")
	el, bl := a3Int(t, in, "maxEntries"), a3Int(t, in, "maxBytes")
	result := "fits"
	if overflow || entries > el || e > el-entries {
		result = "entries"
	} else if size > bl || b > bl-size {
		result = "bytes"
	}
	ae, ab := entries, size
	if result == "fits" {
		ae += e
		ab += b
	}
	return a3Object{"result": result, "usedEntries": entries, "usedBytes": size, "reservedEntries": e, "reservedBytes": b, "afterEntries": ae, "afterBytes": ab}
}
func a3Reservation(t *testing.T, in a3Object) a3Object {
	a3Fields(t, in, "entries bytes reservedEntries reservedBytes actions")
	entries, size, e, b := a3Int(t, in, "entries"), a3Int(t, in, "bytes"), a3Int(t, in, "reservedEntries"), a3Int(t, in, "reservedBytes")
	require.LessOrEqual(t, e, entries)
	require.LessOrEqual(t, b, size)
	transferred, released := false, false
	result := ""
	for _, action := range a3Array(t, in["actions"]) {
		switch action {
		case "transfer":
			require.False(t, transferred)
			require.False(t, released)
			transferred = true
		case "release":
			if released {
				result = "already_released"
				continue
			}
			released = true
			entries -= e
			size -= b
		default:
			t.Fatalf("unsupported reservation action: %v", action)
		}
	}
	require.True(t, transferred)
	require.True(t, released)
	return a3Object{"entries": entries, "bytes": size, "reservedEntries": int64(0), "reservedBytes": int64(0), "error": result}
}

func TestContractCorpus_A3Pending(t *testing.T) {
	f := a3Read(t, "a3-pending")
	a3Fields(t, f, "formatVersion classification family cases runtimeExecuted")
	a3Cases(t, f, []string{"detail-relationship_unknown", "detail-dependency_unavailable", "detail-historical_repair_required", "sample-count", "sample-bytes", "precedence-cancel", "precedence-wire", "precedence-missing-base", "precedence-capacity", "precedence-work", "precedence-mandatory", "precedence-unknown", "precedence-replay-first"}, a3PendingResult)
}
func a3PendingResult(t *testing.T, kind string, in a3Object) a3Object {
	switch kind {
	case "pending":
		a3Fields(t, in, "detail unit refs progress")
		require.Contains(t, []string{"relationship_unknown", "dependency_unavailable", "historical_repair_required"}, in["detail"])
		unit, ok := in["unit"].(string)
		require.True(t, ok)
		require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{64}$`), unit)
		progress := a3Obj(t, in["progress"])
		a3Fields(t, progress, "dirtyRows outboundRows committedBundles")
		for _, field := range []string{"dirtyRows", "outboundRows", "committedBundles"} {
			a3Int(t, progress, field)
		}
		refs := a3Array(t, in["refs"])
		sample := []any{}
		size := 2
		more := false
		for _, ref := range refs {
			r := a3Obj(t, ref)
			a3Fields(t, r, "schema table key")
			for _, name := range []string{"schema", "table"} {
				s, ok := r[name].(string)
				require.True(t, ok)
				require.NotEmpty(t, s)
			}
			key := a3Obj(t, r["key"])
			a3Fields(t, key, "storage value")
			require.Equal(t, "text", key["storage"])
			_, ok := key["value"].(string)
			require.True(t, ok)
			raw, err := json.Marshal(ref)
			require.NoError(t, err)
			need := len(raw)
			if len(sample) > 0 {
				need++
			}
			if len(sample) == 16 || need > 4096-size {
				more = true
				continue
			}
			sample = append(sample, ref)
			size += need
		}
		return a3Object{"detail": in["detail"], "unit": unit, "refs": sample, "hasMore": more, "progress": progress}
	case "precedence":
		a3Fields(t, in, "flags state")
		flags := map[string]bool{}
		for _, value := range a3Array(t, in["flags"]) {
			s, ok := value.(string)
			require.True(t, ok)
			require.False(t, flags[s])
			flags[s] = true
		}
		precedence := []struct{ flag, result string }{{"cancelled", "cancelled"}, {"frozen_outbox", "replay"}, {"malformed", "integrity"}, {"missing_base", "resolution_base_unavailable"}, {"capacity", "sync_workspace_limit"}, {"work", "sync_work_limit"}, {"mandatory_missing", "integrity"}, {"unknown", "relationship_unknown"}}
		result := ""
		for _, p := range precedence {
			if flags[p.flag] && result == "" {
				result = p.result
			}
			delete(flags, p.flag)
		}
		require.Empty(t, flags)
		require.NotEmpty(t, result)
		state := a3Obj(t, in["state"])
		a3Fields(t, state, "A B L clean dirty outbox sourceNext checkpoint confirmedPrefix")
		return a3Object{"result": result, "state": state, "automaticActions": []any{}}
	default:
		t.Fatalf("unsupported A3 pending case kind: %s", kind)
		return nil
	}
}
