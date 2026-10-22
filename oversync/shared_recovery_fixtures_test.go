package oversync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type recoveryRawCase struct {
	Name     string
	Raw      string
	Accepted bool
	Stage    string
}
type recoverySnapshotCase struct {
	Name, Source      string
	Metadata          json.RawMessage
	RequestedRecovery *SnapshotRecovery
	Rows              []struct {
		TableIndex int   `json:"table_index"`
		Key        []any `json:"key_values"`
		Version    int64 `json:"row_version"`
		Payload    json.RawMessage
	}
	Chunkings [][]int
}
type recoveryWorkflowState struct {
	Next           int  `json:"next_sequence"`
	AckPending     bool `json:"ack_pending"`
	Frozen         bool `json:"frozen"`
	Outcome        bool `json:"outcome"`
	Snapshots      int  `json:"snapshots"`
	HistoryCommits int  `json:"history_commits"`
}

func TestSharedRecoveryProtocolFixtures(t *testing.T) {
	root := requireManifestPinnedContracts(t)
	var fixture struct {
		FormatVersion  int
		Contract       string
		HashProjection string
		Generation     SchemaGenerationCapabilities
		Headers        []struct {
			Name     string
			Values   []string
			Accepted bool
		}
		Outcomes, OutcomeInvalid, ResolveRequests, AckRequests, SnapshotRequests, SnapshotMetadataInvalid []recoveryRawCase
		BindingCases                                                                                      []struct {
			Name, Raw                     string
			Binding                       PushOperationBinding
			FrozenContext, CurrentContext map[string]string
			Accepted                      bool
		}
		Snapshots, SnapshotInvalid []recoverySnapshotCase
		Workflows                  []struct {
			Name     string
			Events   []string
			Expected recoveryWorkflowState
		}
		RequestCounts []struct {
			Name             string
			Events, Requests []string
			Count            int
			AckPending       bool
		}
		Errors []struct {
			Code   string
			Status int
		}
	}
	readContractFixture(t, root, "recovery/protocol.json", &fixture)
	var fields map[string]json.RawMessage
	readContractFixture(t, root, "recovery/protocol.json", &fields)
	expected := strings.Fields("formatVersion contract hashProjection generation headers bindingCases outcomes outcomeInvalid resolveRequests ackRequests snapshotRequests snapshots snapshotInvalid snapshotMetadataInvalid workflows requestCounts errors")
	if len(fields) != len(expected) {
		t.Fatal("undeclared recovery family")
	}
	for _, name := range expected {
		if _, ok := fields[name]; !ok {
			t.Fatal("missing recovery family", name)
		}
	}
	if fixture.Contract != "durable-recovery" || fixture.FormatVersion != 1 || fixture.HashProjection != "protocol-integral-decimal-strings" {
		t.Fatal("unsupported recovery contract")
	}
	t.Run("authority-pins", func(t *testing.T) { verifyRecoveryAuthorityPins(t, root) })
	for _, c := range fixture.BindingCases {
		t.Run("binding/"+c.Name, func(t *testing.T) {
			var o PushOutcome
			err := json.Unmarshal([]byte(c.Raw), &o)
			if err == nil {
				if !reflect.DeepEqual(c.FrozenContext, c.CurrentContext) {
					err = fmt.Errorf("authority/scope lifecycle mismatch")
				} else {
					err = ValidatePushOutcomeBinding(o, c.Binding, fixture.Generation)
				}
			}
			if (err == nil) != c.Accepted {
				t.Fatalf("binding accepted=%t: %v", c.Accepted, err)
			}
		})
	}
	for _, c := range fixture.Headers {
		t.Run("header/"+c.Name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range c.Values {
				h.Add(ProtocolVersionHeader, v)
			}
			if (ValidateProtocolHeader(h) == nil) != c.Accepted {
				t.Fatal("protocol header disposition")
			}
		})
	}
	for _, c := range fixture.Outcomes {
		t.Run("outcome/"+c.Name, func(t *testing.T) {
			var o PushOutcome
			if err := json.Unmarshal([]byte(c.Raw), &o); err != nil {
				t.Fatal(err)
			}
			got, err := HashPushOutcome(o, fixture.Generation)
			if err != nil || got != o.OutcomeHash {
				t.Fatalf("hash %s expected %s: %v", got, o.OutcomeHash, err)
			}
		})
	}
	for _, c := range fixture.OutcomeInvalid {
		t.Run("invalid-outcome/"+c.Name, func(t *testing.T) {
			var o PushOutcome
			err := json.Unmarshal([]byte(c.Raw), &o)
			if c.Stage == "hash" && err == nil {
				var hash string
				hash, err = HashPushOutcome(o, fixture.Generation)
				if err == nil && hash != o.OutcomeHash {
					err = fmt.Errorf("hash mismatch")
				}
			}
			if err == nil {
				t.Fatal("invalid outcome accepted")
			}
			t.Logf("rejected at %s boundary: %v", c.Stage, err)
		})
	}
	for family, cases := range map[string][]recoveryRawCase{"resolve": fixture.ResolveRequests, "ack": fixture.AckRequests, "snapshot-request": fixture.SnapshotRequests} {
		if len(cases) == 0 {
			t.Fatal("empty family", family)
		}
		for _, c := range cases {
			t.Run(family+"/"+c.Name, func(t *testing.T) {
				var target any
				switch family {
				case "resolve":
					target = &PushOutcomeResolveRequest{}
				case "ack":
					target = &PushOutcomeAckRequest{}
				default:
					target = &SnapshotSessionCreateRequest{}
				}
				err := json.Unmarshal([]byte(c.Raw), target)
				if (err == nil) != c.Accepted {
					t.Fatalf("accepted=%t: %v", c.Accepted, err)
				}
			})
		}
	}
	for _, c := range fixture.SnapshotMetadataInvalid {
		t.Run("snapshot-metadata/"+c.Name, func(t *testing.T) {
			var metadata CompactSnapshotSessionResponse
			if json.Unmarshal([]byte(c.Raw), &metadata) == nil {
				t.Fatal("invalid metadata accepted")
			}
		})
	}
	for _, c := range fixture.Snapshots {
		t.Run("snapshot/"+c.Name, func(t *testing.T) {
			for _, chunks := range c.Chunkings {
				if err := checkRecoverySnapshot(c, fixture.Generation, chunks); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	for _, c := range fixture.SnapshotInvalid {
		t.Run("invalid-snapshot/"+c.Name, func(t *testing.T) {
			if err := checkRecoverySnapshot(c, fixture.Generation, []int{len(c.Rows)}); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
	for _, c := range fixture.Workflows {
		t.Run("workflow/"+c.Name, func(t *testing.T) {
			got := reduceRecoveryWorkflow(t, c.Events)
			if got != c.Expected {
				t.Fatalf("got %+v want %+v", got, c.Expected)
			}
		})
	}
	for _, c := range fixture.RequestCounts {
		t.Run("request-count/"+c.Name, func(t *testing.T) {
			var requests []string
			ready := c.AckPending
			for _, event := range c.Events {
				switch event {
				case "local_disposition":
					ready = true
				case "reopen":
				case "push":
					requests = append(requests, "POST /sync/push")
				case "resolve":
					requests = append(requests, "POST /sync/push-outcomes/resolve")
				case "ack":
					if !ready {
						t.Fatal("ack before disposition")
					}
					requests = append(requests, "POST /sync/push-outcomes/ack")
					ready = false
				case "pull":
					requests = append(requests, "GET /sync/pull")
				default:
					t.Fatal("unevaluated request event", event)
				}
			}
			if len(requests) != c.Count || !reflect.DeepEqual(requests, c.Requests) {
				t.Fatalf("request trace %v expected %v", requests, c.Requests)
			}
		})
	}
	statuses := map[string]int{"protocol_version_mismatch": 426, "push_outcome_mismatch": 409, "push_outcome_pending": 409, "push_outcome_acknowledged": 410, "push_outcome_too_large": 413, "recovery_floor_ahead": 409, "source_generation_conflict": 409}
	if len(fixture.Errors) != len(statuses) {
		t.Fatal("error family coverage")
	}
	for _, c := range fixture.Errors {
		t.Run("error/"+c.Code, func(t *testing.T) {
			if statuses[c.Code] != c.Status {
				t.Fatal("error mapping")
			}
		})
	}
	t.Logf("executed recovery cases: headers=%d outcomes=%d invalid=%d resolve=%d ack=%d snapshot-requests=%d snapshots=%d invalid-snapshots=%d workflows=%d errors=%d", len(fixture.Headers), len(fixture.Outcomes), len(fixture.OutcomeInvalid), len(fixture.ResolveRequests), len(fixture.AckRequests), len(fixture.SnapshotRequests), len(fixture.Snapshots), len(fixture.SnapshotInvalid), len(fixture.Workflows), len(fixture.Errors))
}
func verifyRecoveryAuthorityPins(t *testing.T, root string) {
	t.Helper()
	var pins map[string]string
	readContractFixture(t, root, "recovery/authority-pins.json", &pins)
	expected := strings.Fields("go/AGENTS.md kotlin/AGENTS.md kotlin/docs-engineering/oversqlite-validation.md go/specs/p0000-oversqlite-protocol-simplification-and-efficiency.md go/specs/p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md go/specs/r0001-oversqlite-durable-recovery.md go/specs/r0001-oversqlite-durable-recovery-checklist.md go/specs/r0001-oversqlite-durable-recovery-validation.md go/swagger/two_way_sync.yaml kotlin/specs/oversqlite-fk-safe-sync-and-automatic-resolution.md kotlin/specs/oversqlite-durable-recovery.md")
	if len(pins) != len(expected) {
		t.Fatal("authority inventory mismatch")
	}
	for _, name := range expected {
		want, ok := pins[name]
		if !ok {
			t.Fatal("missing authority pin", name)
		}
		base, relative, ok := strings.Cut(name, "/")
		if !ok {
			t.Fatal(name)
		}
		parent := ".."
		if base == "kotlin" {
			parent = filepath.Dir(root)
		}
		body, err := os.ReadFile(filepath.Join(parent, relative))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatal("authority hash mismatch", name)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "recovery"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !reflect.DeepEqual(names, []string{"authority-pins.json", "protocol.json"}) {
		t.Fatal("undeclared recovery fixture", names)
	}
}
func checkRecoverySnapshot(c recoverySnapshotCase, generation SchemaGenerationCapabilities, chunks []int) error {
	var metadata CompactSnapshotSessionResponse
	if err := json.Unmarshal(c.Metadata, &metadata); err != nil {
		return err
	}
	if err := ValidateSnapshotRecoveryEcho(metadata, c.RequestedRecovery); err != nil {
		return err
	}
	writer, err := NewSnapshotHashWriter(c.Source, generation, metadata.SnapshotID, metadata.SnapshotBundleSeq, metadata.Recovery)
	if err != nil {
		return err
	}
	offset := 0
	for _, size := range chunks {
		if size < 0 || offset+size > len(c.Rows) {
			return fmt.Errorf("invalid chunks")
		}
		for _, row := range c.Rows[offset : offset+size] {
			if err := writer.Add(row.TableIndex, row.Key, row.Version, row.Payload); err != nil {
				return err
			}
		}
		offset += size
	}
	if offset != len(c.Rows) {
		return fmt.Errorf("incomplete snapshot")
	}
	actual, err := writer.Sum(metadata.RowCount, metadata.LogicalByteCount)
	if err != nil {
		return err
	}
	if actual != metadata.SnapshotHash {
		return fmt.Errorf("snapshot hash mismatch")
	}
	return nil
}

// Pure contract reducer: this is neither persistence nor runtime qualification.
func reduceRecoveryWorkflow(t *testing.T, events []string) recoveryWorkflowState {
	t.Helper()
	state := recoveryWorkflowState{Next: 1}
	serverAcked := false
	for _, event := range events {
		switch event {
		case "freeze":
			if state.Frozen || state.Outcome || state.AckPending {
				t.Fatal("illegal successor freeze")
			}
			state.Frozen = true
		case "committed", "rejected", "cancelled":
			if event == "committed" {
				state.HistoryCommits++
			}
			if !state.Frozen {
				t.Fatal("outcome without frozen identity")
			}
			state.Outcome = true
		case "settle":
			if !state.Outcome {
				t.Fatal("settlement without outcome")
			}
			state.Frozen = false
			state.Outcome = false
			state.AckPending = true
		case "ack":
			if !state.AckPending {
				t.Fatal("ack before disposition")
			}
			state.AckPending = false
			state.Next++
		case "ack_lost":
			if !state.AckPending {
				t.Fatal("ack before disposition")
			}
			serverAcked = true
		case "duplicate_ack":
			if !serverAcked || !state.AckPending {
				t.Fatal("missing latest ack fence")
			}
			state.AckPending = false
			state.Next++
		case "snapshot":
			if !state.Frozen && !state.Outcome && !state.AckPending {
				state.Snapshots++
			}
		case "unsafe", "reopen", "retire_rebind": // Persisted F/C/ack fence survives these observations.
		default:
			t.Fatal("unevaluated workflow event", event)
		}
	}
	return state
}
