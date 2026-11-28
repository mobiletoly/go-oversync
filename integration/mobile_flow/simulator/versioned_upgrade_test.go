package simulator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	mobileUpgradeProcessHelperEnv   = "MOBILE_UPGRADE_PROCESS_HELPER"
	mobileUpgradeProcessDBEnv       = "MOBILE_UPGRADE_PROCESS_DB"
	mobileUpgradeProcessURLEnv      = "MOBILE_UPGRADE_PROCESS_URL"
	mobileUpgradeProcessTokenEnv    = "MOBILE_UPGRADE_PROCESS_TOKEN"
	mobileUpgradeProcessUserEnv     = "MOBILE_UPGRADE_PROCESS_USER"
	mobileUpgradeProcessBoundaryEnv = "MOBILE_UPGRADE_PROCESS_BOUNDARY"
)

type mobileUpgradeCommitted struct {
	seq         int64
	source      string
	sourceSeq   int64
	requestHash string
	rows        []oversync.BundleRow
	hash        string
	bytes       int64
}

type mobileUpgradeAcknowledgment struct {
	request               oversync.PushOutcomeAckRequest
	generationID          string
	generationFingerprint string
}

type mobileUpgradeServerFixture struct {
	t                      *testing.T
	mu                     sync.Mutex
	old                    oversqlite.LocalGenerationContract
	target                 oversqlite.LocalGenerationContract
	nextSeq                int64
	committed              map[string]*mobileUpgradeCommitted
	outcomes               map[string]*oversync.PushOutcome
	acknowledged           map[string]mobileUpgradeAcknowledgment
	replacements           []oversync.SnapshotSourceReplacement
	loseReplacementReply   bool
	replacementReplyLosses int
	offlineOldPull         bool
	rejectNextPushCreate   bool
	offlineSnapshotAfter   int64
	targetRows             []oversync.SnapshotRow
}

func newMobileUpgradeServerFixture(t *testing.T, old, target oversqlite.LocalGenerationContract) *mobileUpgradeServerFixture {
	id := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	return &mobileUpgradeServerFixture{
		t: t, old: old, target: target, nextSeq: 1,
		committed:            map[string]*mobileUpgradeCommitted{},
		outcomes:             map[string]*oversync.PushOutcome{},
		acknowledged:         map[string]mobileUpgradeAcknowledgment{},
		offlineSnapshotAfter: -1,
		targetRows: []oversync.SnapshotRow{{Schema: "main", Table: "upgrade_people", Key: oversync.SyncKey{"id": id}, RowVersion: 1,
			Payload: json.RawMessage(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"server-converted","new_label":"new-only"}`)}},
	}
}

func mobileUpgradeJSON(status int, value any) *http.Response {
	body, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}
}

func mobileUpgradeDiscovery(local oversqlite.LocalGenerationContract) oversync.GenerationDiscoveryResponse {
	tables := make([]oversync.SchemaGenerationTableCapabilities, 0, len(local.Generation.Tables))
	for _, table := range local.Generation.Tables {
		out := oversync.SchemaGenerationTableCapabilities{
			Schema: table.Schema, Table: table.Table, SyncKeyColumns: []string{"id"},
			ForeignKeys: []oversync.SchemaGenerationForeignKeyCapabilities{},
		}
		for _, column := range table.Columns {
			out.Columns = append(out.Columns, oversync.SchemaGenerationColumnCapabilities{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
		}
		tables = append(tables, out)
	}
	return oversync.GenerationDiscoveryResponse{
		ProtocolVersion:  oversync.SyncProtocolVersion,
		SchemaGeneration: oversync.SchemaGenerationCapabilities{ID: fmt.Sprint(local.Proof.ID), Fingerprint: local.Proof.Fingerprint, Tables: tables},
		Limits:           oversync.WireLimits{MaxPushRows: 10000, MaxPushDecodedBytes: 16 << 20, MaxPushOutcomeDecodedBytes: 16*1024*1024 + 8192, MaxPullBundles: 5000, MaxPullDecodedBytes: 32 << 20, MaxSnapshotRowsPerChunk: 1, MaxSnapshotDecodedBytesPerChunk: 8 << 20, MaxSnapshotRows: 10000, MaxSnapshotLogicalBytes: 16 << 20, MaxRowDecodedBytes: 1 << 20},
	}
}

func (s *mobileUpgradeServerFixture) generation(id string) oversync.SchemaGenerationCapabilities {
	if id == fmt.Sprint(s.old.Proof.ID) {
		return mobileUpgradeDiscovery(s.old).SchemaGeneration
	}
	return mobileUpgradeDiscovery(s.target).SchemaGeneration
}

func (s *mobileUpgradeServerFixture) retainOutcome(source string, generation oversync.SchemaGenerationCapabilities, request oversync.PushOutcomeResolveRequest, effect *oversync.CommittedPushEffect, rejection *oversync.PushOutcomeRejection) (*oversync.PushOutcome, error) {
	status := "cancelled"
	if effect != nil {
		status = "committed"
	}
	if rejection != nil {
		status = "rejected"
	}
	outcome := &oversync.PushOutcome{
		SourceID: source, GenerationID: generation.ID, GenerationFingerprint: generation.Fingerprint,
		SourceBundleID: request.SourceBundleID, CanonicalRequestHash: request.CanonicalRequestHash,
		InitializationID: request.InitializationID, Status: status, Committed: effect, Rejection: rejection,
	}
	hash, err := oversync.HashPushOutcome(*outcome, generation)
	if err != nil {
		return nil, err
	}
	outcome.OutcomeHash = hash
	s.outcomes[source+"/"+strconv.FormatInt(request.SourceBundleID, 10)] = outcome
	return outcome, nil
}

func mobileUpgradeOutcomeResponse(outcome *oversync.PushOutcome, source string, generation oversync.SchemaGenerationCapabilities, request oversync.PushOutcomeResolveRequest) (*http.Response, error) {
	err := oversync.ValidatePushOutcomeBinding(*outcome, oversync.PushOperationBinding{
		SourceID: source, GenerationID: generation.ID, GenerationFingerprint: generation.Fingerprint,
		SourceBundleID: request.SourceBundleID, CanonicalRequestHash: request.CanonicalRequestHash, InitializationID: request.InitializationID,
	}, generation)
	return mobileUpgradeJSON(http.StatusOK, outcome), err
}

func (s *mobileUpgradeServerFixture) snapshotMetadata(source string, generation oversync.SchemaGenerationCapabilities, rows []oversync.SnapshotRow, recovery *oversync.SnapshotRecovery) (oversync.CompactSnapshotSessionResponse, error) {
	boundary := max(1, s.nextSeq-1)
	seal, err := oversync.NewSnapshotHashWriter(source, generation, "mobile-upgrade-snapshot", boundary, recovery)
	if err != nil {
		return oversync.CompactSnapshotSessionResponse{}, err
	}
	for _, row := range rows {
		for index, table := range generation.Tables {
			if table.Schema == row.Schema && table.Table == row.Table {
				key := make([]any, len(table.SyncKeyColumns))
				for i, column := range table.SyncKeyColumns {
					key[i] = row.Key[column]
				}
				if err := seal.Add(index, key, row.RowVersion, row.Payload); err != nil {
					return oversync.CompactSnapshotSessionResponse{}, err
				}
				break
			}
		}
	}
	_, logicalBytes, err := oversync.CompactGroupsForSnapshotRowsAt(generation, rows, 1)
	if err != nil {
		return oversync.CompactSnapshotSessionResponse{}, err
	}
	hash, err := seal.Sum(int64(len(rows)), logicalBytes)
	return oversync.CompactSnapshotSessionResponse{SnapshotID: "mobile-upgrade-snapshot", SnapshotBundleSeq: boundary, RowCount: int64(len(rows)), LogicalByteCount: logicalBytes, SnapshotHash: hash, Recovery: recovery, ExpiresAt: "2030-01-01T00:00:00Z"}, err
}

func (s *mobileUpgradeServerFixture) RoundTrip(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
	source := request.Header.Get(oversync.SourceIDHeader)
	s.t.Logf("fixture request %s %s generation=%s source=%s", request.Method, request.URL.Path, generation, source)
	switch {
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/sync/generations/"):
		selected := s.target
		if strings.TrimPrefix(request.URL.Path, "/sync/generations/") == fmt.Sprint(s.old.Proof.ID) {
			selected = s.old
		}
		return mobileUpgradeJSON(http.StatusOK, mobileUpgradeDiscovery(selected)), nil
	case request.Method == http.MethodPost && request.URL.Path == "/sync/connect":
		return mobileUpgradeJSON(http.StatusOK, oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
	case request.Method == http.MethodPost && request.URL.Path == "/sync/push-outcomes/resolve":
		var resolve oversync.PushOutcomeResolveRequest
		if err := json.NewDecoder(request.Body).Decode(&resolve); err != nil {
			return nil, err
		}
		key := source + "/" + strconv.FormatInt(resolve.SourceBundleID, 10)
		if outcome := s.outcomes[key]; outcome != nil {
			return mobileUpgradeOutcomeResponse(outcome, source, s.generation(generation), resolve)
		}
		if resolve.SourceBundleID <= s.acknowledged[source].request.SourceBundleID {
			return mobileUpgradeJSON(http.StatusGone, oversync.ErrorResponse{Error: "push_outcome_acknowledged"}), nil
		}
		outcome, err := s.retainOutcome(source, s.generation(generation), resolve, nil, nil)
		return mobileUpgradeJSON(http.StatusOK, outcome), err
	case request.Method == http.MethodPost && request.URL.Path == "/sync/push-outcomes/ack":
		var ack oversync.PushOutcomeAckRequest
		if err := json.NewDecoder(request.Body).Decode(&ack); err != nil {
			return nil, err
		}
		key := source + "/" + strconv.FormatInt(ack.SourceBundleID, 10)
		if outcome := s.outcomes[key]; outcome != nil {
			if outcome.GenerationID != generation || outcome.GenerationFingerprint != request.Header.Get(oversync.SchemaGenerationFingerprintHeader) || outcome.CanonicalRequestHash != ack.CanonicalRequestHash || outcome.OutcomeHash != ack.OutcomeHash {
				return nil, fmt.Errorf("fixture acknowledgment binding mismatch")
			}
			delete(s.outcomes, key)
			s.acknowledged[source] = mobileUpgradeAcknowledgment{ack, generation, outcome.GenerationFingerprint}
		} else if retained := s.acknowledged[source]; retained.request != ack || retained.generationID != generation || retained.generationFingerprint != request.Header.Get(oversync.SchemaGenerationFingerprintHeader) {
			return nil, fmt.Errorf("fixture acknowledgment has no retained outcome")
		}
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
	case request.Method == http.MethodPost && request.URL.Path == "/sync/push":
		var push oversync.CompactPushRequest
		if err := json.NewDecoder(request.Body).Decode(&push); err != nil {
			return nil, err
		}
		selected := s.target
		if generation == fmt.Sprint(s.old.Proof.ID) {
			selected = s.old
		}
		discovery := mobileUpgradeDiscovery(selected)
		var initializationID *string
		if push.InitializationID != "" {
			initializationID = &push.InitializationID
		}
		operation := oversync.PushOutcomeResolveRequest{SourceBundleID: push.SourceBundleID, CanonicalRequestHash: push.CanonicalRequestHash, InitializationID: initializationID}
		key := source + "/" + strconv.FormatInt(push.SourceBundleID, 10)
		if outcome := s.outcomes[key]; outcome != nil {
			return mobileUpgradeOutcomeResponse(outcome, source, discovery.SchemaGeneration, operation)
		}
		if _, exists := s.committed[key]; exists || push.SourceBundleID <= s.acknowledged[source].request.SourceBundleID {
			return mobileUpgradeJSON(http.StatusGone, oversync.ErrorResponse{Error: "push_outcome_acknowledged"}), nil
		}
		if s.rejectNextPushCreate {
			s.rejectNextPushCreate = false
			outcome, err := s.retainOutcome(source, discovery.SchemaGeneration, operation, nil, &oversync.PushOutcomeRejection{Code: "payload_invalid"})
			return mobileUpgradeJSON(http.StatusOK, outcome), err
		}
		pendingRows, err := oversync.ExpandCompactPushRows(discovery.SchemaGeneration, push.Rows)
		if err != nil {
			return nil, err
		}
		rows := make([]oversync.BundleRow, 0, len(pendingRows))
		for _, pending := range pendingRows {
			rows = append(rows, oversync.BundleRow{Schema: pending.Schema, Table: pending.Table, Key: pending.Key, Op: pending.Op, RowVersion: s.nextSeq, Payload: pending.Payload})
		}
		logical := make([]protocolhash.BundleRow, 0, len(rows))
		for _, row := range rows {
			logical = append(logical, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
		}
		hash, byteCount, err := protocolhash.CommittedBundle(logical)
		if err != nil {
			return nil, err
		}
		committed := &mobileUpgradeCommitted{seq: s.nextSeq, source: source, sourceSeq: push.SourceBundleID, requestHash: push.CanonicalRequestHash, rows: rows, hash: hash, bytes: byteCount}
		s.committed[source+"/"+strconv.FormatInt(push.SourceBundleID, 10)] = committed
		s.nextSeq++
		groups, err := oversync.CompactGroupsForBundleRows(discovery.SchemaGeneration, rows)
		if err != nil {
			return nil, err
		}
		outcome, err := s.retainOutcome(source, discovery.SchemaGeneration, operation,
			&oversync.CommittedPushEffect{BundleSeq: committed.seq, BundleHash: hash, RowCount: int64(len(rows)), Rows: groups}, nil)
		return mobileUpgradeJSON(http.StatusOK, outcome), err
	case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
		if s.offlineOldPull && generation == fmt.Sprint(s.old.Proof.ID) {
			return nil, io.ErrUnexpectedEOF
		}
		after, _ := strconv.ParseInt(request.URL.Query().Get("after_bundle_seq"), 10, 64)
		return mobileUpgradeJSON(http.StatusOK, oversync.CompactPullResponse{StableBundleSeq: after, Bundles: []oversync.CompactBundle{}}), nil
	case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
		var create oversync.SnapshotSessionCreateRequest
		if request.Body != nil {
			_ = json.NewDecoder(request.Body).Decode(&create)
		}
		if create.SourceReplacement != nil {
			s.replacements = append(s.replacements, *create.SourceReplacement)
			if s.replacementReplyLosses > 0 {
				s.replacementReplyLosses--
				return nil, io.ErrUnexpectedEOF
			}
			if s.loseReplacementReply {
				s.loseReplacementReply = false
				return nil, io.ErrUnexpectedEOF
			}
		}
		snapshotRows := append([]oversync.SnapshotRow(nil), s.targetRows...)
		if generation == fmt.Sprint(s.old.Proof.ID) {
			snapshotRows = []oversync.SnapshotRow{}
		}
		selected := s.target
		if generation == fmt.Sprint(s.old.Proof.ID) {
			selected = s.old
		}
		snapshotSource := source
		if create.SourceReplacement != nil {
			snapshotSource = create.SourceReplacement.NewSourceID
		}
		metadata, err := s.snapshotMetadata(snapshotSource, mobileUpgradeDiscovery(selected).SchemaGeneration, snapshotRows, create.Recovery)
		if err != nil {
			return nil, err
		}
		return mobileUpgradeJSON(http.StatusOK, metadata), nil
	case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/mobile-upgrade-snapshot":
		snapshotRows := append([]oversync.SnapshotRow(nil), s.targetRows...)
		if generation == fmt.Sprint(s.old.Proof.ID) {
			snapshotRows = []oversync.SnapshotRow{}
		}
		after, _ := strconv.ParseInt(request.URL.Query().Get("after_row_ordinal"), 10, 64)
		if s.offlineSnapshotAfter >= 0 && after >= s.offlineSnapshotAfter {
			return nil, io.ErrUnexpectedEOF
		}
		end := after + 1
		if end > int64(len(snapshotRows)) {
			end = int64(len(snapshotRows))
		}
		rows := append([]oversync.SnapshotRow{}, snapshotRows[after:end]...)
		selected := s.target
		if generation == fmt.Sprint(s.old.Proof.ID) {
			selected = s.old
		}
		groups, logicalByteCount, err := oversync.CompactGroupsForSnapshotRowsAt(mobileUpgradeDiscovery(selected).SchemaGeneration, rows, after+1)
		if err != nil {
			return nil, err
		}
		return mobileUpgradeJSON(http.StatusOK, oversync.CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: end, HasMore: end < int64(len(snapshotRows)), LogicalByteCount: logicalByteCount}), nil
	case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/mobile-upgrade-snapshot":
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
	}
	return nil, fmt.Errorf("unexpected mobile upgrade request %s %s", request.Method, request.URL.Path)
}

func TestMobileUpgradeFixture_DuplicateAcknowledgmentRetainsOriginalGeneration(t *testing.T) {
	old, target, err := mobileUpgradeDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	fixture := newMobileUpgradeServerFixture(t, old.Contract, target.Contract)
	source := uuid.NewString()
	generation := mobileUpgradeDiscovery(old.Contract).SchemaGeneration
	outcome, err := fixture.retainOutcome(source, generation, oversync.PushOutcomeResolveRequest{
		SourceBundleID: 1, CanonicalRequestHash: strings.Repeat("a", 64),
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ack := oversync.PushOutcomeAckRequest{SourceBundleID: 1, CanonicalRequestHash: outcome.CanonicalRequestHash, OutcomeHash: outcome.OutcomeHash}
	body, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	send := func(id, fingerprint string) error {
		request, err := http.NewRequest(http.MethodPost, "http://mobile.fixture/sync/push-outcomes/ack", bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set(oversync.SourceIDHeader, source)
		request.Header.Set(oversync.SchemaGenerationIDHeader, id)
		request.Header.Set(oversync.SchemaGenerationFingerprintHeader, fingerprint)
		response, err := fixture.RoundTrip(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			return fmt.Errorf("acknowledgment returned %d", response.StatusCode)
		}
		return nil
	}
	if err := send(generation.ID, generation.Fingerprint); err != nil {
		t.Fatal(err)
	}
	retained := fixture.acknowledged[source]
	for _, test := range []struct {
		name, id, fingerprint string
		wantError             bool
	}{
		{"exact duplicate", generation.ID, generation.Fingerprint, false},
		{"changed generation", fmt.Sprint(target.Contract.Proof.ID), generation.Fingerprint, true},
		{"changed fingerprint", generation.ID, target.Contract.Proof.Fingerprint, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := send(test.id, test.fingerprint); (err != nil) != test.wantError {
				t.Fatalf("duplicate ack error=%v wantError=%t", err, test.wantError)
			}
			if fixture.acknowledged[source] != retained || len(fixture.outcomes) != 0 {
				t.Fatal("duplicate acknowledgment changed the original fence or resurrected terminal evidence")
			}
		})
	}
}

func attachVersionedTestApp(t *testing.T, app *versionedApplication, user string) {
	t.Helper()
	result, err := app.Client.Attach(context.Background(), user)
	if err != nil || result.Status != oversqlite.AttachStatusConnected {
		t.Fatalf("attach: result=%+v err=%v", result, err)
	}
}

func TestMobileFlow_UpgradeSameFileUpgradeRestartJourney(t *testing.T) {
	ctx := context.Background()
	old, target, err := mobileUpgradeDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	server := newMobileUpgradeServerFixture(t, old.Contract, target.Contract)
	httpClient := &http.Client{Transport: server}
	token := func(context.Context) (string, error) { return "fixture-token", nil }
	user := "upgrade-user"
	mainPath := filepath.Join(t.TempDir(), "main-device.sqlite")
	secondPath := filepath.Join(t.TempDir(), "old-second-device.sqlite")

	freshPlan := func(id string) oversqlite.UpgradePlan {
		return oversqlite.UpgradePlan{PlanID: "fresh-" + id, Mode: oversqlite.UpgradeModeFresh, DatabaseID: id, ServiceID: "mobile-flow", TargetVersion: 1, TargetGenerationID: old.Contract.Proof.ID, LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator}
	}
	mainOld, _, err := openVersionedApplication(ctx, mainPath, "http://mobile.fixture", token, httpClient, freshPlan("main-device"), old)
	if err != nil {
		t.Fatal(err)
	}
	attachVersionedTestApp(t, mainOld, user)
	if _, err := mainOld.DB.Exec(`INSERT INTO upgrade_local_notes VALUES ('sentinel','survives')`); err != nil {
		t.Fatal(err)
	}
	if _, err := mainOld.Statements["insert"].Exec("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "offline-old"); err != nil {
		t.Fatal(err)
	}
	if err := mainOld.Close(); err != nil {
		t.Fatal(err)
	}

	secondOld, _, err := openVersionedApplication(ctx, secondPath, "http://mobile.fixture", token, httpClient, freshPlan("second-device"), old)
	if err != nil {
		t.Fatal(err)
	}
	attachVersionedTestApp(t, secondOld, user)
	if _, err := secondOld.Statements["insert"].Exec("7ba7b810-9dad-11d1-80b4-00c04fd430c8", "old-device-before"); err != nil {
		t.Fatal(err)
	}
	if _, err := secondOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	server.loseReplacementReply = true
	upgradePlan := oversqlite.UpgradePlan{
		PlanID: "mobile-generation-1-to-3", Mode: oversqlite.UpgradeModeGeneration,
		DatabaseID: "main-device", ServiceID: "mobile-flow", UserID: user,
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID,
		LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator,
	}
	mainNew, result, err := openVersionedApplication(ctx, mainPath, "http://mobile.fixture", token, httpClient, upgradePlan, target)
	if err != nil {
		t.Fatal(err)
	}
	defer mainNew.Close()
	attachVersionedTestApp(t, mainNew, user)
	if result.Status.Outcome != oversqlite.UpgradeOutcomeReady || result.Status.Phase != oversqlite.UpgradePhaseReady {
		t.Fatalf("upgrade result = %+v", result)
	}
	if len(server.replacements) != 2 || server.replacements[0] != server.replacements[1] {
		t.Fatalf("lost replacement reply did not replay exact tuple: %+v", server.replacements)
	}
	var name, label, note string
	if err := mainNew.DB.QueryRow(`SELECT name,new_label FROM upgrade_people WHERE id='6ba7b810-9dad-11d1-80b4-00c04fd430c8'`).Scan(&name, &label); err != nil {
		t.Fatal(err)
	}
	if name != "server-converted" || label != "new-only" {
		t.Fatalf("target hydration = %q/%q", name, label)
	}
	if err := mainNew.DB.QueryRow(`SELECT note FROM upgrade_local_notes WHERE id='sentinel'`).Scan(&note); err != nil || note != "survives" {
		t.Fatalf("local-only sentinel = %q err=%v", note, err)
	}
	if _, err := mainNew.Statements["insert"].Exec("8ba7b810-9dad-11d1-80b4-00c04fd430c8", "new-write", "target-only"); err != nil {
		t.Fatal(err)
	}
	if _, err := mainNew.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := secondOld.Statements["insert"].Exec("9ba7b810-9dad-11d1-80b4-00c04fd430c8", "old-device-after"); err != nil {
		t.Fatal(err)
	}
	if _, err := secondOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	defer secondOld.Close()

	var source, generation string
	var version, checkpoint, pending int64
	if err := mainNew.DB.QueryRow(`SELECT current_source_id,last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&source, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := mainNew.DB.QueryRow(`SELECT generation_id,local_version FROM _sync_upgrade_identity WHERE singleton_key=1`).Scan(&generation, &version); err != nil {
		t.Fatal(err)
	}
	if err := mainNew.DB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	t.Logf("upgrade journey path=%s source=%s generation=%s version=%d checkpoint=%d pending=%d restarts=reopen,reply-loss", mainPath, source, generation, version, checkpoint, pending)
}

func TestMobileFlow_UpgradeControlledFailureRestartJourney(t *testing.T) {
	ctx := context.Background()
	old, target, err := mobileUpgradeDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	baseMigrator := target.Migrator
	failMigration := true
	target.Migrator = mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, step oversqlite.LocalMigrationStep) error {
		if failMigration {
			return fmt.Errorf("controlled migration rollback")
		}
		return baseMigrator.Migrate(ctx, tx, step)
	}}
	server := newMobileUpgradeServerFixture(t, old.Contract, target.Contract)
	server.targetRows = append(server.targetRows, oversync.SnapshotRow{
		Schema: "main", Table: "upgrade_people",
		Key:        oversync.SyncKey{"id": "7ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		RowVersion: 1,
		Payload:    json.RawMessage(`{"id":"7ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"second","new_label":"second-new"}`),
	})
	httpClient := &http.Client{Transport: server}
	token := func(context.Context) (string, error) { return "fixture-token", nil }
	user := "restart-user"
	path := filepath.Join(t.TempDir(), "same-file-restart.sqlite")

	fresh := oversqlite.UpgradePlan{
		PlanID: "restart-fresh-v1", Mode: oversqlite.UpgradeModeFresh,
		DatabaseID: "restart-device", ServiceID: "mobile-flow",
		TargetVersion: 1, TargetGenerationID: old.Contract.Proof.ID, LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator,
	}
	oldApp, _, err := openVersionedApplication(ctx, path, "http://mobile.fixture", token, httpClient, fresh, old)
	if err != nil {
		t.Fatal(err)
	}
	attachVersionedTestApp(t, oldApp, user)
	if _, err := oldApp.DB.Exec(`INSERT INTO upgrade_local_notes VALUES ('sentinel','survives-restarts')`); err != nil {
		t.Fatal(err)
	}
	if _, err := oldApp.Statements["insert"].Exec("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "offline-old"); err != nil {
		t.Fatal(err)
	}
	if err := oldApp.Close(); err != nil {
		t.Fatal(err)
	}

	plan := oversqlite.UpgradePlan{
		PlanID: "restart-generation-1-to-3", Mode: oversqlite.UpgradeModeGeneration,
		DatabaseID: "restart-device", ServiceID: "mobile-flow", UserID: user,
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID,
		LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator,
	}
	server.rejectNextPushCreate = true
	resolutionOpening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
		DatabasePath: path, Plan: &plan, BaseURL: "http://mobile.fixture", Token: token, HTTP: httpClient,
		TargetClientConfig: versionedTargetConfig(plan, target),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolutionOpening.Advance(ctx)
	if err == nil || result.Status.Outcome != oversqlite.UpgradeOutcomeNeedsResolution || result.Status.Phase != oversqlite.UpgradePhaseResolvingOldIntent || result.Status.PendingRequestHash == "" {
		t.Fatalf("outbox-resolution pause result=%+v err=%v", result, err)
	}
	result, err = resolutionOpening.ResolvePendingIntent(ctx, oversqlite.PendingIntentResolution{
		Action: oversqlite.PendingIntentDiscard, PlanID: result.Status.PlanID,
		Revision: result.Status.Revision, RequestHash: result.Status.PendingRequestHash,
	})
	if err == nil || result.Status.Outcome != oversqlite.UpgradeOutcomeLocalMigrationFailed || result.Status.Phase != oversqlite.UpgradePhaseMigratingLocal {
		t.Fatalf("explicit resolution and migration rollback result=%+v err=%v", result, err)
	}
	if err := resolutionOpening.Close(); err != nil {
		t.Fatal(err)
	}
	diagnostic, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var version int64
	if err := diagnostic.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("rolled-back version=%d err=%v", version, err)
	}
	if err := diagnostic.Close(); err != nil {
		t.Fatal(err)
	}

	failMigration = false
	server.replacementReplyLosses = 3
	_, result, err = openVersionedApplication(ctx, path, "http://mobile.fixture", token, httpClient, plan, target)
	if err == nil || result.Status.Outcome != oversqlite.UpgradeOutcomeWaitingForNetwork || result.Status.Phase != oversqlite.UpgradePhaseReattachPending {
		t.Fatalf("replacement reply-loss result=%+v err=%v", result, err)
	}
	if len(server.replacements) != 3 || server.replacements[0] != server.replacements[1] || server.replacements[1] != server.replacements[2] {
		t.Fatalf("replacement retry tuple changed: %+v", server.replacements)
	}

	server.offlineSnapshotAfter = 1
	_, result, err = openVersionedApplication(ctx, path, "http://mobile.fixture", token, httpClient, plan, target)
	if err == nil || result.Status.Outcome != oversqlite.UpgradeOutcomeWaitingForNetwork || result.Status.Phase != oversqlite.UpgradePhaseRebuildRunning || result.Status.SnapshotRowsStaged != 1 {
		t.Fatalf("snapshot staging interruption result=%+v err=%v", result, err)
	}
	for _, replacement := range server.replacements[1:] {
		if replacement != server.replacements[0] {
			t.Fatalf("replacement tuple changed across reopen: %+v", server.replacements)
		}
	}

	server.offlineSnapshotAfter = -1
	ready, result, err := openVersionedApplication(ctx, path, "http://mobile.fixture", token, httpClient, plan, target)
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	if result.Status.Outcome != oversqlite.UpgradeOutcomeReady || result.Status.Phase != oversqlite.UpgradePhaseReady {
		t.Fatalf("final recovery result=%+v", result)
	}
	var rowCount int
	var note, source string
	if err := ready.DB.QueryRow(`SELECT COUNT(*) FROM upgrade_people`).Scan(&rowCount); err != nil || rowCount != 2 {
		t.Fatalf("hydrated row count=%d err=%v", rowCount, err)
	}
	if err := ready.DB.QueryRow(`SELECT note FROM upgrade_local_notes WHERE id='sentinel'`).Scan(&note); err != nil || note != "survives-restarts" {
		t.Fatalf("local-only sentinel=%q err=%v", note, err)
	}
	if err := ready.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&source); err != nil || source == "" || source == "source-old" {
		t.Fatalf("rotated source=%q err=%v", source, err)
	}
	t.Logf("upgrade controlled journey path=%s source=%s generation=%d version=%d restarts=outbox,migration-rollback,migration-commit,replacement-reply-loss,snapshot-stage,final-ready", path, source, target.Contract.Proof.ID, target.LocalVersion)
}

type mobileUpgradeExitAfterReplacement struct{}

func (mobileUpgradeExitAfterReplacement) RoundTrip(request *http.Request) (*http.Response, error) {
	isReplacement := false
	if request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions" {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		var create oversync.SnapshotSessionCreateRequest
		if err := json.Unmarshal(body, &create); err != nil {
			return nil, err
		}
		isReplacement = create.SourceReplacement != nil
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err == nil && response.StatusCode == http.StatusOK && isReplacement {
		os.Exit(96)
	}
	return response, err
}

func TestMobileFlow_UpgradeProductionProcessHelper(t *testing.T) {
	if os.Getenv(mobileUpgradeProcessHelperEnv) != "1" {
		return
	}
	old, target, err := mobileUpgradeDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	user := os.Getenv(mobileUpgradeProcessUserEnv)
	plan := oversqlite.UpgradePlan{
		PlanID: "mobile-production-generation-1-to-3", Mode: oversqlite.UpgradeModeGeneration,
		DatabaseID: "mobile-production-main", ServiceID: "mobile-flow", UserID: user,
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID,
		LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator,
	}
	token := os.Getenv(mobileUpgradeProcessTokenEnv)
	if os.Getenv(mobileUpgradeProcessBoundaryEnv) == "after_final_stage" {
		opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
			DatabasePath: os.Getenv(mobileUpgradeProcessDBEnv), Plan: &plan, BaseURL: os.Getenv(mobileUpgradeProcessURLEnv),
			Token: func(context.Context) (string, error) { return token, nil }, HTTP: http.DefaultClient,
			TargetClientConfig: versionedTargetConfig(plan, target),
			Progress: func(status oversqlite.OpeningStatus) {
				if status.Phase == oversqlite.UpgradePhaseRebuildRunning && status.SnapshotRowCount > 0 && status.SnapshotRowsStaged == status.SnapshotRowCount {
					os.Exit(97)
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := opening.Advance(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Fatal("final-stage boundary was not reached")
	}
	_, _, err = openVersionedApplication(
		context.Background(), os.Getenv(mobileUpgradeProcessDBEnv), os.Getenv(mobileUpgradeProcessURLEnv),
		func(context.Context) (string, error) { return token, nil },
		&http.Client{Transport: mobileUpgradeExitAfterReplacement{}}, plan, target,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMobileFlow_UpgradeProductionServerSameFileProcessDeathJourney(t *testing.T) {
	ctx := context.Background()
	old, target, err := mobileUpgradeDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	databaseURL := createMobileUpgradeDatabase(t)
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		DatabaseURL: databaseURL, JWTSecret: "mobile-upgrade-production-secret",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})),
		AppName: "mobile-upgrade-production", BusinessSchema: "main",
		RegisteredTables:  old.Contract.RegisteredTables,
		SchemaGenerations: []oversync.SchemaGeneration{old.Contract.Generation, target.Contract.Generation},
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			_, err := pool.Exec(ctx, `
				CREATE TABLE main.upgrade_people (
					_sync_scope_id TEXT NOT NULL,
					id UUID NOT NULL,
					legacy_name TEXT NOT NULL DEFAULT '',
					name TEXT NOT NULL DEFAULT 'server-default',
					new_label TEXT NOT NULL DEFAULT 'target-only',
					PRIMARY KEY (_sync_scope_id,id)
				)
			`)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	user := "mobile-production-" + uuid.NewString()
	token, err := server.GenerateToken(user, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	provider := func(context.Context) (string, error) { return token, nil }
	mainPath := filepath.Join(t.TempDir(), "same-mobile-file.sqlite")
	secondPath := filepath.Join(t.TempDir(), "independent-old-device.sqlite")
	freshPlan := func(id string) oversqlite.UpgradePlan {
		return oversqlite.UpgradePlan{
			PlanID: "mobile-production-fresh-" + id, Mode: oversqlite.UpgradeModeFresh,
			DatabaseID: id, ServiceID: "mobile-flow", TargetVersion: 1, TargetGenerationID: old.Contract.Proof.ID,
			LocalOnlyTables: old.LocalOnlyTables, Migrator: old.Migrator,
		}
	}
	mainOld, _, err := openVersionedApplication(ctx, mainPath, server.URL(), provider, http.DefaultClient, freshPlan("mobile-production-main"), old)
	if err != nil {
		t.Fatal(err)
	}
	attachVersionedTestApp(t, mainOld, user)
	secondOld, _, err := openVersionedApplication(ctx, secondPath, server.URL(), provider, http.DefaultClient, freshPlan("mobile-production-second"), old)
	if err != nil {
		t.Fatal(err)
	}
	defer secondOld.Close()
	attachVersionedTestApp(t, secondOld, user)
	if _, err := mainOld.DB.Exec(`INSERT INTO upgrade_local_notes VALUES ('sentinel','production-preserved')`); err != nil {
		t.Fatal(err)
	}
	preUpgradeRow := "5ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if _, err := mainOld.Statements["insert"].Exec(preUpgradeRow, "visible-before-upgrade"); err != nil {
		t.Fatal(err)
	}
	if _, err := mainOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := secondOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	var preUpgradeName string
	if err := secondOld.DB.QueryRow(`SELECT legacy_name FROM upgrade_people WHERE id=?`, preUpgradeRow).Scan(&preUpgradeName); err != nil || preUpgradeName != "visible-before-upgrade" {
		t.Fatalf("old device pre-upgrade synchronization=%q err=%v", preUpgradeName, err)
	}
	mainRow := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if _, err := mainOld.Statements["insert"].Exec(mainRow, "old-offline-value"); err != nil {
		t.Fatal(err)
	}
	var oldSource string
	if err := mainOld.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&oldSource); err != nil {
		t.Fatal(err)
	}
	if err := mainOld.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestMobileFlow_UpgradeProductionProcessHelper$")
	command.Env = append(os.Environ(),
		mobileUpgradeProcessHelperEnv+"=1", mobileUpgradeProcessDBEnv+"="+mainPath,
		mobileUpgradeProcessURLEnv+"="+server.URL(), mobileUpgradeProcessTokenEnv+"="+token,
		mobileUpgradeProcessUserEnv+"="+user,
		mobileUpgradeProcessBoundaryEnv+"=after_replacement",
	)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 96 {
		t.Fatalf("upgrade subprocess exit=%v output=%s", err, output)
	}

	diagnostic, err := sql.Open("sqlite3", mainPath)
	if err != nil {
		t.Fatal(err)
	}
	var phase, newSource string
	if err := diagnostic.QueryRow(`SELECT phase,new_source_id FROM _sync_upgrade_journal WHERE singleton_key=1`).Scan(&phase, &newSource); err != nil {
		t.Fatal(err)
	}
	if phase != string(oversqlite.UpgradePhaseReattachPending) || newSource == "" {
		t.Fatalf("post-death journal phase=%s new_source=%s", phase, newSource)
	}
	if err := diagnostic.Close(); err != nil {
		t.Fatal(err)
	}
	var oldState, replacedBy string
	if err := server.Pool.QueryRow(ctx, `
		SELECT source.state,source.replaced_by_source_id
		FROM sync.user_state users
		JOIN sync.source_state source USING (user_pk)
		WHERE users.user_id=$1 AND source.source_id=$2
	`, user, oldSource).Scan(&oldState, &replacedBy); err != nil {
		t.Fatal(err)
	}
	if oldState != "retired" || replacedBy != newSource {
		t.Fatalf("durable server replacement old_state=%s replaced_by=%s want=%s", oldState, replacedBy, newSource)
	}

	finalStageCommand := exec.Command(os.Args[0], "-test.run=^TestMobileFlow_UpgradeProductionProcessHelper$")
	finalStageCommand.Env = append(os.Environ(),
		mobileUpgradeProcessHelperEnv+"=1", mobileUpgradeProcessDBEnv+"="+mainPath,
		mobileUpgradeProcessURLEnv+"="+server.URL(), mobileUpgradeProcessTokenEnv+"="+token,
		mobileUpgradeProcessUserEnv+"="+user, mobileUpgradeProcessBoundaryEnv+"=after_final_stage",
	)
	output, err = finalStageCommand.CombinedOutput()
	if !errors.As(err, &exit) || exit.ExitCode() != 97 {
		t.Fatalf("final-stage subprocess exit=%v output=%s", err, output)
	}
	diagnostic, err = sql.Open("sqlite3", mainPath)
	if err != nil {
		t.Fatal(err)
	}
	var stagedRows, snapshotRows int64
	if err := diagnostic.QueryRow(`SELECT phase,snapshot_rows_staged,snapshot_row_count FROM _sync_upgrade_journal WHERE singleton_key=1`).Scan(&phase, &stagedRows, &snapshotRows); err != nil {
		t.Fatal(err)
	}
	if phase != string(oversqlite.UpgradePhaseRebuildRunning) || snapshotRows == 0 || stagedRows != snapshotRows {
		t.Fatalf("post-final-stage-death phase=%s staged=%d total=%d", phase, stagedRows, snapshotRows)
	}
	if err := diagnostic.Close(); err != nil {
		t.Fatal(err)
	}

	upgradePlan := oversqlite.UpgradePlan{
		PlanID: "mobile-production-generation-1-to-3", Mode: oversqlite.UpgradeModeGeneration,
		DatabaseID: "mobile-production-main", ServiceID: "mobile-flow", UserID: user,
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: old.Contract.Proof.ID, TargetGenerationID: target.Contract.Proof.ID,
		LocalOnlyTables: target.LocalOnlyTables, Migrator: target.Migrator,
	}
	mainNew, result, err := openVersionedApplication(ctx, mainPath, server.URL(), provider, http.DefaultClient, upgradePlan, target)
	if err != nil {
		t.Fatal(err)
	}
	defer mainNew.Close()
	if result.Status.Outcome != oversqlite.UpgradeOutcomeReady || result.Ready == nil {
		t.Fatalf("recovered upgrade result=%+v", result)
	}
	attachVersionedTestApp(t, mainNew, user)
	var name, label, note string
	if err := mainNew.DB.QueryRow(`SELECT name,new_label FROM upgrade_people WHERE id=?`, mainRow).Scan(&name, &label); err != nil {
		t.Fatal(err)
	}
	if name != "server-default" || label != "target-only" {
		t.Fatalf("production target snapshot=%q/%q", name, label)
	}
	if err := mainNew.DB.QueryRow(`SELECT note FROM upgrade_local_notes WHERE id='sentinel'`).Scan(&note); err != nil || note != "production-preserved" {
		t.Fatalf("production local-only sentinel=%q err=%v", note, err)
	}

	oldAfterRow := "7ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if _, err := secondOld.Statements["insert"].Exec(oldAfterRow, "independent-old-write"); err != nil {
		t.Fatal(err)
	}
	if _, err := secondOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	targetAfterRow := "8ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if _, err := mainNew.Statements["insert"].Exec(targetAfterRow, "target-write", "target-label"); err != nil {
		t.Fatal(err)
	}
	if _, err := mainNew.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := mainNew.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mainNew.DB.QueryRow(`SELECT name FROM upgrade_people WHERE id=?`, oldAfterRow).Scan(&name); err != nil || name != "server-default" {
		t.Fatalf("target client did not pull post-upgrade old-device write name=%q err=%v", name, err)
	}
	if _, err := secondOld.Client.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	var projectedLegacyName string
	if err := secondOld.DB.QueryRow(`SELECT legacy_name FROM upgrade_people WHERE id=?`, targetAfterRow).Scan(&projectedLegacyName); err != nil {
		t.Fatalf("old device did not pull post-upgrade target write: %v", err)
	}
	if observationPath := os.Getenv("OVERSYNC_UPGRADE_MOBILE_OBSERVATION"); observationPath != "" {
		var secondSource string
		if err := secondOld.DB.QueryRow(`SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&secondSource); err != nil {
			t.Fatal(err)
		}
		observation := map[string]any{
			"device_a":            "new_ready",
			"device_b":            "old_ready",
			"independent_sources": newSource != "" && secondSource != "" && newSource != secondSource,
			"same_file":           result.Status.Phase == oversqlite.UpgradePhaseReady && note == "production-preserved",
			"local_only":          note == "production-preserved",
		}
		encoded, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(observationPath, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("production upgrade journey path=%s old_source=%s new_source=%s process_death=after_remote_replacement,after_final_stage pre_upgrade_old_sync=verified post_upgrade_both_generations=verified", mainPath, oldSource, newSource)
}

func createMobileUpgradeDatabase(t *testing.T) string {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		base = "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
	}
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	admin := *parsed
	admin.Path = "/postgres"
	name := "mobile_upgrade_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	isolated := *parsed
	isolated.Path = "/" + name
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, admin.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		_ = connection.Close(ctx)
		t.Fatal(err)
	}
	_ = connection.Close(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanup, err := pgx.Connect(cleanupCtx, admin.String())
		if err != nil {
			t.Errorf("connect for mobile upgrade database cleanup: %v", err)
			return
		}
		defer cleanup.Close(cleanupCtx)
		if _, err := cleanup.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop mobile upgrade database: %v", err)
		}
	})
	return isolated.String()
}
