package oversync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"net/http"
	"strconv"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/internal/sessionid"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const ProtocolVersionHeader = "Oversync-Protocol-Version"
const OutcomeControlBodyLimit int64 = 4096

// ValidateProtocolHeader checks exact values, including duplicate differently-cased keys.
func ValidateProtocolHeader(header http.Header) error {
	values := exactHeaderValues(header, ProtocolVersionHeader)
	if len(values) != 1 || values[0] != SyncProtocolVersion {
		return fmt.Errorf("protocol_version_mismatch")
	}
	return nil
}

type PushOutcome struct {
	SourceID              string                `json:"source_id"`
	GenerationID          string                `json:"schema_generation_id"`
	GenerationFingerprint string                `json:"schema_generation_fingerprint"`
	SourceBundleID        int64                 `json:"source_bundle_id"`
	CanonicalRequestHash  string                `json:"canonical_request_hash"`
	InitializationID      *string               `json:"initialization_id"`
	Status                string                `json:"status"`
	OutcomeHash           string                `json:"outcome_hash"`
	Committed             *CommittedPushEffect  `json:"committed,omitempty"`
	Rejection             *PushOutcomeRejection `json:"rejection,omitempty"`
}
type CommittedPushEffect struct {
	BundleSeq  int64             `json:"bundle_seq"`
	BundleHash string            `json:"bundle_hash"`
	RowCount   int64             `json:"row_count"`
	Rows       []CompactRowGroup `json:"rows"`
}
type PushOutcomeRejection struct {
	Code     string               `json:"code"`
	Conflict *PushConflictDetails `json:"conflict,omitempty"`
}
type PushOutcomeResolveRequest struct {
	SourceBundleID       int64   `json:"source_bundle_id"`
	CanonicalRequestHash string  `json:"canonical_request_hash"`
	InitializationID     *string `json:"initialization_id"`
}
type PushOutcomeAckRequest struct {
	SourceBundleID       int64  `json:"source_bundle_id"`
	CanonicalRequestHash string `json:"canonical_request_hash"`
	OutcomeHash          string `json:"outcome_hash"`
}
type SnapshotRecovery struct {
	MinimumBundleSeq int64 `json:"minimum_bundle_seq"`
}

// decodeRecoveryObject preserves required-nullable presence and rejects duplicates,
// folded names, unknown fields and nulls in every other closed member.
func decodeRecoveryObject(raw []byte, required, optional []string, nullable string, target any) error {
	if err := jcs.ValidateStructure(raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("expected closed object")
	}
	allowed := map[string]bool{}
	for _, name := range append(append([]string{}, required...), optional...) {
		allowed[name] = true
	}
	for name, value := range fields {
		if !allowed[name] {
			return fmt.Errorf("unknown member %q", name)
		}
		if isRecoveryIntegerMember(name) && !canonicalRecoveryInteger(value) {
			return fmt.Errorf("noncanonical integer %s", name)
		}
		if name != nullable && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("null member %s", name)
		}
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("missing member %s", name)
		}
	}
	return json.Unmarshal(raw, target)
}
func validOperationTuple(sequence int64, requestHash string, initialization *string) error {
	if sequence < 1 || !isLowerHexFingerprint(requestHash) {
		return fmt.Errorf("invalid operation tuple")
	}
	if initialization != nil {
		if err := sessionid.Validate(*initialization); err != nil {
			return err
		}
	}
	return nil
}
func (v *PushOutcomeResolveRequest) UnmarshalJSON(raw []byte) error {
	type plain PushOutcomeResolveRequest
	var p plain
	if err := decodeRecoveryObject(raw, []string{"source_bundle_id", "canonical_request_hash", "initialization_id"}, nil, "initialization_id", &p); err != nil {
		return err
	}
	if err := validOperationTuple(p.SourceBundleID, p.CanonicalRequestHash, p.InitializationID); err != nil {
		return err
	}
	*v = PushOutcomeResolveRequest(p)
	return nil
}
func (v *PushOutcomeAckRequest) UnmarshalJSON(raw []byte) error {
	type plain PushOutcomeAckRequest
	var p plain
	if err := decodeRecoveryObject(raw, []string{"source_bundle_id", "canonical_request_hash", "outcome_hash"}, nil, "", &p); err != nil {
		return err
	}
	if err := validOperationTuple(p.SourceBundleID, p.CanonicalRequestHash, nil); err != nil {
		return err
	}
	if !isLowerHexFingerprint(p.OutcomeHash) {
		return fmt.Errorf("invalid outcome hash")
	}
	*v = PushOutcomeAckRequest(p)
	return nil
}
func (v *SnapshotRecovery) UnmarshalJSON(raw []byte) error {
	type plain SnapshotRecovery
	var p plain
	if err := decodeRecoveryObject(raw, []string{"minimum_bundle_seq"}, nil, "", &p); err != nil {
		return err
	}
	if p.MinimumBundleSeq < 0 {
		return fmt.Errorf("negative recovery floor")
	}
	*v = SnapshotRecovery(p)
	return nil
}
func (v *CommittedPushEffect) UnmarshalJSON(raw []byte) error {
	type plain CommittedPushEffect
	var p plain
	if err := decodeRecoveryObject(raw, []string{"bundle_seq", "bundle_hash", "row_count", "rows"}, nil, "", &p); err != nil {
		return err
	}
	if p.BundleSeq < 1 || p.RowCount < 1 || !isLowerHexFingerprint(p.BundleHash) || len(p.Rows) == 0 {
		return fmt.Errorf("invalid committed effect")
	}
	*v = CommittedPushEffect(p)
	return nil
}
func (v *PushOutcomeRejection) UnmarshalJSON(raw []byte) error {
	type plain PushOutcomeRejection
	var p plain
	if err := decodeRecoveryObject(raw, []string{"code"}, []string{"conflict"}, "", &p); err != nil {
		return err
	}
	if !validRejectionCode(p.Code) || (p.Code == "push_conflict") != (p.Conflict != nil) {
		return fmt.Errorf("invalid rejection union")
	}
	if p.Conflict != nil {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		var c map[string]json.RawMessage
		if err := decodeRecoveryObject(fields["conflict"], []string{"schema", "table", "key", "op", "base_row_version", "server_row_version", "server_row_deleted", "server_row"}, nil, "server_row", &c); err != nil {
			return err
		}
		if p.Conflict.BaseRowVersion < 0 || p.Conflict.ServerRowVersion < 0 || p.Conflict.Schema == "" || p.Conflict.Table == "" || len(p.Conflict.Key) == 0 {
			return fmt.Errorf("invalid conflict")
		}
		if p.Conflict.Op != "i" && p.Conflict.Op != "u" && p.Conflict.Op != "d" {
			return fmt.Errorf("invalid conflict operation")
		}
		if !bytes.Equal(bytes.TrimSpace(p.Conflict.ServerRow), []byte("null")) {
			if _, err := jcs.DecodeObjectLossless(p.Conflict.ServerRow); err != nil {
				return err
			}
		}
	}
	*v = PushOutcomeRejection(p)
	return nil
}
func validRejectionCode(code string) bool {
	if len(code) < 1 || len(code) > 64 || code[0] < 'a' || code[0] > 'z' {
		return false
	}
	for _, c := range code {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}
func (v *PushOutcome) UnmarshalJSON(raw []byte) error {
	type plain PushOutcome
	var p plain
	if err := decodeRecoveryObject(raw, []string{"source_id", "schema_generation_id", "schema_generation_fingerprint", "source_bundle_id", "canonical_request_hash", "initialization_id", "status", "outcome_hash"}, []string{"committed", "rejection"}, "initialization_id", &p); err != nil {
		return err
	}
	if err := validOperationTuple(p.SourceBundleID, p.CanonicalRequestHash, p.InitializationID); err != nil {
		return err
	}
	if err := sourceid.Validate(p.SourceID); err != nil {
		return err
	}
	if _, err := parseSchemaGenerationID(p.GenerationID); err != nil {
		return err
	}
	if !isLowerHexFingerprint(p.GenerationFingerprint) || !isLowerHexFingerprint(p.OutcomeHash) {
		return fmt.Errorf("invalid outcome binding hash")
	}
	if p.Status == "committed" && p.Committed != nil && p.Rejection == nil || p.Status == "rejected" && p.Rejection != nil && p.Committed == nil || p.Status == "cancelled" && p.Committed == nil && p.Rejection == nil {
		*v = PushOutcome(p)
		return nil
	}
	return fmt.Errorf("invalid outcome union")
}

// HashPushOutcome expands rows under the pinned generation, verifies complete
// row images/count/bundle hash and hashes the logical terminal union.
func HashPushOutcome(outcome PushOutcome, generation SchemaGenerationCapabilities) (string, error) {
	if outcome.GenerationID != generation.ID || outcome.GenerationFingerprint != generation.Fingerprint {
		return "", fmt.Errorf("outcome generation mismatch")
	}
	var committed, rejection any
	if outcome.Committed != nil {
		var err error
		committed, err = committedOutcomeHashValue(outcome.Committed, generation)
		if err != nil {
			return "", err
		}
	}
	if outcome.Rejection != nil {
		var conflict any
		if c := outcome.Rejection.Conflict; c != nil {
			body, err := json.Marshal(c)
			if err != nil {
				return "", err
			}
			obj, err := jcs.DecodeObjectLossless(body)
			if err != nil {
				return "", err
			}
			obj["base_row_version"] = strconv.FormatInt(c.BaseRowVersion, 10)
			obj["server_row_version"] = strconv.FormatInt(c.ServerRowVersion, 10)
			conflict = obj
		}
		rejection = []any{outcome.Rejection.Code, conflict}
	}
	var initialization any
	if outcome.InitializationID != nil {
		initialization = *outcome.InitializationID
	}
	return recoveryHash([]any{"oversqlite.push-outcome.v1", outcome.SourceID, outcome.GenerationID, outcome.GenerationFingerprint, strconv.FormatInt(outcome.SourceBundleID, 10), outcome.CanonicalRequestHash, initialization, outcome.Status, committed, rejection})
}
func validateOutcomePayload(generation SchemaGenerationCapabilities, row PushRequestRow, payload any) error {
	object, ok := payload.(map[string]any)
	if !ok {
		return fmt.Errorf("effect payload must be object")
	}
	table, err := outcomeTable(generation, row.Schema, row.Table)
	if err != nil {
		return err
	}
	if len(object) != len(table.Columns) {
		return fmt.Errorf("incomplete effect payload")
	}
	for _, column := range table.Columns {
		value, present := object[column.Name]
		if !present {
			return fmt.Errorf("missing effect column %s", column.Name)
		}
		if err := validateGenerationWireValue(GenerationColumn{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable}, value); err != nil {
			return err
		}
	}
	for _, key := range table.SyncKeyColumns {
		payloadKey, err := jcs.Marshal(object[key])
		if err != nil {
			return err
		}
		tupleKey, err := jcs.Marshal(row.Key[key])
		if err != nil {
			return err
		}
		if !bytes.Equal(payloadKey, tupleKey) {
			return fmt.Errorf("effect key mismatch")
		}
	}
	return nil
}

func recoveryHash(value any) (string, error) {
	body, err := jcs.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// SnapshotHashWriter owns only one frame at a time; page/chunk boundaries do not
// enter the digest. LogicalByteCount remains the existing row-accounting value.
type SnapshotHashWriter struct {
	digest       hash.Hash
	finished     bool
	rows         int64
	logicalBytes int64
	generation   SchemaGenerationCapabilities
}

func NewSnapshotHashWriter(sourceID string, generation SchemaGenerationCapabilities, snapshotID string, boundary int64, recovery *SnapshotRecovery) (*SnapshotHashWriter, error) {
	if sourceID == "" || snapshotID == "" || boundary < 0 || recovery != nil && (recovery.MinimumBundleSeq < 0 || boundary < recovery.MinimumBundleSeq) {
		return nil, fmt.Errorf("snapshot below recovery floor")
	}
	var echo any
	if recovery != nil {
		echo = map[string]any{"minimum_bundle_seq": strconv.FormatInt(recovery.MinimumBundleSeq, 10)}
	}
	h := &SnapshotHashWriter{digest: sha256.New(), generation: generation}
	err := h.frame([]any{"oversqlite.snapshot.v1", sourceID, generation.ID, generation.Fingerprint, snapshotID, strconv.FormatInt(boundary, 10), echo})
	return h, err
}
func (h *SnapshotHashWriter) frame(value any) error {
	if h.finished {
		return fmt.Errorf("snapshot hash already finalized")
	}
	body, err := jcs.Marshal(value)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(body)))
	_, _ = h.digest.Write(length[:])
	_, _ = h.digest.Write(body)
	return nil
}
func (h *SnapshotHashWriter) Add(tableIndex int, key []any, version int64, payload json.RawMessage) error {
	if tableIndex < 0 || tableIndex >= len(h.generation.Tables) || version < 1 {
		return fmt.Errorf("invalid snapshot row")
	}
	object, err := jcs.DecodeObjectLossless(payload)
	if err != nil {
		return err
	}
	table := h.generation.Tables[tableIndex]
	if len(key) != len(table.SyncKeyColumns) {
		return fmt.Errorf("snapshot key arity")
	}
	wireKey := SyncKey{}
	for i, name := range table.SyncKeyColumns {
		wireKey[name] = key[i]
	}
	row := PushRequestRow{Schema: table.Schema, Table: table.Table, Key: wireKey, Op: OpInsert, Payload: payload}
	if err := validateOutcomeKey(h.generation, row); err != nil {
		return err
	}
	if err := validateOutcomePayload(h.generation, row, object); err != nil {
		return err
	}
	logical, err := jcs.Marshal([]any{"i", tableIndex, key, version, object})
	if err != nil {
		return err
	}
	if h.rows == math.MaxInt64 || h.logicalBytes > math.MaxInt64-int64(len(logical)) {
		return fmt.Errorf("snapshot totals overflow")
	}
	if err := h.frame([]any{"i", strconv.Itoa(tableIndex), key, strconv.FormatInt(version, 10), object}); err != nil {
		return err
	}
	h.rows++
	h.logicalBytes += int64(len(logical))
	return nil
}
func (h *SnapshotHashWriter) Sum(rowCount, logicalByteCount int64) (string, error) {
	if rowCount != h.rows || logicalByteCount != h.logicalBytes || rowCount < 0 || logicalByteCount < 0 {
		return "", fmt.Errorf("negative snapshot totals")
	}
	if err := h.frame([]any{"end", strconv.FormatInt(rowCount, 10), strconv.FormatInt(logicalByteCount, 10)}); err != nil {
		return "", err
	}
	h.finished = true
	return hex.EncodeToString(h.digest.Sum(nil)), nil
}

func (v *CompactSnapshotSessionResponse) UnmarshalJSON(raw []byte) error {
	type plain CompactSnapshotSessionResponse
	var p plain
	if err := decodeRecoveryObject(raw, []string{"snapshot_id", "snapshot_bundle_seq", "row_count", "logical_byte_count", "expires_at", "snapshot_hash"}, []string{"recovery"}, "", &p); err != nil {
		return err
	}
	if p.SnapshotID == "" || p.SnapshotBundleSeq < 0 || p.RowCount < 0 || p.LogicalByteCount < 0 || !isLowerHexFingerprint(p.SnapshotHash) {
		return fmt.Errorf("invalid snapshot metadata")
	}
	if p.Recovery != nil && p.SnapshotBundleSeq < p.Recovery.MinimumBundleSeq {
		return fmt.Errorf("snapshot below echo floor")
	}
	*v = CompactSnapshotSessionResponse(p)
	return nil
}

func ValidateSnapshotRecoveryEcho(metadata CompactSnapshotSessionResponse, requested *SnapshotRecovery) error {
	if (metadata.Recovery == nil) != (requested == nil) {
		return fmt.Errorf("missing or unsolicited recovery echo")
	}
	if requested != nil && (metadata.Recovery.MinimumBundleSeq != requested.MinimumBundleSeq || metadata.SnapshotBundleSeq < requested.MinimumBundleSeq) {
		return fmt.Errorf("snapshot recovery echo/floor mismatch")
	}
	return nil
}

// PushOperationBinding is the immutable tuple retained with a frozen request.
// Scope/authority/attachment admission remains the calling lifecycle owner's job.
type PushOperationBinding struct {
	SourceID              string  `json:"source_id"`
	GenerationID          string  `json:"schema_generation_id"`
	GenerationFingerprint string  `json:"schema_generation_fingerprint"`
	SourceBundleID        int64   `json:"source_bundle_id"`
	CanonicalRequestHash  string  `json:"canonical_request_hash"`
	InitializationID      *string `json:"initialization_id"`
}

func ValidatePushOutcomeBinding(outcome PushOutcome, binding PushOperationBinding, generation SchemaGenerationCapabilities) error {
	if outcome.SourceID != binding.SourceID || outcome.GenerationID != binding.GenerationID || outcome.GenerationFingerprint != binding.GenerationFingerprint || outcome.SourceBundleID != binding.SourceBundleID || outcome.CanonicalRequestHash != binding.CanonicalRequestHash {
		return fmt.Errorf("outcome operation binding mismatch")
	}
	if (outcome.InitializationID == nil) != (binding.InitializationID == nil) {
		return fmt.Errorf("outcome initialization presence mismatch")
	}
	if outcome.InitializationID != nil && *outcome.InitializationID != *binding.InitializationID {
		return fmt.Errorf("outcome initialization value mismatch")
	}
	actual, err := HashPushOutcome(outcome, generation)
	if err != nil {
		return err
	}
	if actual != outcome.OutcomeHash {
		return fmt.Errorf("outcome hash mismatch")
	}
	return nil
}

func committedOutcomeHashValue(effect *CommittedPushEffect, generation SchemaGenerationCapabilities) (any, error) {
	rows, err := ExpandCompactPushRows(generation, effect.Rows)
	if err != nil {
		return nil, err
	}
	if int64(len(rows)) != effect.RowCount {
		return nil, fmt.Errorf("effect row count mismatch")
	}
	logical := make([]any, 0, len(rows))
	bundleRows := make([]protocolhash.BundleRow, 0, len(rows))
	for _, r := range rows {
		if err := validateOutcomeKey(generation, r); err != nil {
			return nil, err
		}
		if r.BaseRowVersion != effect.BundleSeq {
			return nil, fmt.Errorf("effect version mismatch")
		}
		var payload any
		if r.Op != OpDelete {
			payload, err = jcs.DecodeObjectLossless(r.Payload)
			if err != nil {
				return nil, err
			}
			if err = validateOutcomePayload(generation, r, payload); err != nil {
				return nil, err
			}
		}
		op := map[string]string{OpInsert: "I", OpUpdate: "U", OpDelete: "D"}[r.Op]
		logical = append(logical, []any{r.Schema, r.Table, r.Key, op, strconv.FormatInt(r.BaseRowVersion, 10), payload})
		bundleRows = append(bundleRows, protocolhash.BundleRow{Schema: r.Schema, Table: r.Table, Key: r.Key, Op: r.Op, RowVersion: r.BaseRowVersion, Payload: r.Payload})
	}
	actual, _, err := protocolhash.CommittedBundle(bundleRows)
	if err != nil {
		return nil, err
	}
	if actual != effect.BundleHash {
		return nil, fmt.Errorf("effect bundle hash mismatch")
	}
	return []any{strconv.FormatInt(effect.BundleSeq, 10), effect.BundleHash, strconv.FormatInt(effect.RowCount, 10), logical}, nil
}

func outcomeTable(generation SchemaGenerationCapabilities, schema, table string) (SchemaGenerationTableCapabilities, error) {
	for _, candidate := range generation.Tables {
		if candidate.Schema == schema && candidate.Table == table {
			return candidate, nil
		}
	}
	return SchemaGenerationTableCapabilities{}, fmt.Errorf("unknown outcome table")
}
func validateOutcomeKey(generation SchemaGenerationCapabilities, row PushRequestRow) error {
	table, err := outcomeTable(generation, row.Schema, row.Table)
	if err != nil {
		return err
	}
	columns := map[string]SchemaGenerationColumnCapabilities{}
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	for _, name := range table.SyncKeyColumns {
		column, present := columns[name]
		if !present {
			return fmt.Errorf("unknown outcome key column")
		}
		if err := validateGenerationWireValue(GenerationColumn{Name: column.Name, WireType: column.WireType, Nullable: false}, row.Key[name]); err != nil {
			return err
		}
	}
	return nil
}

func isRecoveryIntegerMember(name string) bool {
	switch name {
	case "source_bundle_id", "bundle_seq", "row_count", "minimum_bundle_seq", "snapshot_bundle_seq", "logical_byte_count", "base_row_version", "server_row_version":
		return true
	}
	return false
}
func canonicalRecoveryInteger(raw []byte) bool {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 {
		return false
	}
	if value[0] == '0' {
		return len(value) == 1
	}
	if value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, c := range value[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(string(value), 10, 64)
	return err == nil
}
