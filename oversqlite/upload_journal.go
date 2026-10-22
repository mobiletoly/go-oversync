package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

// This is the existing outbox's exact wire/evidence owner. Original bases and
// capture guards stay in _sync_outbox_rows; derived replay remains shared.
const uploadJournalDDL = `CREATE TABLE IF NOT EXISTS _sync_upload_journal (
 singleton_key INTEGER NOT NULL PRIMARY KEY CHECK(singleton_key=1),
 binding TEXT NOT NULL CHECK(json_valid(binding) AND json_type(binding)='object'),
 request_body BLOB NOT NULL CHECK(length(request_body)>0),
 request_digest TEXT NOT NULL CHECK(length(request_digest)=64),
 outcome_body BLOB NOT NULL DEFAULT X'',
 outcome_hash TEXT NOT NULL DEFAULT '',
 phase TEXT NOT NULL DEFAULT 'frozen' CHECK(phase IN ('frozen','ack_pending')),
 CHECK((length(outcome_body)=0 AND outcome_hash='' AND phase='frozen') OR
       (length(outcome_body)>0 AND length(outcome_hash)=64))
)`

type uploadBinding struct {
	ScopeID         string                        `json:"scope_id"`
	Authority       string                        `json:"authority"`
	AttachmentEpoch int64                         `json:"attachment_epoch"`
	Operation       oversync.PushOperationBinding `json:"operation"`
}

type uploadJournal struct {
	Binding                           uploadBinding
	Phase, OutcomeHash, RequestDigest string
	RequestBytes, OutcomeBytes        int64
}

// UploadJournalIntegrityError never permits replacement of missing/corrupt F/C.
type UploadJournalIntegrityError struct{ Reason string }

func (e *UploadJournalIntegrityError) Error() string { return "upload journal integrity: " + e.Reason }

func loadUploadJournal(ctx context.Context, q queryRower) (*uploadJournal, error) {
	j := &uploadJournal{}
	var binding string
	err := q.QueryRowContext(ctx, `SELECT binding,phase,outcome_hash,request_digest,length(request_body),length(outcome_body) FROM _sync_upload_journal WHERE singleton_key=1`).Scan(&binding, &j.Phase, &j.OutcomeHash, &j.RequestDigest, &j.RequestBytes, &j.OutcomeBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(binding) > 4096 || jcs.ValidateStructure([]byte(binding)) != nil {
		return nil, &UploadJournalIntegrityError{"invalid binding"}
	}
	decoder := json.NewDecoder(strings.NewReader(binding))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&j.Binding); err != nil {
		return nil, &UploadJournalIntegrityError{"invalid binding"}
	}
	o := j.Binding.Operation
	if j.RequestBytes < 1 || j.RequestBytes > maxCommittedPushBundleBytes || j.OutcomeBytes > maxCommittedPushBundleBytes+8192 ||
		!protocolhash.IsCanonicalHash(j.RequestDigest) || !protocolhash.IsCanonicalHash(o.CanonicalRequestHash) ||
		o.SourceBundleID < 1 || o.GenerationID == "" || !validSchemaGenerationFingerprint(o.GenerationFingerprint) ||
		validateSourceID(o.SourceID) != nil || (j.Phase != "frozen" && j.Phase != "ack_pending") ||
		(j.OutcomeBytes == 0 && (j.OutcomeHash != "" || j.Phase != "frozen")) ||
		(j.OutcomeBytes > 0 && !protocolhash.IsCanonicalHash(j.OutcomeHash)) {
		return nil, &UploadJournalIntegrityError{"invalid durable identity"}
	}
	return j, nil
}

func (c *Client) guardUploadJournal(ctx context.Context, q queryRower, j *uploadJournal) error {
	if j == nil {
		return &UploadJournalIntegrityError{"missing frozen operation"}
	}
	a, err := loadAttachmentState(ctx, q)
	if err != nil {
		return err
	}
	b := j.Binding
	if b.ScopeID != a.AttachedUserID || b.AttachmentEpoch != a.AttachmentEpoch || b.Authority != c.BaseURL ||
		b.Operation.SourceID != a.CurrentSourceID || (c.UserID != "" && c.UserID != b.ScopeID) {
		return &UploadJournalIntegrityError{"attachment or authority changed"}
	}
	return nil
}

func (c *Client) freezeUploadJournalInTx(ctx context.Context, tx sqliteTransaction, record *outboxBundleRecord, body []byte) error {
	a, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	var initialization *string
	if record.InitializationID != "" {
		value := record.InitializationID
		initialization = &value
	}
	b := uploadBinding{a.AttachedUserID, c.BaseURL, a.AttachmentEpoch, oversync.PushOperationBinding{
		SourceID: record.SourceID, GenerationID: strconv.FormatInt(int64(c.schemaGenerationProof.ID), 10),
		GenerationFingerprint: c.schemaGenerationProof.Fingerprint, SourceBundleID: record.SourceBundleID,
		CanonicalRequestHash: record.CanonicalRequestHash, InitializationID: initialization}}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if len(raw) > 4096 {
		return &UploadJournalIntegrityError{"binding too large"}
	}
	charge := workspaceCharge{1, int64(len(body)+6*len(raw)) + 512}
	if err := c.checkAggregateWorkspace(ctx, tx, "upload-freeze", nil, nil, charge); err != nil {
		return err
	}
	digest := sha256.Sum256(body)
	_, err = tx.ExecContext(ctx, `INSERT INTO _sync_upload_journal(singleton_key,binding,request_body,request_digest) VALUES(1,?,?,?)`, string(raw), body, hex.EncodeToString(digest[:]))
	return err
}

func (c *Client) journalRequestBody(ctx context.Context, j *uploadJournal) ([]byte, error) {
	if err := c.guardUploadJournal(ctx, c.db, j); err != nil {
		return nil, err
	}
	if err := c.admitJournalBodyCopy(ctx, j.RequestBytes); err != nil {
		return nil, err
	}
	var raw []byte
	if err := c.db.QueryRowContext(ctx, `SELECT request_body FROM _sync_upload_journal WHERE singleton_key=1`).Scan(&raw); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != j.RequestDigest {
		return nil, &UploadJournalIntegrityError{"frozen request changed"}
	}
	return raw, nil
}

// Keep the frozen encoded request charged beside the response, once per owner.
func (c *Client) admitJournalBodyCopy(ctx context.Context, size int64) error {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil || size <= scope.journalBodyBytes {
		return nil
	}
	charge, err := c.addWorkspaceCharge(scope.unit, scope.base, workspaceCharge{1, 2 * (size - scope.journalBodyBytes)})
	if err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = c.resizeWorkspaceInTx(ctx, tx, scope.token, charge); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	scope.base = charge
	scope.journalBodyBytes = size
	return nil
}

func (c *Client) decodeJournalOutcome(ctx context.Context, raw []byte, j *uploadJournal, uploaded []dirtyRowCapture) (*oversync.PushOutcome, *committedPushBundle, []oversync.PushRequestRow, error) {
	if err := c.guardUploadJournal(ctx, c.db, j); err != nil {
		return nil, nil, nil, err
	}
	if c.discovery == nil || c.discovery.SchemaGeneration.ID != j.Binding.Operation.GenerationID || c.discovery.SchemaGeneration.Fingerprint != j.Binding.Operation.GenerationFingerprint {
		return nil, nil, nil, &UploadJournalIntegrityError{"original generation unavailable"}
	}
	shape, err := responseAllocationShape(ctx, raw)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = c.growPushHTTPBuffers(ctx, 2*int64(len(raw))+65536, shape); err != nil {
		return nil, nil, nil, err
	}
	var outcome oversync.PushOutcome
	if err = json.Unmarshal(raw, &outcome); err != nil {
		return nil, nil, nil, err
	}
	if err = oversync.ValidatePushOutcomeBinding(outcome, j.Binding.Operation, c.discovery.SchemaGeneration); err != nil {
		return nil, nil, nil, err
	}
	if j.OutcomeBytes > 0 && outcome.OutcomeHash != j.OutcomeHash {
		return nil, nil, nil, &UploadJournalIntegrityError{"retained outcome hash changed"}
	}
	if outcome.Rejection != nil && outcome.Rejection.Conflict != nil {
		conflict := localOutcomeConflict(outcome.Rejection.Conflict)
		frozen, matchErr := findConflictingSnapshotRow(&pushOutboundSnapshot{Rows: uploaded}, conflict)
		if matchErr != nil || frozen == nil {
			return nil, nil, nil, errMalformedConflictEvidence
		}
		if err := c.validateAuthoritativeConflictRow(ctx, conflict, frozen); err != nil {
			return nil, nil, nil, errMalformedConflictEvidence
		}
	}
	if outcome.Committed == nil {
		return &outcome, nil, nil, nil
	}
	e := outcome.Committed
	rows, err := oversync.ExpandCompactPushRows(c.discovery.SchemaGeneration, e.Rows)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = c.validateAdmittedResponseRows("push outcome", rows); err != nil {
		return nil, nil, nil, err
	}
	staged := make([]stagedPushBundleRow, 0, len(rows))
	logical := make([]protocolhash.BundleRow, 0, len(rows))
	for _, r := range rows {
		staged = append(staged, stagedPushBundleRow{SchemaName: r.Schema, TableName: r.Table, WireKey: r.Key})
		logical = append(logical, protocolhash.BundleRow{Schema: r.Schema, Table: r.Table, Key: r.Key, Op: r.Op, RowVersion: r.BaseRowVersion, Payload: r.Payload})
	}
	if err = verifyCommittedEffectCoverage(uploaded, staged); err != nil {
		return nil, nil, nil, err
	}
	_, bytes, err := protocolhash.CommittedBundle(logical)
	if err != nil {
		return nil, nil, nil, err
	}
	committed := &committedPushBundle{BundleSeq: e.BundleSeq, SourceID: outcome.SourceID, SourceBundleID: outcome.SourceBundleID, PlannedRowCount: int64(len(uploaded)), RowCount: e.RowCount, ByteCount: bytes, BundleHash: e.BundleHash, CanonicalRequestHash: outcome.CanonicalRequestHash}
	if err = validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, nil, nil, err
	}
	return &outcome, committed, rows, nil
}

func (c *Client) persistUploadOutcome(ctx context.Context, j *uploadJournal, raw []byte, outcome *oversync.PushOutcome) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUploadJournal(ctx, tx)
	if err != nil {
		return err
	}
	if current == nil || !sameUploadOperation(current.Binding.Operation, j.Binding.Operation) || current.Phase != "frozen" {
		return &UploadJournalIntegrityError{"operation changed during outcome capture"}
	}
	if err = c.guardUploadJournal(ctx, tx, current); err != nil {
		return err
	}
	if current.OutcomeBytes != 0 {
		return &UploadJournalIntegrityError{"outcome already owned"}
	}
	if err = c.checkAggregateWorkspace(ctx, tx, "upload-outcome", nil, nil, workspaceCharge{0, int64(len(raw))}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE _sync_upload_journal SET outcome_body=?,outcome_hash=? WHERE singleton_key=1`, raw, outcome.OutcomeHash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Called inside the existing atomic settlement/conflict-disposition writer.
func markUploadDispositionInTx(ctx context.Context, tx sqliteTransaction) error {
	j, err := loadUploadJournal(ctx, tx)
	if err != nil {
		return err
	}
	if j == nil {
		return &UploadJournalIntegrityError{"missing terminal disposition evidence"}
	}
	if j.OutcomeBytes == 0 {
		return &UploadJournalIntegrityError{"disposition has no terminal evidence"}
	}
	if j.Binding.Operation.SourceBundleID == math.MaxInt64 {
		return &UploadJournalIntegrityError{"source sequence exhausted"}
	}
	if err = updateSourceNextBundleID(ctx, tx, j.Binding.Operation.SourceID, j.Binding.Operation.SourceBundleID+1); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_upload_journal SET phase='ack_pending' WHERE singleton_key=1`)
	return err
}

func (c *Client) chargeRecoveryExchange(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if invocation := invocationFromContext(ctx); invocation != nil {
		if invocation.recoveryExchanges >= 64 {
			return c.incompleteSync(ctx, SyncIncompleteRecoveryExchangeLimit)
		}
		invocation.recoveryExchanges++
	}
	return nil
}

func (c *Client) acknowledgeUploadJournal(ctx context.Context) error {
	j, err := loadUploadJournal(ctx, c.db)
	if err != nil || j == nil || j.Phase != "ack_pending" {
		return err
	}
	if err = c.guardUploadJournal(ctx, c.db, j); err != nil {
		return err
	}
	op := j.Binding.Operation
	id, err := strconv.ParseInt(op.GenerationID, 10, 32)
	if err != nil {
		return &UploadJournalIntegrityError{"invalid original generation"}
	}
	body, err := json.Marshal(oversync.PushOutcomeAckRequest{SourceBundleID: op.SourceBundleID, CanonicalRequestHash: op.CanonicalRequestHash, OutcomeHash: j.OutcomeHash})
	if err != nil {
		return err
	}
	result, err := c.doAuthenticatedBoundedRequestForBinding(ctx, "push_ack", http.MethodPost, strings.TrimRight(j.Binding.Authority, "/")+"/sync/push-outcomes/ack", body, "application/json", 4096, 4096, op.SourceID, oversync.SchemaGenerationProof{ID: oversync.SchemaGenerationID(id), Fingerprint: op.GenerationFingerprint}, nil)
	if err != nil {
		return err
	}
	if result.statusCode != http.StatusNoContent {
		return &retryHTTPError{Operation: "push_ack", StatusCode: result.statusCode}
	}
	if len(result.body) != 0 {
		return &UploadJournalIntegrityError{"ack response must be empty"}
	}
	// Ack cleanup is independent of successor admission or generation retirement.
	// A retained initialization token + consumed sequence identifies the existing
	// lifecycle's need to reconnect once this obligation has been removed.
	reconnect := false
	if op.InitializationID != nil {
		var status string
		if err = c.db.QueryRowContext(ctx, `SELECT json_extract(CAST(outcome_body AS TEXT),'$.status') FROM _sync_upload_journal WHERE singleton_key=1`).Scan(&status); err != nil {
			return err
		}
		reconnect = status != "committed"
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUploadJournal(ctx, tx)
	if err != nil {
		return err
	}
	if current == nil || current.Phase != "ack_pending" || current.OutcomeHash != j.OutcomeHash || current.RequestDigest != j.RequestDigest {
		return &UploadJournalIntegrityError{"ack owner changed"}
	}
	if err = c.guardUploadJournal(ctx, tx, current); err != nil {
		return err
	}
	if reconnect {
		if _, err = tx.ExecContext(ctx, `UPDATE _sync_attachment_state SET initialization_reconnect_required=1 WHERE singleton_key=1`); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM _sync_upload_journal WHERE singleton_key=1`); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if reconnect {
		c.sessionConnected = false
	}
	return nil
}

// The rejected outbox is now owned solely by existing policy, with the original
// captures/bases intact. No successor can be frozen while that evidence remains.
func retainRejectedUploadInTx(ctx context.Context, tx sqliteTransaction) error {
	if err := markUploadDispositionInTx(ctx, tx); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE _sync_outbox_bundle SET state='rejected' WHERE singleton_key=1 AND state='prepared'`)
	return err
}

func (c *Client) disposeCancelledUpload(ctx context.Context, snapshot *pushOutboundSnapshot) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	j, err := loadUploadJournal(ctx, tx)
	if err != nil {
		return err
	}
	if err = c.guardUploadJournal(ctx, tx, j); err != nil {
		return err
	}
	if err = c.requeueSnapshotRowsInTx(ctx, tx, snapshot, nil); err != nil {
		return err
	}
	if err = c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func requireNoUnresolvedUpload(ctx context.Context, q queryRower) error {
	j, err := loadUploadJournal(ctx, q)
	if err != nil {
		return err
	}
	outbox, err := loadOutboxBundle(ctx, q)
	if err != nil {
		return err
	}
	var dirty bool
	if err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM _sync_dirty_rows LIMIT 1)`).Scan(&dirty); err != nil {
		return err
	}
	if j != nil || outbox.State != outboxStateNone || dirty {
		return &UploadJournalIntegrityError{"pending upload must settle and acknowledge before source replacement or snapshot"}
	}
	return nil
}

func terminalPushError(outcome *oversync.PushOutcome) error {
	if outcome.Rejection.Code == "push_conflict" {
		return &PushConflictError{Response: oversync.PushConflictResponse{Error: "push_conflict", Conflict: localOutcomeConflict(outcome.Rejection.Conflict)}}
	}
	return &PushApplicationRejectionError{Operation: "push", Status: 200, Category: outcome.Rejection.Code}
}

func sameUploadOperation(a, b oversync.PushOperationBinding) bool {
	if a.SourceID != b.SourceID || a.GenerationID != b.GenerationID || a.GenerationFingerprint != b.GenerationFingerprint || a.SourceBundleID != b.SourceBundleID || a.CanonicalRequestHash != b.CanonicalRequestHash {
		return false
	}
	if a.InitializationID == nil || b.InitializationID == nil {
		return a.InitializationID == nil && b.InitializationID == nil
	}
	return *a.InitializationID == *b.InitializationID
}

func localOutcomeConflict(wire *oversync.PushConflictDetails) *oversync.PushConflictDetails {
	local := *wire
	local.Op = map[string]string{"i": oversync.OpInsert, "u": oversync.OpUpdate, "d": oversync.OpDelete}[wire.Op]
	return &local
}

// Frozen work precedes mutable recovery admission. It never authorizes a new
// freeze, a source rewrite or an overlapping snapshot operation.
func (c *Client) frozenUploadMayResume(ctx context.Context) (bool, error) {
	j, err := loadUploadJournal(ctx, c.db)
	if err != nil || j == nil || j.Phase != "frozen" {
		return false, err
	}
	if err = c.guardUploadJournal(ctx, c.db, j); err != nil {
		return false, err
	}
	outbox, err := loadOutboxBundle(ctx, c.db)
	if err != nil {
		return false, err
	}
	if outbox.State != outboxStatePrepared {
		return false, &UploadJournalIntegrityError{"frozen journal has no frozen outbox"}
	}
	operation, err := loadOperationState(ctx, c.db)
	if err != nil {
		return false, err
	}
	if operation.Kind != operationKindNone && (operation.Kind != operationKindSourceRecovery || operation.StagedSnapshotID != "") {
		return false, &DestructiveTransitionInProgressError{TransitionKind: operation.Kind}
	}
	return true, nil
}
