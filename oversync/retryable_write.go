// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const (
	maximumOperationValidity = 90 * 24 * time.Hour
)

var retryableWriteDelays = [...]time.Duration{25 * time.Millisecond, 50 * time.Millisecond}

func syncMutationTxOptions() pgx.TxOptions {
	return pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}
}

// DatabaseWriteTx is the transaction capability supplied to retryable database-only callbacks.
// It deliberately omits transaction, connection, batch, copy, and prepare ownership methods. The
// capability also rejects transaction/session control and direct set_config calls before PostgreSQL
// sees callback SQL. Invoked database functions remain part of the trusted-caller contract.
type DatabaseWriteTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RetryableDatabaseWrite may be invoked again only after PostgreSQL proves that the prior
// transaction rolled back. It must perform database-only, transaction-pure work using only the
// supplied context and transaction. It must not issue transaction control, change transaction or
// session settings, force constraints, bypass managed triggers, or cause non-transactional effects.
// Oversync rejects unsafe statement classes and direct set_config calls, but cannot sandbox invoked
// database functions; violating the remaining trusted-caller contract invalidates the retry and
// atomicity guarantees.
type RetryableDatabaseWrite func(ctx context.Context, tx DatabaseWriteTx) error

type RetryableWriteOptions struct {
	OperationID         uuid.UUID
	OperationHash       [32]byte
	OperationValidUntil time.Time
}

type RetryableBundleWriteOptions struct {
	OperationHash       [32]byte
	OperationValidUntil time.Time
}

type RegisteredEffectTable struct {
	Schema string
	Table  string
}

type CommittedBundleRef struct {
	BundleSeq            int64
	SourceID             string
	SourceBundleID       int64
	RowCount             int64
	BundleHash           string
	CanonicalRequestHash string
}

type RetryableWriteInvalidError struct{ Message string }

func (e *RetryableWriteInvalidError) Error() string { return e.Message }

type RetryableCallbackViolationError struct{ Message string }

func (e *RetryableCallbackViolationError) Error() string { return e.Message }

type OperationReplayChangedError struct{ Message string }

func (e *OperationReplayChangedError) Error() string {
	if e == nil || e.Message == "" {
		return "operation identity was reused with different content"
	}
	return e.Message
}

type OperationHistoryExpiredError struct{}

func (*OperationHistoryExpiredError) Error() string {
	return "operation history admission deadline has expired"
}

type OperationHistoryUnavailableError struct{}

func (*OperationHistoryUnavailableError) Error() string {
	return "operation history is no longer available"
}

// CommitOutcomeUnknownError means the driver could not prove whether COMMIT took effect.
// Resolve it by replaying the exact public operation identity and content.
type CommitOutcomeUnknownError struct{ Err error }

func (e *CommitOutcomeUnknownError) Error() string {
	if e == nil || e.Err == nil {
		return "transaction commit outcome is unknown"
	}
	return "transaction commit outcome is unknown: " + e.Err.Error()
}

func (e *CommitOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type retryableCallbackContextKey struct{}

type retryableCallbackMarker struct {
	tx      pgx.Tx
	scopeID string
}

func rejectRetryableCallbackContext(ctx context.Context, operation string) error {
	if ctx != nil && ctx.Value(retryableCallbackContextKey{}) != nil {
		return &RetryableCallbackViolationError{Message: operation + " cannot own a transaction from a retryable database-write callback"}
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

type restrictedDatabaseWriteTx struct {
	tx      pgx.Tx
	ctx     context.Context
	marker  *retryableCallbackMarker
	scopeID string
}

func newRestrictedDatabaseWriteTx(parent context.Context, tx pgx.Tx, scopeID string) (context.Context, DatabaseWriteTx, error) {
	if _, err := tx.Exec(parent, `SELECT pg_catalog.set_config('standard_conforming_strings', 'on', true)`); err != nil {
		return nil, nil, fmt.Errorf("normalize retryable callback SQL string parsing: %w", err)
	}
	marker := &retryableCallbackMarker{tx: tx, scopeID: scopeID}
	callbackCtx := context.WithValue(parent, retryableCallbackContextKey{}, marker)
	return callbackCtx, &restrictedDatabaseWriteTx{tx: tx, ctx: callbackCtx, marker: marker, scopeID: scopeID}, nil
}

func (tx *restrictedDatabaseWriteTx) validateCall(ctx context.Context) error {
	if tx == nil || tx.tx == nil || tx.marker == nil {
		return &RetryableCallbackViolationError{Message: "retryable database-write transaction is unavailable"}
	}
	if ctx != tx.ctx {
		return &RetryableCallbackViolationError{Message: "retryable database-write callback must use its supplied context"}
	}
	marker, _ := ctx.Value(retryableCallbackContextKey{}).(*retryableCallbackMarker)
	if marker != tx.marker || marker.tx != tx.tx || marker.scopeID != tx.scopeID {
		return &RetryableCallbackViolationError{Message: "retryable database-write transaction capability does not match the active callback"}
	}
	return nil
}

func (tx *restrictedDatabaseWriteTx) Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error) {
	if err := tx.validateCall(ctx); err != nil {
		return pgconn.CommandTag{}, err
	}
	if err := validateRetryableCallbackSQL(query); err != nil {
		return pgconn.CommandTag{}, err
	}
	return tx.tx.Exec(ctx, query, arguments...)
}

func (tx *restrictedDatabaseWriteTx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if err := tx.validateCall(ctx); err != nil {
		return nil, err
	}
	if err := validateRetryableCallbackSQL(query); err != nil {
		return nil, err
	}
	rows, err := tx.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &restrictedRows{Rows: rows}, nil
}

func (tx *restrictedDatabaseWriteTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if err := tx.validateCall(ctx); err != nil {
		return errorRow{err: err}
	}
	if err := validateRetryableCallbackSQL(query); err != nil {
		return errorRow{err: err}
	}
	return tx.tx.QueryRow(ctx, query, args...)
}

func validateRetryableCallbackSQL(query string) error {
	identifiers, err := retryableSQLIdentifiers(query)
	if err != nil {
		return &RetryableCallbackViolationError{Message: err.Error()}
	}
	if len(identifiers) == 0 {
		return &RetryableCallbackViolationError{Message: "retryable database-write callback SQL is empty"}
	}
	first := strings.ToLower(identifiers[0])
	switch first {
	case "select", "insert", "update", "delete", "merge", "with", "values", "table", "show":
	default:
		return &RetryableCallbackViolationError{Message: "retryable database-write callback SQL statement class is not allowed: " + first}
	}
	for _, identifier := range identifiers {
		if strings.EqualFold(identifier, "set_config") {
			return &RetryableCallbackViolationError{Message: "retryable database-write callback SQL cannot call set_config"}
		}
	}
	return nil
}

// retryableSQLIdentifiers performs the small lexical task required by the callback boundary. It
// keeps quoted identifiers, ignores comments and string/dollar-quoted literals, rejects multiple
// statements, and fails closed on Unicode-escaped identifiers rather than trying to reproduce
// PostgreSQL's configurable Unicode escape rules.
func retryableSQLIdentifiers(query string) ([]string, error) {
	identifiers := make([]string, 0, 8)
	statementEnded := false
	for i := 0; i < len(query); {
		switch {
		case isSQLSpace(query[i]):
			i++
		case (query[i] == 'e' || query[i] == 'E') && i+1 < len(query) && query[i+1] == '\'':
			var closed bool
			i, closed = skipSQLEscapeString(query, i+1)
			if !closed {
				return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated escape string literal")
			}
		case (query[i] == 'u' || query[i] == 'U') && i+2 < len(query) && query[i+1] == '&' && query[i+2] == '\'':
			var closed bool
			i, closed = skipSQLQuoted(query, i+2, '\'')
			if !closed {
				return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated Unicode string literal")
			}
		case query[i] == '-' && i+1 < len(query) && query[i+1] == '-':
			i += 2
			for i < len(query) && query[i] != '\n' && query[i] != '\r' {
				i++
			}
		case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
			depth := 1
			i += 2
			for i < len(query) && depth != 0 {
				switch {
				case i+1 < len(query) && query[i] == '/' && query[i+1] == '*':
					depth++
					i += 2
				case i+1 < len(query) && query[i] == '*' && query[i+1] == '/':
					depth--
					i += 2
				default:
					i++
				}
			}
			if depth != 0 {
				return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated block comment")
			}
		case query[i] == '\'':
			var closed bool
			i, closed = skipSQLQuoted(query, i, '\'')
			if !closed {
				return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated string literal")
			}
		case query[i] == '$':
			delimiterEnd := i + 1
			for delimiterEnd < len(query) && isSQLDollarTag(query[delimiterEnd]) {
				delimiterEnd++
			}
			if delimiterEnd < len(query) && query[delimiterEnd] == '$' {
				delimiter := query[i : delimiterEnd+1]
				bodyEnd := strings.Index(query[delimiterEnd+1:], delimiter)
				if bodyEnd < 0 {
					return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated dollar-quoted literal")
				}
				i = delimiterEnd + 1 + bodyEnd + len(delimiter)
			} else {
				i++
			}
		case query[i] == '"':
			if i >= 2 && query[i-1] == '&' && (query[i-2] == 'u' || query[i-2] == 'U') {
				return nil, fmt.Errorf("retryable database-write callback SQL cannot use Unicode-escaped identifiers")
			}
			identifier, next, closed := readSQLQuotedIdentifier(query, i)
			if !closed {
				return nil, fmt.Errorf("retryable database-write callback SQL has an unterminated quoted identifier")
			}
			if statementEnded {
				return nil, fmt.Errorf("retryable database-write callback SQL must contain exactly one statement")
			}
			identifiers = append(identifiers, identifier)
			i = next
		case query[i] == ';':
			statementEnded = len(identifiers) != 0
			i++
		case isSQLIdentifierStart(query[i]):
			start := i
			i++
			for i < len(query) && isSQLIdentifierContinue(query[i]) {
				i++
			}
			if statementEnded {
				return nil, fmt.Errorf("retryable database-write callback SQL must contain exactly one statement")
			}
			identifiers = append(identifiers, query[start:i])
		default:
			i++
		}
	}
	return identifiers, nil
}

func skipSQLQuoted(query string, start int, quote byte) (int, bool) {
	for i := start + 1; i < len(query); i++ {
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++
			continue
		}
		return i + 1, true
	}
	return len(query), false
}

func skipSQLEscapeString(query string, start int) (int, bool) {
	for i := start + 1; i < len(query); i++ {
		switch query[i] {
		case '\\':
			if i+1 < len(query) {
				i++
			}
		case '\'':
			if i+1 < len(query) && query[i+1] == '\'' {
				i++
				continue
			}
			return i + 1, true
		}
	}
	return len(query), false
}

func readSQLQuotedIdentifier(query string, start int) (string, int, bool) {
	var value strings.Builder
	for i := start + 1; i < len(query); i++ {
		if query[i] != '"' {
			value.WriteByte(query[i])
			continue
		}
		if i+1 < len(query) && query[i+1] == '"' {
			value.WriteByte('"')
			i++
			continue
		}
		return value.String(), i + 1, true
	}
	return "", len(query), false
}

func isSQLSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\n', '\r', '\f':
		return true
	default:
		return false
	}
}

func isSQLIdentifierStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= 0x80
}

func isSQLIdentifierContinue(value byte) bool {
	return isSQLIdentifierStart(value) || value >= '0' && value <= '9' || value == '$'
}

func isSQLDollarTag(value byte) bool {
	return isSQLIdentifierStart(value) || value >= '0' && value <= '9'
}

type restrictedRows struct{ pgx.Rows }

func (rows *restrictedRows) Conn() *pgx.Conn { return nil }

type errorRow struct{ err error }

func (row errorRow) Scan(...any) error { return row.err }

func validateRetryableWriteOptions(opts RetryableWriteOptions) error {
	if opts.OperationID == uuid.Nil {
		return &RetryableWriteInvalidError{Message: "operation_id must be a nonzero UUID"}
	}
	if opts.OperationHash == ([32]byte{}) {
		return &RetryableWriteInvalidError{Message: "operation_hash must be a nonzero SHA-256 digest"}
	}
	if err := validateOperationDeadline(opts.OperationValidUntil); err != nil {
		return err
	}
	return nil
}

func validateOperationDeadline(deadline time.Time) error {
	if deadline.IsZero() || deadline.Year() < 1 || deadline.Year() > 9999 {
		return &RetryableWriteInvalidError{Message: "operation_valid_until must be a finite RFC 3339 timestamp"}
	}
	if deadline.Nanosecond()%1000 != 0 {
		return &RetryableWriteInvalidError{Message: "operation_valid_until must not contain sub-microsecond precision"}
	}
	return nil
}

func validateCanonicalRequestHash(hash string) error {
	if hash == "" {
		return nil
	}
	if len(hash) != 64 {
		return &RetryableWriteInvalidError{Message: "canonical_request_hash must be empty or 64 lowercase hexadecimal characters"}
	}
	for _, char := range []byte(hash) {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return &RetryableWriteInvalidError{Message: "canonical_request_hash must be empty or 64 lowercase hexadecimal characters"}
		}
	}
	return nil
}

func appendFrameField(dst []byte, value []byte) []byte {
	dst = append(dst, 0x01)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}

var withinSyncBundleOperationNamespace = uuid.MustParse("61f1b2cd-8dd4-5a91-a937-4d06f4ac2f97")

func withinSyncBundleOperationID(scopeID, sourceID string, sourceBundleID int64) uuid.UUID {
	frame := make([]byte, 0, len(scopeID)+len(sourceID)+96)
	frame = appendFrameField(frame, []byte("oversync.within-sync-bundle-operation-id.v1"))
	frame = appendFrameField(frame, []byte(scopeID))
	frame = appendFrameField(frame, []byte(sourceID))
	frame = appendFrameField(frame, []byte(fmt.Sprintf("%d", sourceBundleID)))
	return uuid.NewSHA1(withinSyncBundleOperationNamespace, frame)
}

type canonicalEffectSet struct {
	tables []RegisteredEffectTable
	hash   [32]byte
	keys   map[string]struct{}
}

func (s *SyncService) canonicalizeEffectSet(input []RegisteredEffectTable) (canonicalEffectSet, error) {
	result := canonicalEffectSet{keys: make(map[string]struct{}, len(input))}
	for _, table := range input {
		normalized := RegisteredEffectTable{
			Schema: strings.ToLower(strings.TrimSpace(table.Schema)),
			Table:  strings.ToLower(strings.TrimSpace(table.Table)),
		}
		if normalized.Schema == "" {
			normalized.Schema = "public"
		}
		if !isValidSchemaName(normalized.Schema) || !isValidTableName(normalized.Table) {
			return canonicalEffectSet{}, &RetryableWriteInvalidError{Message: "effect table contains an invalid schema or table name"}
		}
		key := Key(normalized.Schema, normalized.Table)
		if !s.IsTableRegistered(normalized.Schema, normalized.Table) {
			return canonicalEffectSet{}, &RetryableWriteInvalidError{Message: "effect table " + key + " is not registered"}
		}
		if _, exists := result.keys[key]; exists {
			continue
		}
		result.keys[key] = struct{}{}
		result.tables = append(result.tables, normalized)
	}
	slicesSortEffectTables(result.tables)
	pairs := make([][2]string, 0, len(result.tables))
	for _, table := range result.tables {
		pairs = append(pairs, [2]string{table.Schema, table.Table})
	}
	encoded, err := json.Marshal([]any{"oversync.effect-table-set.v1", pairs})
	if err != nil {
		return canonicalEffectSet{}, fmt.Errorf("encode canonical effect table set: %w", err)
	}
	result.hash = sha256.Sum256(encoded)
	return result, nil
}

func slicesSortEffectTables(tables []RegisteredEffectTable) {
	for i := 1; i < len(tables); i++ {
		for j := i; j > 0; j-- {
			left, right := tables[j-1], tables[j]
			if left.Schema < right.Schema || (left.Schema == right.Schema && left.Table <= right.Table) {
				break
			}
			tables[j-1], tables[j] = tables[j], tables[j-1]
		}
	}
}

func validateDisjointEffectSets(required, forbidden canonicalEffectSet) error {
	for key := range required.keys {
		if _, overlaps := forbidden.keys[key]; overlaps {
			return &RetryableWriteInvalidError{Message: "required and forbidden effect table sets must be disjoint"}
		}
	}
	return nil
}

type scopeWriteReceiptSpec struct {
	userPK                 int64
	operationID            uuid.UUID
	operationHash          [32]byte
	requiredEffectsHash    [32]byte
	forbiddenEffectsHash   [32]byte
	writerID               string
	explicitSourceBundleID *int64
	operationValidUntil    time.Time
}

type scopeWriteReceiptDecision struct {
	inserted bool
	replay   *ScopeWriteResult
}

func acquireScopeWriteReceipt(ctx context.Context, tx pgx.Tx, spec scopeWriteReceiptSpec) (scopeWriteReceiptDecision, error) {
	row, err := loadScopeWriteReceipt(ctx, tx, spec.userPK, spec.operationID)
	if err != nil {
		return scopeWriteReceiptDecision{}, err
	}
	if row != nil {
		if err := row.matches(spec); err != nil {
			return scopeWriteReceiptDecision{}, err
		}
		if row.state != "completed" {
			return scopeWriteReceiptDecision{}, fmt.Errorf("scope operation receipt remained in nonterminal state %q", row.state)
		}
		return scopeWriteReceiptDecision{replay: row.result()}, nil
	}

	var databaseTime time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseTime); err != nil {
		return scopeWriteReceiptDecision{}, fmt.Errorf("sample scope operation admission time: %w", err)
	}
	databaseTime = databaseTime.UTC()
	deadline := spec.operationValidUntil.UTC()
	if !deadline.After(databaseTime) {
		return scopeWriteReceiptDecision{}, &OperationHistoryExpiredError{}
	}
	if deadline.After(databaseTime.Add(maximumOperationValidity)) {
		return scopeWriteReceiptDecision{}, &RetryableWriteInvalidError{Message: "operation_valid_until must be no more than 90 days after database admission time"}
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO sync.scope_write_receipts (
			user_pk, operation_id, operation_hash, required_effect_tables_hash,
			forbidden_effect_tables_hash, writer_id, explicit_source_bundle_id,
			operation_valid_until, receipt_state, receipt_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, 'in_progress', $8::timestamptz + interval '24 hours')
		ON CONFLICT (user_pk, operation_id) DO NOTHING
	`, spec.userPK, spec.operationID, spec.operationHash[:], spec.requiredEffectsHash[:], spec.forbiddenEffectsHash[:], spec.writerID, spec.explicitSourceBundleID, deadline)
	if err != nil {
		return scopeWriteReceiptDecision{}, fmt.Errorf("insert scope operation receipt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		row, err := loadScopeWriteReceipt(ctx, tx, spec.userPK, spec.operationID)
		if err != nil {
			return scopeWriteReceiptDecision{}, err
		}
		if row == nil {
			return scopeWriteReceiptDecision{}, fmt.Errorf("scope operation receipt conflict resolved without an authoritative row")
		}
		if err := row.matches(spec); err != nil {
			return scopeWriteReceiptDecision{}, err
		}
		if row.state != "completed" {
			return scopeWriteReceiptDecision{}, fmt.Errorf("scope operation receipt remained in nonterminal state %q", row.state)
		}
		return scopeWriteReceiptDecision{replay: row.result()}, nil
	}
	if tag.RowsAffected() != 1 {
		return scopeWriteReceiptDecision{}, fmt.Errorf("insert scope operation receipt affected %d rows", tag.RowsAffected())
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseTime); err != nil {
		return scopeWriteReceiptDecision{}, fmt.Errorf("resample scope operation admission time: %w", err)
	}
	if !deadline.After(databaseTime.UTC()) {
		return scopeWriteReceiptDecision{}, &OperationHistoryExpiredError{}
	}
	return scopeWriteReceiptDecision{inserted: true}, nil
}

type storedScopeWriteReceipt struct {
	operationHash             []byte
	requiredEffectsHash       []byte
	forbiddenEffectsHash      []byte
	writerID                  string
	explicitSourceBundleID    sql.NullInt64
	operationValidUntil       time.Time
	state                     string
	committedSourceBundleID   sql.NullInt64
	committedBundleSeq        sql.NullInt64
	committedRowCount         sql.NullInt64
	committedBundleHash       []byte
	committedCanonicalRequest sql.NullString
	autoInitialized           sql.NullBool
}

func loadScopeWriteReceipt(ctx context.Context, tx pgx.Tx, userPK int64, operationID uuid.UUID) (*storedScopeWriteReceipt, error) {
	var row storedScopeWriteReceipt
	err := tx.QueryRow(ctx, `
		SELECT operation_hash, required_effect_tables_hash, forbidden_effect_tables_hash,
		       writer_id, explicit_source_bundle_id, operation_valid_until, receipt_state,
		       committed_source_bundle_id, committed_bundle_seq, committed_row_count,
		       committed_bundle_hash, committed_canonical_request_hash, auto_initialized
		FROM sync.scope_write_receipts
		WHERE user_pk = $1 AND operation_id = $2
		FOR UPDATE
	`, userPK, operationID).Scan(
		&row.operationHash, &row.requiredEffectsHash, &row.forbiddenEffectsHash,
		&row.writerID, &row.explicitSourceBundleID, &row.operationValidUntil, &row.state,
		&row.committedSourceBundleID, &row.committedBundleSeq, &row.committedRowCount,
		&row.committedBundleHash, &row.committedCanonicalRequest, &row.autoInitialized,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load scope operation receipt: %w", err)
	}
	return &row, nil
}

func (row *storedScopeWriteReceipt) matches(spec scopeWriteReceiptSpec) error {
	changed := !bytes.Equal(row.operationHash, spec.operationHash[:]) ||
		!bytes.Equal(row.requiredEffectsHash, spec.requiredEffectsHash[:]) ||
		!bytes.Equal(row.forbiddenEffectsHash, spec.forbiddenEffectsHash[:]) ||
		row.writerID != spec.writerID ||
		!row.operationValidUntil.Equal(spec.operationValidUntil)
	if spec.explicitSourceBundleID == nil {
		changed = changed || row.explicitSourceBundleID.Valid
	} else {
		changed = changed || !row.explicitSourceBundleID.Valid || row.explicitSourceBundleID.Int64 != *spec.explicitSourceBundleID
	}
	if changed {
		return &OperationReplayChangedError{}
	}
	return nil
}

func (row *storedScopeWriteReceipt) result() *ScopeWriteResult {
	if row == nil || row.state != "completed" {
		return nil
	}
	return &ScopeWriteResult{
		AutoInitialized: row.autoInitialized.Bool,
		Bundle: CommittedBundleRef{
			BundleSeq:            row.committedBundleSeq.Int64,
			SourceID:             row.writerID,
			SourceBundleID:       row.committedSourceBundleID.Int64,
			RowCount:             row.committedRowCount.Int64,
			BundleHash:           renderBundleHash(row.committedBundleHash),
			CanonicalRequestHash: row.committedCanonicalRequest.String,
		},
	}
}

func completeScopeWriteReceipt(ctx context.Context, tx pgx.Tx, userPK int64, operationID uuid.UUID, result *ScopeWriteResult) error {
	if result == nil {
		return fmt.Errorf("complete scope operation receipt requires a result")
	}
	bundleHash, err := decodeBundleHash(result.Bundle.BundleHash)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		WITH completion_clock AS (
			SELECT clock_timestamp() AS completed_at
		)
		UPDATE sync.scope_write_receipts
		SET receipt_state = 'completed',
		    committed_source_bundle_id = $3,
		    committed_bundle_seq = $4,
		    committed_row_count = $5,
		    committed_bundle_hash = $6,
		    committed_canonical_request_hash = $7,
		    auto_initialized = $8,
		    completed_at = completion_clock.completed_at,
		    receipt_expires_at = GREATEST(operation_valid_until, completion_clock.completed_at) + interval '24 hours'
		FROM completion_clock
		WHERE user_pk = $1 AND operation_id = $2 AND receipt_state = 'in_progress'
	`, userPK, operationID, result.Bundle.SourceBundleID, result.Bundle.BundleSeq, result.Bundle.RowCount, bundleHash, result.Bundle.CanonicalRequestHash, result.AutoInitialized)
	if err != nil {
		return fmt.Errorf("complete scope operation receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete scope operation receipt affected %d rows", tag.RowsAffected())
	}
	return nil
}

func decodeBundleHash(value string) ([]byte, error) {
	if len(value) != 64 {
		return nil, fmt.Errorf("committed bundle hash is not a SHA-256 digest")
	}
	decoded := make([]byte, 32)
	for i := 0; i < len(decoded); i++ {
		hi, ok := hexNibble(value[i*2])
		if !ok {
			return nil, fmt.Errorf("committed bundle hash is not lowercase hexadecimal")
		}
		lo, ok := hexNibble(value[i*2+1])
		if !ok {
			return nil, fmt.Errorf("committed bundle hash is not lowercase hexadecimal")
		}
		decoded[i] = hi<<4 | lo
	}
	return decoded, nil
}

func hexNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	default:
		return 0, false
	}
}

func committedBundleRef(bundle *Bundle) CommittedBundleRef {
	return CommittedBundleRef{
		BundleSeq:            bundle.BundleSeq,
		SourceID:             bundle.SourceID,
		SourceBundleID:       bundle.SourceBundleID,
		RowCount:             bundle.RowCount,
		BundleHash:           bundle.BundleHash,
		CanonicalRequestHash: bundle.CanonicalRequestHash,
	}
}

func runRetryableWriteTransaction[T any](
	ctx context.Context,
	conn *pgxpool.Conn,
	fn func(pgx.Tx) (T, error),
) (T, error) {
	return runRetryableWriteTransactionWithDriver(ctx, retryableWriteDriver{
		begin: func(beginCtx context.Context) (pgx.Tx, error) {
			if err := attestOriginSession(beginCtx, conn); err != nil {
				return nil, err
			}
			return conn.BeginTx(beginCtx, syncMutationTxOptions())
		},
		commitRollbackProven: commitRollbackProven,
		wait:                 sleepWithContext,
	}, fn)
}

func commitRollbackProven(commitErr error) bool {
	// An idle connection proves only that COMMIT finished processing; it cannot
	// distinguish a committed transaction whose response was lost from a
	// transaction PostgreSQL rolled back. An explicit PostgreSQL error response
	// or pgx's dedicated rollback sentinel is positive rollback proof; a
	// transport or context error is not.
	if errors.Is(commitErr, pgx.ErrTxCommitRollback) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(commitErr, &pgErr)
}

type retryableWriteDriver struct {
	begin                func(context.Context) (pgx.Tx, error)
	commitRollbackProven func(error) bool
	wait                 func(context.Context, time.Duration) error
}

func runRetryableWriteTransactionWithDriver[T any](
	ctx context.Context,
	driver retryableWriteDriver,
	fn func(pgx.Tx) (T, error),
) (T, error) {
	var zero T
	for attempt := 0; attempt < 3; attempt++ {
		tx, err := driver.begin(ctx)
		if err != nil {
			if !isRetryablePGTxError(err) || attempt == 2 {
				return zero, err
			}
			if err := driver.wait(ctx, retryableWriteDelays[attempt]); err != nil {
				return zero, err
			}
			continue
		}

		value, attemptErr := fn(tx)
		if attemptErr != nil {
			rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			rollbackErr := tx.Rollback(rollbackCtx)
			cancel()
			if rollbackErr != nil {
				return zero, &WriteRollbackUnconfirmedError{Err: errors.Join(attemptErr, rollbackErr)}
			}
			if !isRetryablePGTxError(attemptErr) || attempt == 2 {
				return zero, attemptErr
			}
			if err := driver.wait(ctx, retryableWriteDelays[attempt]); err != nil {
				return zero, err
			}
			continue
		}

		commitErr := tx.Commit(ctx)
		if commitErr == nil {
			return value, nil
		}
		rollbackProven := driver.commitRollbackProven(commitErr)
		if !rollbackProven {
			return zero, &CommitOutcomeUnknownError{Err: commitErr}
		}
		if !isRetryablePGTxError(commitErr) || attempt == 2 {
			return zero, commitErr
		}
		if err := driver.wait(ctx, retryableWriteDelays[attempt]); err != nil {
			return zero, err
		}
	}
	return zero, fmt.Errorf("retryable database-write transaction exhausted attempts")
}

func runRetryableOwnedTransaction(ctx context.Context, conn *pgxpool.Conn, fn func(pgx.Tx) error) error {
	_, err := runRetryableWriteTransaction(ctx, conn, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, fn(tx)
	})
	return err
}

func validateWriterID(writerID string) error {
	if err := sourceid.Validate(writerID); err != nil {
		return &RetryableWriteInvalidError{Message: "writer_id must be an exact visible ASCII SourceID"}
	}
	return nil
}

// WriteRollbackUnconfirmedError prevents callers from checkpointing failure when
// cleanup did not establish that the database transaction has stopped.
type WriteRollbackUnconfirmedError struct{ Err error }

func (e *WriteRollbackUnconfirmedError) Error() string { return "database write rollback unconfirmed" }
func (e *WriteRollbackUnconfirmedError) Unwrap() error { return e.Err }
