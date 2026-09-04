package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

const unconfirmedLocalSchema = "local"

var retainedRuntimeManagedIndexes = map[string]struct{}{
	"_sync_apply_rows_role_ordinal_idx": {},
	"_sync_apply_rows_pending_idx":      {},
	"_sync_snapshot_stage_identity_idx": {},
	"idx_sync_dirty_rows_dirty_ordinal": {}, // retained prototype identity
	"_sync_relationships_parent_idx":    {},
	"_sync_row_state_unknown_idx":       {},
	"_sync_dirty_rows_order_idx":        {},
}

type retainedRuntimeState int

const (
	retainedRuntimeFresh retainedRuntimeState = iota
	retainedRuntimeCurrent
)

// This identity exists even before discovery. Admitted server descriptors are
// separate immutable records, never reconstructed from an operation journal.
func prepareRetainedContract(ctx context.Context, db *sql.DB, config *Config) error {
	return prepareRetainedContractWithFinalize(ctx, db, config, nil)
}

func prepareRetainedContractWithFinalize(ctx context.Context, db *sql.DB, config *Config, beforeCommit func(context.Context, *immediateTx) error, freshLease ...bool) error {
	if config.DatabaseID == "" || config.ServiceID == "" || config.LocalVersion < 0 {
		return fmt.Errorf("database/service identity and a nonnegative local version are required")
	}
	preflightState, err := inspectRetainedRuntime(ctx, db)
	if err != nil {
		return err
	}
	if preflightState == retainedRuntimeCurrent {
		preflightShape, err := managedPhysicalShape(ctx, db, config.Tables)
		if err != nil {
			return err
		}
		retainedConfig := config
		if len(freshLease) == 1 && freshLease[0] {
			copy := *config
			copy.MaxSyncWorkspaceEntries = int64(^uint64(0) >> 1)
			copy.MaxSyncWorkspaceBytes = int64(^uint64(0) >> 1)
			retainedConfig = &copy
		}
		if err := readRetainedContract(ctx, db, retainedConfig, preflightShape); err != nil {
			return err
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to reserve retained-runtime writer: %w", err)
	}
	defer conn.Close()
	return prepareRetainedContractOnConnection(ctx, conn, config, nil, beforeCommit, freshLease...)
}

func prepareRetainedContractOnConnection(ctx context.Context, conn *sql.Conn, config *Config, afterBegin, beforeCommit func(context.Context, *immediateTx) error, freshLease ...bool) (resultErr error) {
	var originalForeignKeys int
	if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&originalForeignKeys); err != nil {
		return fmt.Errorf("inspect original foreign_keys: %w", err)
	}
	var originalJournalMode string
	if err := conn.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&originalJournalMode); err != nil {
		return fmt.Errorf("inspect original journal_mode: %w", err)
	}
	configured := false
	committed := false
	defer func() {
		if !configured || committed {
			return
		}
		restoreErr := restoreSQLiteSettings(conn, originalForeignKeys, originalJournalMode)
		if restoreErr != nil {
			resultErr = errors.Join(resultErr, restoreErr)
		}
	}()
	if _, err := conn.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	configured = true
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("failed to begin immediate retained-runtime transaction: %w", err)
	}
	tx := &immediateTx{conn: conn, ctx: ctx}
	defer tx.Rollback()
	if afterBegin != nil {
		if err := afterBegin(ctx, tx); err != nil {
			return err
		}
	}
	shape, err := managedPhysicalShape(ctx, tx, config.Tables)
	if err != nil {
		return err
	}
	state, err := inspectRetainedRuntime(ctx, tx)
	if err != nil {
		return err
	}
	if state == retainedRuntimeCurrent && len(freshLease) == 1 && freshLease[0] {
		if err := reclaimWorkspaceReservations(ctx, tx, config); err != nil {
			return err
		}
	}
	if state == retainedRuntimeFresh {
		if err := initializeDatabaseInTx(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO _sync_contract_identity(singleton_key,database_id,service_id,generation_id,local_version,physical_shape) VALUES(1,?,?,?,?,?)`, config.DatabaseID, config.ServiceID, config.GenerationID, config.LocalVersion, shape); err != nil {
			return err
		}
	} else if err := readRetainedContract(ctx, tx, config, shape); err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx, tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func restoreSQLiteSettings(conn *sql.Conn, foreignKeys int, journalMode string) error {
	restoreCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result error
	if _, err := conn.ExecContext(restoreCtx, fmt.Sprintf(`PRAGMA foreign_keys=%d`, foreignKeys)); err != nil {
		result = errors.Join(result, fmt.Errorf("restore foreign_keys: %w", err))
	}
	if _, err := conn.ExecContext(restoreCtx, `PRAGMA journal_mode=`+quoteIdent(journalMode)); err != nil {
		result = errors.Join(result, fmt.Errorf("restore journal_mode: %w", err))
	}
	var gotForeignKeys int
	if err := conn.QueryRowContext(restoreCtx, `PRAGMA foreign_keys`).Scan(&gotForeignKeys); err != nil {
		result = errors.Join(result, fmt.Errorf("verify foreign_keys restoration: %w", err))
	} else if gotForeignKeys != foreignKeys {
		result = errors.Join(result, fmt.Errorf("verify foreign_keys restoration: got %d, want %d", gotForeignKeys, foreignKeys))
	}
	var gotJournalMode string
	if err := conn.QueryRowContext(restoreCtx, `PRAGMA journal_mode`).Scan(&gotJournalMode); err != nil {
		result = errors.Join(result, fmt.Errorf("verify journal_mode restoration: %w", err))
	} else if !strings.EqualFold(gotJournalMode, journalMode) {
		result = errors.Join(result, fmt.Errorf("verify journal_mode restoration: got %q, want %q", gotJournalMode, journalMode))
	}
	return result
}

func inspectRetainedRuntime(ctx context.Context, q upgradeQueryer) (retainedRuntimeState, error) {
	identityExists, err := sqliteNamedTableExistsNoCase(ctx, q, "_sync_contract_identity")
	if err != nil {
		return retainedRuntimeFresh, err
	}
	if !identityExists {
		partialObject, err := firstRetainedRuntimeObject(ctx, q)
		if err != nil {
			return retainedRuntimeFresh, err
		}
		if partialObject != "" {
			return retainedRuntimeFresh, fmt.Errorf("existing sync state has no retained contract identity (%s remains); recreate the incompatible development database explicitly", partialObject)
		}
		return retainedRuntimeFresh, nil
	}
	if err := validateCompleteRetainedRuntime(ctx, q); err != nil {
		return retainedRuntimeFresh, fmt.Errorf("incompatible retained sync runtime: %w; recreate the development database explicitly", err)
	}
	return retainedRuntimeCurrent, nil
}

type retainedColumnMetadata struct {
	cid, notNull, primaryKey, hidden int
	name, declaredType, defaultSQL   string
	defaultValid                     bool
}

type retainedSchemaObject struct {
	name, table, sql string
}

type retainedIndexListEntry struct {
	unique, partial int
	origin          string
}

type retainedIndexColumn struct {
	sequence, columnID, descending, key int
	name, collation                     string
	nameValid, collationValid           bool
}

func validateCompleteRetainedRuntime(ctx context.Context, actual upgradeQueryer) error {
	expectedDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return fmt.Errorf("open retained-runtime reference: %w", err)
	}
	defer expectedDB.Close()
	expected, err := expectedDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin retained-runtime reference: %w", err)
	}
	defer expected.Rollback()
	if err := initializeDatabaseInTx(ctx, expected); err != nil {
		return fmt.Errorf("initialize retained-runtime reference: %w", err)
	}
	for _, name := range syncMetadataTableNames {
		want, err := retainedTableMetadata(ctx, expected, name)
		if err != nil {
			return err
		}
		got, err := retainedTableMetadata(ctx, actual, name)
		if err != nil {
			return err
		}
		if err := compareRetainedColumns(name, got, want); err != nil {
			return err
		}
		expectedDDL, err := retainedObject(ctx, expected, "table", name)
		if err != nil {
			return err
		}
		actualDDL, err := retainedObject(ctx, actual, "table", name)
		if err != nil {
			return err
		}
		if retainedDDLSignature(actualDDL.sql) != retainedDDLSignature(expectedDDL.sql) {
			return fmt.Errorf("table %s constraints differ", name)
		}
	}
	for name := range retainedManagedIndexes() {
		if name == "idx_sync_dirty_rows_dirty_ordinal" {
			continue
		}
		if err := compareRetainedIndex(ctx, actual, expected, name); err != nil {
			return err
		}
	}
	legacy, err := retainedObject(ctx, actual, "index", "idx_sync_dirty_rows_dirty_ordinal")
	if err != nil {
		return err
	}
	if legacy != nil {
		return fmt.Errorf("retained prototype index %s must be absent", legacy.name)
	}
	for _, name := range []string{"_sync_conflict_revision_insert", "_sync_conflict_revision_update", "_sync_conflict_revision_delete", "_sync_baseline_revision_insert", "_sync_baseline_revision_update", "_sync_baseline_revision_delete"} {
		if err := compareRetainedTrigger(ctx, actual, expected, name); err != nil {
			return err
		}
	}
	for name := range stageAccountingDDL() {
		if err := compareRetainedTrigger(ctx, actual, expected, name); err != nil {
			return err
		}
	}
	if err := validateRetainedCaptureTriggers(ctx, actual); err != nil {
		return err
	}
	if err := rejectUnexpectedRetainedTableTriggers(ctx, actual); err != nil {
		return err
	}
	return nil
}

func rejectUnexpectedRetainedTableTriggers(ctx context.Context, q upgradeQueryer) error {
	allowed := map[string]struct{}{
		"_sync_conflict_revision_insert": {},
		"_sync_conflict_revision_update": {},
		"_sync_conflict_revision_delete": {},
		"_sync_baseline_revision_insert": {},
		"_sync_baseline_revision_update": {},
		"_sync_baseline_revision_delete": {},
	}
	for name := range stageAccountingDDL() {
		allowed[name] = struct{}{}
	}
	ownedTables := make(map[string]struct{}, len(syncMetadataTableNames))
	for _, name := range syncMetadataTableNames {
		ownedTables[name] = struct{}{}
	}
	rows, err := q.QueryContext(ctx, `SELECT name,tbl_name FROM sqlite_master WHERE type='trigger' ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, table string
		if err := rows.Scan(&name, &table); err != nil {
			return err
		}
		name, table = strings.ToLower(name), strings.ToLower(table)
		if _, owned := ownedTables[table]; !owned {
			continue
		}
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("unexpected trigger %s targets retained runtime table %s", name, table)
		}
	}
	return rows.Err()
}

func retainedTableMetadata(ctx context.Context, q upgradeQueryer, name string) ([]retainedColumnMetadata, error) {
	object, err := retainedObject(ctx, q, "table", name)
	if err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("required table %s is missing", name)
	}
	rows, err := q.QueryContext(ctx, "PRAGMA table_xinfo("+quoteIdent(object.name)+")")
	if err != nil {
		return nil, fmt.Errorf("inspect retained table %s: %w", name, err)
	}
	defer rows.Close()
	var result []retainedColumnMetadata
	for rows.Next() {
		var column retainedColumnMetadata
		var defaultSQL sql.NullString
		if err := rows.Scan(&column.cid, &column.name, &column.declaredType, &column.notNull, &defaultSQL, &column.primaryKey, &column.hidden); err != nil {
			return nil, fmt.Errorf("scan retained table %s: %w", name, err)
		}
		column.name = strings.ToLower(column.name)
		column.declaredType = strings.ToUpper(strings.TrimSpace(column.declaredType))
		column.defaultValid = defaultSQL.Valid
		if defaultSQL.Valid {
			column.defaultSQL = normalizeRetainedSQL(defaultSQL.String)
		}
		result = append(result, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained table %s: %w", name, err)
	}
	return result, nil
}

func compareRetainedColumns(table string, got, want []retainedColumnMetadata) error {
	if len(got) != len(want) {
		return fmt.Errorf("table %s has %d columns, expected %d", table, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("table %s column %d metadata differs", table, i)
		}
	}
	return nil
}

func compareRetainedIndex(ctx context.Context, actual, expected upgradeQueryer, name string) error {
	want, err := retainedObject(ctx, expected, "index", name)
	if err != nil {
		return err
	}
	got, err := retainedObject(ctx, actual, "index", name)
	if err != nil {
		return err
	}
	if got == nil || want == nil {
		return fmt.Errorf("required index %s is missing", name)
	}
	if !strings.EqualFold(got.table, want.table) {
		return fmt.Errorf("index %s belongs to %s, expected %s", name, got.table, want.table)
	}
	wantList, err := retainedIndexListEntryFor(ctx, expected, want.table, want.name)
	if err != nil {
		return err
	}
	gotList, err := retainedIndexListEntryFor(ctx, actual, got.table, got.name)
	if err != nil {
		return err
	}
	if gotList != wantList {
		return fmt.Errorf("index %s uniqueness, origin or partial metadata differs", name)
	}
	wantColumns, err := retainedIndexColumns(ctx, expected, want.name)
	if err != nil {
		return err
	}
	gotColumns, err := retainedIndexColumns(ctx, actual, got.name)
	if err != nil {
		return err
	}
	if len(gotColumns) != len(wantColumns) {
		return fmt.Errorf("index %s columns differ", name)
	}
	for i := range wantColumns {
		if gotColumns[i] != wantColumns[i] {
			return fmt.Errorf("index %s column %d metadata differs", name, i)
		}
	}
	return nil
}

func retainedIndexListEntryFor(ctx context.Context, q upgradeQueryer, table, name string) (retainedIndexListEntry, error) {
	rows, err := q.QueryContext(ctx, "PRAGMA index_list("+quoteIdent(table)+")")
	if err != nil {
		return retainedIndexListEntry{}, fmt.Errorf("inspect retained index list for %s: %w", name, err)
	}
	defer rows.Close()
	var result *retainedIndexListEntry
	for rows.Next() {
		var sequence int
		var candidate string
		var entry retainedIndexListEntry
		if err := rows.Scan(&sequence, &candidate, &entry.unique, &entry.origin, &entry.partial); err != nil {
			return retainedIndexListEntry{}, fmt.Errorf("scan retained index list for %s: %w", name, err)
		}
		if strings.EqualFold(candidate, name) {
			copy := entry
			copy.origin = strings.ToLower(copy.origin)
			result = &copy
		}
	}
	if err := rows.Err(); err != nil {
		return retainedIndexListEntry{}, fmt.Errorf("iterate retained index list for %s: %w", name, err)
	}
	if result == nil {
		return retainedIndexListEntry{}, fmt.Errorf("required index %s is missing from its owner", name)
	}
	return *result, nil
}

func retainedIndexColumns(ctx context.Context, q upgradeQueryer, name string) ([]retainedIndexColumn, error) {
	rows, err := q.QueryContext(ctx, "PRAGMA index_xinfo("+quoteIdent(name)+")")
	if err != nil {
		return nil, fmt.Errorf("inspect retained index %s: %w", name, err)
	}
	defer rows.Close()
	var result []retainedIndexColumn
	for rows.Next() {
		var column retainedIndexColumn
		var nameValue, collation sql.NullString
		if err := rows.Scan(&column.sequence, &column.columnID, &nameValue, &column.descending, &collation, &column.key); err != nil {
			return nil, fmt.Errorf("scan retained index %s: %w", name, err)
		}
		column.nameValid = nameValue.Valid
		column.collationValid = collation.Valid
		if nameValue.Valid {
			column.name = strings.ToLower(nameValue.String)
		}
		if collation.Valid {
			column.collation = strings.ToUpper(collation.String)
		}
		result = append(result, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained index %s: %w", name, err)
	}
	return result, nil
}

func compareRetainedTrigger(ctx context.Context, actual, expected upgradeQueryer, name string) error {
	want, err := retainedObject(ctx, expected, "trigger", name)
	if err != nil {
		return err
	}
	got, err := retainedObject(ctx, actual, "trigger", name)
	if err != nil {
		return err
	}
	if got == nil || want == nil {
		return fmt.Errorf("required trigger %s is missing", name)
	}
	if !strings.EqualFold(got.table, want.table) || retainedDDLSignature(got.sql) != retainedDDLSignature(want.sql) {
		return fmt.Errorf("trigger %s metadata differs", name)
	}
	return nil
}

func retainedObject(ctx context.Context, q upgradeQueryer, objectType, name string) (*retainedSchemaObject, error) {
	rows, err := q.QueryContext(ctx, `SELECT name,tbl_name,COALESCE(sql,'') FROM sqlite_master WHERE lower(type)=? AND lower(name)=?`, strings.ToLower(objectType), strings.ToLower(name))
	if err != nil {
		return nil, fmt.Errorf("inspect retained %s %s: %w", objectType, name, err)
	}
	defer rows.Close()
	var result *retainedSchemaObject
	for rows.Next() {
		if result != nil {
			return nil, fmt.Errorf("retained %s %s is ambiguous", objectType, name)
		}
		var object retainedSchemaObject
		if err := rows.Scan(&object.name, &object.table, &object.sql); err != nil {
			return nil, fmt.Errorf("scan retained %s %s: %w", objectType, name, err)
		}
		result = &object
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained %s %s: %w", objectType, name, err)
	}
	return result, nil
}

func normalizeRetainedSQL(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), ""))
}

func retainedDDLSignature(value string) string {
	var signature strings.Builder
	for _, token := range scanRetainedDDLSignatureTokens(value) {
		signature.WriteString(token)
		signature.WriteByte(0)
	}
	return signature.String()
}

func firstRetainedRuntimeObject(ctx context.Context, q upgradeQueryer) (string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT type, name, COALESCE(sql, '')
		FROM sqlite_master
		ORDER BY type, name
	`)
	if err != nil {
		return "", fmt.Errorf("inspect existing sync runtime objects: %w", err)
	}
	defer rows.Close()

	ownedTables := make(map[string]struct{}, len(syncMetadataTableNames))
	for _, name := range syncMetadataTableNames {
		ownedTables[name] = struct{}{}
	}
	for rows.Next() {
		var objectType, name, sqlText string
		if err := rows.Scan(&objectType, &name, &sqlText); err != nil {
			return "", fmt.Errorf("scan existing sync runtime object: %w", err)
		}
		objectType = strings.ToLower(objectType)
		name = strings.ToLower(name)
		_, managedIndex := retainedManagedIndexes()[name]
		reservedConflictTrigger := name == "_sync_conflict_revision_insert" ||
			name == "_sync_conflict_revision_update" ||
			name == "_sync_conflict_revision_delete"
		if _, owned := ownedTables[name]; owned ||
			(objectType == "index" && managedIndex) ||
			(objectType == "trigger" && reservedConflictTrigger) ||
			(objectType == "trigger" && triggerReferencesRetainedTable(sqlText, ownedTables)) {
			return objectType + " " + name, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate existing sync runtime objects: %w", err)
	}
	return "", nil
}

// scanRetainedDDLSignatureTokens is a bounded signature lexer for runtime-owned
// static trigger DDL. It preserves literals, operators and punctuation and is
// deliberately not used as a general SQL parser.
func scanRetainedDDLSignatureTokens(value string) []string {
	var tokens []string
	for i := 0; i < len(value); {
		switch {
		case value[i] == ' ' || value[i] == '\t' || value[i] == '\r' || value[i] == '\n':
			i++
		case strings.HasPrefix(value[i:], "--"):
			i += 2
			for i < len(value) && value[i] != '\n' && value[i] != '\r' {
				i++
			}
		case strings.HasPrefix(value[i:], "/*"):
			end := strings.Index(value[i+2:], "*/")
			if end < 0 {
				i = len(value)
			} else {
				i += end + 4
			}
		case value[i] == '\'':
			start := i
			i++
			for i < len(value) {
				if value[i] != '\'' {
					i++
					continue
				}
				i++
				if i < len(value) && value[i] == '\'' {
					i++
					continue
				}
				break
			}
			tokens = append(tokens, "string:"+value[start:i])
		case value[i] == '"' || value[i] == '`' || value[i] == '[':
			opening := value[i]
			closing := opening
			if opening == '[' {
				closing = ']'
			}
			i++
			var identifier strings.Builder
			for i < len(value) {
				if value[i] != closing {
					identifier.WriteByte(value[i])
					i++
					continue
				}
				i++
				if opening != '[' && i < len(value) && value[i] == closing {
					identifier.WriteByte(closing)
					i++
					continue
				}
				break
			}
			tokens = append(tokens, strings.ToLower(identifier.String()))
		case sqliteIdentifierStart(value[i]) || value[i] >= '0' && value[i] <= '9':
			start := i
			i++
			for i < len(value) && (sqliteIdentifierContinue(value[i]) || value[i] >= '0' && value[i] <= '9') {
				i++
			}
			tokens = append(tokens, strings.ToLower(value[start:i]))
		default:
			operator := ""
			for _, candidate := range []string{"->>", "||", "==", "!=", "<>", "<=", ">=", "<<", ">>", "->"} {
				if strings.HasPrefix(value[i:], candidate) {
					operator = candidate
					break
				}
			}
			if operator != "" {
				tokens = append(tokens, operator)
				i += len(operator)
			} else {
				tokens = append(tokens, value[i:i+1])
				i++
			}
		}
	}
	for i := 0; i+3 < len(tokens); i++ {
		if (tokens[i] == "trigger" || tokens[i] == "table" || tokens[i] == "index") && tokens[i+1] == "if" && tokens[i+2] == "not" && tokens[i+3] == "exists" {
			tokens = append(tokens[:i+1], tokens[i+4:]...)
			break
		}
	}
	return tokens
}

type sqliteDDLToken struct {
	identifier string
	symbol     byte
	quoted     bool
}

func triggerReferencesRetainedTable(sqlText string, ownedTables map[string]struct{}) bool {
	tokens := scanSQLiteDDLTokens(sqlText)
	inTriggerHeader := true
	triggerTargetSeen := false
	for i, token := range tokens {
		if token.identifier == "" || token.quoted {
			continue
		}
		switch token.identifier {
		case "begin":
			inTriggerHeader = false
		case "from":
			if sqliteFromClauseReferencesOwnedTable(tokens, i+1, ownedTables) {
				return true
			}
		case "join", "into", "in":
			if sqliteTableReferenceAt(tokens, i+1, ownedTables, false) {
				return true
			}
		case "update":
			if sqliteTableReferenceAt(tokens, i+1, ownedTables, true) {
				return true
			}
		case "on":
			if inTriggerHeader && !triggerTargetSeen {
				triggerTargetSeen = true
				if sqliteTableReferenceAt(tokens, i+1, ownedTables, false) {
					return true
				}
			}
		}
	}
	return false
}

func sqliteFromClauseReferencesOwnedTable(tokens []sqliteDDLToken, start int, ownedTables map[string]struct{}) bool {
	expectingTable := true
	depth := 0
	for i := start; i < len(tokens); i++ {
		token := tokens[i]
		if token.symbol != 0 {
			switch token.symbol {
			case '(':
				depth++
				expectingTable = false
			case ')':
				if depth == 0 {
					return false
				}
				depth--
			case ',':
				if depth == 0 {
					expectingTable = true
				}
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if expectingTable {
			if sqliteTableReferenceAt(tokens, i, ownedTables, false) {
				return true
			}
			expectingTable = false
			continue
		}
		if !token.quoted {
			switch token.identifier {
			case "where", "group", "having", "order", "limit", "window", "returning", "union", "except", "intersect", "begin", "end":
				return false
			case "join":
				expectingTable = true
			}
		}
	}
	return false
}

func sqliteTableReferenceAt(tokens []sqliteDDLToken, start int, ownedTables map[string]struct{}, allowUpdateConflict bool) bool {
	if allowUpdateConflict && start+1 < len(tokens) && sqliteDDLKeyword(tokens[start], "or") {
		start += 2
	}
	if start >= len(tokens) || tokens[start].identifier == "" {
		return false
	}
	if _, owned := ownedTables[tokens[start].identifier]; owned {
		return true
	}
	if start+2 >= len(tokens) || tokens[start+1].symbol != '.' || tokens[start+2].identifier == "" {
		return false
	}
	_, owned := ownedTables[tokens[start+2].identifier]
	return owned
}

func sqliteDDLKeyword(token sqliteDDLToken, keyword string) bool {
	return token.identifier == keyword && !token.quoted
}

func scanSQLiteDDLTokens(sqlText string) []sqliteDDLToken {
	tokens := make([]sqliteDDLToken, 0, len(sqlText)/8)
	for i := 0; i < len(sqlText); {
		switch sqlText[i] {
		case '\'', '"', '`', '[':
			opening := sqlText[i]
			closing := opening
			if opening == '[' {
				closing = ']'
			}
			i++
			start := i
			var identifier strings.Builder
			for i < len(sqlText) {
				if sqlText[i] != closing {
					i++
					continue
				}
				if i+1 < len(sqlText) && sqlText[i+1] == closing {
					identifier.WriteString(sqlText[start:i])
					identifier.WriteByte(closing)
					i += 2
					start = i
					continue
				}
				identifier.WriteString(sqlText[start:i])
				i++
				break
			}
			tokens = append(tokens, sqliteDDLToken{identifier: strings.ToLower(identifier.String()), quoted: true})
		case '-':
			if i+1 < len(sqlText) && sqlText[i+1] == '-' {
				i += 2
				for i < len(sqlText) && sqlText[i] != '\n' && sqlText[i] != '\r' {
					i++
				}
				continue
			}
			i++
		case '/':
			if i+1 < len(sqlText) && sqlText[i+1] == '*' {
				i += 2
				for i+1 < len(sqlText) && !(sqlText[i] == '*' && sqlText[i+1] == '/') {
					i++
				}
				if i+1 < len(sqlText) {
					i += 2
				}
				continue
			}
			i++
		case '.', ',', '(', ')', ';':
			tokens = append(tokens, sqliteDDLToken{symbol: sqlText[i]})
			i++
		default:
			if sqliteIdentifierStart(sqlText[i]) {
				start := i
				i++
				for i < len(sqlText) && sqliteIdentifierContinue(sqlText[i]) {
					i++
				}
				tokens = append(tokens, sqliteDDLToken{identifier: strings.ToLower(sqlText[start:i])})
				continue
			}
			i++
		}
	}
	return tokens
}

func sqliteIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= 0x80
}

func sqliteIdentifierContinue(ch byte) bool {
	return sqliteIdentifierStart(ch) || ch >= '0' && ch <= '9' || ch == '$'
}

func readRetainedContract(ctx context.Context, q upgradeQueryer, config *Config, shape string) error {
	if err:=reconcileWorkspaceOwners(ctx,q,config); err!=nil { return err }
	if err := checkRetainedApplyWorkspace(ctx, q, config); err != nil {
		return err
	}
	var databaseID, serviceID, storedShape, fingerprint string
	var generationID, version int64
	if err := q.QueryRowContext(ctx, `SELECT database_id,service_id,generation_id,local_version,physical_shape,fingerprint FROM _sync_contract_identity WHERE singleton_key=1`).Scan(&databaseID, &serviceID, &generationID, &version, &storedShape, &fingerprint); err != nil {
		return fmt.Errorf("retained contract identity is missing or corrupt: %w", err)
	}
	if databaseID != config.DatabaseID || serviceID != config.ServiceID || generationID != int64(config.GenerationID) || version != config.LocalVersion || storedShape != shape {
		return fmt.Errorf("installed identity or managed physical schema changed; an explicit supported upgrade is required")
	}
	var descriptor, digest, admittedFingerprint string
	err := q.QueryRowContext(ctx, `SELECT descriptor,descriptor_hash,fingerprint FROM _sync_admitted_contracts WHERE generation_id=? AND length(CAST(descriptor AS BLOB))<=65536 AND length(fingerprint)=64 AND length(descriptor_hash)=64`, generationID).Scan(&descriptor, &digest, &admittedFingerprint)
	if fingerprint == "" {
		var admissions int
		if countErr := q.QueryRowContext(ctx, `SELECT count(*) FROM _sync_admitted_contracts`).Scan(&admissions); countErr != nil {
			return countErr
		}
		if err != sql.ErrNoRows || admissions != 0 {
			return fmt.Errorf("unconfirmed contract has inconsistent admission state")
		}
		var binding string
		if err := q.QueryRowContext(ctx, `SELECT binding_state FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&binding); err != nil {
			return err
		}
		if binding != "anonymous" {
			return fmt.Errorf("attached state is missing its admitted contract")
		}
		return nil
	}
	if err != nil || admittedFingerprint != fingerprint || contractBytesHash(descriptor) != digest {
		return fmt.Errorf("admitted contract is missing or corrupt")
	}
	response, err := decodeDiscoveredContract([]byte(descriptor), config.GenerationID)
	if err != nil {
		return err
	}
	if response.SchemaGeneration.Fingerprint != fingerprint {
		return fmt.Errorf("admitted contract fingerprint differs from installed identity")
	}
	local, err := deriveLocalContract(ctx, q, config.GenerationID, config.Tables, response.SchemaGeneration)
	if err != nil {
		return err
	}
	config.localContract, config.generationProof, config.discovery = &local, local.Proof, response
	return nil
}

func contractBytesHash(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(digest[:])
}

// Capture metadata from SQLite itself, including FK actions and table flags.
// Unrelated application tables do not become part of the sync contract.
func managedPhysicalShape(ctx context.Context, q upgradeQueryer, tables []SyncTable) (string, error) {
	ordered := append([]SyncTable(nil), tables...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].TableName < ordered[j].TableName })
	shape := make([]any, 0, len(ordered))
	for _, table := range ordered {
		keys, err := normalizedSyncKeyColumns(table)
		if err != nil {
			return "", err
		}
		var kind string
		var withoutRowID, strict int
		if err := q.QueryRowContext(ctx, `SELECT type,wr,strict FROM pragma_table_list WHERE schema='main' AND name=?`, table.TableName).Scan(&kind, &withoutRowID, &strict); err != nil {
			return "", err
		}
		if kind != "table" {
			return "", fmt.Errorf("managed %s must be an ordinary SQLite table", table.TableName)
		}
		entry := []any{table.TableName, keys, withoutRowID, strict}
		for _, query := range []string{`SELECT name,type,"notnull",pk,hidden FROM pragma_table_xinfo(?) ORDER BY name`, `SELECT "from","to","table",on_update,on_delete,match,seq FROM pragma_foreign_key_list(?) ORDER BY "from","to","table",seq`} {
			facts, err := physicalMetadataRows(ctx, q, query, table.TableName)
			if err != nil {
				return "", err
			}
			entry = append(entry, facts)
		}
		shape = append(shape, entry)
	}
	raw, err := jcs.Marshal(shape)
	return string(raw), err
}

func physicalMetadataRows(ctx context.Context, q upgradeQueryer, query, table string) ([][]any, error) {
	rows, err := q.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	facts := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		facts = append(facts, values)
	}
	return facts, rows.Err()
}

func (c *Client) schemaForTable(table string) string {
	if c.localGeneration != nil {
		for _, registered := range c.localGeneration.localTables {
			if registered.Table == table {
				return registered.Schema
			}
		}
	}
	return c.config.schema
}

// Adapt the existing schema reader to the retained opening's read-only connection.
type retainedTableQuery struct {
	ctx context.Context
	q   upgradeQueryer
}

func (r retainedTableQuery) Query(query string, args ...any) (*sql.Rows, error) {
	return r.q.QueryContext(r.ctx, query, args...)
}

func validateRetainedCaptureTriggers(ctx context.Context, q upgradeQueryer) error {
	rows, err := q.QueryContext(ctx, `SELECT schema_name,table_name FROM _sync_managed_tables`)
	if err != nil {
		return err
	}
	type managed struct{ schema, table string }
	var tables []managed
	for rows.Next() {
		var m managed
		if err := rows.Scan(&m.schema, &m.table); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, m)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	provider := NewTableInfoProvider()
	allowed := map[string]struct{}{}
	for name := range baselineMetadataTriggerSQL() {
		allowed[name] = struct{}{}
	}
	for _, op := range []string{"insert", "update", "delete"} {
		allowed["_sync_conflict_revision_"+op] = struct{}{}
	}
	for _, m := range tables {
		info, err := provider.Get(&retainedTableQuery{ctx, q}, m.table)
		if err != nil {
			return err
		}
		key := ""
		for _, col := range info.Columns {
			if col.IsPrimaryKey {
				key = col.Name
			}
		}
		expected, err := expectedTriggerSQLByName(m.schema, SyncTable{TableName: m.table, SyncKeyColumnName: key}, info)
		if err != nil {
			return err
		}
		for name, sqlText := range expected {
			allowed[name] = struct{}{}
			actual, err := retainedObject(ctx, q, "trigger", name)
			if err != nil {
				return err
			}
			if actual == nil || retainedDDLSignature(actual.sql) != retainedDDLSignature(sqlText) {
				return fmt.Errorf("incompatible retained capture trigger %s", name)
			}
		}
	}
	return rejectUnexpectedCaptureTriggers(ctx, q, allowed)
}

// Scan non-capture triggers separately from required trigger/body validation.
func rejectUnexpectedCaptureTriggers(ctx context.Context, q upgradeQueryer, allowed map[string]struct{}) error {
	stageTriggers := stageAccountingDDL()
	ownedTables := map[string]struct{}{}
	for _, name := range syncMetadataTableNames {
		ownedTables[name] = struct{}{}
	}
	triggers, err := q.QueryContext(ctx, `SELECT name,tbl_name,COALESCE(sql,'') FROM sqlite_master WHERE type='trigger'`)
	if err != nil {
		return err
	}
	defer triggers.Close()
	for triggers.Next() {
		var name, table, body string
		if err := triggers.Scan(&name, &table, &body); err != nil {
			return err
		}
		if expected, ok := stageTriggers[name]; ok {
			if !sameTriggerSQL(body, expected) {
				return fmt.Errorf("incompatible retained stage trigger %s", name)
			}
			continue
		}
		if strings.HasPrefix(strings.ToLower(name), "_sync_upgrade_guard_") {
			_, retainedTable := ownedTables[strings.ToLower(table)]
			if retainedTable || !isCanonicalLocalOnlyUpgradeGuard(name, table, body) {
				return fmt.Errorf("incompatible retained local-only upgrade guard %s", name)
			}
			continue
		}
		// Opening can publish guards before ordinary capture registers the table.
		// Admit only the exact existing guard bodies, never an arbitrary metadata reader.
		guardOnly := false
		for suffix, template := range map[string]string{"bi_guard": guardInsertTriggerTemplate, "bu_guard": guardUpdateTriggerTemplate, "bd_guard": guardDeleteTriggerTemplate} {
			if name == "trg_"+table+"_"+suffix && sameTriggerSQL(body, strings.ReplaceAll(template, "{{.TableName}}", table)) {
				guardOnly = true
			}
		}
		if _, ok := allowed[strings.ToLower(name)]; !ok && !guardOnly && triggerReferencesRetainedTable(body, ownedTables) {
			return fmt.Errorf("unexpected retained trigger %s", name)
		}
	}
	return triggers.Err()
}

// Construction recognizes the existing opening owner; the opening lifecycle
// still verifies the complete required guard set before advancing or removing it.
func isCanonicalLocalOnlyUpgradeGuard(name, table, body string) bool {
	for _, guard := range localOnlyUpgradeGuards([]string{table}) {
		if name == guard.Name && body == guard.SQL {
			return true
		}
	}
	return false
}

func retainedManagedIndexes() map[string]struct{} {
	result := make(map[string]struct{}, len(retainedRuntimeManagedIndexes)+len(pushIndexDDL))
	for name := range retainedRuntimeManagedIndexes {
		result[name] = struct{}{}
	}
	for name := range pushIndexDDL {
		result[name] = struct{}{}
	}
	return result
}
