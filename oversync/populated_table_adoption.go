// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
)

const adoptionSourcePrefix = "oversync-adoption:"

const adoptionScopeBatchSize = 32

type PopulatedTableAdoptionError struct {
	Reason           string
	UserID           string
	Table            string
	BusinessRowCount int64
	RowStateCount    int64
	Detail           string
}

func (e *PopulatedTableAdoptionError) Error() string {
	if e == nil {
		return "populated-table adoption failed"
	}
	location := ""
	if e.UserID != "" {
		location = " for scope " + e.UserID
	}
	if e.Table != "" {
		location += " at " + e.Table
	}
	return fmt.Sprintf(
		"populated-table adoption %s%s (business_rows=%d row_state=%d): %s",
		e.Reason,
		location,
		e.BusinessRowCount,
		e.RowStateCount,
		e.Detail,
	)
}

type adoptionBusinessRow struct {
	tableInfo   registeredTableRuntimeInfo
	keyText     []byte
	keyBytes    []byte
	payloadWire []byte
}

type adoptionLogicalRowKey struct {
	tableID  int32
	keyBytes string
}

func newAdoptionLogicalRowKey(tableID int32, keyBytes []byte) adoptionLogicalRowKey {
	return adoptionLogicalRowKey{tableID: tableID, keyBytes: string(keyBytes)}
}

type adoptionScopeState struct {
	found                bool
	userPK               int64
	nextBundleSeq        int64
	retainedBundleFloor  int64
	scopeStateCode       sql.NullInt16
	rowStateCount        int64
	liveRowStateCount    int64
	bundleCount          int64
	maxBundleSeq         sql.NullInt64
	sourceCount          int64
	sourceMismatchCount  int64
	snapshotSessionCount int64
	captureStageCount    int64
}

type adoptionRetainedBundle struct {
	bundleSeq  int64
	rowCount   int64
	byteCount  int64
	bundleHash []byte
}

type adoptionRetainedRow struct {
	op          string
	payloadWire []byte
}

type adoptionRetainedEvidence struct {
	count int64
	row   adoptionRetainedRow
}

type adoptionCurrentRowState struct {
	tableID   int32
	keyBytes  []byte
	bundleSeq int64
	deleted   bool
}

func (s *SyncService) lockRegisteredTablesForAdoption(ctx context.Context, tx pgx.Tx) error {
	if s == nil {
		return nil
	}
	return lockRegisteredTables(ctx, tx, s.activeRegisteredTableDeclarations())
}

func lockRegisteredTables(ctx context.Context, tx pgx.Tx, tables []RegisteredTable) error {
	targetsByKey := make(map[string]registeredTableTriggerTarget)
	for _, table := range tables {
		targets, err := loadRegisteredTableTriggerTargets(ctx, tx, table)
		if err != nil {
			return err
		}
		for _, target := range targets {
			targetsByKey[Key(target.schemaName, target.tableName)] = target
		}
	}
	targets := make([]registeredTableTriggerTarget, 0, len(targetsByKey))
	for _, target := range targetsByKey {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].schemaName == targets[j].schemaName {
			return targets[i].tableName < targets[j].tableName
		}
		return targets[i].schemaName < targets[j].schemaName
	})
	for _, target := range targets {
		ident := pgx.Identifier{target.schemaName, target.tableName}.Sanitize()
		if _, err := tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE`, ident)); err != nil {
			return fmt.Errorf("lock registered table %s.%s for adoption: %w", target.schemaName, target.tableName, err)
		}
	}
	return nil
}

type adoptedScopeSummary struct {
	userID   string
	rowCount int64
}

func (s *SyncService) adoptFreshPopulatedRegisteredTables(ctx context.Context, tx pgx.Tx, attempt int) (err error) {
	operation := "bootstrap"
	timingEnabled := s.stageTimingEnabled()
	var (
		businessScanDuration time.Duration
		coherentDuration     time.Duration
		persistenceDuration  time.Duration
		businessScanHadError bool
		coherentHadError     bool
		persistenceHadError  bool
		businessRowCount     int64
		processedScopeCount  int
		adoptedScopes        []adoptedScopeSummary
	)
	defer func() {
		s.observeStageDuration(ctx, operation, "business_row_scan_canonicalization", businessScanDuration, int(businessRowCount), attempt, businessScanHadError)
		s.observeStageDuration(ctx, operation, "coherent_state_history_validation", coherentDuration, processedScopeCount, attempt, coherentHadError)
		s.observeStageDuration(ctx, operation, "pristine_baseline_persistence", persistenceDuration, len(adoptedScopes), attempt, persistenceHadError)
	}()

	discoveryStartedAt := s.stageStart()
	userIDs, err := s.loadAdoptionUserIDs(ctx, tx)
	if err != nil {
		s.observeStageErr(ctx, operation, "scope_state_discovery", discoveryStartedAt, 0, attempt, err)
		return err
	}
	states, err := loadAdoptionScopeStates(ctx, tx, userIDs)
	if err != nil {
		s.observeStageErr(ctx, operation, "scope_state_discovery", discoveryStartedAt, len(userIDs), attempt, err)
		return err
	}
	for _, state := range states {
		if state.found {
			err := fmt.Errorf("internal invariant: managed scope state exists at fresh populated-adoption entry")
			s.observeStageErr(ctx, operation, "scope_state_discovery", discoveryStartedAt, len(userIDs), attempt, err)
			return err
		}
	}
	s.observeStageErr(ctx, operation, "scope_state_discovery", discoveryStartedAt, len(userIDs), attempt, nil)

	progress := newBootstrapProgress(s, len(userIDs))
	coherentProgress := func() {
		progress.maybeLog("coherent_state_history_validation", processedScopeCount, businessRowCount, len(adoptedScopes))
	}
	persistenceProgress := func() {
		progress.maybeLog("pristine_baseline_persistence", processedScopeCount, businessRowCount, len(adoptedScopes))
	}
	infos := s.sortedAdoptionTableInfos()
	for batchStart := 0; batchStart < len(userIDs); batchStart += adoptionScopeBatchSize {
		batchEnd := min(batchStart+adoptionScopeBatchSize, len(userIDs))
		batchUserIDs := userIDs[batchStart:batchEnd]
		batchStartedAt := time.Time{}
		coherentBefore := coherentDuration
		persistenceBefore := persistenceDuration
		callbackHadError := false
		if timingEnabled {
			batchStartedAt = time.Now()
		}
		reader, readerErr := newAdoptionBusinessScopeReader(ctx, tx, infos, s.columnTypesByTable, batchUserIDs, func(schemaName, tableName string, payload []byte) ([]byte, error) {
			canonical, canonicalErr := s.canonicalizeWirePayload(schemaName, tableName, payload)
			if canonicalErr != nil {
				return nil, canonicalErr
			}
			if s.adoptionHooks != nil {
				if s.adoptionHooks.onCanonicalized != nil {
					s.adoptionHooks.onCanonicalized(attempt)
				}
				if s.adoptionHooks.afterCanonicalize != nil {
					if hookErr := s.adoptionHooks.afterCanonicalize(attempt); hookErr != nil {
						return nil, hookErr
					}
				}
			}
			return canonical, nil
		}, s.config.MaxBytesPerBundle)
		if readerErr != nil {
			businessScanHadError = true
			if timingEnabled {
				businessScanDuration += time.Since(batchStartedAt)
			}
			return readerErr
		}
		reader.spoolOwner = newAdoptionSpoolOwner(s.adoptionSpoolDir)
		reader.maxRowsPerBundle = s.config.MaxRowsPerBundle
		reader.onProgress = func(scannedRows int64) {
			businessRowCount += scannedRows
			progress.maybeLog("business_row_scan_canonicalization", processedScopeCount, businessRowCount, len(adoptedScopes))
		}
		if s.adoptionHooks != nil && s.adoptionHooks.afterFreshBusinessRowsRead != nil {
			reader.afterBusinessRowsRead = func() error {
				return s.adoptionHooks.afterFreshBusinessRowsRead(ctx, tx, attempt)
			}
		}
		if s.adoptionHooks != nil {
			if s.adoptionHooks.onReaderCreated != nil {
				s.adoptionHooks.onReaderCreated(attempt, reader)
			}
			if s.adoptionHooks.onSpoolCreated != nil {
				reader.onSpoolCreated = func(path string) {
					s.adoptionHooks.onSpoolCreated(attempt, path)
				}
			}
			if s.adoptionHooks.onSpoolRemoved != nil {
				reader.onSpoolRemoved = func(path string, bytes int64, cleanupErr error) {
					s.adoptionHooks.onSpoolRemoved(attempt, path, bytes, cleanupErr)
				}
			}
		}
		readerErr = reader.forEach(func(userID string, rows []adoptionBusinessRow) error {
			businessRows := int64(len(rows))
			if businessRows > 0 {
				persistStartedAt := time.Time{}
				if timingEnabled {
					persistStartedAt = time.Now()
				}
				persistedState, persistErr := s.persistAdoptionBaseline(ctx, tx, userID, rows, persistenceProgress)
				if timingEnabled {
					persistenceDuration += time.Since(persistStartedAt)
				}
				if persistErr != nil {
					persistenceHadError = true
					callbackHadError = true
					return persistErr
				}

				validationStartedAt := time.Time{}
				if timingEnabled {
					validationStartedAt = time.Now()
				}
				validateErr := s.validateCoherentAdoptionScope(ctx, tx, userID, rows, persistedState, coherentProgress)
				if timingEnabled {
					coherentDuration += time.Since(validationStartedAt)
				}
				if validateErr != nil {
					coherentHadError = true
					callbackHadError = true
					return validateErr
				}
				adoptedScopes = append(adoptedScopes, adoptedScopeSummary{userID: userID, rowCount: businessRows})
			}
			processedScopeCount++
			coherentProgress()
			return nil
		})
		if timingEnabled {
			batchDuration := time.Since(batchStartedAt)
			callbackDuration := coherentDuration - coherentBefore + persistenceDuration - persistenceBefore
			if callbackDuration < batchDuration {
				businessScanDuration += batchDuration - callbackDuration
			}
		}
		if readerErr != nil {
			if !callbackHadError {
				businessScanHadError = true
			}
			return readerErr
		}
	}

	if len(adoptedScopes) > 0 {
		validationStartedAt := time.Time{}
		if timingEnabled {
			validationStartedAt = time.Now()
		}
		finalValidationErr := func() error {
			adoptedUserIDs := make([]string, len(adoptedScopes))
			for i, adopted := range adoptedScopes {
				adoptedUserIDs[i] = adopted.userID
			}
			finalStates, loadErr := loadAdoptionScopeStates(ctx, tx, adoptedUserIDs)
			if loadErr != nil {
				return loadErr
			}
			for _, adopted := range adoptedScopes {
				coherentProgress()
				if validateErr := validatePersistedAdoptionState(adopted, finalStates[adopted.userID]); validateErr != nil {
					return validateErr
				}
			}
			return nil
		}()
		if timingEnabled {
			coherentDuration += time.Since(validationStartedAt)
		}
		if finalValidationErr != nil {
			coherentHadError = true
			return finalValidationErr
		}
	}

	s.logger.Info("Populated registered-table adoption validated",
		"registered_table_count", len(s.registeredTableByID),
		"scope_count", len(userIDs),
		"adopted_scope_count", len(adoptedScopes),
		"business_row_count", businessRowCount)
	return nil
}

func (s *SyncService) sortedAdoptionTableInfos() []registeredTableRuntimeInfo {
	infos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].tableID < infos[j].tableID })
	return infos
}

type adoptionPayloadCanonicalizer func(schemaName, tableName string, payload []byte) ([]byte, error)

type adoptionProgressFunc func()

type adoptionTestHooks struct {
	onReaderCreated            func(attempt int, reader *adoptionBusinessScopeReader)
	onCanonicalized            func(attempt int)
	afterCanonicalize          func(attempt int) error
	afterFreshBusinessRowsRead func(ctx context.Context, tx pgx.Tx, attempt int) error
	onCommittedBundleHash      func()
	onSpoolCreated             func(attempt int, path string)
	onSpoolRemoved             func(attempt int, path string, bytes int64, cleanupErr error)
}

type adoptionSpoolFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Name() string
	Stat() (os.FileInfo, error)
}

type adoptionSpoolOwner struct {
	create func() (adoptionSpoolFile, error)
	remove func(string) error
}

func newAdoptionSpoolOwner(dir string) adoptionSpoolOwner {
	return adoptionSpoolOwner{
		create: func() (adoptionSpoolFile, error) {
			return os.CreateTemp(dir, "oversync-adoption-")
		},
		remove: os.Remove,
	}
}

type adoptionBusinessScopeReader struct {
	ctx                     context.Context
	rows                    pgx.Rows
	infosByID               map[int32]registeredTableRuntimeInfo
	userIDs                 []string
	canonicalize            adoptionPayloadCanonicalizer
	spoolOwner              adoptionSpoolOwner
	afterBusinessRowsRead   func() error
	afterBoundedRowScan     func(keyTextValid bool, keyByteCount int64, payloadPresent bool, payloadByteCount int64)
	onProgress              func(scannedRows int64)
	onSpoolCreated          func(path string)
	onSpoolRemoved          func(path string, bytes int64, cleanupErr error)
	maxRowsPerBundle        int
	maxBytesPerBundle       int
	maxPhysicalPayloadBytes int64
}

func newAdoptionBusinessScopeReader(
	ctx context.Context,
	tx pgx.Tx,
	infos []registeredTableRuntimeInfo,
	columnTypesByTable map[string]map[string]string,
	userIDs []string,
	canonicalize adoptionPayloadCanonicalizer,
	maxBytesPerBundle int,
) (*adoptionBusinessScopeReader, error) {
	maxPhysicalPayloadBytes := int64(0)
	for _, info := range infos {
		tableLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(int64(maxBytesPerBundle), columnTypesByTable[Key(info.schemaName, info.tableName)])
		if err != nil {
			return nil, err
		}
		maxPhysicalPayloadBytes = max(maxPhysicalPayloadBytes, tableLimit)
	}
	if maxPhysicalPayloadBytes == 0 {
		var err error
		maxPhysicalPayloadBytes, err = physicalStoredPushPayloadByteLimit(int64(maxBytesPerBundle))
		if err != nil {
			return nil, err
		}
	}
	reader := &adoptionBusinessScopeReader{
		ctx:                     ctx,
		infosByID:               make(map[int32]registeredTableRuntimeInfo, len(infos)),
		userIDs:                 userIDs,
		canonicalize:            canonicalize,
		spoolOwner:              newAdoptionSpoolOwner(""),
		maxBytesPerBundle:       maxBytesPerBundle,
		maxPhysicalPayloadBytes: maxPhysicalPayloadBytes,
	}
	for _, info := range infos {
		reader.infosByID[info.tableID] = info
	}
	if len(userIDs) == 0 || len(infos) == 0 {
		return reader, nil
	}

	var query strings.Builder
	query.WriteString(`
		/* oversync:adoption-business-scan */
		SELECT requested.scope_ordinal, business.table_id,
			CASE WHEN business.key_byte_count <= $2 THEN business.key_text END,
			business.key_byte_count,
			CASE WHEN business.payload_byte_count <= business.physical_payload_limit THEN business.payload_db END,
			business.payload_byte_count
		FROM (
			SELECT scope_id, scope_ordinal
			FROM unnest($1::text[]) WITH ORDINALITY AS input(scope_id, scope_ordinal)
			ORDER BY scope_ordinal
		) AS requested
		CROSS JOIN LATERAL (
	`)
	for i, info := range infos {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		payloadExpression := databaseWirePayloadExpression("src", columnTypesByTable[Key(info.schemaName, info.tableName)])
		tablePhysicalLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(int64(maxBytesPerBundle), columnTypesByTable[Key(info.schemaName, info.tableName)])
		if err != nil {
			return nil, err
		}
		keyOrder := fmt.Sprintf("convert_to(CAST(src.%s AS text), 'UTF8')", keyIdent)
		if info.syncKeyType == syncKeyTypeUUID {
			keyOrder = fmt.Sprintf("uuid_send(src.%s)", keyIdent)
		}
		fmt.Fprintf(&query, `(
			SELECT %d::integer AS table_id,
				CAST(src.%s AS text) AS key_text,
				octet_length(CAST(src.%s AS text)) AS key_byte_count,
				%s AS key_order,
				%s AS payload_db,
				octet_length((%s)::text) AS payload_byte_count,
				%d::bigint AS physical_payload_limit
			FROM %s AS src
			WHERE src.%s = requested.scope_id
			ORDER BY key_order
		)`, info.tableID, keyIdent, keyIdent, keyOrder, payloadExpression, payloadExpression, tablePhysicalLimit, tableIdent, ownerIdent)
	}
	query.WriteString(`
		) AS business
		ORDER BY requested.scope_ordinal, business.table_id, business.key_order
	`)

	rows, err := tx.Query(ctx, query.String(), userIDs, maxBytesPerBundle)
	if err != nil {
		return nil, fmt.Errorf("query bounded populated-table adoption batch: %w", err)
	}
	reader.rows = rows
	return reader, nil
}

func (r *adoptionBusinessScopeReader) forEach(consume func(userID string, rows []adoptionBusinessRow) error) (returnErr error) {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if len(r.userIDs) == 0 {
		return nil
	}
	if r.rows == nil {
		for _, userID := range r.userIDs {
			if err := r.ctx.Err(); err != nil {
				return err
			}
			if err := consume(userID, nil); err != nil {
				return err
			}
		}
		return nil
	}
	defer r.rows.Close()

	owner := r.spoolOwner
	if owner.create == nil || owner.remove == nil {
		owner = newAdoptionSpoolOwner("")
	}
	spool, err := owner.create()
	if err != nil {
		return fmt.Errorf("create populated-table adoption spool: %w", err)
	}
	spoolName := spool.Name()
	if r.onSpoolCreated != nil {
		r.onSpoolCreated(spoolName)
	}
	defer func() {
		var spoolBytes int64
		var cleanupErr error
		if info, statErr := spool.Stat(); statErr == nil {
			spoolBytes = info.Size()
		} else {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stat populated-table adoption spool: %w", statErr))
		}
		if closeErr := spool.Close(); closeErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close populated-table adoption spool: %w", closeErr))
		}
		if removeErr := owner.remove(spoolName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove populated-table adoption spool: %w", removeErr))
		}
		if r.onSpoolRemoved != nil {
			r.onSpoolRemoved(spoolName, spoolBytes, cleanupErr)
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()
	writer := bufio.NewWriterSize(spool, adoptionSpoolBufferSize)
	rowCounts := make([]int, len(r.userIDs))
	bundleHashers := make([]*protocolhash.CommittedBundleHasher, len(r.userIDs))
	currentOrdinal := int64(0)

	for r.rows.Next() {
		var (
			scopeOrdinal int64
			tableID      int32
			keyText      sql.NullString
			keyByteCount sql.NullInt64
			payloadDB    []byte
			payloadBytes int64
		)
		if err := r.rows.Scan(&scopeOrdinal, &tableID, &keyText, &keyByteCount, &payloadDB, &payloadBytes); err != nil {
			return fmt.Errorf("scan bounded populated-table adoption batch: %w", err)
		}
		if r.afterBoundedRowScan != nil {
			reportedKeyBytes := int64(-1)
			if keyByteCount.Valid {
				reportedKeyBytes = keyByteCount.Int64
			}
			r.afterBoundedRowScan(keyText.Valid, reportedKeyBytes, payloadDB != nil, payloadBytes)
		}
		if scopeOrdinal < currentOrdinal || scopeOrdinal < 1 || scopeOrdinal > int64(len(r.userIDs)) {
			return fmt.Errorf("bounded populated-table adoption batch returned invalid scope ordinal %d", scopeOrdinal)
		}
		if scopeOrdinal != currentOrdinal {
			currentOrdinal = scopeOrdinal
		}
		info, ok := r.infosByID[tableID]
		if !ok {
			return &PopulatedTableAdoptionError{
				Reason: "identity_unrepresentable",
				UserID: r.userIDs[currentOrdinal-1],
				Detail: fmt.Sprintf("registered row has unknown table identity %d", tableID),
			}
		}
		if keyByteCount.Valid && (keyByteCount.Int64 < 0 || r.maxBytesPerBundle <= 0 || keyByteCount.Int64 > int64(r.maxBytesPerBundle)) {
			return projectionError(BundleLimitExceeded, "adoption", "adoption baseline exceeds configured bundle limits")
		}
		if !keyText.Valid || !keyByteCount.Valid {
			return &PopulatedTableAdoptionError{
				Reason: "identity_unrepresentable",
				UserID: r.userIDs[currentOrdinal-1],
				Table:  Key(info.schemaName, info.tableName),
				Detail: "registered row has a NULL sync key",
			}
		}
		scopeIndex := int(scopeOrdinal - 1)
		if r.maxRowsPerBundle > 0 && rowCounts[scopeIndex] >= r.maxRowsPerBundle {
			return projectionError(BundleLimitExceeded, "adoption", "adoption baseline exceeds configured bundle limits")
		}
		if payloadBytes < 0 || r.maxPhysicalPayloadBytes <= 0 || payloadBytes > r.maxPhysicalPayloadBytes || payloadDB == nil {
			return projectionError(BundleLimitExceeded, "adoption", "adoption baseline exceeds configured bundle limits")
		}
		keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyText.String)
		if err != nil {
			return &PopulatedTableAdoptionError{
				Reason: "identity_unrepresentable",
				UserID: r.userIDs[currentOrdinal-1],
				Table:  Key(info.schemaName, info.tableName),
				Detail: err.Error(),
			}
		}
		payloadWire, err := r.canonicalize(info.schemaName, info.tableName, payloadDB)
		if err != nil {
			return &PopulatedTableAdoptionError{
				Reason: "baseline_unrepresentable",
				UserID: r.userIDs[currentOrdinal-1],
				Table:  Key(info.schemaName, info.tableName),
				Detail: err.Error(),
			}
		}
		row := adoptionBusinessRow{
			tableInfo:   info,
			keyText:     []byte(keyText.String),
			keyBytes:    keyBytes,
			payloadWire: payloadWire,
		}
		if bundleHashers[scopeIndex] == nil {
			bundleHashers[scopeIndex] = protocolhash.NewCommittedBundleHasher()
		}
		if err := bundleHashers[scopeIndex].Add(protocolhash.BundleRow{
			Schema: info.schemaName, Table: info.tableName,
			Key: SyncKey{info.syncKeyColumn: keyText.String}, Op: OpInsert,
			RowVersion: 1, Payload: payloadWire,
		}); err != nil {
			return &PopulatedTableAdoptionError{Reason: "baseline_unrepresentable", UserID: r.userIDs[scopeIndex], Table: Key(info.schemaName, info.tableName), Detail: err.Error()}
		}
		if r.maxBytesPerBundle > 0 && bundleHashers[scopeIndex].CurrentByteCount() > int64(r.maxBytesPerBundle) {
			return projectionError(BundleLimitExceeded, "adoption", "adoption baseline exceeds configured bundle limits")
		}
		if err := writeAdoptionSpoolRow(writer, row); err != nil {
			return err
		}
		rowCounts[scopeIndex]++
		if r.onProgress != nil {
			r.onProgress(1)
		}
	}
	if err := r.rows.Err(); err != nil {
		return fmt.Errorf("iterate bounded populated-table adoption batch: %w", err)
	}
	r.rows.Close()
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush populated-table adoption spool: %w", err)
	}
	if r.afterBusinessRowsRead != nil {
		if err := r.afterBusinessRowsRead(); err != nil {
			return err
		}
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind populated-table adoption spool: %w", err)
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(spool, adoptionSpoolBufferSize)
	for scopeIndex, userID := range r.userIDs {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		scopeRows := make([]adoptionBusinessRow, 0, rowCounts[scopeIndex])
		for range rowCounts[scopeIndex] {
			row, err := readAdoptionSpoolRow(reader, r.infosByID)
			if err != nil {
				return err
			}
			if r.onProgress != nil {
				r.onProgress(0)
			}
			if err := r.ctx.Err(); err != nil {
				return err
			}
			if len(scopeRows) > 0 {
				previous := scopeRows[len(scopeRows)-1]
				keyComparison := bytes.Compare(previous.keyBytes, row.keyBytes)
				switch {
				case previous.tableInfo.tableID > row.tableInfo.tableID,
					previous.tableInfo.tableID == row.tableInfo.tableID && keyComparison > 0:
					return fmt.Errorf("populated-table adoption spool is not ordered by table and key")
				case previous.tableInfo.tableID == row.tableInfo.tableID && keyComparison == 0:
					return &PopulatedTableAdoptionError{
						Reason: "duplicate_identity",
						UserID: userID,
						Table:  Key(row.tableInfo.schemaName, row.tableInfo.tableName),
						Detail: "duplicate logical scope/table/key identity",
					}
				}
			}
			scopeRows = append(scopeRows, row)
		}
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if err := consume(userID, scopeRows); err != nil {
			return err
		}
	}
	return nil
}

const adoptionSpoolHeaderSize = 4 + 8 + 8 + 8

const adoptionSpoolBufferSize = 1 << 20

func writeAdoptionSpoolRow(writer io.Writer, row adoptionBusinessRow) error {
	var header [adoptionSpoolHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(row.tableInfo.tableID))
	binary.LittleEndian.PutUint64(header[4:12], uint64(len(row.keyText)))
	binary.LittleEndian.PutUint64(header[12:20], uint64(len(row.keyBytes)))
	binary.LittleEndian.PutUint64(header[20:28], uint64(len(row.payloadWire)))
	if _, err := writer.Write(header[:]); err != nil {
		return fmt.Errorf("write populated-table adoption spool header: %w", err)
	}
	for _, value := range [][]byte{row.keyText, row.keyBytes, row.payloadWire} {
		if _, err := writer.Write(value); err != nil {
			return fmt.Errorf("write populated-table adoption spool row: %w", err)
		}
	}
	return nil
}

func readAdoptionSpoolRow(reader io.Reader, infosByID map[int32]registeredTableRuntimeInfo) (adoptionBusinessRow, error) {
	var header [adoptionSpoolHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return adoptionBusinessRow{}, fmt.Errorf("read populated-table adoption spool header: %w", err)
	}
	tableID := int32(binary.LittleEndian.Uint32(header[0:4]))
	info, ok := infosByID[tableID]
	if !ok {
		return adoptionBusinessRow{}, fmt.Errorf("read populated-table adoption spool with unknown table identity %d", tableID)
	}
	keyTextLength := binary.LittleEndian.Uint64(header[4:12])
	keyBytesLength := binary.LittleEndian.Uint64(header[12:20])
	payloadLength := binary.LittleEndian.Uint64(header[20:28])
	maxInt := uint64(^uint(0) >> 1)
	if keyTextLength > maxInt || keyBytesLength > maxInt-keyTextLength || payloadLength > maxInt-keyTextLength-keyBytesLength {
		return adoptionBusinessRow{}, fmt.Errorf("populated-table adoption spool value is too large")
	}
	value := make([]byte, int(keyTextLength+keyBytesLength+payloadLength))
	if _, err := io.ReadFull(reader, value); err != nil {
		return adoptionBusinessRow{}, fmt.Errorf("read populated-table adoption spool row: %w", err)
	}
	keyTextEnd := int(keyTextLength)
	keyBytesEnd := keyTextEnd + int(keyBytesLength)
	return adoptionBusinessRow{
		tableInfo:   info,
		keyText:     value[:keyTextEnd],
		keyBytes:    value[keyTextEnd:keyBytesEnd],
		payloadWire: value[keyBytesEnd:],
	}, nil
}

func (s *SyncService) loadAdoptionUserIDs(
	ctx context.Context,
	tx pgx.Tx,
) ([]string, error) {
	userSet := make(map[string]struct{})
	infos := s.sortedAdoptionTableInfos()
	var query strings.Builder
	query.WriteString("/* oversync:adoption-scope-discovery */ SELECT scope_id, source_ordinal FROM (")
	for i, info := range infos {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		fmt.Fprintf(&query, "SELECT DISTINCT CAST(src.%s AS text) AS scope_id, %d::integer AS source_ordinal FROM %s AS src", ownerIdent, i, tableIdent)
	}
	if len(infos) > 0 {
		query.WriteString(" UNION ALL ")
	}
	query.WriteString("SELECT user_id AS scope_id, -1::integer AS source_ordinal FROM sync.user_state")
	query.WriteString(") AS discovered ORDER BY scope_id, source_ordinal")

	rows, err := tx.Query(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("discover populated-table adoption scopes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			userID        sql.NullString
			sourceOrdinal int
		)
		if err := rows.Scan(&userID, &sourceOrdinal); err != nil {
			return nil, fmt.Errorf("scan populated-table adoption scope: %w", err)
		}
		if !userID.Valid || (userID.String == "" && sourceOrdinal >= 0) {
			tableName := ""
			if sourceOrdinal >= 0 && sourceOrdinal < len(infos) {
				tableName = Key(infos[sourceOrdinal].schemaName, infos[sourceOrdinal].tableName)
			}
			return nil, &PopulatedTableAdoptionError{
				Reason: "identity_unrepresentable",
				Table:  tableName,
				Detail: "registered row has a NULL or empty scope",
			}
		}
		userSet[userID.String] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate populated-table adoption scopes: %w", err)
	}
	userIDs := make([]string, 0, len(userSet))
	for userID := range userSet {
		userIDs = append(userIDs, userID)
	}
	sort.Strings(userIDs)
	return userIDs, nil
}

func loadAdoptionScopeStates(ctx context.Context, tx pgx.Tx, userIDs []string) (map[string]adoptionScopeState, error) {
	states := make(map[string]adoptionScopeState, len(userIDs))
	for _, userID := range userIDs {
		states[userID] = adoptionScopeState{}
	}
	if len(userIDs) == 0 {
		return states, nil
	}

	userRows, err := tx.Query(ctx, `
		/* oversync:adoption-scope-state */
		SELECT user_id, user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = ANY($1::text[])
		ORDER BY user_pk
		FOR UPDATE
	`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("lock user_state rows for populated-table adoption: %w", err)
	}
	for userRows.Next() {
		var (
			userID string
			state  adoptionScopeState
		)
		if err := userRows.Scan(&userID, &state.userPK, &state.nextBundleSeq, &state.retainedBundleFloor); err != nil {
			userRows.Close()
			return nil, fmt.Errorf("scan locked user_state row for populated-table adoption: %w", err)
		}
		state.found = true
		states[userID] = state
	}
	if err := userRows.Err(); err != nil {
		userRows.Close()
		return nil, fmt.Errorf("iterate locked user_state rows for populated-table adoption: %w", err)
	}
	userRows.Close()

	scopeRows, err := tx.Query(ctx, `
		/* oversync:adoption-scope-state */
		SELECT users.user_id, scope.state_code
		FROM sync.user_state AS users
		JOIN sync.scope_state AS scope ON scope.user_pk = users.user_pk
		WHERE users.user_id = ANY($1::text[])
		ORDER BY users.user_pk
		FOR UPDATE OF scope
	`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("lock scope_state rows for populated-table adoption: %w", err)
	}
	for scopeRows.Next() {
		var (
			userID    string
			stateCode int16
		)
		if err := scopeRows.Scan(&userID, &stateCode); err != nil {
			scopeRows.Close()
			return nil, fmt.Errorf("scan locked scope_state row for populated-table adoption: %w", err)
		}
		state := states[userID]
		state.scopeStateCode = sql.NullInt16{Int16: stateCode, Valid: true}
		states[userID] = state
	}
	if err := scopeRows.Err(); err != nil {
		scopeRows.Close()
		return nil, fmt.Errorf("iterate locked scope_state rows for populated-table adoption: %w", err)
	}
	scopeRows.Close()

	countRows, err := tx.Query(ctx, `
		/* oversync:adoption-scope-state */
		WITH requested AS (
			SELECT user_id, user_pk
			FROM sync.user_state
			WHERE user_id = ANY($1::text[])
		), row_counts AS (
			SELECT rows.user_pk, COUNT(*) AS row_count,
				COUNT(*) FILTER (WHERE NOT rows.deleted) AS live_row_count
			FROM sync.row_state AS rows
			JOIN requested USING (user_pk)
			GROUP BY rows.user_pk
		), bundle_counts AS (
			SELECT bundles.user_pk, COUNT(*) AS bundle_count, MAX(bundles.bundle_seq) AS max_bundle_seq,
				COUNT(*) FILTER (
					WHERE sources.user_pk IS NULL
					   OR sources.max_committed_source_bundle_id < bundles.source_bundle_id
				) AS source_mismatch_count
			FROM sync.bundle_log AS bundles
			JOIN requested USING (user_pk)
			LEFT JOIN sync.source_state AS sources
			  ON sources.user_pk = bundles.user_pk AND sources.source_id = bundles.source_id
			GROUP BY bundles.user_pk
		), source_counts AS (
			SELECT sources.user_pk, COUNT(*) AS source_count
			FROM sync.source_state AS sources
			JOIN requested USING (user_pk)
			GROUP BY sources.user_pk
		), snapshot_counts AS (
			SELECT sessions.user_pk, COUNT(*) AS snapshot_session_count
			FROM sync.snapshot_sessions AS sessions
			JOIN requested USING (user_pk)
			GROUP BY sessions.user_pk
		), capture_counts AS (
			SELECT capture.user_pk, COUNT(*) AS capture_stage_count
			FROM sync.bundle_capture_stage AS capture
			JOIN requested USING (user_pk)
			GROUP BY capture.user_pk
		)
		SELECT requested.user_id,
			COALESCE(row_counts.row_count, 0),
			COALESCE(row_counts.live_row_count, 0),
			COALESCE(bundle_counts.bundle_count, 0),
			bundle_counts.max_bundle_seq,
			COALESCE(source_counts.source_count, 0),
			COALESCE(bundle_counts.source_mismatch_count, 0),
			COALESCE(snapshot_counts.snapshot_session_count, 0),
			COALESCE(capture_counts.capture_stage_count, 0)
		FROM requested
		LEFT JOIN row_counts USING (user_pk)
		LEFT JOIN bundle_counts USING (user_pk)
		LEFT JOIN source_counts USING (user_pk)
		LEFT JOIN snapshot_counts USING (user_pk)
		LEFT JOIN capture_counts USING (user_pk)
		ORDER BY requested.user_pk
	`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("load grouped populated-table adoption state: %w", err)
	}
	for countRows.Next() {
		var userID string
		state := adoptionScopeState{}
		if err := countRows.Scan(
			&userID,
			&state.rowStateCount,
			&state.liveRowStateCount,
			&state.bundleCount,
			&state.maxBundleSeq,
			&state.sourceCount,
			&state.sourceMismatchCount,
			&state.snapshotSessionCount,
			&state.captureStageCount,
		); err != nil {
			countRows.Close()
			return nil, fmt.Errorf("scan grouped populated-table adoption state: %w", err)
		}
		lockedState := states[userID]
		state.found = lockedState.found
		state.userPK = lockedState.userPK
		state.nextBundleSeq = lockedState.nextBundleSeq
		state.retainedBundleFloor = lockedState.retainedBundleFloor
		state.scopeStateCode = lockedState.scopeStateCode
		states[userID] = state
	}
	if err := countRows.Err(); err != nil {
		countRows.Close()
		return nil, fmt.Errorf("iterate grouped populated-table adoption state: %w", err)
	}
	countRows.Close()
	return states, nil
}

func adoptionStateError(
	reason string,
	userID string,
	businessRowCount int64,
	state adoptionScopeState,
	detail string,
) error {
	return &PopulatedTableAdoptionError{
		Reason:           reason,
		UserID:           userID,
		BusinessRowCount: businessRowCount,
		RowStateCount:    state.rowStateCount,
		Detail:           detail,
	}
}

func (s *SyncService) validateCoherentAdoptionScope(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	businessRows []adoptionBusinessRow,
	state adoptionScopeState,
	progress adoptionProgressFunc,
) error {
	businessRowCount := int64(len(businessRows))
	highest := state.nextBundleSeq - 1
	if highest <= 0 || state.retainedBundleFloor < 0 || state.retainedBundleFloor > highest {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "user sequence or retained floor is invalid")
	}
	expectedRetainedBundleCount := highest - state.retainedBundleFloor
	if state.bundleCount != expectedRetainedBundleCount {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "retained bundle count does not form a contiguous history window")
	}
	if expectedRetainedBundleCount == 0 {
		if state.maxBundleSeq.Valid {
			return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "retained history exists at or below the retained floor")
		}
	} else if !state.maxBundleSeq.Valid || state.maxBundleSeq.Int64 != highest {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "latest retained bundle does not match next_bundle_seq")
	}
	currentRows, err := s.loadAdoptionCurrentRowState(ctx, tx, userID, state, highest, progress)
	if err != nil {
		return err
	}
	retainedRows, err := s.validateRetainedAdoptionBundles(ctx, tx, userID, state, currentRows, progress)
	if err != nil {
		return err
	}
	if state.sourceMismatchCount != 0 {
		return adoptionStateError("source_watermark_mismatch", userID, state.liveRowStateCount, state, "committed bundle source watermark is missing or behind")
	}
	if businessRowCount != state.liveRowStateCount {
		return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, "live business and row_state counts differ")
	}
	businessIndex := 0
	for _, current := range currentRows {
		if progress != nil {
			progress()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		comparison := 1
		var liveRow adoptionBusinessRow
		if businessIndex < len(businessRows) {
			liveRow = businessRows[businessIndex]
			switch {
			case liveRow.tableInfo.tableID < current.tableID:
				comparison = -1
			case liveRow.tableInfo.tableID > current.tableID:
				comparison = 1
			default:
				comparison = bytes.Compare(liveRow.keyBytes, current.keyBytes)
			}
		}
		if comparison < 0 {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, "materialized business row is missing a live row_state identity")
		}
		if current.deleted {
			if comparison == 0 {
				return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("tombstone at version %d still has a materialized business row", current.bundleSeq))
			}
		} else if comparison != 0 {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("live row_state at version %d is missing a materialized business row", current.bundleSeq))
		} else {
			businessIndex++
		}
		if current.bundleSeq <= state.retainedBundleFloor {
			continue
		}
		logicalKey := newAdoptionLogicalRowKey(current.tableID, current.keyBytes)
		evidence := retainedRows[logicalKey]
		rowKind := "live row"
		if current.deleted {
			rowKind = "tombstone"
		}
		if evidence.count != 1 {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("%s at version %d has %d matching retained bundle rows", rowKind, current.bundleSeq, evidence.count))
		}
		if current.deleted {
			if evidence.row.op != OpDelete {
				return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("tombstone at version %d matches a retained %s", current.bundleSeq, evidence.row.op))
			}
			continue
		}
		if evidence.row.op == OpDelete {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("live row at version %d matches a retained delete", current.bundleSeq))
		}
		if bytes.Equal(liveRow.payloadWire, evidence.row.payloadWire) {
			continue
		}
		retainedPayload, err := canonicalJSON(evidence.row.payloadWire)
		if err != nil {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("canonicalize retained payload at version %d: %v", current.bundleSeq, err))
		}
		livePayload, err := canonicalJSON(liveRow.payloadWire)
		if err != nil {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("canonicalize live payload at version %d: %v", current.bundleSeq, err))
		}
		if !bytes.Equal(livePayload, retainedPayload) {
			return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, fmt.Sprintf("live payload differs from retained bundle row at version %d", current.bundleSeq))
		}
	}
	if businessIndex != len(businessRows) {
		return adoptionStateError("business_row_state_mismatch", userID, businessRowCount, state, "materialized business row is missing a live row_state identity")
	}
	return nil
}

func validatePersistedAdoptionState(adopted adoptedScopeSummary, state adoptionScopeState) error {
	valid := state.found &&
		state.scopeStateCode.Valid && state.scopeStateCode.Int16 == scopeStateCodeInitialized &&
		state.nextBundleSeq == 2 && state.retainedBundleFloor == 0 &&
		state.rowStateCount == adopted.rowCount && state.liveRowStateCount == adopted.rowCount &&
		state.bundleCount == 1 && state.maxBundleSeq.Valid && state.maxBundleSeq.Int64 == 1 &&
		state.sourceCount == 1 && state.sourceMismatchCount == 0 &&
		state.snapshotSessionCount == 0 && state.captureStageCount == 0
	if valid {
		return nil
	}
	return adoptionStateError(
		"partial_sync_state",
		adopted.userID,
		adopted.rowCount,
		state,
		"scope does not contain the complete persisted adoption baseline",
	)
}

func (s *SyncService) loadAdoptionCurrentRowState(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	state adoptionScopeState,
	highest int64,
	progress adoptionProgressFunc,
) ([]adoptionCurrentRowState, error) {
	rows, err := tx.Query(ctx, `
		SELECT table_id, key_bytes, bundle_seq, deleted
		FROM sync.row_state
		WHERE user_pk = $1
		ORDER BY table_id, key_bytes
	`, state.userPK)
	if err != nil {
		return nil, fmt.Errorf("query row_state for adoption scope %s: %w", userID, err)
	}
	defer rows.Close()

	currentRows := make([]adoptionCurrentRowState, 0, state.rowStateCount)
	var liveCount int64
	for rows.Next() {
		if progress != nil {
			progress()
		}
		var current adoptionCurrentRowState
		if err := rows.Scan(&current.tableID, &current.keyBytes, &current.bundleSeq, &current.deleted); err != nil {
			return nil, fmt.Errorf("scan row_state for adoption scope %s: %w", userID, err)
		}
		if current.bundleSeq <= 0 || current.bundleSeq > highest {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "row_state contains a non-positive or future version")
		}
		info, err := s.tableInfoForID(current.tableID)
		if err != nil {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, fmt.Sprintf("row_state has invalid table identity: %v", err))
		}
		if _, err := wireSyncKeyFromBytes(info, current.keyBytes); err != nil {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, fmt.Sprintf("row_state has invalid key for table_id %d: %v", current.tableID, err))
		}
		currentRows = append(currentRows, current)
		if !current.deleted {
			liveCount++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate row_state for adoption scope %s: %w", userID, err)
	}
	if int64(len(currentRows)) != state.rowStateCount || liveCount != state.liveRowStateCount {
		return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "row_state counts changed during adoption validation")
	}
	return currentRows, nil
}

func (s *SyncService) validateRetainedAdoptionBundles(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	state adoptionScopeState,
	currentRows []adoptionCurrentRowState,
	progress adoptionProgressFunc,
) (map[adoptionLogicalRowKey]adoptionRetainedEvidence, error) {
	historyMismatch := func(detail string) error {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, detail)
	}
	targets := make(map[int64]map[adoptionLogicalRowKey]struct{})
	for _, current := range currentRows {
		if progress != nil {
			progress()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if current.bundleSeq <= state.retainedBundleFloor {
			continue
		}
		if targets[current.bundleSeq] == nil {
			targets[current.bundleSeq] = make(map[adoptionLogicalRowKey]struct{})
		}
		targets[current.bundleSeq][newAdoptionLogicalRowKey(current.tableID, current.keyBytes)] = struct{}{}
	}
	retainedRows := make(map[adoptionLogicalRowKey]adoptionRetainedEvidence, len(currentRows))
	rowRows, err := tx.Query(ctx, `
		SELECT
			bundle.bundle_seq, bundle.row_count, bundle.byte_count, bundle.bundle_hash,
			rows.row_ordinal, rows.table_id, rows.key_bytes, rows.op_code, rows.payload_wire
		FROM sync.bundle_log AS bundle
		LEFT JOIN sync.bundle_rows AS rows
		  ON rows.user_pk = bundle.user_pk AND rows.bundle_seq = bundle.bundle_seq
		WHERE bundle.user_pk = $1
		ORDER BY bundle.bundle_seq, rows.row_ordinal
	`, state.userPK)
	if err != nil {
		return nil, fmt.Errorf("query retained bundles for adoption scope %s: %w", userID, err)
	}
	defer rowRows.Close()

	expectedBundleSeq := state.retainedBundleFloor + 1
	var (
		currentBundle  *adoptionRetainedBundle
		bundleHasher   *protocolhash.CommittedBundleHasher
		bundleRowCount int64
		bundleCount    int64
	)
	finishBundle := func() error {
		if currentBundle == nil {
			return nil
		}
		if bundleRowCount != currentBundle.rowCount {
			return historyMismatch(fmt.Sprintf("bundle %d row count does not match stored history", currentBundle.bundleSeq))
		}
		bundleHash, byteCount, err := bundleHasher.Sum()
		if err != nil {
			return historyMismatch(fmt.Sprintf("recompute bundle %d hash: %v", currentBundle.bundleSeq, err))
		}
		if bundleHash != hex.EncodeToString(currentBundle.bundleHash) {
			return historyMismatch(fmt.Sprintf("bundle %d hash does not match stored history", currentBundle.bundleSeq))
		}
		if byteCount != currentBundle.byteCount {
			return historyMismatch(fmt.Sprintf("bundle %d byte count does not match stored history", currentBundle.bundleSeq))
		}
		return nil
	}
	for rowRows.Next() {
		if progress != nil {
			progress()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var (
			bundle     adoptionRetainedBundle
			rowOrdinal sql.NullInt64
			tableID    sql.NullInt64
			opCode     sql.NullInt64
			keyBytes   []byte
			payload    []byte
		)
		if err := rowRows.Scan(
			&bundle.bundleSeq,
			&bundle.rowCount,
			&bundle.byteCount,
			&bundle.bundleHash,
			&rowOrdinal,
			&tableID,
			&keyBytes,
			&opCode,
			&payload,
		); err != nil {
			return nil, fmt.Errorf("scan retained bundle for adoption scope %s: %w", userID, err)
		}
		if currentBundle == nil || bundle.bundleSeq != currentBundle.bundleSeq {
			if err := finishBundle(); err != nil {
				return nil, err
			}
			if bundle.bundleSeq != expectedBundleSeq {
				return nil, historyMismatch(fmt.Sprintf("retained history expected bundle %d but found %d", expectedBundleSeq, bundle.bundleSeq))
			}
			currentBundle = &bundle
			if s.adoptionHooks != nil && s.adoptionHooks.onCommittedBundleHash != nil {
				s.adoptionHooks.onCommittedBundleHash()
			}
			bundleHasher = protocolhash.NewCommittedBundleHasher()
			bundleRowCount = 0
			bundleCount++
			expectedBundleSeq++
		}
		if !rowOrdinal.Valid {
			if tableID.Valid || opCode.Valid || keyBytes != nil || payload != nil {
				return nil, historyMismatch(fmt.Sprintf("bundle %d has a malformed empty row", bundle.bundleSeq))
			}
			continue
		}
		if !tableID.Valid || !opCode.Valid || keyBytes == nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d is incomplete", bundle.bundleSeq, rowOrdinal.Int64))
		}
		expectedOrdinal := bundleRowCount + 1
		if rowOrdinal.Int64 != expectedOrdinal {
			return nil, historyMismatch(fmt.Sprintf("bundle %d expected row ordinal %d but found %d", bundle.bundleSeq, expectedOrdinal, rowOrdinal.Int64))
		}
		if tableID.Int64 < 1 || tableID.Int64 > int64(^uint32(0)>>1) {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid table identity %d", bundle.bundleSeq, rowOrdinal.Int64, tableID.Int64))
		}
		tableIDValue := int32(tableID.Int64)
		info, err := s.tableInfoForID(tableIDValue)
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid table identity: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		key, err := wireSyncKeyFromBytes(info, keyBytes)
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid key: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		if opCode.Int64 < 1 || opCode.Int64 > int64(^uint16(0)>>1) {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid operation %d", bundle.bundleSeq, rowOrdinal.Int64, opCode.Int64))
		}
		op, err := opStringFromCode(int16(opCode.Int64))
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid operation: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		payloadWire := payload
		if err := bundleHasher.Add(protocolhash.BundleRow{
			Schema:     info.schemaName,
			Table:      info.tableName,
			Key:        key,
			Op:         op,
			RowVersion: bundle.bundleSeq,
			Payload:    payloadWire,
		}); err != nil {
			return nil, historyMismatch(fmt.Sprintf("recompute bundle %d hash: %v", currentBundle.bundleSeq, err))
		}
		bundleRowCount++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		logicalKey := newAdoptionLogicalRowKey(tableIDValue, keyBytes)
		if _, wanted := targets[bundle.bundleSeq][logicalKey]; wanted {
			evidence := retainedRows[logicalKey]
			evidence.count++
			if evidence.count == 1 {
				evidence.row = adoptionRetainedRow{
					op:          op,
					payloadWire: append([]byte(nil), payloadWire...),
				}
			}
			retainedRows[logicalKey] = evidence
		}
	}
	if err := rowRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained bundles for adoption scope %s: %w", userID, err)
	}
	if err := finishBundle(); err != nil {
		return nil, err
	}
	if bundleCount != state.bundleCount {
		return nil, historyMismatch("retained bundle metadata changed during validation")
	}
	return retainedRows, nil
}

func (s *SyncService) persistAdoptionBaseline(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	rows []adoptionBusinessRow,
	progress adoptionProgressFunc,
) (adoptionScopeState, error) {
	if len(rows) == 0 {
		return adoptionScopeState{}, nil
	}
	if err := ensureScopeStateExistsWithExec(ctx, tx, userID); err != nil {
		return adoptionScopeState{}, err
	}
	scope, err := loadScopeStateForUpdate(ctx, tx, userID)
	if err != nil {
		return adoptionScopeState{}, err
	}
	if scope.State == scopeStateInitializing {
		return adoptionScopeState{}, &PopulatedTableAdoptionError{
			Reason:           "active_initialization_or_session",
			UserID:           userID,
			BusinessRowCount: int64(len(rows)),
			Detail:           "scope is initializing",
		}
	}
	bundleSeq, err := reserveUserBundleSeq(ctx, tx, scope.UserPK)
	if err != nil {
		return adoptionScopeState{}, err
	}
	if bundleSeq != 1 {
		return adoptionScopeState{}, &PopulatedTableAdoptionError{
			Reason:           "history_mismatch",
			UserID:           userID,
			BusinessRowCount: int64(len(rows)),
			Detail:           fmt.Sprintf("adoption baseline reserved unexpected bundle_seq %d", bundleSeq),
		}
	}

	storageRows := make([]committedBundleStorageRow, 0, len(rows))
	bundleRows := make([]BundleRow, 0, len(rows))
	requestRows := make([]PushRequestRow, 0, len(rows))
	for _, row := range rows {
		if progress != nil {
			progress()
		}
		if err := ctx.Err(); err != nil {
			return adoptionScopeState{}, err
		}
		key := SyncKey{row.tableInfo.syncKeyColumn: string(row.keyText)}
		storageRows = append(storageRows, committedBundleStorageRow{
			tableID:     row.tableInfo.tableID,
			keyBytes:    append([]byte(nil), row.keyBytes...),
			opCode:      opCodeInsert,
			payloadWire: append([]byte(nil), row.payloadWire...),
		})
		bundleRows = append(bundleRows, BundleRow{
			Schema:     row.tableInfo.schemaName,
			Table:      row.tableInfo.tableName,
			Key:        key,
			Op:         OpInsert,
			RowVersion: bundleSeq,
			Payload:    append([]byte(nil), row.payloadWire...),
		})
		requestRows = append(requestRows, PushRequestRow{
			Schema:         row.tableInfo.schemaName,
			Table:          row.tableInfo.tableName,
			Key:            key,
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        append([]byte(nil), row.payloadWire...),
		})
	}
	requestHash, err := computeCanonicalPushRequestHash(requestRows)
	if err != nil {
		return adoptionScopeState{}, &PopulatedTableAdoptionError{Reason: "baseline_unrepresentable", UserID: userID, BusinessRowCount: int64(len(rows)), Detail: err.Error()}
	}
	bundleHash, byteCount, err := computeCommittedBundleHash(bundleRows)
	if err != nil {
		return adoptionScopeState{}, &PopulatedTableAdoptionError{Reason: "baseline_unrepresentable", UserID: userID, BusinessRowCount: int64(len(rows)), Detail: err.Error()}
	}
	var sourceID string
	for {
		sourceID = adoptionSourcePrefix + uuid.NewString()
		var collision bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync.source_state WHERE user_pk = $1 AND source_id = $2)`, scope.UserPK, sourceID).Scan(&collision); err != nil {
			return adoptionScopeState{}, fmt.Errorf("check adoption source collision for scope %s: %w", userID, err)
		}
		if !collision {
			break
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, row_count,
			byte_count, bundle_hash, canonical_request_hash, committed_at
		) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, now())
	`, scope.UserPK, bundleSeq, sourceID, len(bundleRows), byteCount, bundleHash, requestHash); err != nil {
		return adoptionScopeState{}, fmt.Errorf("insert adoption baseline bundle for scope %s: %w", userID, err)
	}
	if err := persistCommittedBundleRows(ctx, tx, scope.UserPK, bundleSeq, storageRows); err != nil {
		return adoptionScopeState{}, err
	}
	if err := activateSourceState(ctx, tx, scope.UserPK, userID, sourceID, 1); err != nil {
		return adoptionScopeState{}, err
	}
	if scope.State != scopeStateInitialized {
		if err := transitionScopeToInitialized(ctx, tx, userID, sourceID); err != nil {
			return adoptionScopeState{}, err
		}
	}
	if err := s.emitBundleChangeNotify(ctx, tx, bundleChangeNotification{
		UserPK:         scope.UserPK,
		BundleSeq:      bundleSeq,
		SourceID:       sourceID,
		SourceBundleID: 1,
	}); err != nil {
		return adoptionScopeState{}, err
	}
	rowCount := int64(len(rows))
	return adoptionScopeState{
		found:               true,
		userPK:              scope.UserPK,
		nextBundleSeq:       2,
		retainedBundleFloor: 0,
		scopeStateCode:      sql.NullInt16{Int16: scopeStateCodeInitialized, Valid: true},
		rowStateCount:       rowCount,
		liveRowStateCount:   rowCount,
		bundleCount:         1,
		maxBundleSeq:        sql.NullInt64{Int64: 1, Valid: true},
		sourceCount:         1,
	}, nil
}
