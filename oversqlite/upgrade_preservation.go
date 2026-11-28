package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
)

const preservationChunkBytes = 64 * 1024
const preservationMultisetTable = "_sqlitenow_preservation_multiset"

type upgradePreservationQueryer interface {
	upgradeQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func localOnlyDigest(ctx context.Context, q upgradePreservationQueryer, tableNames []string) (string, error) {
	return localOnlyDigestExcludingGuards(ctx, q, tableNames, nil)
}

func localOnlyDigestWithUpgradeGuards(ctx context.Context, q upgradePreservationQueryer, tableNames []string) (string, error) {
	guards := localOnlyUpgradeGuards(tableNames)
	if err := verifyLocalOnlyUpgradeGuards(ctx, q, guards); err != nil {
		return "", err
	}
	excluded := make(map[string]string, len(guards))
	for _, guard := range guards {
		excluded[guard.Name] = guard.SQL
	}
	return localOnlyDigestExcludingGuards(ctx, q, tableNames, excluded)
}

type preservationColumn struct {
	cid          int64
	name         string
	declared     string
	notNull      int64
	defaultValue sql.NullString
	primaryKey   int64
	hidden       int64
}

func localOnlyDigestExcludingGuards(ctx context.Context, q upgradePreservationQueryer, tableNames []string, excludedGuards map[string]string) (digest string, resultErr error) {
	if _, err := q.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS `+preservationMultisetTable+`(digest TEXT PRIMARY KEY,multiplicity INTEGER NOT NULL)`); err != nil {
		return "", err
	}
	defer func() {
		_, cleanupErr := q.ExecContext(context.WithoutCancel(ctx), `DROP TABLE IF EXISTS `+preservationMultisetTable)
		if resultErr == nil && cleanupErr != nil {
			resultErr = cleanupErr
		}
	}()

	tables := append([]string(nil), tableNames...)
	sort.Strings(tables)
	digest = preservationHash([]byte("se03-preservation-chain-v2"))
	var totalRows int64
	for _, table := range tables {
		if !canonicalUpgradeIdentifier(table) {
			return "", fmt.Errorf("invalid preserved table identifier %q", table)
		}
		digest = preservationFold(digest, "table", []byte(table))
		foundTable := false
		schemaRows, err := q.QueryContext(ctx, `SELECT type,name,COALESCE(sql,'') FROM sqlite_schema WHERE (type='table' AND lower(name)=?) OR (type IN ('index','trigger') AND lower(tbl_name)=?) ORDER BY type COLLATE BINARY,name COLLATE BINARY`, table, table)
		if err != nil {
			return "", err
		}
		for schemaRows.Next() {
			var kind, name, statement string
			if err := schemaRows.Scan(&kind, &name, &statement); err != nil {
				schemaRows.Close()
				return "", err
			}
			if expected, excluded := excludedGuards[name]; excluded {
				if kind != "trigger" || statement != expected {
					schemaRows.Close()
					return "", fmt.Errorf("local-only upgrade guard %q does not match its canonical definition", name)
				}
				continue
			}
			if kind == "table" && strings.EqualFold(name, table) {
				foundTable = true
			}
			digest = preservationFold(digest, "object-type", []byte(kind))
			digest = preservationFold(digest, "object-name", []byte(name))
			digest = preservationFold(digest, "object-sql", []byte(statement))
		}
		if err := schemaRows.Close(); err != nil {
			return "", err
		}
		if !foundTable {
			return "", fmt.Errorf("declared preserved table %s is missing", table)
		}

		columns, err := loadPreservationColumns(ctx, q, table)
		if err != nil {
			return "", err
		}
		if len(columns) == 0 {
			return "", fmt.Errorf("preserved table %s has no visible shape", table)
		}
		columnNames := make([]string, 0, len(columns))
		for _, column := range columns {
			columnNames = append(columnNames, column.name)
			defaultValue := "<null>"
			if column.defaultValue.Valid {
				defaultValue = column.defaultValue.String
			}
			for _, value := range []string{fmt.Sprint(column.cid), column.name, column.declared, fmt.Sprint(column.notNull), defaultValue, fmt.Sprint(column.primaryKey), fmt.Sprint(column.hidden)} {
				digest = preservationFold(digest, "column", []byte(value))
			}
		}
		withoutRowID, err := sqliteTableWithoutRowID(ctx, q, table)
		if err != nil {
			return "", err
		}
		identityColumn := observableSQLiteRowID(columnNames, withoutRowID)
		digest = preservationFold(digest, "accessible-rowid", []byte(identityColumn))
		sequence, err := preservationSequenceFrame(ctx, q, table)
		if err != nil {
			return "", err
		}
		digest = preservationFold(digest, "sqlite-sequence", sequence)

		tableHash := sha256.Sum256([]byte(table))
		tempTable := "_sqlitenow_preservation_" + hex.EncodeToString(tableHash[:8])
		if _, err := q.ExecContext(ctx, `DROP TABLE IF EXISTS `+tempTable); err != nil {
			return "", err
		}
		func() {
			defer func() {
				_, cleanupErr := q.ExecContext(context.WithoutCancel(ctx), `DROP TABLE IF EXISTS `+tempTable)
				if resultErr == nil && cleanupErr != nil {
					resultErr = cleanupErr
				}
			}()
			sourceRowIDColumn := preservationSourceRowIDColumn(columnNames)
			projection := make([]string, 0, len(columns)+1)
			if identityColumn != "" {
				projection = append(projection, quoteIdent(identityColumn)+` AS `+quoteIdent(sourceRowIDColumn))
			}
			for _, column := range columns {
				projection = append(projection, quoteIdent(column.name))
			}
			if _, err = q.ExecContext(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s AS SELECT %s FROM %s`, tempTable, joinSQLList(projection), quoteIdent(table))); err != nil {
				resultErr = err
				return
			}
			if _, err = q.ExecContext(ctx, `DELETE FROM `+preservationMultisetTable); err != nil {
				resultErr = err
				return
			}
			var tableRows int64
			lastRowID := int64(-1 << 63)
			for {
				rowIDs, queryErr := q.QueryContext(ctx, `SELECT rowid FROM `+tempTable+` WHERE rowid>? ORDER BY rowid LIMIT 128`, lastRowID)
				if queryErr != nil {
					resultErr = queryErr
					return
				}
				page := make([]int64, 0, 128)
				for rowIDs.Next() {
					var rowID int64
					if scanErr := rowIDs.Scan(&rowID); scanErr != nil {
						rowIDs.Close()
						resultErr = scanErr
						return
					}
					page = append(page, rowID)
				}
				if closeErr := rowIDs.Close(); closeErr != nil {
					resultErr = closeErr
					return
				}
				if len(page) == 0 {
					break
				}
				for _, rowID := range page {
					rowDigest, digestErr := digestPreservationRow(ctx, q, tempTable, rowID, columns, sourceRowIDColumn, identityColumn != "")
					if digestErr != nil {
						resultErr = digestErr
						return
					}
					if _, execErr := q.ExecContext(ctx, `INSERT INTO `+preservationMultisetTable+`(digest,multiplicity) VALUES(?,1) ON CONFLICT(digest) DO UPDATE SET multiplicity=multiplicity+1`, rowDigest); execErr != nil {
						resultErr = execErr
						return
					}
					tableRows++
					totalRows++
				}
				lastRowID = page[len(page)-1]
			}
			digest = preservationFold(digest, "table-row-count", signedInt64Bytes(tableRows))
			multiset, queryErr := q.QueryContext(ctx, `SELECT digest,multiplicity FROM `+preservationMultisetTable+` ORDER BY digest COLLATE BINARY`)
			if queryErr != nil {
				resultErr = queryErr
				return
			}
			for multiset.Next() {
				var rowDigest string
				var multiplicity int64
				if scanErr := multiset.Scan(&rowDigest, &multiplicity); scanErr != nil {
					multiset.Close()
					resultErr = scanErr
					return
				}
				digest = preservationFold(digest, "row-digest", []byte(rowDigest))
				digest = preservationFold(digest, "multiplicity", signedInt64Bytes(multiplicity))
			}
			if closeErr := multiset.Close(); closeErr != nil {
				resultErr = closeErr
			}
		}()
		if resultErr != nil {
			return "", resultErr
		}
	}
	return fmt.Sprintf("%d:%s", totalRows, digest), nil
}

func loadPreservationColumns(ctx context.Context, q upgradeQueryer, table string) ([]preservationColumn, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_xinfo(%s)`, quoteIdent(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := make([]preservationColumn, 0)
	for rows.Next() {
		var column preservationColumn
		if err := rows.Scan(&column.cid, &column.name, &column.declared, &column.notNull, &column.defaultValue, &column.primaryKey, &column.hidden); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func digestPreservationRow(ctx context.Context, q upgradeQueryer, tempTable string, rowID int64, columns []preservationColumn, sourceRowIDColumn string, includeRowID bool) (string, error) {
	digest := preservationHash([]byte("se03-preservation-row-v2"))
	if includeRowID {
		var sourceRowID int64
		if err := q.QueryRowContext(ctx, `SELECT `+quoteIdent(sourceRowIDColumn)+` FROM `+tempTable+` WHERE rowid=?`, rowID).Scan(&sourceRowID); err != nil {
			return "", err
		}
		digest = preservationFold(digest, "rowid", signedInt64Bytes(sourceRowID))
	}
	for _, column := range columns {
		quoted := quoteIdent(column.name)
		var storageClass string
		var integer sql.NullInt64
		var real sql.NullFloat64
		var length sql.NullInt64
		if err := q.QueryRowContext(ctx, `SELECT typeof(`+quoted+`),CASE WHEN typeof(`+quoted+`)='integer' THEN `+quoted+` END,CASE WHEN typeof(`+quoted+`)='real' THEN `+quoted+` END,length(CAST(`+quoted+` AS BLOB)) FROM `+tempTable+` WHERE rowid=?`, rowID).Scan(&storageClass, &integer, &real, &length); err != nil {
			return "", err
		}
		switch storageClass {
		case "null":
			digest = preservationFold(digest, "null", nil)
		case "integer":
			digest = preservationFold(digest, "integer", signedInt64Bytes(integer.Int64))
		case "real":
			digest = preservationFold(digest, "real", signedInt64Bytes(int64(math.Float64bits(real.Float64))))
		case "text", "blob":
			digest = preservationFold(digest, storageClass, signedInt64Bytes(length.Int64))
			for offset := int64(1); offset <= length.Int64; {
				var chunk []byte
				if err := q.QueryRowContext(ctx, `SELECT substr(CAST(`+quoted+` AS BLOB),?,?) FROM `+tempTable+` WHERE rowid=?`, offset, preservationChunkBytes, rowID).Scan(&chunk); err != nil {
					return "", err
				}
				if len(chunk) == 0 {
					return "", fmt.Errorf("SQLite returned an empty preservation chunk before the declared boundary")
				}
				digest = preservationFold(digest, "chunk", chunk)
				offset += int64(len(chunk))
			}
		default:
			return "", fmt.Errorf("unsupported SQLite value type %s", storageClass)
		}
	}
	return digest, nil
}

func preservationSequenceFrame(ctx context.Context, q upgradeQueryer, table string) ([]byte, error) {
	var exists int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND lower(name)='sqlite_sequence'`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return []byte("absent"), nil
	}
	rows, err := q.QueryContext(ctx, `SELECT seq FROM sqlite_sequence WHERE lower(name)=?`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]int64, 0, 1)
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []byte("none"), nil
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("sqlite_sequence contains duplicate entries for %s", table)
	}
	return append([]byte{1}, signedInt64Bytes(values[0])...), nil
}

func preservationSourceRowIDColumn(columns []string) string {
	used := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		used[strings.ToLower(column)] = struct{}{}
	}
	name := "_sqlitenow_source_rowid"
	for {
		if _, exists := used[strings.ToLower(name)]; !exists {
			return name
		}
		name = "_" + name
	}
}

func canonicalUpgradeIdentifier(value string) bool {
	if len(value) < 1 || len(value) > 63 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

func preservationHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func preservationFold(previous, label string, value []byte) string {
	prefix := []byte(fmt.Sprintf("%s|%d:%s|%d:", previous, len([]byte(label)), label, len(value)))
	return preservationHash(append(prefix, value...))
}

func signedInt64Bytes(value int64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(value))
	return result
}

func sqliteTableWithoutRowID(ctx context.Context, q upgradeQueryer, table string) (bool, error) {
	rows, err := q.QueryContext(ctx, `PRAGMA main.table_list`)
	if err != nil {
		return false, fmt.Errorf("inspect local-only table metadata for %s: %w", table, err)
	}
	defer rows.Close()
	found := false
	withoutRowID := false
	for rows.Next() {
		var schema, name, kind string
		var columns, wr, strict int
		if err := rows.Scan(&schema, &name, &kind, &columns, &wr, &strict); err != nil {
			return false, err
		}
		if schema != "main" || name != table {
			continue
		}
		if found || kind != "table" || (wr != 0 && wr != 1) {
			return false, fmt.Errorf("local-only table %q has contradictory table_list metadata", table)
		}
		found = true
		withoutRowID = wr == 1
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("local-only table %q is missing from table_list metadata", table)
	}
	return withoutRowID, nil
}

func observableSQLiteRowID(columns []string, withoutRowID bool) string {
	if withoutRowID {
		return ""
	}
	declared := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		declared[strings.ToLower(column)] = struct{}{}
	}
	for _, alias := range []string{"rowid", "_rowid_", "oid"} {
		if _, shadowed := declared[alias]; !shadowed {
			return alias
		}
	}
	return ""
}

func joinSQLList(values []string) string {
	return strings.Join(values, ",")
}
