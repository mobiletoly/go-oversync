package oversqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
)

// The retry witness includes unmanaged incoming rows. Their writes do not bump
// the managed dirty revision. A streaming multiset digest avoids a host graph or
// an unbounded SQL sort; typed values and duplicate multiplicity participate.
func (c *Client) recoveryDependencyIdentity(ctx context.Context, tx sqliteTransaction, table string) (string, error) {
	h := sha256.New()
	encoder := json.NewEncoder(h)
	children, catalog, err := c.recoveryDependencyTables(ctx, tx)
	if err != nil {
		return "", err
	}
	for _, child := range children {
		if _, managed := c.pkByTable[strings.ToLower(child)]; managed || strings.HasPrefix(child, "_sync_") {
			continue
		}
		relevant, err := c.recoveryDependencyRelevant(ctx, tx, child, table, catalog)
		if err != nil {
			return "", err
		}
		if !relevant {
			continue
		}
		digest, count, err := c.recoveryDependencyRows(ctx, tx, child, catalog)
		if err != nil {
			return "", err
		}
		if err = encoder.Encode([]any{child, count, fmt.Sprintf("%x", digest)}); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
func (c *Client) recoveryDependencyTables(ctx context.Context, tx sqliteTransaction) ([]string, workspaceCharge, error) {
	var entries, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM sqlite_schema`).Scan(&entries, &bytes); err != nil {
		return nil, workspaceCharge{}, err
	}
	catalog := workspaceCharge{16 + 4*entries, 4096 + 16*bytes + 32*entries}
	if err := c.checkAggregateWorkspace(ctx, tx, "recovery-dependencies", nil, nil, catalog); err != nil {
		return nil, workspaceCharge{}, err
	}
	// Count the measurement, one full catalog traversal and the host sort.
	if err := c.reserveReadySyncWork(ctx, "recovery-dependency-catalog", saturatingWorkAdd(saturatingWorkMultiply(entries, 128+16*int64(bits.Len64(uint64(entries)))), saturatingWorkMultiply(bytes, 16*(1+int64(bits.Len64(uint64(entries))))))); err != nil {
		return nil, workspaceCharge{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, workspaceCharge{}, err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return nil, workspaceCharge{}, err
		}
		tables = append(tables, name)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, workspaceCharge{}, err
	}
	sort.Strings(tables)
	return tables, catalog, nil
}
func (c *Client) recoveryDependencyRelevant(ctx context.Context, tx sqliteTransaction, child, table string, catalog workspaceCharge) (bool, error) {
	var columns, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST("table" AS BLOB))),0) FROM pragma_foreign_key_list(?)`, child).Scan(&columns, &bytes); err != nil {
		return false, err
	}
	if err := c.checkAggregateWorkspace(ctx, tx, "recovery-dependency-foreign-keys", nil, nil, workspaceCharge{catalog.entries + 8, catalog.bytes + 4096 + 16*bytes}); err != nil {
		return false, err
	}
	if err := c.reserveReadySyncWork(ctx, "recovery-dependency-catalog", 128*columns); err != nil {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT "table" FROM pragma_foreign_key_list(?)`, child)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var parent string
		if err = rows.Scan(&parent); err != nil {
			return false, err
		}
		target := strings.ToLower(parent)
		if table != "" && target != strings.ToLower(table) {
			continue
		}
		if _, managed := c.pkByTable[target]; managed {
			return true, nil
		}
	}
	return false, rows.Err()
}
func (c *Client) recoveryDependencyRows(ctx context.Context, tx sqliteTransaction, table string, catalog workspaceCharge) ([32]byte, int64, error) {
	var zero [32]byte
	var columns, metadata int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM pragma_table_info(?)`, table).Scan(&columns, &metadata); err != nil {
		return zero, 0, err
	}
	if err := c.checkAggregateWorkspace(ctx, tx, "recovery-dependency-columns", nil, nil, workspaceCharge{catalog.entries + 16 + 8*columns, catalog.bytes + 4096 + 2048*columns + 64*metadata}); err != nil {
		return zero, 0, err
	}
	columnWork := saturatingWorkAdd(saturatingWorkMultiply(columns, 128+16*int64(bits.Len64(uint64(columns)))), saturatingWorkMultiply(metadata, 64))
	if err := c.reserveReadySyncWork(ctx, "recovery-dependency-column-descriptions", columnWork); err != nil {
		return zero, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		return zero, 0, err
	}
	var sizes, selected []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return zero, 0, err
		}
		quoted := quoteIdent(name)
		sizes = append(sizes, "COALESCE(length(CAST("+quoted+" AS BLOB)),0)")
		selected = append(selected, "typeof("+quoted+")", quoted)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return zero, 0, err
	}
	var count, total, maxRow int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent(table)).Scan(&count); err != nil {
		return zero, 0, err
	}
	work := saturatingWorkMultiply(count, saturatingWorkAdd(128, 16*columns))
	if err = c.reserveReadySyncWork(ctx, "recovery-dependency-visits", work); err != nil {
		return zero, 0, err
	}
	expression := strings.Join(sizes, "+")
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(`+expression+`),0),COALESCE(max(`+expression+`),0) FROM `+quoteIdent(table)).Scan(&total, &maxRow); err != nil {
		return zero, 0, err
	}
	if err = c.checkAggregateWorkspace(ctx, tx, "recovery-dependency-host", nil, nil, workspaceCharge{catalog.entries + 32 + 8*columns, catalog.bytes + 8192 + 2048*columns + 64*metadata + 16*maxRow}); err != nil {
		return zero, 0, err
	}
	if err = c.reserveReadySyncWork(ctx, "recovery-dependency-bytes", saturatingWorkMultiply(total, 2)); err != nil {
		return zero, 0, err
	}
	digest, err := streamRecoveryDependencyRows(ctx, tx, `SELECT `+strings.Join(selected, ",")+` FROM `+quoteIdent(table), int(columns*2))
	return digest, count, err
}
func streamRecoveryDependencyRows(ctx context.Context, tx sqliteTransaction, query string, columns int) ([32]byte, error) {
	var sum [32]byte
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return sum, err
	}
	defer rows.Close()
	values := make([]any, columns)
	pointers := make([]any, columns)
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err = rows.Scan(pointers...); err != nil {
			return sum, err
		}
		h := sha256.New()
		if err = json.NewEncoder(h).Encode(values); err != nil {
			return sum, err
		}
		digest := h.Sum(nil)
		carry := 0
		for i := 31; i >= 0; i-- {
			n := int(sum[i]) + int(digest[i]) + carry
			sum[i] = byte(n)
			carry = n >> 8
		}
	}
	return sum, rows.Err()
}
