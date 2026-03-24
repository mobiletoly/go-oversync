// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type execContexter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type txStmtCache struct {
	tx    *sql.Tx
	stmts map[string]*sql.Stmt
}

func newTxStmtCache(tx *sql.Tx) *txStmtCache {
	return &txStmtCache{
		tx:    tx,
		stmts: make(map[string]*sql.Stmt),
	}
}

func (c *txStmtCache) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if c == nil || c.tx == nil {
		return nil, fmt.Errorf("transaction statement cache requires a transaction")
	}

	stmt, ok := c.stmts[query]
	if !ok {
		var err error
		stmt, err = c.tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		c.stmts[query] = stmt
	}
	return stmt, nil
}

func (c *txStmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (c *txStmtCache) Close() error {
	if c == nil {
		return nil
	}

	var firstErr error
	for query, stmt := range c.stmts {
		if stmt == nil {
			continue
		}
		if err := stmt.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close prepared statement %q: %w", query, err)
		}
	}
	c.stmts = nil
	return firstErr
}
