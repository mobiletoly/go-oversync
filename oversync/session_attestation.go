package oversync

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Attest the actual connection after pool initialization and acquisition hooks.
// Config equality cannot establish what a hook or a server startup default did.
func attestOriginSession(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) error {
	var origin bool
	if err := q.QueryRow(ctx, `SELECT pg_catalog.current_setting('session_replication_role')='origin'`).Scan(&origin); err != nil {
		return err
	}
	if !origin {
		return fmt.Errorf("sync execution requires session_replication_role=origin")
	}
	return nil
}

// All pool-backed core transactions inspect the connection selected for this
// transaction, including read paths. Both pools remain owned by the caller.
func withOriginTransaction(ctx context.Context, pool *pgxpool.Pool, options pgx.TxOptions, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, pool, options, func(tx pgx.Tx) error {
		if err := attestOriginSession(ctx, tx); err != nil {
			return err
		}
		return fn(tx)
	})
}
