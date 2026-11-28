// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type readinessReader struct {
	mu        sync.Mutex
	ctx       context.Context
	tx        pgx.Tx
	remaining int
	closed    bool
	rows      []pgx.Rows
}

func (r *readinessReader) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.remaining == 0 {
		return nil, readinessFailure(ReadinessEvidenceUnavailable)
	}
	r.remaining--
	rows, err := r.tx.Query(r.ctx, query, args...)
	if err == nil {
		r.rows = append(r.rows, rows)
	}
	return rows, err
}

func (r *readinessReader) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	rows, err := r.Query(ctx, query, args...)
	return readinessRow{rows: rows, err: err}
}

func (r *readinessReader) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, rows := range r.rows {
		rows.Close()
	}
	r.rows = nil
}

type readinessRow struct {
	rows pgx.Rows
	err  error
}

func (r readinessRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// beginReadinessTransaction never retains a transaction during contention
// backoff. Its one absolute contention deadline also bounds pool acquisition.
func (s *SyncService) beginReadinessTransaction(ctx context.Context, deadline time.Time) (*pgxpool.Conn, pgx.Tx, error) {
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	delay := 25 * time.Millisecond
	for {
		conn, err := s.pool.Acquire(waitCtx)
		if err != nil {
			return nil, nil, readinessAdapterError(err)
		}
		tx, acquired, err := tryReadinessTransaction(waitCtx, conn, s.config.Readiness.AuthorityLock)
		if err == nil && acquired {
			return conn, tx, nil
		}
		// tryReadinessTransaction has already rolled back or discarded the socket.
		conn.Release()
		if err != nil {
			return nil, nil, readinessAdapterError(err)
		}
		if s.readinessContentionHook != nil {
			s.readinessContentionHook()
		}
		timer := time.NewTimer(delay)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return nil, nil, readinessAdapterError(waitCtx.Err())
		case <-timer.C:
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

func tryReadinessTransaction(ctx context.Context, conn *pgxpool.Conn, lock [2]int32) (pgx.Tx, bool, error) {
	tx, err := conn.BeginTx(ctx, syncMutationTxOptions())
	if err != nil {
		return nil, false, err
	}
	if err := attestOriginSession(ctx, tx); err != nil {
		cleanupReadinessTransaction(conn, tx)
		return nil, false, err
	}
	var acquired bool
	err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, lock[0], lock[1]).Scan(&acquired)
	if err == nil && acquired {
		return tx, true, nil
	}
	cleanupReadinessTransaction(conn, tx)
	return nil, false, err
}

func cleanupReadinessTransaction(conn *pgxpool.Conn, tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(cleanup); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// Never return an unconfirmed transaction/snapshot to the pool.
		_ = conn.Conn().Close(cleanup)
	}
}

func (s *SyncService) withReadinessTransaction(ctx context.Context, deadline time.Time, fn func(pgx.Tx) error) error {
	conn, tx, err := s.beginReadinessTransaction(ctx, deadline)
	if err != nil {
		return err
	}
	defer conn.Release()
	defer cleanupReadinessTransaction(conn, tx)
	if err := inspectReadiness(ctx, tx, s.config.Readiness); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := s.inspectCatalogReadiness(ctx, tx); err != nil {
		return err
	}
	if err := inspectReadiness(ctx, tx, s.config.Readiness); err != nil {
		return err
	}
	// An ambiguous commit is never published; the caller must Bootstrap anew.
	return tx.Commit(ctx)
}

func (s *SyncService) inspectCatalogReadiness(ctx context.Context, tx pgx.Tx) error {
	adapter, ok := s.config.Readiness.Adapter.(CatalogReadinessAdapter)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Readiness.InspectionTimeout)
	defer cancel()
	r := &readinessReader{ctx: ctx, tx: tx, remaining: s.config.Readiness.MaxQueries}
	defer r.close()
	var revision int64
	var fingerprint string
	if err := r.QueryRow(ctx, `SELECT catalog_revision,catalog_fingerprint FROM sync.meta WHERE singleton_key=TRUE`).Scan(&revision, &fingerprint); err != nil {
		return err
	}
	if err := adapter.ValidateCatalogReadiness(ctx, r, revision, fingerprint); err != nil {
		return readinessAdapterError(err)
	}
	return nil
}
