// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MaterializeUpsert is a single business-table upsert operation (id + payload).
// It is only used for optional batched materialization hooks.
type MaterializeUpsert struct {
	PK               uuid.UUID
	Payload          []byte
	AttemptedVersion int64
}

// BatchMaterializationHandler is an optional extension to MaterializationHandler that allows
// batching multiple upserts/deletes into fewer database round-trips.
//
// Implementations MUST be idempotent and must only use the provided transaction.
// If a batched call returns an error, oversync will fall back to per-row materialization
// (with per-row savepoints) to preserve best-effort semantics and per-row failure records.
type BatchMaterializationHandler interface {
	ApplyUpsertBatch(ctx context.Context, tx pgx.Tx, schema, table string, upserts []MaterializeUpsert) error
	ApplyDeleteBatch(ctx context.Context, tx pgx.Tx, schema, table string, pks []uuid.UUID) error
}
