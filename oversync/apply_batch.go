// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var errUploadBatchedApplyFailed = errors.New("upload batched apply failed")

type sidecarApplyOutcome struct {
	code           int
	newServerVer64 int64
}

const uploadApplyBatchChunkSize = 128

func (s *SyncService) applyUpsertsSidecarBatched(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	sourceID string,
	upserts []ChangeUpload,
	applyIdx []int,
) ([]sidecarApplyOutcome, error) {
	outcomes := make([]sidecarApplyOutcome, 0, len(applyIdx))

	for start := 0; start < len(applyIdx); start += uploadApplyBatchChunkSize {
		end := start + uploadApplyBatchChunkSize
		if end > len(applyIdx) {
			end = len(applyIdx)
		}
		chunkIdx := applyIdx[start:end]

		b := &pgx.Batch{}
		for _, idx := range chunkIdx {
			ch := upserts[idx]
			b.Queue(
				stmtApplyUpsert,
				userID,
				ch.Schema,
				ch.Table,
				ch.Op,
				ch.PK,
				ch.Payload,
				sourceID,
				ch.SourceChangeID,
				ch.ServerVersion,
			).QueryRow(func(row pgx.Row) error {
				var o sidecarApplyOutcome
				if err := row.Scan(&o.code, &o.newServerVer64); err != nil {
					return err
				}
				outcomes = append(outcomes, o)
				return nil
			})
		}

		br := tx.SendBatch(ctx, b)
		if err := br.Close(); err != nil {
			return nil, fmt.Errorf("%w: %w", errUploadBatchedApplyFailed, err)
		}
	}

	return outcomes, nil
}

func (s *SyncService) applyDeletesSidecarBatched(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	sourceID string,
	deletes []ChangeUpload,
	applyIdx []int,
) ([]sidecarApplyOutcome, error) {
	outcomes := make([]sidecarApplyOutcome, 0, len(applyIdx))

	for start := 0; start < len(applyIdx); start += uploadApplyBatchChunkSize {
		end := start + uploadApplyBatchChunkSize
		if end > len(applyIdx) {
			end = len(applyIdx)
		}
		chunkIdx := applyIdx[start:end]

		b := &pgx.Batch{}
		for _, idx := range chunkIdx {
			ch := deletes[idx]
			b.Queue(
				stmtApplyDelete,
				userID,
				ch.Schema,
				ch.Table,
				ch.Op,
				ch.PK,
				sourceID,
				ch.SourceChangeID,
				ch.ServerVersion,
			).QueryRow(func(row pgx.Row) error {
				var o sidecarApplyOutcome
				if err := row.Scan(&o.code, &o.newServerVer64); err != nil {
					return err
				}
				outcomes = append(outcomes, o)
				return nil
			})
		}

		br := tx.SendBatch(ctx, b)
		if err := br.Close(); err != nil {
			return nil, fmt.Errorf("%w: %w", errUploadBatchedApplyFailed, err)
		}
	}

	return outcomes, nil
}
