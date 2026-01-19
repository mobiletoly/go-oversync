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

		schemaNames := make([]string, 0, len(chunkIdx))
		tableNames := make([]string, 0, len(chunkIdx))
		ops := make([]string, 0, len(chunkIdx))
		pkUUIDs := make([]string, 0, len(chunkIdx))
		payloads := make([]string, 0, len(chunkIdx))
		sourceChangeIDs := make([]int64, 0, len(chunkIdx))
		baseVersions := make([]int64, 0, len(chunkIdx))

		for _, idx := range chunkIdx {
			ch := upserts[idx]
			schemaNames = append(schemaNames, ch.Schema)
			tableNames = append(tableNames, ch.Table)
			ops = append(ops, ch.Op)
			pkUUIDs = append(pkUUIDs, ch.PK)
			payloads = append(payloads, string(ch.Payload))
			sourceChangeIDs = append(sourceChangeIDs, ch.SourceChangeID)
			baseVersions = append(baseVersions, ch.ServerVersion)
		}

		rows, err := tx.Query(ctx,
			stmtApplyUpsertBatch,
			userID,
			sourceID,
			schemaNames,
			tableNames,
			ops,
			pkUUIDs,
			payloads,
			sourceChangeIDs,
			baseVersions,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errUploadBatchedApplyFailed, err)
		}

		got := 0
		for rows.Next() {
			var o sidecarApplyOutcome
			if scanErr := rows.Scan(&o.code, &o.newServerVer64); scanErr != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: %w", errUploadBatchedApplyFailed, scanErr)
			}
			outcomes = append(outcomes, o)
			got++
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: %w", errUploadBatchedApplyFailed, rowsErr)
		}
		rows.Close()
		if got != len(chunkIdx) {
			return nil, fmt.Errorf("%w: unexpected row count in batch apply: got=%d want=%d", errUploadBatchedApplyFailed, got, len(chunkIdx))
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
