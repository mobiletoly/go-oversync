package server

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadOrCreateOperationDeadline durably assigns one admission deadline before a
// retryable callback can run. A restart therefore reuses the same deadline even
// when the public write rolled back and left no Oversync receipt.
func LoadOrCreateOperationDeadline(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	operationID uuid.UUID,
	validFor time.Duration,
) (time.Time, error) {
	if pool == nil {
		return time.Time{}, fmt.Errorf("operation deadline requires a PostgreSQL pool")
	}
	if operationID == uuid.Nil {
		return time.Time{}, fmt.Errorf("operation deadline requires a nonzero operation ID")
	}
	if validFor <= 0 || validFor > 90*24*time.Hour || validFor%time.Microsecond != 0 {
		return time.Time{}, fmt.Errorf("operation deadline validity must be a positive microsecond-aligned duration of at most 90 days")
	}

	table := qualifiedTable(schema, "server_operation_deadlines")
	var deadline time.Time
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (operation_id, operation_valid_until)
			VALUES ($1, clock_timestamp() + ($2 * interval '1 microsecond'))
			ON CONFLICT (operation_id) DO NOTHING
		`, table), operationID, validFor.Microseconds()); err != nil {
			return fmt.Errorf("persist operation deadline: %w", err)
		}
		if err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT operation_valid_until
			FROM %s
			WHERE operation_id = $1
		`, table), operationID).Scan(&deadline); err != nil {
			return fmt.Errorf("load operation deadline: %w", err)
		}
		return nil
	})
	return deadline.UTC(), err
}
