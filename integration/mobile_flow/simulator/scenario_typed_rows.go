package simulator

import (
	"context"
	"fmt"
)

const (
	typedNullRowID = "11111111-1111-4111-8111-111111111111"
	typedRichRowID = "22222222-2222-4222-8222-222222222222"
)

type TypedRowsScenario struct {
	*BaseScenario
}

func NewTypedRowsScenario(simulator *Simulator) Scenario {
	return &TypedRowsScenario{
		BaseScenario: NewBaseScenario(simulator, "typed-rows"),
	}
}

func (s *TypedRowsScenario) Name() string {
	return "Typed Rows"
}

func (s *TypedRowsScenario) Description() string {
	return "Verifies exact typed-row conversion through the declared per-column SQLite codecs"
}

func (s *TypedRowsScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("Executing Typed Rows Scenario")

	if err := s.app.onSignIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in: %w", err)
	}

	if _, err := s.app.db.Exec(`
		INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
		VALUES(?, ?, NULL, NULL, ?, NULL, NULL, NULL)
	`, typedNullRowID, "Typed Null", 0); err != nil {
		return fmt.Errorf("failed to insert null typed row: %w", err)
	}

	if _, err := s.app.db.Exec(`
		INSERT INTO typed_rows(
			id, name, note, count_value, small_count, medium_count, exact_amount,
			enabled_flag, rating, float4_value, data, created_at
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		typedRichRowID,
		"Typed Rich",
		"second-device",
		int64(9007199254740993),
		int64(-32768),
		int64(2147483647),
		"12345678901234567890.1234567890",
		1,
		1.25,
		3.5,
		[]byte{0xca, 0xfe, 0xba, 0xbe},
		"2026-03-24T18:42:11Z",
	); err != nil {
		return fmt.Errorf("failed to insert rich typed row: %w", err)
	}

	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("typed-row upload failed: %w", err)
	}
	return nil
}

func (s *TypedRowsScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	logger := s.simulator.GetLogger()
	logger.Info("Verifying typed rows scenario")

	count, err := verifier.CountActualBusinessRecords(ctx, "business.typed_rows", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count typed rows: %w", err)
	}
	if count != 2 {
		return fmt.Errorf("expected exact typed-row conversion to commit two authoritative rows for %s, got %d", s.config.UserID, count)
	}

	logger.Info("Typed rows exact conversion verified")
	return nil
}
