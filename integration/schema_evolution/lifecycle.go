package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// DeleteScope keeps the immutable incarnation tombstone in the same registered
// transaction as physical deletion. Empty scopes use a locked, read-only business
// recheck before committing their application-only tombstone.
func DeleteScope(ctx context.Context, pool *pgxpool.Pool, manager *oversync.ScopeManager, scope string, incarnation uuid.UUID, opts oversync.ScopeWriteOptions) error {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := manager.ExecWrite(bounded, scope, opts, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if err := lockLifecycleScope(ctx, tx, scope, incarnation); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM main.upgrade_people WHERE _sync_scope_id=$1`, scope); err != nil {
			return err
		}
		return deleteScopeInventoryRow(ctx, tx, scope, incarnation)
	})
	var noop *oversync.ScopeWriteNoCapturedChangesError
	if !errors.As(err, &noop) {
		return err
	}
	return pgx.BeginTxFunc(bounded, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := lockLifecycleScope(bounded, tx, scope, incarnation); err != nil {
			return err
		}
		var remaining bool
		if err := tx.QueryRow(bounded, `SELECT EXISTS(SELECT 1 FROM main.upgrade_people WHERE _sync_scope_id=$1)`, scope).Scan(&remaining); err != nil {
			return err
		}
		if remaining {
			return fmt.Errorf("scope_delete_revalidation_failed")
		}
		return deleteScopeInventoryRow(bounded, tx, scope, incarnation)
	})
}

func createScopeInventoryRow(ctx context.Context, tx applicationTx, scope string, incarnation uuid.UUID) error {
	var actualScope string
	var actualIncarnation uuid.UUID
	var deleted bool
	err := tx.QueryRow(ctx, `INSERT INTO app_evolution.scope_inventory(scope_id,incarnation) VALUES($1,$2) RETURNING scope_id,incarnation,deleted`, scope, incarnation).Scan(&actualScope, &actualIncarnation, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("scope_inventory_create_not_applied")
	}
	if err != nil {
		return err
	}
	if actualScope != scope || actualIncarnation != incarnation || deleted {
		return fmt.Errorf("scope_inventory_create_not_applied")
	}
	return verifyScopeInventoryMutationContract(ctx, tx)
}

func deleteScopeInventoryRow(ctx context.Context, tx applicationTx, scope string, incarnation uuid.UUID) error {
	var actualScope string
	var actualIncarnation uuid.UUID
	var deleted bool
	err := tx.QueryRow(ctx, `UPDATE app_evolution.scope_inventory SET deleted=true WHERE scope_id=$1 AND incarnation=$2 RETURNING scope_id,incarnation,deleted`, scope, incarnation).Scan(&actualScope, &actualIncarnation, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("scope_inventory_delete_not_applied")
	}
	if err != nil {
		return err
	}
	if actualScope != scope || actualIncarnation != incarnation || !deleted {
		return fmt.Errorf("scope_inventory_delete_not_applied")
	}
	return verifyScopeInventoryMutationContract(ctx, tx)
}

func verifyScopeInventoryMutationContract(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	var extras int
	if err := r.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM pg_trigger WHERE tgrelid='app_evolution.scope_inventory'::regclass AND tgname<>'aa_app_scope_guard')+
 (SELECT count(*) FROM pg_rewrite WHERE ev_class='app_evolution.scope_inventory'::regclass AND rulename<>'_RETURN')`).Scan(&extras); err != nil {
		return err
	}
	if extras != 0 {
		return fmt.Errorf("scope_inventory_mutation_contract_changed")
	}
	expected := expectedMechanisms(1, "")[2]
	actual, err := readMechanism(ctx, r, expected)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("scope_inventory_mutation_contract_changed")
	}
	return nil
}

func lockLifecycleScope(ctx context.Context, tx applicationTx, scope string, incarnation uuid.UUID) error {
	if err := lockAdmission(ctx, tx, false); err != nil {
		return err
	}
	var closed bool
	if err := tx.QueryRow(ctx, `SELECT closed OR (SELECT requested FROM app_evolution.maintenance WHERE singleton) FROM app_evolution.barrier WHERE singleton`).Scan(&closed); err != nil {
		return err
	}
	if closed {
		return fmt.Errorf("application_admission_closed")
	}
	var deleted bool
	if err := tx.QueryRow(ctx, `SELECT deleted FROM app_evolution.scope_inventory WHERE scope_id=$1 AND incarnation=$2 FOR UPDATE`, scope, incarnation).Scan(&deleted); err != nil {
		return err
	}
	if deleted {
		return fmt.Errorf("scope_incarnation_deleted")
	}
	return nil
}
