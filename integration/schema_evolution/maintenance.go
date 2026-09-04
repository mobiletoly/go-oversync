package schemaevolution

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type migrationCapability struct {
	epoch int64
	owner uuid.UUID
	pid   int32
}
type migrationContextKey struct{}

func (a ledgerAdapter) Admit(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	if err := lockAdmission(ctx, r, false); err != nil {
		return err
	}
	var closed bool
	var epoch, revision int64
	var registry string
	if err := r.QueryRow(ctx, `SELECT closed,revision FROM app_evolution.barrier WHERE singleton`).Scan(&closed, &epoch); err != nil {
		return err
	}
	var requested bool
	if err := r.QueryRow(ctx, `SELECT requested FROM app_evolution.maintenance WHERE singleton`).Scan(&requested); err != nil {
		return err
	}
	if err := r.QueryRow(ctx, `SELECT revision,contract->>'RegistryFingerprint' FROM app_evolution.deployment WHERE singleton`).Scan(&revision, &registry); err != nil {
		return err
	}
	if revision != a.revision || registry != a.registry {
		return fmt.Errorf("stale_deployment")
	}
	if !closed && !requested {
		return nil
	}
	capability, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok || capability.epoch != epoch {
		return fmt.Errorf("maintenance_in_progress")
	}
	return verifyWorker(ctx, r, capability)
}

func verifyWorker(ctx context.Context, r oversync.DatabaseReadinessReader, c migrationCapability) error {
	var valid bool
	err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.maintenance m JOIN pg_locks l ON l.pid=m.worker_pid AND l.locktype='advisory' AND l.classid=1869833075 AND l.objid=7 AND l.objsubid=2 AND l.granted WHERE m.singleton AND m.requested AND m.epoch=$1 AND m.worker=$2 AND m.worker_pid=$3)`, c.epoch, c.owner, c.pid).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("migration_worker_fenced")
	}
	return nil
}

// fenceWorkerMetadata serializes ownership verification and the entire metadata
// transaction with claimWorker's exclusive admission lock. A transaction admitted
// before lock loss finishes before takeover; a later predecessor cannot mutate state.
func fenceWorkerMetadata(ctx context.Context, tx pgx.Tx) error {
	capability, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok {
		return fmt.Errorf("migration_worker_required")
	}
	if err := lockAdmission(ctx, tx, false); err != nil {
		return err
	}
	return verifyWorker(ctx, tx, capability)
}

func claimWorker(ctx context.Context, conn *pgxpool.Conn) (migrationCapability, error) {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = bounded
	c := migrationCapability{owner: uuid.New(), pid: int32(conn.Conn().PgConn().PID())}
	err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		// A successor must wait for every operation admitted by its predecessor.
		if err := lockAdmission(ctx, tx, true); err != nil {
			return err
		}
		var closed bool
		if err := tx.QueryRow(ctx, `SELECT closed,revision FROM app_evolution.barrier WHERE singleton FOR UPDATE`).Scan(&closed, &c.epoch); err != nil {
			return err
		}
		if !closed {
			return fmt.Errorf("worker_requires_completed_drain")
		}
		tag, err := tx.Exec(ctx, `UPDATE app_evolution.maintenance SET worker=$1,worker_pid=$2 WHERE singleton AND requested AND epoch=$3`, c.owner, c.pid, c.epoch)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("maintenance_epoch_changed")
		}
		return nil
	})
	return c, err
}

func permitWorkerTransaction(ctx context.Context, tx oversync.DatabaseWriteTx) error {
	c, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok {
		return fmt.Errorf("migration_worker_required")
	}
	if err := verifyWorker(ctx, tx, c); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO app_evolution.worker_transactions(backend,transaction_id,epoch,worker) VALUES(pg_backend_pid(),pg_current_xact_id(),$1,$2) ON CONFLICT(backend) DO UPDATE SET transaction_id=excluded.transaction_id,epoch=excluded.epoch,worker=excluded.worker`, c.epoch, c.owner)
	return err
}

// RequirePreservingClients records the deployment prerequisite separately from
// schema generation proofs. The controller must install the matching independent
// authentication/allowlist policy on every serving route before calling this.
func (a *Authority) RequirePreservingClients(ctx context.Context, policy string) error {
	if policy == "" {
		return fmt.Errorf("client admission policy required")
	}
	_, err := a.conn.Exec(ctx, `UPDATE app_evolution.maintenance SET client_policy=$1 WHERE singleton AND requested`, policy)
	return err
}

// ClientRuntimeGuard accepts only application-authenticated evidence. A bare
// self-reported build header is insufficient. Rejection never changes sync state.
func ClientRuntimeGuard(supported func(*http.Request) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if supported == nil || !supported(r) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusUpgradeRequired)
			_, _ = w.Write([]byte(`{"error":"client_runtime_upgrade_required","message":"Update the application; keep local data and pending sync work"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func reopenMaintenance(ctx context.Context, pool *pgxpool.Pool, d oversync.DeploymentContract) error {
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1::int,$2::int)`, workerLock[0], workerLock[1]).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return fmt.Errorf("migration_worker_not_stopped")
		}
		var requested, closed bool
		var epoch, drained int64
		if err := tx.QueryRow(ctx, `SELECT m.requested,m.epoch,b.closed,b.revision FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b WHERE m.singleton AND b.singleton FOR UPDATE OF m,b`).Scan(&requested, &epoch, &closed, &drained); err != nil {
			return err
		}
		if requested && (!closed || epoch != drained) {
			return fmt.Errorf("maintenance_drain_incomplete")
		}
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT revision FROM app_evolution.deployment WHERE singleton FOR SHARE`).Scan(&revision); err != nil {
			return err
		}
		if revision != d.Revision {
			return fmt.Errorf("deployment_changed")
		}
		if err := verifyReopenTarget(ctx, tx, d); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, `UPDATE app_evolution.barrier SET closed=false WHERE singleton`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE app_evolution.maintenance SET requested=false,worker=NULL,worker_pid=NULL,completed_at=$1 WHERE singleton`, time.Now().UTC())
		return err
	})
}

// Transaction advisory locks require no UPDATE permission on control evidence.
func lockAdmission(ctx context.Context, r oversync.DatabaseReadinessReader, exclusive bool) error {
	statement := `SELECT pg_advisory_xact_lock_shared(1869833075,9)`
	if exclusive {
		statement = `SELECT pg_advisory_xact_lock(1869833075,9)`
	}
	var ignored any
	return r.QueryRow(ctx, statement).Scan(&ignored)
}

func verifyReopenTarget(ctx context.Context, tx pgx.Tx, d oversync.DeploymentContract) error {
	if d.TransitionPhase == "tables_serving" {
		return verifyTableReopen(ctx, tx, d)
	}
	if d.TransitionPhase == "coexistence" {
		var policy string
		if err := tx.QueryRow(ctx, `SELECT client_policy FROM app_evolution.maintenance WHERE singleton FOR UPDATE`).Scan(&policy); err != nil {
			return err
		}
		if policy == "" {
			return fmt.Errorf("preserving_client_prerequisite_missing")
		}
		if err := verifyCensus(ctx, tx); err != nil {
			return err
		}
		if err := verifyCertificateFence(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// MaintenanceStatus is the controller's durable operator report. A private
// SyncService's successful Bootstrap does not mean public admission is open.
type MaintenanceReport struct {
	Category      string
	Epoch         int64
	AdmissionOpen bool
	Drained       bool
}

func MaintenanceStatus(ctx context.Context, pool *pgxpool.Pool) (MaintenanceReport, error) {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var result MaintenanceReport
	var requested, closed bool
	var drainedEpoch int64
	err := pool.QueryRow(bounded, `SELECT m.requested,m.epoch,b.closed,b.revision FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b WHERE m.singleton AND b.singleton`).Scan(&requested, &result.Epoch, &closed, &drainedEpoch)
	if err != nil {
		return result, err
	}
	result.AdmissionOpen = !requested && !closed
	result.Drained = closed && drainedEpoch == result.Epoch
	if !result.AdmissionOpen {
		result.Category = "maintenance_in_progress"
	}
	return result, nil
}
