package schemaevolution

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

// validateAllRows scans the complete live physical population, including
// post-watermark scopes. A preserving transition or the closed writer barrier
// must protect this read-only scan. Its digest identifies the invariant policy,
// not mutable business values, so preserving ordinary writes do not stale it.
func validateAllRows(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.InvariantValidation, error) {
	result := oversync.InvariantValidation{Requirement: backfillRequirement, ResultSHA256: digest([]byte("person-encoding-invariant-v1:zero-violations"))}
	if err := r.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM app_evolution.scope_inventory`).Scan(&result.CoverageWatermark); err != nil {
		return result, err
	}
	if err := r.QueryRow(ctx, `SELECT count(*) FROM sync.scope_state WHERE state_code=1`).Scan(&result.Violations); err != nil {
		return result, err
	}
	scope := ""
	key := uuid.Nil
	for {
		rows, err := r.Query(ctx, `SELECT p._sync_scope_id,p.id,
   p.legacy_name IS NOT NULL AND p.name IS NOT NULL AND p.new_label IS NOT NULL
   AND p.name COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND p.new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$'
   AND p.legacy_name=p.name||'|'||p.new_label
   AND EXISTS(SELECT 1 FROM app_evolution.scope_inventory i WHERE i.scope_id=p._sync_scope_id AND NOT i.deleted)
   FROM main.upgrade_people p WHERE (p._sync_scope_id COLLATE "C",p.id)>($1 COLLATE "C",$2::uuid)
   ORDER BY p._sync_scope_id COLLATE "C",p.id LIMIT 100`, scope, key)
		if err != nil {
			return result, err
		}
		count := 0
		for rows.Next() {
			var valid bool
			if err := rows.Scan(&scope, &key, &valid); err != nil {
				rows.Close()
				return result, err
			}
			count++
			if !valid {
				result.Violations++
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return result, err
		}
		if count < 100 {
			return result, nil
		}
	}
}

func (a *Authority) Certify(ctx context.Context, through int64) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	tx, err := a.conn.Begin(bounded)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	var workerStopped bool
	if err := tx.QueryRow(bounded, `SELECT pg_try_advisory_xact_lock($1::int,$2::int)`, workerLock[0], workerLock[1]).Scan(&workerStopped); err != nil {
		return err
	}
	if !workerStopped {
		return fmt.Errorf("migration_worker_not_stopped")
	}
	if err := lockAdmission(bounded, tx, true); err != nil {
		return err
	}
	var fence bool
	if err := tx.QueryRow(bounded, `SELECT closed FROM app_evolution.barrier WHERE singleton FOR UPDATE`).Scan(&fence); err != nil {
		return err
	}
	if !fence {
		return fmt.Errorf("validation requires closed writer/lifecycle barrier")
	}
	if err := verifyCensus(bounded, a.conn); err != nil {
		return err
	}
	var closed bool
	if err := a.conn.QueryRow(bounded, `SELECT closed FROM app_evolution.barrier WHERE singleton`).Scan(&closed); err != nil {
		return err
	}
	if !closed {
		return fmt.Errorf("validation requires closed writer/lifecycle barrier")
	}
	var jobReady bool
	if err := a.conn.QueryRow(bounded, `SELECT EXISTS(SELECT 1 FROM app_evolution.jobs WHERE job=$1 AND revision=$2 AND algorithm=$3 AND transition_revision=$4 AND phase='maintaining' AND state IN ('traversed','validated'))`, JobID, JobRevision, backfillRequirement.AlgorithmSHA256, TransitionRevision).Scan(&jobReady); err != nil {
		return err
	}
	if !jobReady {
		return fmt.Errorf("backfill_job_evidence_missing")
	}
	if err := a.checkAppliedPrefix(bounded, through); err != nil {
		return err
	}
	for _, expected := range expectedMechanisms(through, map[bool]string{false: "maintaining", true: "coexistence"}[through == 4]) {
		actual, err := readMechanism(bounded, a.conn, expected)
		if err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("transition_mechanism_mismatch")
		}
	}
	var unresolved int64
	if err := a.conn.QueryRow(bounded, `SELECT (SELECT count(*) FROM app_evolution.row_work)+(SELECT count(*) FROM app_evolution.batches WHERE state='pending')+(SELECT count(*) FROM app_evolution.jobs WHERE state NOT IN ('traversed','validated'))`).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved != 0 {
		return fmt.Errorf("backfill_incomplete")
	}
	validation, err := validateAllRows(bounded, a.conn)
	if err != nil {
		return err
	}
	if validation.Violations != 0 {
		return fmt.Errorf("backfill_validation_failed")
	}
	if through < 2 || through > 4 {
		return fmt.Errorf("invalid certificate migration boundary")
	}
	phase := "maintaining"
	if through == 4 {
		phase = "coexistence"
	}
	cert := oversync.BackfillCertificate{Requirement: backfillRequirement, MigrationDigest: oversync.MigrationManifestDigest(a.manifest[:through]), TransitionRevision: TransitionRevision, TransitionPhase: phase, CoverageWatermark: validation.CoverageWatermark, ValidatedAt: time.Now().UTC(), ResultSHA256: validation.ResultSHA256}
	var frozen maintenanceCertificate
	frozen.BackfillCertificate = cert
	if err := a.conn.QueryRow(bounded, `SELECT epoch,census FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&frozen.Epoch, &frozen.Census); err != nil {
		return err
	}
	if err := persistCertificateInTx(bounded, tx, frozen); err != nil {
		return err
	}
	return tx.Commit(bounded)
}

// Persist bounded certificate evidence and its terminal job state together.
func persistCertificateInTx(ctx context.Context, tx pgx.Tx, frozen maintenanceCertificate) error {
	var bounds Bounds
	if err := tx.QueryRow(ctx, `SELECT bounds FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&bounds); err != nil {
		return err
	}
	if err := checkEvidenceCapacity(ctx, tx, bounds, int64(len(encodeJSON(frozen))+1024)); err != nil {
		return err
	}
	// Certificate and job terminal state commit atomically while the authority
	// keeps the barrier closed. Previous certificates remain historical evidence.
	if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.certificates(certificate) VALUES($1)`, frozen); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE app_evolution.jobs SET state='validated',pause_reason='' WHERE job=$1`, JobID); err != nil {
		return err
	}
	if err := checkEvidenceCapacity(ctx, tx, bounds, 0); err != nil {
		return err
	}
	return nil
}

func (a *Authority) requireMaintainingCertificate(ctx context.Context) error {
	if err := verifyCensus(ctx, a.conn); err != nil {
		return err
	}
	var cert oversync.BackfillCertificate
	if err := a.conn.QueryRow(ctx, `SELECT certificate FROM app_evolution.certificates ORDER BY certificate_id DESC LIMIT 1`).Scan(&cert); err != nil {
		return err
	}
	if cert.Requirement != backfillRequirement || cert.TransitionPhase != "maintaining" || cert.TransitionRevision != TransitionRevision || cert.MigrationDigest != oversync.MigrationManifestDigest(a.manifest[:3]) || cert.Unresolved != 0 || cert.Errors != 0 {
		return fmt.Errorf("maintaining_certificate_mismatch")
	}
	validation, err := validateAllRows(ctx, a.conn)
	if err != nil {
		return err
	}
	if validation.Violations != 0 || validation.ResultSHA256 != cert.ResultSHA256 {
		return fmt.Errorf("maintaining_certificate_invalid")
	}
	return nil
}
