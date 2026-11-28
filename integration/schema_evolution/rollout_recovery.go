package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// RecoveryCut must be retained outside the database and its backup, under the
// operator's custody. The digest of this record must be bound by the recovery
// plan. An earlier archive with a matching lifecycle ledger is insufficient.
type RecoveryCut struct {
	Version       int
	ID            string
	Database      string
	AuthorityID   string
	Epoch         int64
	TerminalLSN   string
	CapturedAt    time.Time
	PlanDigest    string
	StateSHA256   string
	ArchiveSHA256 string
	ArchiveBytes  int64
	StateBytes    int64
	MaxBytes      int64
	Ledger        *oversync.GenerationLedger
}

// RecoveryVerification is the durable result that a reopen plan binds to the
// independently retained recovery cut.
type RecoveryVerification struct {
	Version       int
	CutID         string
	CutSHA256     string
	ArchiveSHA256 string
	StateSHA256   string
	Database      string
	AuthorityID   string
	Epoch         int64
	VerifiedAt    time.Time
}

func RecoveryVerificationDigest(v RecoveryVerification) (string, error) {
	if v.Version != 1 || v.CutID == "" || v.Database == "" || v.AuthorityID == "" || v.Epoch < 1 || v.VerifiedAt.IsZero() {
		return "", fmt.Errorf("recovery_verification_incomplete")
	}
	for _, value := range []string{v.CutSHA256, v.ArchiveSHA256, v.StateSHA256} {
		if !rolloutDigestPattern.MatchString(value) {
			return "", fmt.Errorf("recovery_verification_identity_invalid")
		}
	}
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return digest(body), nil
}

// RecoveryArchiveDriver exports the exact PostgreSQL snapshot supplied by the
// authority. Restore runs only into an isolated, unrouted, writer-fenced database.
// It must restore this archive, not replay client requests to imitate its state.
type RecoveryArchiveDriver interface {
	Dump(context.Context, string, io.Writer) error
	Restore(context.Context, io.Reader) error
}

func RecoveryCutDigest(c RecoveryCut) (string, error) {
	if c.Version != 1 || c.ID == "" || c.Database == "" || c.AuthorityID == "" || c.Epoch < 1 || c.TerminalLSN == "" || c.CapturedAt.IsZero() || c.MaxBytes < 1 || c.ArchiveBytes < 1 || c.ArchiveBytes > c.MaxBytes || c.StateBytes < 1 || c.StateBytes > c.MaxBytes || c.Ledger == nil {
		return "", fmt.Errorf("recovery_cut_incomplete")
	}
	for _, fp := range []string{c.PlanDigest, c.StateSHA256, c.ArchiveSHA256} {
		if !rolloutDigestPattern.MatchString(fp) {
			return "", fmt.Errorf("recovery_cut_identity_invalid")
		}
	}
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	if len(b) > 1<<20 {
		return "", fmt.Errorf("recovery_cut_capacity")
	}
	return digest(b), nil
}

// CaptureRecoveryCut runs after the terminal writer fence and keeps its snapshot
// and authority session pinned until the archive and full-state hashes finish.
// The caller atomically retains the returned cut and archive outside this database
// before releasing the predecessor fence or admitting a successor.
func (a *Authority) CaptureRecoveryCut(ctx context.Context, p RolloutPlan, id string, maxBytes int64, out io.Writer, driver RecoveryArchiveDriver) (RecoveryCut, error) {
	var cut RecoveryCut
	if a == nil || a.conn == nil || out == nil || driver == nil || id == "" || maxBytes < 1 || maxBytes > 1<<40 {
		return cut, fmt.Errorf("recovery_capture_bounds_required")
	}
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	planDigest, err := RolloutPlanDigest(p)
	if err != nil {
		return cut, err
	}
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		report, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if !recoveryPreparationReady(report.Blockers) {
			return fmt.Errorf("recovery_cut_writer_fence_incomplete")
		}
		if _, err = tx.Exec(ctx, `INSERT INTO app_evolution.recovery_gate(singleton,cut_id,plan_digest,database_name,authority_id,epoch,state) VALUES(true,$1,$2,$3,$4,$5,'required') ON CONFLICT(singleton) DO NOTHING`, id, planDigest, p.Database, p.AuthorityID, p.Epoch); err != nil {
			return err
		}
		var gateID, gatePlan, gateDatabase, gateAuthority, gateState string
		var gateEpoch int64
		if err = tx.QueryRow(ctx, `SELECT cut_id,plan_digest,database_name,authority_id,epoch,state FROM app_evolution.recovery_gate WHERE singleton FOR UPDATE`).Scan(&gateID, &gatePlan, &gateDatabase, &gateAuthority, &gateEpoch, &gateState); err != nil {
			return err
		}
		if gateID != id || gatePlan != planDigest || gateDatabase != p.Database || gateAuthority != p.AuthorityID || gateEpoch != p.Epoch || gateState != "required" {
			return fmt.Errorf("recovery_gate_identity_mismatch")
		}
		return nil
	})
	if err != nil {
		return cut, err
	}
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		report, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if !recoveryPreparationReady(report.Blockers) {
			return fmt.Errorf("recovery_cut_writer_fence_incomplete")
		}
		var gateID, gatePlan string
		if err = tx.QueryRow(ctx, `SELECT cut_id,plan_digest FROM app_evolution.recovery_gate WHERE singleton AND state='required'`).Scan(&gateID, &gatePlan); err != nil {
			return err
		}
		if gateID != id || gatePlan != planDigest {
			return fmt.Errorf("recovery_gate_identity_mismatch")
		}
		cut = RecoveryCut{Version: 1, ID: id, Database: p.Database, AuthorityID: p.AuthorityID, Epoch: p.Epoch, PlanDigest: report.PlanDigest, MaxBytes: maxBytes, Ledger: report.Ledger}
		if err = tx.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text,transaction_timestamp()`).Scan(&cut.TerminalLSN, &cut.CapturedAt); err != nil {
			return err
		}
		var snapshot string
		if err = tx.QueryRow(ctx, `SELECT pg_export_snapshot()`).Scan(&snapshot); err != nil {
			return err
		}
		cut.StateSHA256, cut.StateBytes, err = hashRecoveryState(ctx, tx, maxBytes)
		if err != nil {
			return err
		}
		archive := newRecoveryHashWriter(out, maxBytes)
		if err = driver.Dump(ctx, snapshot, archive); err != nil {
			return err
		}
		cut.ArchiveSHA256 = hex.EncodeToString(archive.hash.Sum(nil))
		cut.ArchiveBytes = archive.count
		_, err = RecoveryCutDigest(cut)
		return err
	})
	if err != nil {
		return cut, err
	}
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM app_evolution.recovery_gate WHERE singleton AND state='required' AND cut_id=$1 AND plan_digest=$2`, id, planDigest)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("recovery_gate_changed")
		}
		return nil
	})
	return cut, err
}

// VerifyRecoveredCut must precede every recovery writer or ordinary admission.
// expectedCutDigest and currentLedger come from independent external custody.
// This first-release adapter accepts only exact fenced-cut restoration. It does
// not claim a replay capability for an older base backup.
func VerifyRecoveredCut(ctx context.Context, pool *pgxpool.Pool, cut RecoveryCut, expectedCutDigest, archiveDigest string, currentLedger *oversync.GenerationLedger, p RolloutPlan) (RecoveryVerification, error) {
	var verification RecoveryVerification
	actual, err := RecoveryCutDigest(cut)
	if err != nil {
		return verification, err
	}
	if actual != expectedCutDigest || archiveDigest != cut.ArchiveSHA256 {
		return verification, fmt.Errorf("recovery_external_artifact_mismatch")
	}
	expectedLedger, err := json.Marshal(currentLedger)
	if err != nil {
		return verification, err
	}
	cutLedger, err := json.Marshal(cut.Ledger)
	if err != nil {
		return verification, err
	}
	if digest(expectedLedger) != digest(cutLedger) {
		return verification, fmt.Errorf("recovery_lifecycle_journal_requires_qualified_forward_recovery")
	}
	a, err := OpenClosedRolloutAuthority(ctx, pool)
	if err != nil {
		return verification, err
	}
	defer a.Close()
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		report, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if !recoveryPreparationReady(report.Blockers) {
			return fmt.Errorf("recovered_writer_fence_incomplete")
		}
		if p.Database != cut.Database || p.AuthorityID != cut.AuthorityID || p.Epoch != cut.Epoch {
			return fmt.Errorf("recovery_authority_cut_mismatch")
		}
		var gateID, gatePlan, gateDatabase, gateAuthority, gateState, storedCut, storedArchive, storedDigest string
		var gateEpoch int64
		var storedBody []byte
		if err = tx.QueryRow(ctx, `SELECT cut_id,plan_digest,database_name,authority_id,epoch,state,COALESCE(cut_sha256,''),COALESCE(archive_sha256,''),COALESCE(verification_sha256,''),COALESCE(verification,'null'::jsonb) FROM app_evolution.recovery_gate WHERE singleton FOR UPDATE`).Scan(&gateID, &gatePlan, &gateDatabase, &gateAuthority, &gateEpoch, &gateState, &storedCut, &storedArchive, &storedDigest, &storedBody); err != nil {
			return fmt.Errorf("recovery_gate_missing: %w", err)
		}
		if gateID != cut.ID || gatePlan != cut.PlanDigest || gateDatabase != cut.Database || gateAuthority != cut.AuthorityID || gateEpoch != cut.Epoch {
			return fmt.Errorf("recovery_gate_identity_mismatch")
		}
		if gateState == "verified" {
			if err = json.Unmarshal(storedBody, &verification); err != nil {
				return fmt.Errorf("recovery_verification_corrupt: %w", err)
			}
			verificationDigest, digestErr := RecoveryVerificationDigest(verification)
			if digestErr != nil || verificationDigest != storedDigest || storedCut != actual || storedArchive != cut.ArchiveSHA256 || verification.StateSHA256 != cut.StateSHA256 {
				return fmt.Errorf("recovery_verification_identity_mismatch")
			}
			return nil
		}
		if gateState != "required" {
			return fmt.Errorf("recovery_gate_identity_mismatch")
		}
		state, bytes, err := hashRecoveryState(ctx, tx, cut.MaxBytes)
		if err != nil {
			return err
		}
		if state != cut.StateSHA256 || bytes != cut.StateBytes {
			return fmt.Errorf("recovery_missing_or_mismatched_committed_state")
		}
		verification = RecoveryVerification{Version: 1, CutID: cut.ID, CutSHA256: actual, ArchiveSHA256: cut.ArchiveSHA256, StateSHA256: cut.StateSHA256, Database: cut.Database, AuthorityID: cut.AuthorityID, Epoch: cut.Epoch, VerifiedAt: time.Now().UTC()}
		verificationDigest, err := RecoveryVerificationDigest(verification)
		if err != nil {
			return err
		}
		body, err := json.Marshal(verification)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE app_evolution.recovery_gate SET state='verified',cut_sha256=$1,archive_sha256=$2,verification_sha256=$3,verification=$4 WHERE singleton AND state='required'`, actual, cut.ArchiveSHA256, verificationDigest, body)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("recovery_gate_changed")
		}
		return nil
	})
	return verification, err
}

func validateRecoveryAdmission(ctx context.Context, r oversync.DatabaseReadinessReader, p *RolloutPlan) error {
	var installed bool
	if err := r.QueryRow(ctx, `SELECT to_regclass('app_evolution.recovery_gate') IS NOT NULL`).Scan(&installed); err != nil {
		return err
	}
	if !installed {
		return nil
	}
	var state, cutDigest, resultDigest string
	err := r.QueryRow(ctx, `SELECT state,COALESCE(cut_sha256,''),COALESCE(verification_sha256,'') FROM app_evolution.recovery_gate WHERE singleton`).Scan(&state, &cutDigest, &resultDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state != "verified" {
		return fmt.Errorf("recovery_verification_required")
	}
	if p != nil && (p.RecoveryCutSHA256 != cutDigest || p.RecoveryResultSHA256 != resultDigest) {
		return fmt.Errorf("recovery_reopen_identity_mismatch")
	}
	return nil
}

func recoveryPreparationReady(blockers []string) bool {
	for _, blocker := range blockers {
		if blocker != "recovery_cut_not_armed" && blocker != "recovery_completion_missing" {
			return false
		}
	}
	return true
}

type recoveryHashWriter struct {
	out          io.Writer
	hash         hash.Hash
	limit, count int64
}

func newRecoveryHashWriter(out io.Writer, limit int64) *recoveryHashWriter {
	return &recoveryHashWriter{out: out, hash: sha256.New(), limit: limit}
}
func (w *recoveryHashWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.limit-w.count {
		return 0, fmt.Errorf("recovery_byte_budget_exceeded")
	}
	n, err := w.out.Write(b)
	if n > 0 {
		_, _ = w.hash.Write(b[:n])
		w.count += int64(n)
	}
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return n, err
}

func hashRecoveryState(ctx context.Context, tx pgx.Tx, maxBytes int64) (string, int64, error) {
	if _, err := tx.Exec(ctx, `SELECT set_config('TimeZone','UTC',true),set_config('DateStyle','ISO, YMD',true),set_config('bytea_output','hex',true),set_config('work_mem','4MB',true)`); err != nil {
		return "", 0, err
	}
	rows, err := tx.Query(ctx, `SELECT n.nspname,c.relname,c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('main','sync','app_evolution') AND c.relkind IN ('r','p','S') AND NOT c.relispartition ORDER BY n.nspname COLLATE "C",c.relname COLLATE "C" LIMIT 257`)
	if err != nil {
		return "", 0, err
	}
	type relation struct{ schema, name, kind string }
	var relations []relation
	for rows.Next() {
		var rel relation
		if err = rows.Scan(&rel.schema, &rel.name, &rel.kind); err != nil {
			rows.Close()
			return "", 0, err
		}
		relations = append(relations, rel)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return "", 0, err
	}
	if len(relations) == 0 || len(relations) > 256 {
		return "", 0, fmt.Errorf("recovery_inventory_capacity")
	}
	writer := newRecoveryHashWriter(io.Discard, maxBytes)
	for _, rel := range relations {
		identity := pgx.Identifier{rel.schema, rel.name}.Sanitize()
		if _, err = fmt.Fprintf(writer, "%d:%s:%s\n", len(identity), identity, rel.kind); err != nil {
			return "", 0, err
		}
		query := `SELECT row_to_json(t)::text AS value FROM ` + identity + ` t`
		if rel.kind == "S" {
			query = `SELECT json_build_array(last_value,is_called)::text AS value FROM ` + identity
		}
		_, err = tx.Conn().PgConn().CopyTo(ctx, writer, `COPY (SELECT value FROM (`+query+`) state_rows ORDER BY value COLLATE "C") TO STDOUT`)
		if err != nil {
			return "", 0, err
		}
	}
	return hex.EncodeToString(writer.hash.Sum(nil)), writer.count, nil
}

// RestoreRecoveryArchive checks external cut identity before restoring and checks
// the entire archive stream before returning success. It grants no admission.
func RestoreRecoveryArchive(ctx context.Context, driver RecoveryArchiveDriver, in io.Reader, cut RecoveryCut, expectedCutDigest string) error {
	actual, err := RecoveryCutDigest(cut)
	if err != nil {
		return err
	}
	if actual != expectedCutDigest {
		return fmt.Errorf("recovery_external_cut_mismatch")
	}
	if driver == nil || in == nil {
		return fmt.Errorf("recovery_archive_required")
	}
	writer := newRecoveryHashWriter(io.Discard, cut.MaxBytes)
	stream := io.TeeReader(in, writer)
	if err = driver.Restore(ctx, stream); err != nil {
		return err
	}
	if _, err = io.Copy(io.Discard, stream); err != nil {
		return err
	}
	if writer.count != cut.ArchiveBytes || hex.EncodeToString(writer.hash.Sum(nil)) != cut.ArchiveSHA256 {
		return fmt.Errorf("recovery_archive_integrity_mismatch")
	}
	return nil
}

// PrepareRecoveredCatalog restores three exact catalog CHECK declarations after
// pg_dump's SQL deparser flattens their BETWEEN expression trees. Full committed
// state must already match the external cut. No predicate is weakened, and any
// expression other than the two known exact renderings fails closed.
func PrepareRecoveredCatalog(ctx context.Context, pool *pgxpool.Pool, cut RecoveryCut, externalDigest string, p RolloutPlan) error {
	actual, err := RecoveryCutDigest(cut)
	if err != nil {
		return err
	}
	if actual != externalDigest {
		return fmt.Errorf("recovery_external_cut_mismatch")
	}
	a, err := OpenClosedRolloutAuthority(ctx, pool)
	if err != nil {
		return err
	}
	defer a.Close()
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		report, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if !recoveryPreparationReady(report.Blockers) {
			return fmt.Errorf("recovery_fence_incomplete")
		}
		state, bytes, err := hashRecoveryState(ctx, tx, cut.MaxBytes)
		if err != nil {
			return err
		}
		if state != cut.StateSHA256 || bytes != cut.StateBytes {
			return fmt.Errorf("recovery_missing_or_mismatched_committed_state")
		}
		for _, column := range []string{"schema_name", "table_name", "sync_key_column"} {
			name := "table_catalog_" + column + "_chk"
			suffix := ""
			sqlSuffix := ""
			if column == "sync_key_column" {
				suffix = " AND (sync_key_column <> '_sync_scope_id'::text)"
				sqlSuffix = " AND sync_key_column <> '_sync_scope_id'"
			}
			flattened := fmt.Sprintf("((octet_length(%s) >= 1) AND (octet_length(%s) <= 63) AND (%s ~ '^[a-z0-9_]+$'::text)%s)", column, column, column, suffix)
			canonical := fmt.Sprintf("(((octet_length(%s) >= 1) AND (octet_length(%s) <= 63)) AND (%s ~ '^[a-z0-9_]+$'::text)%s)", column, column, column, suffix)
			var expression string
			if err = tx.QueryRow(ctx, `SELECT pg_get_expr(conbin,conrelid) FROM pg_constraint WHERE conrelid='sync.table_catalog'::regclass AND conname=$1 AND convalidated AND contype='c'`, name).Scan(&expression); err != nil {
				return err
			}
			if expression == canonical {
				continue
			}
			if expression != flattened {
				return fmt.Errorf("restored_catalog_expression_unqualified")
			}
			statement := `ALTER TABLE sync.table_catalog DROP CONSTRAINT ` + pgx.Identifier{name}.Sanitize() + `, ADD CONSTRAINT ` + pgx.Identifier{name}.Sanitize() + ` CHECK (octet_length(` + column + `) BETWEEN 1 AND 63 AND ` + column + ` ~ '^[a-z0-9_]+$'` + sqlSuffix + `)`
			if _, err = tx.Exec(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}
