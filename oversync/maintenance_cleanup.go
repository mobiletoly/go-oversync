package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// StagingCleanupAdapter authorizes only private staging deletion. The trusted
// application must hold its shared admission lock through the caller's entire
// transaction and verify its closed epoch, worker lock and cleanup phase there.
// Ordinary Admit is deliberately not a substitute for this capability.
type StagingCleanupAdapter interface {
	AuthorizeStagingCleanup(context.Context, DatabaseReadinessReader) (StagingCleanupPermit, error)
}

// StagingCleanupPermit is copied from the adapter on each batch, never supplied
// by a client. Limits/deadline belong to the immutable application cleanup plan.
type StagingCleanupPermit struct {
	Epoch, DeploymentRevision, CatalogRevision              int64
	Worker, PlanID, RegistryFingerprint, CatalogFingerprint string
	Deadline                                                time.Time
	QueryTimeout                                            time.Duration
	MaxSessions, MaxRows, MaxEvidenceBytes                  int
}

// ExpiredStaging records removal eligibility, not whether an upload committed.
// Authoritative source watermarks and retained results settle that question.
type ExpiredStaging struct {
	Kind, ID              string
	ExpiresAt             time.Time
	UserPK                int64
	SourceID              string
	GenerationID          int64
	GenerationFingerprint string
	SourceBundleID        int64
	RequestHash           string
	DeletedRows           int64
	DeletedHeader         bool
}

// StagingCleanupBatch must be recorded in the application's audit ledger in
// the deletion transaction. A result is provisional until that transaction commits.
type StagingCleanupBatch struct {
	Permit   StagingCleanupPermit
	Cutoff   time.Time
	Sessions []ExpiredStaging
}

// CleanupExpiredStaging deletes a bounded batch using the caller-owned transaction.
// It neither commits nor rolls back. Any error requires rollback of the whole
// transaction; success requires atomic audit/accounting before commit. The worker
// must finish that transaction before stopping the private runtime. No HTTP route
// exposes this method, and it cannot create sessions or prune replay evidence.
func (s *SyncService) CleanupExpiredStaging(ctx context.Context, tx pgx.Tx) (StagingCleanupBatch, error) {
	var result StagingCleanupBatch
	if tx == nil || s == nil || s.config == nil || s.config.Readiness == nil || !s.config.Readiness.DisableBackgroundWorkers {
		return result, fmt.Errorf("staging cleanup requires a private maintenance runtime and transaction")
	}
	done, err := s.beginLocalOperation()
	if err != nil {
		return result, err
	}
	defer done()
	adapter, ok := s.config.Readiness.Adapter.(StagingCleanupAdapter)
	if !ok {
		return result, fmt.Errorf("staging cleanup capability required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Readiness.InspectionTimeout)
	defer cancel()
	if err := attestOriginSession(ctx, tx); err != nil {
		return result, err
	}
	reader := &readinessReader{ctx: ctx, tx: tx, remaining: s.config.Readiness.MaxQueries}
	defer reader.close()
	permit, err := adapter.AuthorizeStagingCleanup(ctx, reader)
	if err != nil {
		return result, err
	}
	if err := s.validateStagingCleanupPermit(permit); err != nil {
		return result, err
	}
	ctx, stop := context.WithDeadline(ctx, permit.Deadline)
	defer stop()
	ctx, timeout := context.WithTimeout(ctx, permit.QueryTimeout)
	defer timeout()
	result.Permit = permit
	if err := validateCleanupCatalog(ctx, tx, permit); err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&result.Cutoff); err != nil {
		return result, err
	}
	selected, err := selectExpiredStaging(ctx, tx, result.Cutoff, permit.MaxSessions)
	if err != nil {
		return result, err
	}
	result.Sessions = selected
	// Reserve conservative outcome space before any deletion. Strings were bounded
	// by the byte budget and no staged payload was read or copied.
	encoded, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if len(encoded)+128*len(result.Sessions) > permit.MaxEvidenceBytes {
		return result, fmt.Errorf("staging cleanup evidence limit exceeded")
	}
	remaining := permit.MaxRows
	for i := range result.Sessions {
		if err := deleteExpiredStaging(ctx, tx, &result.Sessions[i], remaining); err != nil {
			return result, err
		}
		remaining -= int(result.Sessions[i].DeletedRows)
	}
	return result, nil
}

func (s *SyncService) validateStagingCleanupPermit(p StagingCleanupPermit) error {
	if p.Epoch <= 0 || p.Worker == "" || p.PlanID == "" || !time.Now().Before(p.Deadline) || p.QueryTimeout <= 0 || p.QueryTimeout > 30*time.Second || p.MaxSessions < 1 || p.MaxSessions > 64 || p.MaxRows < 1 || p.MaxRows > 4096 || p.MaxEvidenceBytes < 1024 || p.MaxEvidenceBytes > 1<<20 {
		return fmt.Errorf("invalid staging cleanup permit or bounds")
	}
	if p.DeploymentRevision != s.config.Readiness.Expected.Revision || p.RegistryFingerprint != s.schemaGenerationRegistry.Fingerprint() || p.CatalogRevision != s.catalogMetadata.catalogRevision || p.CatalogFingerprint != s.catalogMetadata.catalogFingerprint {
		return fmt.Errorf("staging cleanup deployment/catalog mismatch")
	}
	return nil
}

func validateCleanupCatalog(ctx context.Context, tx pgx.Tx, p StagingCleanupPermit) error {
	var revision int64
	var fingerprint string
	if err := tx.QueryRow(ctx, `SELECT catalog_revision,catalog_fingerprint FROM sync.meta WHERE singleton_key=TRUE FOR SHARE`).Scan(&revision, &fingerprint); err != nil {
		return err
	}
	if revision != p.CatalogRevision || fingerprint != p.CatalogFingerprint {
		return fmt.Errorf("staging cleanup catalog drift")
	}
	return nil
}

func selectExpiredStaging(ctx context.Context, tx pgx.Tx, cutoff time.Time, limit int) ([]ExpiredStaging, error) {
	if limit == 0 {
		return nil, nil
	}
	query := `SELECT snapshot_id::text,expires_at,user_pk,source_id,schema_generation_id,schema_generation_fingerprint,0::bigint,''::text FROM sync.snapshot_sessions WHERE expires_at <= $1 ORDER BY expires_at,snapshot_id LIMIT $2 FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, query, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var selected []ExpiredStaging
	for rows.Next() {
		item := ExpiredStaging{Kind: "snapshot"}
		if err := rows.Scan(&item.ID, &item.ExpiresAt, &item.UserPK, &item.SourceID, &item.GenerationID, &item.GenerationFingerprint, &item.SourceBundleID, &item.RequestHash); err != nil {
			return nil, err
		}
		selected = append(selected, item)
	}
	return selected, rows.Err()
}

func deleteExpiredStaging(ctx context.Context, tx pgx.Tx, item *ExpiredStaging, limit int) error {
	if item.Kind != "snapshot" {
		return fmt.Errorf("unsupported staging kind %q", item.Kind)
	}
	query := `WITH doomed AS (SELECT row_ordinal FROM sync.snapshot_session_rows WHERE snapshot_id=$1::uuid ORDER BY row_ordinal LIMIT $2) DELETE FROM sync.snapshot_session_rows AS r USING doomed WHERE r.snapshot_id=$1::uuid AND r.row_ordinal=doomed.row_ordinal`
	tag, err := tx.Exec(ctx, query, item.ID, limit)
	if err != nil {
		return err
	}
	item.DeletedRows = tag.RowsAffected()
	query = `DELETE FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid AND NOT EXISTS(SELECT 1 FROM sync.snapshot_session_rows WHERE snapshot_id=$1::uuid)`
	tag, err = tx.Exec(ctx, query, item.ID)
	if err != nil {
		return err
	}
	item.DeletedHeader = tag.RowsAffected() == 1
	return nil
}
