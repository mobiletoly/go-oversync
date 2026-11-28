// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadinessCategory is safe to expose in operator health reports. Adapter errors
// and evidence contents are deliberately excluded from the public HTTP response.
type ReadinessCategory string

const (
	MigrationMissing             ReadinessCategory = "migration_missing"
	MigrationAhead               ReadinessCategory = "migration_ahead"
	MigrationChecksumMismatch    ReadinessCategory = "migration_checksum_mismatch"
	MigrationIncomplete          ReadinessCategory = "migration_incomplete"
	DeploymentContractMismatch   ReadinessCategory = "deployment_contract_mismatch"
	TransitionMechanismMismatch  ReadinessCategory = "transition_mechanism_mismatch"
	BackfillIncomplete           ReadinessCategory = "backfill_incomplete"
	BackfillValidationFailed     ReadinessCategory = "backfill_validation_failed"
	ReadinessEvidenceUnavailable ReadinessCategory = "readiness_evidence_unavailable"
)

type ReadinessError struct {
	Category ReadinessCategory
	Err      error
}

func (e *ReadinessError) Error() string { return "application readiness: " + string(e.Category) }
func (e *ReadinessError) Unwrap() error { return e.Err }

// DatabaseReadinessReader belongs to one bounded Bootstrap inspection. Close
// rows before returning. The trusted adapter may only read database evidence;
// it must not perform remote I/O, writes, transaction control or acquire locks.
// Methods use the supplied inspection deadline even if a caller substitutes ctx.
type DatabaseReadinessReader interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type ReadinessAdapter interface {
	Inspect(context.Context, DatabaseReadinessReader) (ReadinessEvidence, error)
}

// CatalogReadinessAdapter checks the validated post-Bootstrap candidate inside
// its transaction. It can bind an application's append plan to the final catalog
// without mistaking the pre-append state for the proposed target.
type CatalogReadinessAdapter interface {
	ValidateCatalogReadiness(context.Context, DatabaseReadinessReader, int64, string) error
}

type MigrationIdentity struct {
	Version       int64
	Filename      string
	SHA256        string
	Transactional bool
	Class         string
}

type MigrationEvidence struct {
	Identity    MigrationIdentity
	State       string
	StartedAt   time.Time
	CompletedAt time.Time
	FailedAt    time.Time
}

// MechanismIdentity carries the complete application-controlled catalog
// identity (including definition, enablement, relation, event and order facts),
// encoded by the adapter in Definition. Core compares exact bytes.
type MechanismIdentity struct{ Kind, Identity, Definition string }

type DeploymentContract struct {
	GenerationLedger      *GenerationLedger
	WriteRejections       []ApplicationWriteRejection
	AuthorityID           string
	Revision              int64
	RegistryFingerprint   string
	Migrations            []MigrationIdentity
	TransitionRevision    string
	TransitionPhase       string
	Mechanisms            []MechanismIdentity
	ApplicationCodeSHA256 string
	QualificationSHA256   string
}

type BackfillRequirement struct {
	Job                string
	Revision           string
	AlgorithmSHA256    string
	ValidationRevision string
}

type GenerationReadiness struct {
	Proof                   SchemaGenerationProof
	MinimumMigrationVersion int64
	Backfills               []BackfillRequirement
}

type GenerationEnablement struct {
	Requirement        GenerationReadiness
	DeploymentRevision int64
	Enabled            bool
}

type BackfillCertificate struct {
	Requirement        BackfillRequirement
	MigrationDigest    string
	TransitionRevision string
	TransitionPhase    string
	CoverageWatermark  int64
	Unresolved         int64
	Errors             int64
	ValidatedAt        time.Time
	ResultSHA256       string
}

// InvariantValidation is fresh read-only validation, never a cached certificate
// success flag. The adapter executes its declared database predicate each time.
type InvariantValidation struct {
	Requirement       BackfillRequirement
	CoverageWatermark int64
	Violations        int64
	ResultSHA256      string
}

type ReadinessEvidence struct {
	Deployment   DeploymentContract
	Migrations   []MigrationEvidence
	Mechanisms   []MechanismIdentity
	Certificates []BackfillCertificate
	Validations  []InvariantValidation
	Generations  []GenerationEnablement
}

type ReadinessConfig struct {
	// DisableBackgroundWorkers is for private, controller-owned maintenance
	// runtimes. Ordinary operations still pass through application admission.
	DisableBackgroundWorkers bool
	// PoolPair is required for OperationAdmissionAdapter. The pair is created from
	// one immutable connection configuration so routing callbacks retain the same
	// captured state. Both pools remain caller-owned.
	PoolPair *ReadinessPoolPair
	// AdmissionPool is retained for source compatibility but is not accepted for
	// OperationAdmissionAdapter because independently created pools cannot prove
	// equivalent captured routing state.
	// Deprecated: use NewReadinessPoolPair and PoolPair.
	AdmissionPool *pgxpool.Pool
	Adapter       ReadinessAdapter
	AuthorityLock [2]int32
	Expected      DeploymentContract
	Generations   []GenerationReadiness
	// Zero selects the reference 10s contention / 30s inspection budgets.
	LockTimeout       time.Duration
	InspectionTimeout time.Duration
	MaxQueries        int
}

type ReadinessReport struct {
	DeploymentSHA256         string
	GenerationLedgerRevision int64
	ApplicationCodeSHA256    string
	QualificationSHA256      string
	CatalogRevision          int64
	CatalogFingerprint       string
	Ready                    bool
	ProcessStartID           string
	DeploymentRevision       int64
	RegistryFingerprint      string
	MigrationPosition        int64
	MigrationDigest          string
	TransitionRevision       string
	CompletedBackfills       []BackfillRequirement
	FailureCategory          ReadinessCategory
}

var readinessSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func readinessFailure(code ReadinessCategory) error { return &ReadinessError{Category: code} }

// MigrationManifestDigest binds every ordered identity, including filename,
// exact-byte checksum, lifecycle class and transaction mode.
func DeploymentContractDigest(contract DeploymentContract) string {
	b, _ := json.Marshal(contract)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func MigrationManifestDigest(entries []MigrationIdentity) string {
	b, _ := json.Marshal(entries)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func cloneReadinessConfig(in *ReadinessConfig) *ReadinessConfig {
	if in == nil {
		return nil
	}
	out := *in
	// All evidence types are data-only. Round-tripping gives independently owned
	// nested slices while retaining the application adapter separately.
	b, _ := json.Marshal(in.Expected)
	out.Expected = DeploymentContract{}
	_ = json.Unmarshal(b, &out.Expected)
	b, _ = json.Marshal(in.Generations)
	out.Generations = nil
	_ = json.Unmarshal(b, &out.Generations)
	return &out
}

func validateReadinessConfig(c *ReadinessConfig, registry *SchemaGenerationRegistry) error {
	if c == nil || c.Adapter == nil {
		return readinessFailure(ReadinessEvidenceUnavailable)
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = 10 * time.Second
	}
	if c.InspectionTimeout == 0 {
		c.InspectionTimeout = 30 * time.Second
	}
	if c.MaxQueries == 0 {
		c.MaxQueries = 10000
	}
	if c.LockTimeout <= 0 || c.InspectionTimeout <= 0 || c.MaxQueries < 1 || c.MaxQueries > 100000 {
		return readinessFailure(ReadinessEvidenceUnavailable)
	}
	d := c.Expected
	if d.GenerationLedger != nil {
		if err := ValidateGenerationLedger(d.GenerationLedger, registry); err != nil {
			return err
		}
	}
	if d.AuthorityID == "" || d.Revision < 1 || d.RegistryFingerprint != registry.Fingerprint() ||
		d.TransitionRevision == "" || d.TransitionPhase == "" || !readinessSHA256.MatchString(d.ApplicationCodeSHA256) || !readinessSHA256.MatchString(d.QualificationSHA256) {
		return readinessFailure(DeploymentContractMismatch)
	}
	if err := validateApplicationWriteRejections(d.WriteRejections); err != nil {
		return err
	}
	if err := validateMigrationManifest(d.Migrations); err != nil {
		return err
	}
	if err := validateMechanismInventory(d.Mechanisms); err != nil {
		return err
	}
	return validateReadinessGenerations(c.Generations, registry, d.Migrations)
}

func validateMigrationManifest(entries []MigrationIdentity) error {
	var previous int64
	names := map[string]bool{}
	for _, m := range entries {
		if m.Version <= previous || m.Filename == "" || names[m.Filename] || !readinessSHA256.MatchString(m.SHA256) || (m.Class != "expand" && m.Class != "constraint" && m.Class != "contract") {
			return readinessFailure(MigrationChecksumMismatch)
		}
		previous = m.Version
		names[m.Filename] = true
	}
	return nil
}

func validateMechanismInventory(entries []MechanismIdentity) error {
	seen := map[string]bool{}
	for _, m := range entries {
		key := m.Kind + "\x00" + m.Identity
		if m.Kind == "" || m.Identity == "" || m.Definition == "" || seen[key] {
			return readinessFailure(TransitionMechanismMismatch)
		}
		seen[key] = true
	}
	return nil
}

func validBackfillRequirement(r BackfillRequirement) bool {
	return r.Job != "" && r.Revision != "" && r.ValidationRevision != "" && readinessSHA256.MatchString(r.AlgorithmSHA256)
}

func validateReadinessGenerations(generations []GenerationReadiness, registry *SchemaGenerationRegistry, migrations []MigrationIdentity) error {
	if len(generations) != len(registry.GenerationIDs()) {
		return readinessFailure(DeploymentContractMismatch)
	}
	seen := map[SchemaGenerationID]bool{}
	var position int64
	if len(migrations) > 0 {
		position = migrations[len(migrations)-1].Version
	}
	for _, g := range generations {
		contract, ok := registry.Generation(g.Proof.ID)
		if !ok || seen[g.Proof.ID] || contract.Fingerprint() != g.Proof.Fingerprint || g.MinimumMigrationVersion < 0 || g.MinimumMigrationVersion > position {
			return readinessFailure(DeploymentContractMismatch)
		}
		seen[g.Proof.ID] = true
		jobs := map[string]bool{}
		for _, b := range g.Backfills {
			if !validBackfillRequirement(b) || jobs[b.Job] {
				return readinessFailure(BackfillIncomplete)
			}
			jobs[b.Job] = true
		}
	}
	return nil
}

func validateMigrationEvidence(expected []MigrationIdentity, actual []MigrationEvidence) error {
	if len(actual) > len(expected) {
		return readinessFailure(MigrationAhead)
	}
	for i, m := range actual {
		if m.Identity != expected[i] {
			return readinessFailure(MigrationChecksumMismatch)
		}
		if m.State != "completed" || m.StartedAt.IsZero() || m.CompletedAt.Before(m.StartedAt) || !m.FailedAt.IsZero() {
			return readinessFailure(MigrationIncomplete)
		}
	}
	if len(actual) < len(expected) {
		return readinessFailure(MigrationMissing)
	}
	return nil
}

func validateReadinessEvidence(c *ReadinessConfig, e ReadinessEvidence) error {
	if err := validateMigrationEvidence(c.Expected.Migrations, e.Migrations); err != nil {
		return err
	}
	if !reflect.DeepEqual(c.Expected, e.Deployment) {
		return readinessFailure(DeploymentContractMismatch)
	}
	if err := validateMechanismInventory(e.Mechanisms); err != nil {
		return err
	}
	if !reflect.DeepEqual(c.Expected.Mechanisms, e.Mechanisms) {
		return readinessFailure(TransitionMechanismMismatch)
	}
	if err := validateEvidenceUniqueness(e); err != nil {
		return err
	}
	for _, g := range c.Generations {
		if err := validateGenerationEvidence(c, g, e); err != nil {
			return err
		}
	}
	return nil
}

func validateEvidenceUniqueness(e ReadinessEvidence) error {
	generations := map[SchemaGenerationID]bool{}
	for _, g := range e.Generations {
		if generations[g.Requirement.Proof.ID] || g.Requirement.Proof.ID <= 0 || !readinessSHA256.MatchString(g.Requirement.Proof.Fingerprint) {
			return readinessFailure(DeploymentContractMismatch)
		}
		generations[g.Requirement.Proof.ID] = true
	}
	certificates := map[BackfillRequirement]bool{}
	for _, c := range e.Certificates {
		if certificates[c.Requirement] || !validBackfillRequirement(c.Requirement) {
			return readinessFailure(BackfillIncomplete)
		}
		certificates[c.Requirement] = true
	}
	validations := map[BackfillRequirement]bool{}
	for _, v := range e.Validations {
		if validations[v.Requirement] || !validBackfillRequirement(v.Requirement) {
			return readinessFailure(BackfillValidationFailed)
		}
		validations[v.Requirement] = true
	}
	return nil
}

func validateGenerationEvidence(c *ReadinessConfig, g GenerationReadiness, e ReadinessEvidence) error {
	found := false
	for _, enabled := range e.Generations {
		if enabled.Requirement.Proof.ID != g.Proof.ID {
			continue
		}
		found = enabled.Enabled && enabled.DeploymentRevision == c.Expected.Revision && reflect.DeepEqual(g, enabled.Requirement)
	}
	if !found {
		return readinessFailure(DeploymentContractMismatch)
	}
	for _, requirement := range g.Backfills {
		if err := validateBackfillEvidence(c.Expected, requirement, e); err != nil {
			return err
		}
	}
	return nil
}

func validateBackfillEvidence(d DeploymentContract, r BackfillRequirement, e ReadinessEvidence) error {
	var certificate *BackfillCertificate
	for i := range e.Certificates {
		if e.Certificates[i].Requirement == r {
			certificate = &e.Certificates[i]
		}
	}
	if certificate == nil || certificate.Unresolved != 0 || certificate.Errors != 0 {
		return readinessFailure(BackfillIncomplete)
	}
	c := certificate
	if c.MigrationDigest != MigrationManifestDigest(d.Migrations) || c.TransitionRevision != d.TransitionRevision || c.TransitionPhase != d.TransitionPhase || c.CoverageWatermark < 0 || c.ValidatedAt.IsZero() || !readinessSHA256.MatchString(c.ResultSHA256) {
		return readinessFailure(BackfillValidationFailed)
	}
	for _, v := range e.Validations {
		if v.Requirement == r && v.Violations == 0 && v.CoverageWatermark >= c.CoverageWatermark && v.ResultSHA256 == c.ResultSHA256 {
			return nil
		}
	}
	return readinessFailure(BackfillValidationFailed)
}

// Readiness returns a copied startup attestation, not a lease or fleet drain.
func (s *SyncService) Readiness() ReadinessReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	report := s.readinessReport
	report.Ready = s.bootstrapReadiness == bootstrapReadinessReady && s.lifecycle == serviceLifecycleRunning && !s.readinessWithdrawn
	report.CompletedBackfills = append([]BackfillRequirement(nil), report.CompletedBackfills...)
	return report
}

// WithdrawReadiness closes new admission after the application discovers invalid
// evidence. It is irreversible for this instance: drain Close and start a fresh
// process after correction. Already active operations must be drained separately.
func (s *SyncService) WithdrawReadiness(category ReadinessCategory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readinessWithdrawn = true
	s.readinessReport.FailureCategory = category
}

func (s *SyncService) recordReadinessFailure(err error) {
	var failure *ReadinessError
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readinessReport.FailureCategory = ""
	if errors.As(err, &failure) {
		s.readinessReport.FailureCategory = failure.Category
	}
}

func (s *SyncService) publishReadinessReport() {
	c := s.config.Readiness
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readinessReport.DeploymentRevision = c.Expected.Revision
	s.readinessReport.DeploymentSHA256 = DeploymentContractDigest(c.Expected)
	s.readinessReport.ApplicationCodeSHA256 = c.Expected.ApplicationCodeSHA256
	s.readinessReport.QualificationSHA256 = c.Expected.QualificationSHA256
	if c.Expected.GenerationLedger != nil {
		s.readinessReport.GenerationLedgerRevision = c.Expected.GenerationLedger.Revision
	}
	s.readinessReport.CatalogRevision = s.catalogMetadata.catalogRevision
	s.readinessReport.CatalogFingerprint = s.catalogMetadata.catalogFingerprint
	s.readinessReport.RegistryFingerprint = c.Expected.RegistryFingerprint
	s.readinessReport.MigrationDigest = MigrationManifestDigest(c.Expected.Migrations)
	s.readinessReport.TransitionRevision = c.Expected.TransitionRevision
	s.readinessReport.FailureCategory = ""
	if len(c.Expected.Migrations) > 0 {
		s.readinessReport.MigrationPosition = c.Expected.Migrations[len(c.Expected.Migrations)-1].Version
	}
	s.readinessReport.CompletedBackfills = nil
	for _, g := range c.Generations {
		s.readinessReport.CompletedBackfills = append(s.readinessReport.CompletedBackfills, g.Backfills...)
	}
}

func readinessAdapterError(err error) error {
	var failure *ReadinessError
	if errors.As(err, &failure) {
		return err
	}
	return &ReadinessError{Category: ReadinessEvidenceUnavailable, Err: err}
}

func inspectReadiness(ctx context.Context, tx pgx.Tx, c *ReadinessConfig) (err error) {
	bounded, cancel := context.WithTimeout(ctx, c.InspectionTimeout)
	defer cancel()
	reader := &readinessReader{ctx: bounded, tx: tx, remaining: c.MaxQueries}
	defer reader.close()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = readinessAdapterError(fmt.Errorf("readiness adapter panicked: %T", recovered))
		}
	}()
	evidence, err := c.Adapter.Inspect(bounded, reader)
	if err != nil {
		return readinessAdapterError(err)
	}
	if err := bounded.Err(); err != nil {
		return readinessAdapterError(err)
	}
	return validateReadinessEvidence(c, evidence)
}

// ValidateApplicationReadiness lets an application authority validate a prepared
// deployment with the same typed evidence rules used by Bootstrap. It performs
// no database work and cannot substitute for Bootstrap's live managed validation.
func ValidateApplicationReadiness(config *ReadinessConfig, registry *SchemaGenerationRegistry, evidence ReadinessEvidence) error {
	if registry == nil {
		return readinessFailure(DeploymentContractMismatch)
	}
	copy := cloneReadinessConfig(config)
	if err := validateReadinessConfig(copy, registry); err != nil {
		return err
	}
	return validateReadinessEvidence(copy, evidence)
}
