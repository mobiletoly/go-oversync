package schemaevolution

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

type RetirementPolicy struct {
	Revision                 string
	OwnerApproval            string
	ClientAdmissionPolicy    string
	ReplacementAvailableAt   time.Time
	NoticeAt                 time.Time
	SupportDuration          time.Duration
	NoticeDuration           time.Duration
	ChannelEvidence          []string
	OfflineIntentDisposition string
	BackupEvidence           string
	RollbackHolds            []string
}

func (p RetirementPolicy) eligible(now time.Time) error {
	if p.Revision == "" || p.OwnerApproval == "" || p.ClientAdmissionPolicy == "" || p.BackupEvidence == "" || len(p.ChannelEvidence) == 0 || p.SupportDuration <= 0 || p.NoticeDuration <= 0 || p.ReplacementAvailableAt.IsZero() || p.NoticeAt.IsZero() {
		return fmt.Errorf("support_policy_incomplete")
	}
	if !rolloutDigestPattern.MatchString(p.BackupEvidence) {
		return fmt.Errorf("backup_evidence_identity_invalid")
	}
	for _, e := range p.ChannelEvidence {
		if e == "" {
			return fmt.Errorf("replacement_channel_unavailable")
		}
	}
	if p.OfflineIntentDisposition != "preserve_unresolved_no_retired_upload" {
		return fmt.Errorf("offline_intent_policy_required")
	}
	if now.Before(p.ReplacementAvailableAt.Add(p.SupportDuration)) || now.Before(p.NoticeAt.Add(p.NoticeDuration)) {
		return fmt.Errorf("support_or_notice_window_unelapsed")
	}
	if len(p.RollbackHolds) > 0 {
		return fmt.Errorf("rollback_hold")
	}
	return nil
}

type RolloutMember struct {
	Name                   string
	Role                   string
	ProcessStartID         string
	ApplicationCodeSHA256  string
	BinarySHA256           string
	RuntimeAdmissionSHA256 string
}

// RolloutPlan binds one independently resumable phase. Schemas and SQL remain
// in application code; TargetSHA256 identifies that compiled application target.
type RolloutPlan struct {
	// TargetSelection retains the compiled CLI target for restart by operation ID.
	TargetSelection      *RolloutTargetSelection `json:",omitempty"`
	Version              int
	ID                   string
	Action               string
	AuthorityID          string
	Database             string
	Epoch                int64
	DeploymentRevision   int64
	LedgerRevision       int64
	CatalogRevision      int64
	CatalogFingerprint   string
	RegistryFingerprint  string
	TargetSHA256         string
	Retiring             oversync.SchemaGenerationProof
	Activating           oversync.SchemaGenerationProof
	Contract             *oversync.MigrationIdentity
	Columns              []string
	TableIDs             []int32
	Policy               RetirementPolicy
	Members              []RolloutMember
	Deadline             time.Time
	MaxRows              int
	RetentionWindow      int64
	MaxBatches           int
	MaxAttempts          int
	QueryTimeout         time.Duration
	MaxEvidenceBytes     int
	TableWritePolicy     string `json:",omitempty"`
	RecoveryCutSHA256    string `json:",omitempty"`
	RecoveryResultSHA256 string `json:",omitempty"`
}

var rolloutDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var rolloutRoleName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func (p RolloutPlan) validate() error {
	if p.Version != 1 || p.ID == "" || len(p.ID) > 128 || p.AuthorityID == "" || p.Database == "" || p.Epoch < 1 || p.DeploymentRevision < 1 || p.LedgerRevision < 1 || p.CatalogRevision < 1 || p.Deadline.IsZero() {
		return fmt.Errorf("invalid_rollout_identity")
	}
	if !rolloutDigestPattern.MatchString(p.CatalogFingerprint) || !rolloutDigestPattern.MatchString(p.RegistryFingerprint) || !rolloutDigestPattern.MatchString(p.TargetSHA256) {
		return fmt.Errorf("invalid_rollout_digest")
	}
	if p.MaxBatches < 1 || p.MaxBatches > 1000000 || p.MaxAttempts < 1 || p.MaxAttempts > 1000000 || p.QueryTimeout <= 0 || p.QueryTimeout > 30*time.Second || p.MaxRows < 1 || p.MaxRows > 4096 || p.MaxEvidenceBytes < 1024 || p.MaxEvidenceBytes > 1<<20 || len(p.Members) < 1 || len(p.Members) > 64 || len(p.TableIDs) > 4096 {
		return fmt.Errorf("invalid_rollout_bounds")
	}
	if err := p.validateMembers(); err != nil {
		return err
	}
	if !slices.IsSorted(p.TableIDs) {
		return fmt.Errorf("retirement_ids_not_sorted")
	}
	for i, id := range p.TableIDs {
		if id < 1 || i > 0 && p.TableIDs[i-1] == id {
			return fmt.Errorf("invalid_retirement_id")
		}
	}
	return p.validateAction()
}

func (p RolloutPlan) validateMembers() error {
	for i, m := range p.Members {
		if p.Action == "reopen" && (!rolloutDigestPattern.MatchString(m.BinarySHA256) || !rolloutDigestPattern.MatchString(m.RuntimeAdmissionSHA256)) {
			return fmt.Errorf("fleet_binary_identity_required")
		}
		if m.Name == "" || !rolloutRoleName.MatchString(m.Role) || m.ProcessStartID == "" || !rolloutDigestPattern.MatchString(m.ApplicationCodeSHA256) {
			return fmt.Errorf("invalid_rollout_member")
		}
		if i > 0 && p.Members[i-1].Name >= m.Name {
			return fmt.Errorf("members_not_sorted_unique")
		}
		for _, previous := range p.Members[:i] {
			if previous.Role == m.Role {
				return fmt.Errorf("member_database_role_reused")
			}
		}
	}
	return nil
}
func (p RolloutPlan) validateAction() error {
	switch p.Action {
	case "retire_generation":
		if p.Retiring.ID < 1 || !rolloutDigestPattern.MatchString(p.Retiring.Fingerprint) {
			return fmt.Errorf("retiring_proof_required")
		}
	case "retire_tables", "prune_table_state":
		if len(p.TableIDs) == 0 {
			return fmt.Errorf("retiring_tables_required")
		}
		if p.Action == "retire_tables" && p.TableWritePolicy != "deny_and_revoke_atomically" {
			return fmt.Errorf("retired_table_write_policy_required")
		}
	case "enable_generation":
		if p.Activating.ID < 1 || !rolloutDigestPattern.MatchString(p.Activating.Fingerprint) {
			return fmt.Errorf("activating_proof_required")
		}
	case "expire_history":
		if len(p.TableIDs) == 0 || p.RetentionWindow < 1 {
			return fmt.Errorf("explicit_history_expiry_policy_required")
		}
	case "contract":
		if p.Contract == nil {
			return fmt.Errorf("exact_contract_migration_required")
		}
		switch *p.Contract {
		case notesContractIdentity():
			if len(p.TableIDs) != 1 || p.TableIDs[0] != 1 || len(p.Columns) != 0 {
				return fmt.Errorf("exact_notes_contract_required")
			}
		case personContractIdentity():
			if len(p.TableIDs) != 0 || len(p.Columns) != 1 || p.Columns[0] != "main.upgrade_people.legacy_name" {
				return fmt.Errorf("exact_person_contract_required")
			}
		default:
			return fmt.Errorf("exact_contract_migration_required")
		}
	case "prepare", "restore", "reopen", "cleanup_staging":
		if p.Action == "reopen" && ((p.RecoveryCutSHA256 == "") != (p.RecoveryResultSHA256 == "")) {
			return fmt.Errorf("recovery_reopen_identity_incomplete")
		}
		if p.RecoveryCutSHA256 != "" && (!rolloutDigestPattern.MatchString(p.RecoveryCutSHA256) || !rolloutDigestPattern.MatchString(p.RecoveryResultSHA256)) {
			return fmt.Errorf("recovery_reopen_identity_invalid")
		}
	default:
		return fmt.Errorf("unknown_rollout_action")
	}
	return nil
}

func RolloutPlanDigest(p RolloutPlan) (string, error) {
	if err := p.validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	if len(b) > p.MaxEvidenceBytes {
		return "", fmt.Errorf("rollout_plan_capacity")
	}
	return digest(b), nil
}

// RolloutTargetDigest identifies the application-owned target without its
// runtime adapter or connection pools. It does not authorize the target itself.
func RolloutTargetDigest(cfg *oversync.ServiceConfig) (string, error) {
	if cfg == nil || cfg.Readiness == nil {
		return "", fmt.Errorf("target_readiness_required")
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(struct {
		Registry   string
		Catalog    int64
		Deployment oversync.DeploymentContract
	}{registry.Fingerprint(), cfg.RegisteredTableCatalogRevision, cfg.Readiness.Expected})
	if err != nil {
		return "", err
	}
	return digest(b), nil
}
