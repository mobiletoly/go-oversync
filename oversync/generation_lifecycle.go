package oversync

import "slices"

// GenerationLifecycle is application-authority evidence, separate from the
// immutable generation contract. Entries and allocated IDs are never reclaimed.
type GenerationLifecycle struct {
	Proof             SchemaGenerationProof
	State             string
	AllocatedRevision int64
	RetiredRevision   int64
}

// GenerationLedger is a bounded immutable snapshot bound to a deployment.
// The application's readiness adapter must read the actual durable snapshot;
// copying expected configuration is not evidence of historical allocation.
type GenerationLedger struct {
	Revision int64
	Entries  []GenerationLifecycle
}

const MaxGenerationLedgerEntries = 4096

// ValidateGenerationLedger checks structural integrity and exact agreement with
// the active registry. Durable immutability and completeness belong to the
// application's allocation authority and database identity guards.
func ValidateGenerationLedger(ledger *GenerationLedger, registry *SchemaGenerationRegistry) error {
	if ledger == nil || registry == nil || ledger.Revision < 1 || len(ledger.Entries) == 0 || len(ledger.Entries) > MaxGenerationLedgerEntries {
		return readinessFailure(ReadinessEvidenceUnavailable)
	}
	var previous SchemaGenerationID
	var allocation int64
	supported := 0
	for _, entry := range ledger.Entries {
		if err := validateLifecycleEntry(entry, ledger.Revision, previous, allocation); err != nil {
			return err
		}
		previous, allocation = entry.Proof.ID, entry.AllocatedRevision
		contract, active := registry.Generation(entry.Proof.ID)
		if active != (entry.State == "supported") {
			return readinessFailure(DeploymentContractMismatch)
		}
		if active {
			if contract.Fingerprint() != entry.Proof.Fingerprint {
				return readinessFailure(DeploymentContractMismatch)
			}
			supported++
		}
	}
	if supported != len(registry.GenerationIDs()) || supported < 1 || supported > 2 {
		return readinessFailure(DeploymentContractMismatch)
	}
	return nil
}

func validateLifecycleEntry(entry GenerationLifecycle, revision int64, previous SchemaGenerationID, allocation int64) error {
	if validateSchemaGenerationProof(entry.Proof) != nil || entry.Proof.ID <= previous || entry.AllocatedRevision <= allocation || entry.AllocatedRevision > revision {
		return readinessFailure(ReadinessEvidenceUnavailable)
	}
	switch entry.State {
	case "supported", "allocated":
		if entry.RetiredRevision != 0 {
			return readinessFailure(ReadinessEvidenceUnavailable)
		}
	case "retired", "abandoned":
		if entry.RetiredRevision <= entry.AllocatedRevision || entry.RetiredRevision > revision {
			return readinessFailure(ReadinessEvidenceUnavailable)
		}
	default:
		return readinessFailure(ReadinessEvidenceUnavailable)
	}
	return nil
}

func (s *SyncService) retiredGenerationError(proof SchemaGenerationProof) error {
	if s.config == nil || s.config.Readiness == nil || s.config.Readiness.Expected.GenerationLedger == nil {
		return nil
	}
	ledger := s.config.Readiness.Expected.GenerationLedger
	i, found := slices.BinarySearchFunc(ledger.Entries, proof.ID, func(entry GenerationLifecycle, id SchemaGenerationID) int {
		if entry.Proof.ID < id {
			return -1
		}
		if entry.Proof.ID > id {
			return 1
		}
		return 0
	})
	if !found || ledger.Entries[i].State != "retired" {
		return nil
	}
	if ledger.Entries[i].Proof.Fingerprint != proof.Fingerprint {
		return &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMismatch, Field: "schema_generation_fingerprint", GenerationID: proof.ID, Message: "schema generation fingerprint does not match"}
	}
	return &SchemaGenerationRequestError{Code: SchemaGenerationRetired, GenerationID: proof.ID, Message: "Update the application; preserve local data and pending sync work"}
}
