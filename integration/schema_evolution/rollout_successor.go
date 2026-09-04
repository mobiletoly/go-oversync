package schemaevolution

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mobiletoly/go-oversync/oversync"
)

// RolloutSuccessorConfiguration advances the one/two-generation window for an
// unchanged surviving wire schema. A changed schema needs its own application
// migration and qualification constructor; this helper cannot certify one.
func RolloutSuccessorConfiguration(previous *oversync.ServiceConfig, next oversync.SchemaGeneration, ledger *oversync.GenerationLedger, revision int64, qualification string) (*oversync.ServiceConfig, error) {
	if previous == nil || previous.Readiness == nil || revision <= previous.Readiness.Expected.Revision || len(previous.SchemaGenerations) != 1 || !rolloutDigestPattern.MatchString(qualification) {
		return nil, fmt.Errorf("successor_boundary_invalid")
	}
	currentRegistry, err := oversync.CompileSchemaGenerationRegistry(previous.RegisteredTables, previous.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	nextRegistry, err := oversync.CompileSchemaGenerationRegistry(previous.RegisteredTables, []oversync.SchemaGeneration{next})
	if err != nil {
		return nil, err
	}
	current, _ := currentRegistry.Generation(previous.SchemaGenerations[0].ID)
	future, _ := nextRegistry.Generation(next.ID)
	if !reflect.DeepEqual(current.Tables(), future.Tables()) {
		return nil, fmt.Errorf("successor_schema_requires_separate_qualification")
	}
	cfg := *previous
	readiness := *previous.Readiness
	cfg.Readiness = &readiness
	bytes, err := json.Marshal(previous.Readiness.Expected)
	if err != nil {
		return nil, err
	}
	readiness.Expected = oversync.DeploymentContract{}
	if err = json.Unmarshal(bytes, &readiness.Expected); err != nil {
		return nil, err
	}
	bytes, err = json.Marshal(previous.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	cfg.SchemaGenerations = nil
	if err = json.Unmarshal(bytes, &cfg.SchemaGenerations); err != nil {
		return nil, err
	}
	cfg.SchemaGenerations = append(cfg.SchemaGenerations, next)
	bytes, err = json.Marshal(previous.Readiness.Generations)
	if err != nil {
		return nil, err
	}
	readiness.Generations = nil
	if err = json.Unmarshal(bytes, &readiness.Generations); err != nil {
		return nil, err
	}
	requirement := readiness.Generations[0]
	requirement.Proof = oversync.SchemaGenerationProof{ID: future.ID(), Fingerprint: future.Fingerprint()}
	readiness.Generations = append(readiness.Generations, requirement)
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	readiness.Expected.Revision = revision
	readiness.Expected.RegistryFingerprint = registry.Fingerprint()
	readiness.Expected.QualificationSHA256 = qualification
	wrapper, ok := previous.Readiness.Adapter.(rolloutLedgerAdapter)
	if !ok {
		return nil, fmt.Errorf("successor_application_adapter_required")
	}
	switch base := wrapper.base.(type) {
	case rolloutTableAdapter:
		base.deploymentRevision = revision
		base.registry = registry.Fingerprint()
		readiness.Adapter = base
	case rolloutColumnAdapter:
		base.revision = revision
		base.registry = registry.Fingerprint()
		readiness.Adapter = base
	default:
		return nil, fmt.Errorf("successor_application_adapter_required")
	}
	if err = WithGenerationLedger(&cfg, ledger); err != nil {
		return nil, err
	}
	return &cfg, nil
}
