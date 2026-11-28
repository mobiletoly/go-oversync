// Package baselinereadiness supplies explicit durable application evidence to
// static-schema applications. The schema_evolution integration harness owns actual migrations,
// transitions and backfill; this baseline declares no such prerequisites.
package baselinereadiness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

const manifestFormat = "go-oversync.static-baseline.v1"

type manifestIdentity struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
}

type manifest struct {
	Format                string           `json:"format"`
	AuthorityID           string           `json:"authority_id"`
	DeploymentRevision    int64            `json:"deployment_revision"`
	TransitionRevision    string           `json:"transition_revision"`
	TransitionPhase       string           `json:"transition_phase"`
	ApplicationIdentity   manifestIdentity `json:"application"`
	QualificationIdentity manifestIdentity `json:"qualification"`
}

type canonicalIdentity struct {
	Format      string `json:"format"`
	AuthorityID string `json:"authority_id"`
	ID          string `json:"id"`
	Revision    int64  `json:"revision"`
}

type adapter struct{}

func (adapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var evidence oversync.ReadinessEvidence
	err := r.QueryRow(ctx, `SELECT evidence FROM app_baseline.readiness WHERE singleton`).Scan(&evidence)
	return evidence, err
}

// Prepare is application deployment setup, never invoked from Bootstrap. Its
// first durable contract is immutable; a changed registry needs explicit drained
// application deployment rather than automatically updating this baseline.
func Prepare(ctx context.Context, pool *pgxpool.Pool, cfg *oversync.ServiceConfig, manifestJSON []byte) error {
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return err
	}
	d, err := deploymentContract(manifestJSON, registry.Fingerprint())
	if err != nil {
		return err
	}
	c := &oversync.ReadinessConfig{Adapter: adapter{}, AuthorityLock: [2]int32{1869833075, 6}, Expected: d}
	e := oversync.ReadinessEvidence{Deployment: d}
	for _, id := range registry.GenerationIDs() {
		contract, _ := registry.Generation(id)
		g := oversync.GenerationReadiness{Proof: oversync.SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}}
		c.Generations = append(c.Generations, g)
		e.Generations = append(e.Generations, oversync.GenerationEnablement{Requirement: g, DeploymentRevision: d.Revision, Enabled: true})
	}
	_, err = pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS app_baseline; CREATE TABLE IF NOT EXISTS app_baseline.readiness(singleton boolean PRIMARY KEY CHECK(singleton),evidence jsonb NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `INSERT INTO app_baseline.readiness VALUES(true,$1) ON CONFLICT DO NOTHING`, e)
	if err == nil {
		cfg.Readiness = c
	}
	return err
}

func deploymentContract(manifestJSON []byte, registryFingerprint string) (oversync.DeploymentContract, error) {
	m, err := parseManifest(manifestJSON)
	if err != nil {
		return oversync.DeploymentContract{}, err
	}
	return oversync.DeploymentContract{
		AuthorityID:           m.AuthorityID,
		Revision:              m.DeploymentRevision,
		RegistryFingerprint:   registryFingerprint,
		TransitionRevision:    m.TransitionRevision,
		TransitionPhase:       m.TransitionPhase,
		ApplicationCodeSHA256: identityDigest("application-code", m, m.ApplicationIdentity),
		QualificationSHA256:   identityDigest("qualification", m, m.QualificationIdentity),
	}, nil
}

func parseManifest(manifestJSON []byte) (manifest, error) {
	if err := validateManifestJSONKeys(manifestJSON); err != nil {
		return manifest{}, fmt.Errorf("baseline readiness manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(manifestJSON))
	decoder.DisallowUnknownFields()
	var m manifest
	if err := decoder.Decode(&m); err != nil {
		return manifest{}, fmt.Errorf("baseline readiness manifest: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return manifest{}, err
	}
	if m.Format != manifestFormat || m.AuthorityID == "" || m.DeploymentRevision <= 0 ||
		m.TransitionRevision == "" || m.TransitionPhase == "" ||
		m.ApplicationIdentity.ID == "" || m.ApplicationIdentity.Revision <= 0 ||
		m.QualificationIdentity.ID == "" || m.QualificationIdentity.Revision <= 0 {
		return manifest{}, fmt.Errorf("baseline readiness manifest: invalid semantic identity")
	}
	return m, nil
}

func validateManifestJSONKeys(manifestJSON []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(manifestJSON))
	return validateManifestJSONValue(decoder, "manifest")
}

func validateManifestJSONValue(decoder *json.Decoder, objectName string) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object field name is not a string")
			}
			if _, ok := seen[key]; ok {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if !manifestJSONFieldAllowed(objectName, key) {
				return fmt.Errorf("unknown or non-canonical field %q", key)
			}
			childObjectName := ""
			if objectName == "manifest" && (key == "application" || key == "qualification") {
				childObjectName = key
			}
			if err := validateManifestJSONValue(decoder, childObjectName); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return fmt.Errorf("object did not end with }")
		}
	case '[':
		for decoder.More() {
			if err := validateManifestJSONValue(decoder, ""); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return fmt.Errorf("array did not end with ]")
		}
	}
	return nil
}

func manifestJSONFieldAllowed(objectName, field string) bool {
	switch objectName {
	case "manifest":
		switch field {
		case "format", "authority_id", "deployment_revision", "transition_revision", "transition_phase", "application", "qualification":
			return true
		}
	case "application", "qualification":
		return field == "id" || field == "revision"
	default:
		return true
	}
	return false
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("baseline readiness manifest: trailing JSON value")
		}
		return fmt.Errorf("baseline readiness manifest: %w", err)
	}
	return nil
}

func identityDigest(domain string, m manifest, identity manifestIdentity) string {
	canonical, _ := json.Marshal(canonicalIdentity{
		Format:      m.Format,
		AuthorityID: m.AuthorityID,
		ID:          identity.ID,
		Revision:    identity.Revision,
	})
	hash := sha256.New()
	_, _ = hash.Write([]byte("go-oversync/static-baseline/" + domain + "\x00"))
	_, _ = hash.Write(canonical)
	return hex.EncodeToString(hash.Sum(nil))
}
