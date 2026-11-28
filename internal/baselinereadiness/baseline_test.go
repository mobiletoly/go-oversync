package baselinereadiness

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testManifest = `{
  "format":"go-oversync.static-baseline.v1",
  "authority_id":"test.application",
  "deployment_revision":1,
  "transition_revision":"static-schema-v1",
  "transition_phase":"baseline",
  "application":{"id":"application","revision":1},
  "qualification":{"id":"qualification","revision":1}
}`

func TestDeploymentContract_CanonicalSemanticIdentity(t *testing.T) {
	formatted := `{
    "qualification": {"revision": 1, "id": "qualification"},
    "transition_phase": "baseline",
    "application": {"revision": 1, "id": "application"},
    "deployment_revision": 1,
    "authority_id": "test.application",
    "transition_revision": "static-schema-v1",
    "format": "go-oversync.static-baseline.v1"
  }`

	first, err := deploymentContract([]byte(testManifest), strings.Repeat("a", 64))
	require.NoError(t, err)
	second, err := deploymentContract([]byte(formatted), strings.Repeat("a", 64))
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first.ApplicationCodeSHA256, 64)
	require.Len(t, first.QualificationSHA256, 64)
	require.NotEqual(t, first.ApplicationCodeSHA256, first.QualificationSHA256)
}

func TestDeploymentContract_IdentitiesAreApplicationScoped(t *testing.T) {
	first, err := deploymentContract([]byte(testManifest), strings.Repeat("a", 64))
	require.NoError(t, err)
	other := strings.Replace(testManifest, `"test.application"`, `"test.other-application"`, 1)
	second, err := deploymentContract([]byte(other), strings.Repeat("a", 64))
	require.NoError(t, err)

	require.NotEqual(t, first.AuthorityID, second.AuthorityID)
	require.NotEqual(t, first.ApplicationCodeSHA256, second.ApplicationCodeSHA256)
	require.NotEqual(t, first.QualificationSHA256, second.QualificationSHA256)
}

func TestDeploymentContract_IdentityRevisionsAreIndependent(t *testing.T) {
	base, err := deploymentContract([]byte(testManifest), strings.Repeat("a", 64))
	require.NoError(t, err)
	applicationRevision, err := deploymentContract([]byte(strings.Replace(testManifest, `"application":{"id":"application","revision":1}`, `"application":{"id":"application","revision":2}`, 1)), strings.Repeat("a", 64))
	require.NoError(t, err)
	qualificationRevision, err := deploymentContract([]byte(strings.Replace(testManifest, `"qualification":{"id":"qualification","revision":1}`, `"qualification":{"id":"qualification","revision":2}`, 1)), strings.Repeat("a", 64))
	require.NoError(t, err)

	require.NotEqual(t, base.ApplicationCodeSHA256, applicationRevision.ApplicationCodeSHA256)
	require.Equal(t, base.QualificationSHA256, applicationRevision.QualificationSHA256)
	require.Equal(t, base.ApplicationCodeSHA256, qualificationRevision.ApplicationCodeSHA256)
	require.NotEqual(t, base.QualificationSHA256, qualificationRevision.QualificationSHA256)
}

func TestParseManifest_RejectsInvalidDocuments(t *testing.T) {
	tests := map[string]string{
		"unknown field":       strings.Replace(testManifest, `"format":`, `"unknown":true,"format":`, 1),
		"case-variant field":  strings.Replace(testManifest, `"format":`, `"FORMAT":`, 1),
		"duplicate field":     strings.Replace(testManifest, `"authority_id":"test.application"`, `"authority_id":"test.application","authority_id":"test.other"`, 1),
		"nested duplicate":    strings.Replace(testManifest, `"application":{"id":"application","revision":1}`, `"application":{"id":"application","revision":1,"revision":2}`, 1),
		"wrong format":        strings.Replace(testManifest, manifestFormat, "other-format", 1),
		"missing authority":   strings.Replace(testManifest, `"authority_id":"test.application",`, "", 1),
		"zero deployment":     strings.Replace(testManifest, `"deployment_revision":1`, `"deployment_revision":0`, 1),
		"empty transition":    strings.Replace(testManifest, `"transition_phase":"baseline"`, `"transition_phase":""`, 1),
		"zero application":    strings.Replace(testManifest, `"application":{"id":"application","revision":1}`, `"application":{"id":"application","revision":0}`, 1),
		"zero qualification":  strings.Replace(testManifest, `"qualification":{"id":"qualification","revision":1}`, `"qualification":{"id":"qualification","revision":0}`, 1),
		"trailing JSON value": testManifest + `{}`,
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := parseManifest([]byte(document))
			require.Error(t, err)
		})
	}
}
