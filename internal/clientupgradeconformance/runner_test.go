package clientupgradeconformance

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestClientUpgradeConformance_I12PortableEvidence(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	root := filepath.Join(filepath.Dir(source), "..", "..", "conformance", "client-upgrade", "v1")
	if err := Validate(root); err != nil {
		t.Fatal(err)
	}
}
