package simulator

import (
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
)

func TestWatchScenarioWriteServiceConfigIncludesGenerationDeclarations(t *testing.T) {
	config := watchScenarioWriteServiceConfig()
	if len(config.SchemaGenerations) == 0 {
		t.Fatal("watch write service has no schema generation declarations")
	}
	service, err := oversync.NewRuntimeService(nil, config, nil)
	if err != nil {
		t.Fatalf("watch write service configuration is invalid: %v", err)
	}
	if err := service.Close(t.Context()); err != nil {
		t.Fatalf("close watch write service: %v", err)
	}
}
