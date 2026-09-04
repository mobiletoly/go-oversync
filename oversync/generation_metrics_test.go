package oversync

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGenerationObservation_BoundedLabelsAndUnavailable(t *testing.T) {
	registered := []RegisteredTable{{Schema: "business", Table: "items", SyncKeyColumns: []string{"id"}}}
	registry, err := CompileSchemaGenerationRegistry(registered, testSchemaGenerationsForRegisteredTables(registered))
	require.NoError(t, err)
	contract, _ := registry.Generation(1)
	service := &SyncService{schemaGenerationRegistry: registry, config: &ServiceConfig{GenerationObservation: GenerationObservationConfig{Window: time.Minute, MaxSources: 1}}}
	proof := SchemaGenerationProof{ID: 1, Fingerprint: contract.Fingerprint()}
	zero := service.GenerationObservations()
	require.True(t, zero.RecentAvailable)
	require.Equal(t, 0, *zero.Supported[0].RecentSources)
	service.observeGenerationProof(Actor{UserID: "sensitive-user", SourceID: "first-sensitive-source"}, proof, nil)
	exact := service.GenerationObservations()
	require.Equal(t, 1, *exact.Supported[0].RecentSources)
	require.NotNil(t, exact.Supported[0].LastSeen)
	service.observeGenerationProof(Actor{UserID: "sensitive-user", SourceID: "second-sensitive-source"}, proof, nil)
	full := service.GenerationObservations()
	require.False(t, full.RecentAvailable)
	require.Nil(t, full.Supported[0].RecentSources)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for n := 0; n < 100; n++ {
				service.observeGenerationProof(Actor{}, SchemaGenerationProof{ID: 99999}, &SchemaGenerationRequestError{Code: SchemaGenerationRetired})
				_ = service.GenerationObservations()
			}
		}()
	}
	workers.Wait()
	final := service.GenerationObservations()
	require.EqualValues(t, 800, final.Retired)
	require.Len(t, final.Supported, 1)
	raw, err := json.Marshal(final)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sensitive")
	require.NotContains(t, string(raw), "99999")
	require.NotContains(t, string(raw), proof.Fingerprint)
	service.config.GenerationObservation = GenerationObservationConfig{}
	require.False(t, service.GenerationObservations().RecentAvailable)
	// An arbitrary malformed ID never becomes a configured label.
	_, err = service.resolveRequestGeneration(context.Background(), Actor{}, SchemaGenerationProof{ID: -9, Fingerprint: strings.Repeat("a", 64)})
	require.Error(t, err)
	require.EqualValues(t, 1, service.GenerationObservations().Invalid)
}
