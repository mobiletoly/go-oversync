package oversync

import (
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

// GenerationObservationConfig enables a bounded process-local observation window.
// It is telemetry, never evidence that an offline client no longer exists.
type GenerationObservationConfig struct {
	Window     time.Duration
	MaxSources int
}
type GenerationActivity struct {
	GenerationID  SchemaGenerationID
	ProofRequests uint64
	RecentSources *int
	LastSeen      *time.Time
}
type GenerationObservation struct {
	Window                    time.Duration
	ActiveLocalOperations     int
	Supported                 []GenerationActivity
	Unknown, Retired, Invalid uint64
	CheckpointOnlyBundles     int64
	ProjectionIntegrityErrors int64
	RecentAvailable           bool
}
type generationActivityState struct {
	requests uint64
	last     time.Time
	sources  map[[32]byte]time.Time
	overflow time.Time
}
type generationRuntimeMetrics struct {
	mu                        sync.Mutex
	states                    map[SchemaGenerationID]*generationActivityState
	unknown, retired, invalid uint64
}

func (s *SyncService) observeGenerationProof(actor Actor, proof SchemaGenerationProof, err error) {
	if s == nil {
		return
	}
	m := &s.generationMetrics
	m.mu.Lock()
	defer m.mu.Unlock()
	var requestError *SchemaGenerationRequestError
	if err != nil {
		switch {
		case errors.As(err, &requestError) && requestError.Code == SchemaGenerationRetired:
			m.retired++
		case requestError != nil && (requestError.Code == SchemaGenerationUnknown || requestError.Code == SchemaGenerationUnsupported):
			m.unknown++
		default:
			m.invalid++
		}
		return
	}
	// Only compiled active IDs can become metric labels.
	if _, ok := s.schemaGenerationRegistry.Generation(proof.ID); !ok {
		return
	}
	if m.states == nil {
		m.states = make(map[SchemaGenerationID]*generationActivityState)
	}
	state := m.states[proof.ID]
	if state == nil {
		state = &generationActivityState{}
		m.states[proof.ID] = state
	}
	state.requests++
	now := time.Now()
	state.last = now
	cfg := s.config.GenerationObservation
	if !validGenerationObservation(cfg) || actor.UserID == "" || actor.SourceID == "" {
		return
	}
	if state.sources == nil {
		state.sources = make(map[[32]byte]time.Time)
	}
	expireGenerationSources(state, now.Add(-cfg.Window))
	key := sha256.Sum256([]byte(actor.UserID + "\x00" + actor.SourceID))
	if _, exists := state.sources[key]; !exists && len(state.sources) >= cfg.MaxSources {
		state.overflow = now
		return
	}
	state.sources[key] = now
}

func validGenerationObservation(c GenerationObservationConfig) bool {
	return c.Window > 0 && c.Window <= 30*24*time.Hour && c.MaxSources > 0 && c.MaxSources <= 65536
}
func expireGenerationSources(s *generationActivityState, cutoff time.Time) {
	for key, last := range s.sources {
		if last.Before(cutoff) {
			delete(s.sources, key)
		}
	}
}

// GenerationObservations returns only configured generation labels and aggregate
// rejection buckets. Source identities and fingerprints are never exposed.
// RecentSources is nil when disabled or incomplete because its capacity filled.
func (s *SyncService) GenerationObservations() GenerationObservation {
	var result GenerationObservation
	if s == nil || s.config == nil {
		return result
	}
	s.mu.RLock()
	result.ActiveLocalOperations = s.inFlightOps
	s.mu.RUnlock()
	result.CheckpointOnlyBundles = s.historyProjectionMetrics.checkpointOnly.Load()
	result.ProjectionIntegrityErrors = s.historyProjectionMetrics.integrityFailures.Load()
	cfg := s.config.GenerationObservation
	result.Window = cfg.Window
	result.RecentAvailable = validGenerationObservation(cfg)
	m := &s.generationMetrics
	m.mu.Lock()
	defer m.mu.Unlock()
	result.Unknown, result.Retired, result.Invalid = m.unknown, m.retired, m.invalid
	cutoff := time.Now().Add(-cfg.Window)
	for _, id := range s.schemaGenerationRegistry.GenerationIDs() {
		entry := GenerationActivity{GenerationID: id}
		state := m.states[id]
		if state != nil {
			entry.ProofRequests = state.requests
			last := state.last
			entry.LastSeen = &last
			expireGenerationSources(state, cutoff)
			if !state.overflow.IsZero() && !state.overflow.Before(cutoff) {
				result.RecentAvailable = false
			}
		}
		if validGenerationObservation(cfg) && (state == nil || state.overflow.IsZero() || state.overflow.Before(cutoff)) {
			count := 0
			if state != nil {
				count = len(state.sources)
			}
			entry.RecentSources = &count
		}
		result.Supported = append(result.Supported, entry)
	}
	return result
}
