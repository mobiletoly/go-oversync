package simulator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Reporter handles scenario reporting and metrics
type Reporter struct {
	outputFile string
	logger     *slog.Logger

	// Report data
	reports []ScenarioReport
	mu      sync.Mutex
}

// NewReporter creates a new reporter
func NewReporter(outputFile string, logger *slog.Logger) *Reporter {
	return &Reporter{
		outputFile: outputFile,
		logger:     logger,
		reports:    make([]ScenarioReport, 0),
	}
}

// StartScenario starts tracking a new scenario
func (r *Reporter) StartScenario(name, description string) *ScenarioReport {
	r.mu.Lock()
	defer r.mu.Unlock()

	report := &ScenarioReport{
		Name:        name,
		Description: description,
		StartTime:   time.Now(),
		Status:      "running",
		Metrics:     make(map[string]interface{}),
	}

	r.reports = append(r.reports, *report)

	if verboseLog {
		r.logger.Info("📊 Started scenario report", "name", name)
	}

	return report
}

// Close finalizes the reporter and writes output if configured
func (r *Reporter) Close() error {
	if r.outputFile == "" {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Create final report
	finalReport := FinalReport{
		GeneratedAt:    time.Now(),
		TotalScenarios: len(r.reports),
		SuccessfulRuns: 0,
		FailedRuns:     0,
		TotalDuration:  0,
		Scenarios:      r.reports,
	}

	// Calculate summary statistics
	for _, report := range r.reports {
		if report.Status == "success" {
			finalReport.SuccessfulRuns++
		} else if report.Status == "failed" {
			finalReport.FailedRuns++
		}
		finalReport.TotalDuration += report.Duration
	}

	// Write to file
	data, err := json.MarshalIndent(finalReport, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	if err := os.WriteFile(r.outputFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	r.logger.Info("📊 Report written",
		"file", r.outputFile,
		"scenarios", finalReport.TotalScenarios,
		"successful", finalReport.SuccessfulRuns,
		"failed", finalReport.FailedRuns)

	return nil
}

// ScenarioReport tracks metrics for a single scenario
type ScenarioReport struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Duration    time.Duration          `json:"duration"`
	Status      string                 `json:"status"` // running, success, failed
	Error       string                 `json:"error,omitempty"`
	Metrics     map[string]interface{} `json:"metrics"`
}

// SetDuration sets the scenario duration
func (sr *ScenarioReport) SetDuration(duration time.Duration) {
	sr.Duration = duration
	sr.EndTime = sr.StartTime.Add(duration)
}

// SetSuccess marks the scenario as successful
func (sr *ScenarioReport) SetSuccess() {
	sr.Status = "success"
}

// SetError marks the scenario as failed with an error
func (sr *ScenarioReport) SetError(err error) {
	sr.Status = "failed"
	sr.Error = err.Error()
}

// AddMetric adds a metric to the scenario report
func (sr *ScenarioReport) AddMetric(key string, value interface{}) {
	sr.Metrics[key] = value
}

// FinalReport contains the complete test run report
type FinalReport struct {
	GeneratedAt    time.Time        `json:"generated_at"`
	TotalScenarios int              `json:"total_scenarios"`
	SuccessfulRuns int              `json:"successful_runs"`
	FailedRuns     int              `json:"failed_runs"`
	TotalDuration  time.Duration    `json:"total_duration"`
	Scenarios      []ScenarioReport `json:"scenarios"`
}
