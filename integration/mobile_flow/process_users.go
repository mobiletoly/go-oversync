package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/integration/mobile_flow/simulator"
)

const processWorkerEnvironment = "OVERSYNC_MOBILE_FLOW_WORKER"

type simulationUserResult struct {
	userID       string
	duration     time.Duration
	err          error
	sqliteDBPath string
}

type processUserJob struct {
	ServerURL, DatabaseURL, JWTSecret      string
	Scenario, UserID, DeviceID, ResultPath string
	Verify, PreserveDB, Verbose            bool
}

type processUserResult struct {
	Version           int
	UserID            string
	Completed         bool
	Duration          time.Duration
	SQLitePath, Error string
}

type childLogWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *childLogWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}

type processUserRunner struct {
	executable, directory string
	output                *childLogWriter
}

func newProcessUserRunner() (*processUserRunner, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "mobile_flow_process_results_")
	if err != nil {
		return nil, err
	}
	return &processUserRunner{executable: executable, directory: directory, output: &childLogWriter{writer: os.Stdout}}, nil
}

func (p *processUserRunner) cleanup() error { return os.RemoveAll(p.directory) }

func (p *processUserRunner) run(ctx context.Context, cfg *config.Config, scenario, user, device string, ordinal int) simulationUserResult {
	started := time.Now()
	result := simulationUserResult{userID: user}
	if err := ctx.Err(); err != nil {
		result.err = err
		return result
	}
	job := processUserJob{ServerURL: cfg.ServerURL, DatabaseURL: cfg.DatabaseURL, JWTSecret: cfg.JWTSecret,
		Scenario: scenario, UserID: user, DeviceID: device, ResultPath: filepath.Join(p.directory, fmt.Sprintf("user-%03d.json", ordinal)),
		Verify: cfg.EnableVerify, PreserveDB: cfg.PreserveDB, Verbose: cfg.Logger.Enabled(ctx, slog.LevelDebug)}
	body, err := json.Marshal(job)
	if err != nil {
		result.err = err
		return result
	}
	command := exec.CommandContext(ctx, p.executable)
	command.Env = append(os.Environ(), processWorkerEnvironment+"=1")
	command.Stdin = bytes.NewReader(body)
	command.Stdout, command.Stderr = p.output, p.output
	command.Cancel = func() error { return command.Process.Signal(os.Interrupt) }
	command.WaitDelay = 5 * time.Second
	commandErr := command.Run()
	result.duration = time.Since(started)
	child, err := readProcessUserResult(job.ResultPath, user)
	result.err = errors.Join(commandErr, err)
	if err == nil {
		result.sqliteDBPath = child.SQLitePath
		if !child.Completed {
			result.err = errors.Join(result.err, fmt.Errorf("user worker: %s", child.Error))
		}
	}
	if cancelled := ctx.Err(); cancelled != nil {
		result.err = errors.Join(cancelled, result.err)
	}
	return result
}

func readProcessUserResult(path, user string) (processUserResult, error) {
	var result processUserResult
	file, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("read owned worker result: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > 65536 {
		return result, fmt.Errorf("worker result exceeds its bounded record size or cannot be measured")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 65537))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode owned worker result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return result, fmt.Errorf("worker result has trailing data")
	}
	if result.Version != 1 || result.UserID != user || result.Duration < 0 || (result.Completed && (result.Error != "" || result.SQLitePath == "")) || (!result.Completed && result.Error == "") {
		return result, fmt.Errorf("worker result identity or completion is invalid")
	}
	return result, nil
}

func runProcessWorker(ctx context.Context, input io.Reader) error {
	var job processUserJob
	decoder := json.NewDecoder(io.LimitReader(input, 65537))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&job); err != nil {
		return fmt.Errorf("decode private worker input: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("private worker input has trailing data")
	}
	if job.UserID == "" || job.DeviceID == "" || job.ResultPath == "" {
		return fmt.Errorf("incomplete private worker identity")
	}
	level := slog.LevelInfo
	if job.Verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("user_id", job.UserID, "worker_pid", os.Getpid())
	slog.SetDefault(logger)
	cfg := &config.Config{ServerURL: job.ServerURL, DatabaseURL: job.DatabaseURL, JWTSecret: job.JWTSecret, PreserveDB: job.PreserveDB, Logger: logger}
	var verifier *simulator.DatabaseVerifier
	var err error
	if job.Verify {
		verifier, err = simulator.NewWorkerDatabaseVerifier(ctx, job.DatabaseURL, logger)
		if err != nil {
			return writeProcessUserResult(job, simulationUserResult{userID: job.UserID, err: err})
		}
		defer verifier.Close()
	}
	return writeProcessUserResult(job, runSimulationUser(ctx, cfg, job.Scenario, job.UserID, job.DeviceID, verifier))
}

func writeProcessUserResult(job processUserJob, result simulationUserResult) error {
	child := processUserResult{Version: 1, UserID: result.userID, Completed: result.err == nil, Duration: result.duration, SQLitePath: result.sqliteDBPath}
	if result.err != nil {
		child.Error = result.err.Error()
	}
	body, err := json.Marshal(child)
	if err == nil {
		err = os.WriteFile(job.ResultPath, body, 0600)
	}
	return errors.Join(result.err, err)
}

func runSimulationUser(ctx context.Context, cfg *config.Config, scenario, user, device string, verifier *simulator.DatabaseVerifier) (result simulationUserResult) {
	result.userID = user
	started := time.Now()
	defer func() { result.duration = time.Since(started) }()
	if err := ctx.Err(); err != nil {
		result.err = err
		return result
	}
	userConfig := &config.Config{ServerURL: cfg.ServerURL, DatabaseURL: cfg.DatabaseURL, JWTSecret: cfg.JWTSecret, PreserveDB: cfg.PreserveDB, Logger: cfg.Logger}
	sim, err := simulator.NewSimulatorWithUserConfigAndVerifier(userConfig, user, device, verifier)
	if err != nil {
		result.err = err
		return result
	}
	defer func() { result.err = errors.Join(result.err, sim.Close()) }()
	result.err = runScenarioForUser(ctx, sim, scenario, user)
	if app := sim.GetCurrentApp(); app != nil {
		result.sqliteDBPath = app.GetDatabasePath()
	}
	return result
}
