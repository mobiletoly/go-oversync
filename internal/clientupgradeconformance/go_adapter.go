package clientupgradeconformance

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
)

type goTestTarget struct {
	workdir string
	pattern string
}

type scenarioExecutionResult struct {
	ID                string         `json:"id"`
	FixtureSHA256     string         `json:"fixture_sha256"`
	Observed          map[string]any `json:"observed"`
	Preserved         []string       `json:"preserved"`
	ActionCount       int            `json:"action_count"`
	ExchangeCount     int            `json:"exchange_count"`
	FaultCount        int            `json:"fault_count"`
	ConsumedActions   []string       `json:"consumed_actions"`
	ConsumedExchanges []string       `json:"consumed_exchanges"`
	ConsumedFaults    []string       `json:"consumed_faults"`
}

// ExecuteGoAdapters discovers every test named by the portable scenario and
// invariant mappings, executes each exact test, and requires a Go test PASS
// event. This binds the checked corpus to running product evidence instead of
// accepting source-text references as proof.
func ExecuteGoAdapters(conformanceRoot, repositoryRoot, toolchain string) error {
	requested, err := mappedGoTests(conformanceRoot)
	if err != nil {
		return err
	}
	targets := []goTestTarget{
		{workdir: repositoryRoot, pattern: "./oversync"},
		{workdir: repositoryRoot, pattern: "./oversqlite"},
		{workdir: repositoryRoot, pattern: "./oversqlite_e2e"},
		{workdir: repositoryRoot, pattern: "./internal/clientupgradeconformance"},
		{workdir: filepath.Join(repositoryRoot, "integration", "mobile_flow"), pattern: "./simulator"},
	}
	byTarget := make([][]string, len(targets))
	for idx, target := range targets {
		listed, err := listGoTests(target, toolchain)
		if err != nil {
			return err
		}
		for name := range requested {
			if listed[name] {
				byTarget[idx] = append(byTarget[idx], name)
			}
		}
	}
	for name := range requested {
		owners := 0
		for _, names := range byTarget {
			for _, candidate := range names {
				if candidate == name {
					owners++
				}
			}
		}
		if owners != 1 {
			return fmt.Errorf("mapped Go test %s resolved to %d executable packages; want exactly one", name, owners)
		}
	}
	for idx, names := range byTarget {
		if len(names) == 0 {
			continue
		}
		sort.Strings(names)
		if err := runMappedGoTests(targets[idx], names, toolchain); err != nil {
			return err
		}
	}
	return executePortableScenarioAdapters(conformanceRoot, repositoryRoot, toolchain)
}

func executePortableScenarioAdapters(conformanceRoot, repositoryRoot, toolchain string) error {
	adaptersBody, err := os.ReadFile(filepath.Join(conformanceRoot, "go-adapter.json"))
	if err != nil {
		return err
	}
	var adapters goAdapterFile
	if err := decodeExact(adaptersBody, &adapters); err != nil {
		return err
	}
	scenarios, err := loadPortableScenarios(conformanceRoot)
	if err != nil {
		return err
	}
	for _, adapter := range adapters.GoScenarios {
		item, ok := scenarios[adapter.ID]
		if !ok {
			return fmt.Errorf("Go scenario executor %s has no fixture", adapter.ID)
		}
		fixture, err := json.Marshal(item)
		if err != nil {
			return err
		}
		temp, err := os.MkdirTemp("", "oversync-conformance-")
		if err != nil {
			return err
		}
		fixturePath := filepath.Join(temp, "scenario.json")
		outputPath := filepath.Join(temp, "observed.json")
		if err := os.WriteFile(fixturePath, fixture, 0o600); err != nil {
			os.RemoveAll(temp)
			return err
		}
		command := exec.Command("go", "test", adapter.ExecutorPackage, "-run", "^"+regexp.QuoteMeta(adapter.ExecutorTest)+"$", "-count=1", "-timeout=3m30s")
		command.Dir = repositoryRoot
		command.Env = withGoToolchain(os.Environ(), toolchain)
		command.Env = append(command.Env,
			"OVERSYNC_CLIENT_UPGRADE_SCENARIO="+fixturePath,
			"OVERSYNC_CLIENT_UPGRADE_OBSERVATION="+outputPath,
		)
		output, runErr := command.CombinedOutput()
		if runErr != nil {
			os.RemoveAll(temp)
			return fmt.Errorf("execute portable scenario %s: %w\n%s", adapter.ID, runErr, output)
		}
		observedBody, err := os.ReadFile(outputPath)
		if err != nil {
			os.RemoveAll(temp)
			return fmt.Errorf("portable scenario %s did not produce observations: %w", adapter.ID, err)
		}
		var observed scenarioExecutionResult
		if err := decodeExact(observedBody, &observed); err != nil {
			os.RemoveAll(temp)
			return fmt.Errorf("portable scenario %s observation: %w", adapter.ID, err)
		}
		os.RemoveAll(temp)
		sum := sha256.Sum256(fixture)
		expectedActions := make([]string, 0, len(item.Actions))
		for index, action := range item.Actions {
			expectedActions = append(expectedActions, fmt.Sprintf("%d:%v", index, action["kind"]))
		}
		expectedExchanges := make([]string, 0, len(item.ProtocolExchanges))
		for index, exchange := range item.ProtocolExchanges {
			expectedExchanges = append(expectedExchanges, fmt.Sprintf("%d:%v->%v", index, exchange["request"], exchange["response"]))
		}
		expectedFaults := make([]string, 0, len(item.Faults))
		for index, fault := range item.Faults {
			expectedFaults = append(expectedFaults, fmt.Sprintf("%d:%v:%v", index, fault["at"], fault["effect"]))
		}
		if observed.ID != item.ID || observed.FixtureSHA256 != fmt.Sprintf("%x", sum) ||
			observed.ActionCount != len(item.Actions) || observed.ExchangeCount != len(item.ProtocolExchanges) || observed.FaultCount != len(item.Faults) ||
			!slices.Equal(observed.ConsumedActions, expectedActions) || !slices.Equal(observed.ConsumedExchanges, expectedExchanges) ||
			!slices.Equal(observed.ConsumedFaults, expectedFaults) {
			return fmt.Errorf("portable scenario %s did not consume its complete fixture: observed actions=%v exchanges=%v faults=%v counts=%d/%d/%d fixture_sha=%s; want actions=%v exchanges=%v faults=%v counts=%d/%d/%d fixture_sha=%x",
				adapter.ID, observed.ConsumedActions, observed.ConsumedExchanges, observed.ConsumedFaults,
				observed.ActionCount, observed.ExchangeCount, observed.FaultCount, observed.FixtureSHA256,
				expectedActions, expectedExchanges, expectedFaults, len(item.Actions), len(item.ProtocolExchanges), len(item.Faults), sum)
		}
		if !reflect.DeepEqual(observed.Observed, item.Expected) || !reflect.DeepEqual(observed.Preserved, item.Preserved) {
			return fmt.Errorf("portable scenario %s observations differ from fixed outcomes: observed=%v expected=%v preserved=%v expected_preserved=%v",
				adapter.ID, observed.Observed, item.Expected, observed.Preserved, item.Preserved)
		}
	}
	return nil
}

func loadPortableScenarios(root string) (map[string]scenario, error) {
	result := make(map[string]scenario)
	for _, relative := range []string{"scenarios/core.json", "scenarios/boundaries.json"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return nil, err
		}
		var file scenarioFile
		if err := decodeExact(body, &file); err != nil {
			return nil, err
		}
		for _, item := range file.Scenarios {
			result[item.ID] = item
		}
	}
	return result, nil
}

func mappedGoTests(root string) (map[string]bool, error) {
	result := make(map[string]bool)
	body, err := os.ReadFile(filepath.Join(root, "go-adapter.json"))
	if err != nil {
		return nil, err
	}
	var adapters goAdapterFile
	if err := decodeExact(body, &adapters); err != nil {
		return nil, err
	}
	for _, adapter := range adapters.GoScenarios {
		for _, name := range adapter.GoTests {
			result[name] = true
		}
	}
	body, err = os.ReadFile(filepath.Join(root, "invariants.json"))
	if err != nil {
		return nil, err
	}
	var invariants invariantsFile
	if err := decodeExact(body, &invariants); err != nil {
		return nil, err
	}
	for _, invariant := range invariants.Invariants {
		for _, name := range invariant.GoTests {
			result[name] = true
		}
	}
	return result, nil
}

func listGoTests(target goTestTarget, toolchain string) (map[string]bool, error) {
	command := exec.Command("go", "test", target.pattern, "-list", "^Test")
	command.Dir = target.workdir
	command.Env = withGoToolchain(os.Environ(), toolchain)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list Go conformance tests in %s %s: %w\n%s", target.workdir, target.pattern, err, output)
	}
	result := make(map[string]bool)
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Test") && !strings.ContainsAny(line, " \t") {
			result[line] = true
		}
	}
	return result, nil
}

func runMappedGoTests(target goTestTarget, names []string, toolchain string) error {
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, regexp.QuoteMeta(name))
	}
	pattern := "^(" + strings.Join(parts, "|") + ")$"
	command := exec.Command("go", "test", "-json", target.pattern, "-run", pattern, "-count=1", "-timeout=3m30s")
	command.Dir = target.workdir
	command.Env = withGoToolchain(os.Environ(), toolchain)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("execute Go conformance adapters in %s %s: %w\n%s", target.workdir, target.pattern, err, output)
	}
	passed := make(map[string]bool)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		var event struct {
			Action string `json:"Action"`
			Test   string `json:"Test"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) == nil && event.Action == "pass" && event.Test != "" {
			passed[event.Test] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, name := range names {
		if !passed[name] {
			return fmt.Errorf("mapped Go test %s did not emit PASS in %s", name, target.pattern)
		}
	}
	return nil
}

func withGoToolchain(environment []string, toolchain string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, value := range environment {
		if !strings.HasPrefix(value, "GOTOOLCHAIN=") {
			result = append(result, value)
		}
	}
	if toolchain != "" {
		result = append(result, "GOTOOLCHAIN="+toolchain)
	}
	return result
}
