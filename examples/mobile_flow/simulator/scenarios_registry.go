package simulator

// scenarios_registry.go - Central registry for all available scenarios
// This file replaces the large scenarios.go file and imports all individual scenario files

// GetScenario creates a scenario instance by name
func GetScenario(simulator *Simulator, scenarioName string) Scenario {
	switch scenarioName {
	case "fresh-install":
		return NewFreshInstallScenario(simulator)
	case "normal-usage":
		return NewNormalUsageScenario(simulator)
	case "reinstall":
		return NewReinstallScenario(simulator)
	case "device-replacement":
		return NewDeviceReplacementScenario(simulator)
	case "offline-online":
		return NewOfflineOnlineScenario(simulator)
	case "conflicts":
		return NewConflictsScenario(simulator)
	case "user-switch":
		return NewUserSwitchScenario(simulator)
	case "fk-batch-retry":
		return NewFKBatchRetryScenario(simulator)
	case "complex-multi-batch":
		return NewComplexMultiBatchScenario(simulator)
	case "multi-device-sync":
		return NewMultiDeviceSyncScenario(simulator)
	case "multi-device-complex":
		return NewMultiDeviceComplexScenario(simulator)
	case "files-sync":
		return NewFilesSyncScenario(simulator)
	default:
		return nil
	}
}

// GetAvailableScenarios returns a list of all available scenario names
func GetAvailableScenarios() []string {
	return []string{
		"fresh-install",
		"normal-usage",
		"reinstall",
		"device-replacement",
		"offline-online",
		"conflicts",
		"user-switch",
		"fk-batch-retry",
		"complex-multi-batch",
		"multi-device-sync",
		"multi-device-complex",
		"files-sync",
	}
}
