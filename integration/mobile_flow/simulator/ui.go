package simulator

import (
	"log/slog"
	"sync"
)

// UISimulator simulates mobile app UI state and user feedback
type UISimulator struct {
	logger *slog.Logger

	// UI State
	banner       string
	pendingBadge int
	isOnline     bool

	// Conflict indicators
	conflicts map[string]bool // table.pk -> has conflict

	mu sync.RWMutex
}

// NewUISimulator creates a new UI simulator
func NewUISimulator(logger *slog.Logger) *UISimulator {
	return &UISimulator{
		logger:    logger,
		isOnline:  true,
		conflicts: make(map[string]bool),
	}
}

// SetBanner updates the app banner message
func (ui *UISimulator) SetBanner(message string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.banner != message {
		ui.banner = message
		if verboseLog {
			ui.logger.Info("📱 UI Banner", "message", message)
		}
	}
}

// GetBanner returns the current banner message
func (ui *UISimulator) GetBanner() string {
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	return ui.banner
}

// SetPendingBadge updates the pending changes badge count
func (ui *UISimulator) SetPendingBadge(count int) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.pendingBadge != count {
		ui.pendingBadge = count
		if count > 0 {
			//ui.logger.Info("🔄 Pending Changes", "count", count)
		} else {
			if verboseLog {
				ui.logger.Info("✅ All Changes Synced")
			}
		}
	}
}

// GetPendingBadge returns the current pending badge count
func (ui *UISimulator) GetPendingBadge() int {
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	return ui.pendingBadge
}

// SetOnlineStatus updates the network connectivity status
func (ui *UISimulator) SetOnlineStatus(online bool) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	if ui.isOnline != online {
		ui.isOnline = online
		status := "🔴 Offline"
		if online {
			status = "🟢 Online"
		}
		ui.logger.Info("📶 Network Status", "status", status)

		if online {
			ui.banner = "Syncing..."
		} else {
			ui.banner = "Offline"
		}
	}
}

// IsOnline returns the current online status
func (ui *UISimulator) IsOnline() bool {
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	return ui.isOnline
}

// ShowConflictIndicator shows a conflict indicator for a specific record
func (ui *UISimulator) ShowConflictIndicator(table, pk string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	key := table + "." + pk
	if !ui.conflicts[key] {
		ui.conflicts[key] = true
		ui.logger.Warn("⚠️ Conflict Detected", "table", table, "pk", pk)
	}
}

// ClearConflictIndicator clears a conflict indicator
func (ui *UISimulator) ClearConflictIndicator(table, pk string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	key := table + "." + pk
	if ui.conflicts[key] {
		delete(ui.conflicts, key)
		ui.logger.Info("✅ Conflict Resolved", "table", table, "pk", pk)
	}
}

// GetConflicts returns all current conflicts
func (ui *UISimulator) GetConflicts() map[string]bool {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	conflicts := make(map[string]bool)
	for k, v := range ui.conflicts {
		conflicts[k] = v
	}
	return conflicts
}

// ShowSpinner simulates showing a loading spinner
func (ui *UISimulator) ShowSpinner(message string) {
	ui.logger.Info("⏳ Loading", "message", message)
}

// HideSpinner simulates hiding the loading spinner
func (ui *UISimulator) HideSpinner() {
	ui.logger.Info("✅ Loading Complete")
}

// ShowToast simulates showing a toast message
func (ui *UISimulator) ShowToast(message string) {
	ui.logger.Info("💬 Toast", "message", message)
}

// ShowError simulates showing an error message
func (ui *UISimulator) ShowError(message string) {
	ui.logger.Error("❌ Error", "message", message)
}

// ShowSuccess simulates showing a success message
func (ui *UISimulator) ShowSuccess(message string) {
	ui.logger.Info("✅ Success", "message", message)
}

// SimulateNetworkChange simulates a network connectivity change
func (ui *UISimulator) SimulateNetworkChange(online bool) {
	ui.SetOnlineStatus(online)

	if online {
		ui.ShowToast("Network connection restored")
		ui.SetBanner("Syncing...")
	} else {
		ui.ShowToast("Network connection lost")
		ui.SetBanner("Offline")
	}
}

// GetUIState returns the current UI state for verification
func (ui *UISimulator) GetUIState() UIState {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	conflicts := make([]string, 0, len(ui.conflicts))
	for key := range ui.conflicts {
		conflicts = append(conflicts, key)
	}

	return UIState{
		Banner:       ui.banner,
		PendingBadge: ui.pendingBadge,
		IsOnline:     ui.isOnline,
		Conflicts:    conflicts,
	}
}

// UIState represents the current state of the UI
type UIState struct {
	Banner       string   `json:"banner"`
	PendingBadge int      `json:"pending_badge"`
	IsOnline     bool     `json:"is_online"`
	Conflicts    []string `json:"conflicts"`
}
