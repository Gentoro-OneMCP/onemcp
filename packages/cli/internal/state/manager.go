package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
)

// StateFile represents the JSON structure of state.json
type StateFile struct {
	Version   int               `json:"version"`
	Checksums map[string]string `json:"checksums"`
	Metadata  StateMetadata     `json:"metadata"`
}

type StateMetadata struct {
	LastSync *time.Time `json:"last_sync,omitempty"`
}

// Manager implements StateManager using JSON file storage
type Manager struct {
	filePath string
	mu       sync.RWMutex
	state    *StateFile
}

// NewManager creates a new state manager
func NewManager() *Manager {
	return &Manager{}
}

// Initialize loads or creates the state file
func (m *Manager) Initialize(filePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.filePath = filePath

	// Check if JSON file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Create new state file
		m.state = &StateFile{
			Version:   1,
			Checksums: make(map[string]string),
			Metadata:  StateMetadata{},
		}
		return m.save()
	}

	// Load existing file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return errors.NewGenericError("failed to read state file", err)
	}

	var state StateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return errors.NewGenericError("failed to parse state file", err)
	}

	// Ensure checksums map is initialized
	if state.Checksums == nil {
		state.Checksums = make(map[string]string)
	}

	m.state = &state
	return nil
}

// Close is a no-op for JSON-based storage (kept for interface compatibility)
func (m *Manager) Close() error {
	return nil
}

// GetHandbookHashes returns all file checksums
// Note: Named GetHandbookHashes to match StateManager interface
func (m *Manager) GetHandbookHashes() (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state == nil {
		return nil, errors.NewGenericError("state not initialized", nil)
	}

	// Return a copy to prevent external modifications
	checksums := make(map[string]string, len(m.state.Checksums))
	for k, v := range m.state.Checksums {
		checksums[k] = v
	}

	return checksums, nil
}

// GetLastSync returns the timestamp of the last successful sync
func (m *Manager) GetLastSync() (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state == nil {
		return time.Time{}, errors.NewGenericError("state not initialized", nil)
	}

	if m.state.Metadata.LastSync == nil {
		// No sync has occurred yet, return zero time
		return time.Time{}, nil
	}

	return *m.state.Metadata.LastSync, nil
}

// UpdateHandbookSync updates checksums and last sync timestamp
func (m *Manager) UpdateHandbookSync(checksums map[string]string, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == nil {
		return errors.NewGenericError("state not initialized", nil)
	}

	m.state.Checksums = checksums
	m.state.Metadata.LastSync = &timestamp

	return m.save()
}

// UpdateServerState is a no-op (server state removed)
func (m *Manager) UpdateServerState(status interfaces.ServerStatus) error {
	// No longer storing server state in JSON
	// Server state can be queried from Docker API when needed
	return nil
}

// save writes the state to disk (must be called with lock held)
func (m *Manager) save() error {
	// Ensure directory exists
	dir := filepath.Dir(m.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.NewGenericError("failed to create state directory", err)
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return errors.NewGenericError("failed to marshal state", err)
	}

	// Write atomically using temp file + rename
	tmpPath := m.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return errors.NewGenericError("failed to write state file", err)
	}

	if err := os.Rename(tmpPath, m.filePath); err != nil {
		os.Remove(tmpPath) // Clean up temp file
		return errors.NewGenericError("failed to update state file", err)
	}

	return nil
}
