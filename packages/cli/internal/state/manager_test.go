package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onemcp/cli/internal/interfaces"
)

func TestInitialize(t *testing.T) {
	t.Run("creates JSON file with correct schema", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "state.json")

		manager := NewManager()
		defer manager.Close()

		err := manager.Initialize(statePath)
		if err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// Verify JSON file exists
		if _, err := os.Stat(statePath); os.IsNotExist(err) {
			t.Fatal("State file was not created")
		}

		// Verify JSON structure
		data, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatalf("Failed to read state file: %v", err)
		}

		var state StateFile
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("Failed to parse state file: %v", err)
		}

		if state.Version != 1 {
			t.Errorf("Expected version 1, got %d", state.Version)
		}

		if state.Checksums == nil {
			t.Error("Checksums map should be initialized")
		}
	})

	t.Run("handles corrupted JSON file", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "corrupted.json")

		// Create a corrupted JSON file
		if err := os.WriteFile(statePath, []byte("not valid json {{{"), 0600); err != nil {
			t.Fatalf("Failed to create corrupted file: %v", err)
		}

		manager := NewManager()
		defer manager.Close()

		err := manager.Initialize(statePath)
		if err == nil {
			t.Fatal("Expected error for corrupted JSON, got nil")
		}
	})

	t.Run("loads existing state file", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "existing.json")

		// Create existing state file
		existingState := StateFile{
			Version: 1,
			Checksums: map[string]string{
				"test.yaml": "abc123",
			},
			Metadata: StateMetadata{},
		}
		data, _ := json.Marshal(existingState)
		os.WriteFile(statePath, data, 0644)

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		hashes, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("GetHandbookHashes failed: %v", err)
		}

		if len(hashes) != 1 {
			t.Errorf("Expected 1 hash, got %d", len(hashes))
		}

		if hashes["test.yaml"] != "abc123" {
			t.Errorf("Expected hash 'abc123', got '%s'", hashes["test.yaml"])
		}
	})
}

func TestUpdateHandbookSync(t *testing.T) {
	t.Run("stores file hashes and timestamps", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		hashes := map[string]string{
			"docs/readme.md":            "abc123",
			"apis/api.yaml":             "def456",
			"regression-suites/test.md": "ghi789",
		}
		timestamp := time.Now()

		err := manager.UpdateHandbookSync(hashes, timestamp)
		if err != nil {
			t.Fatalf("UpdateHandbookSync failed: %v", err)
		}

		// Verify hashes were stored
		retrieved, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("GetHandbookHashes failed: %v", err)
		}

		if len(retrieved) != len(hashes) {
			t.Errorf("Expected %d hashes, got %d", len(hashes), len(retrieved))
		}

		for path, expectedHash := range hashes {
			if retrieved[path] != expectedHash {
				t.Errorf("For %s: expected hash '%s', got '%s'", path, expectedHash, retrieved[path])
			}
		}

		// Verify timestamp was stored
		lastSync, err := manager.GetLastSync()
		if err != nil {
			t.Fatalf("GetLastSync failed: %v", err)
		}

		// Compare timestamps (truncate to millisecond for JSON precision)
		if !lastSync.Truncate(time.Millisecond).Equal(timestamp.Truncate(time.Millisecond)) {
			t.Errorf("Expected timestamp %v, got %v", timestamp, lastSync)
		}
	})

	t.Run("replaces existing hashes on update", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// First sync
		hashes1 := map[string]string{
			"docs/readme.md": "abc123",
		}
		if err := manager.UpdateHandbookSync(hashes1, time.Now()); err != nil {
			t.Fatalf("First UpdateHandbookSync failed: %v", err)
		}

		// Second sync with different files
		hashes2 := map[string]string{
			"docs/guide.md": "xyz789",
		}
		if err := manager.UpdateHandbookSync(hashes2, time.Now()); err != nil {
			t.Fatalf("Second UpdateHandbookSync failed: %v", err)
		}

		// Verify only second sync files exist
		retrieved, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("GetHandbookHashes failed: %v", err)
		}

		if len(retrieved) != 1 {
			t.Errorf("Expected 1 hash after replacement, got %d", len(retrieved))
		}

		if retrieved["docs/guide.md"] != "xyz789" {
			t.Error("Second sync file not found or has wrong hash")
		}

		if _, exists := retrieved["docs/readme.md"]; exists {
			t.Error("First sync file should have been replaced")
		}
	})

	t.Run("returns error when state not initialized", func(t *testing.T) {
		manager := NewManager()

		err := manager.UpdateHandbookSync(map[string]string{}, time.Now())
		if err == nil {
			t.Fatal("Expected error for uninitialized state, got nil")
		}
	})
}

func TestGetHandbookHashes(t *testing.T) {
	t.Run("returns stored hashes", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		expected := map[string]string{
			"file1.yaml": "hash1",
			"file2.yaml": "hash2",
		}

		if err := manager.UpdateHandbookSync(expected, time.Now()); err != nil {
			t.Fatalf("UpdateHandbookSync failed: %v", err)
		}

		retrieved, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("GetHandbookHashes failed: %v", err)
		}

		if len(retrieved) != len(expected) {
			t.Errorf("Expected %d hashes, got %d", len(expected), len(retrieved))
		}

		for k, v := range expected {
			if retrieved[k] != v {
				t.Errorf("Expected hash '%s' for '%s', got '%s'", v, k, retrieved[k])
			}
		}
	})

	t.Run("returns copy of hashes", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		original := map[string]string{
			"file.yaml": "hash",
		}

		if err := manager.UpdateHandbookSync(original, time.Now()); err != nil {
			t.Fatalf("UpdateHandbookSync failed: %v", err)
		}

		retrieved, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("GetHandbookHashes failed: %v", err)
		}

		// Modify retrieved map
		retrieved["new.yaml"] = "newhash"

		// Get again and verify original wasn't modified
		retrieved2, err := manager.GetHandbookHashes()
		if err != nil {
			t.Fatalf("Second GetHandbookHashes failed: %v", err)
		}

		if len(retrieved2) != 1 {
			t.Error("Original map was modified by external changes")
		}
	})

	t.Run("returns error when state not initialized", func(t *testing.T) {
		manager := NewManager()

		_, err := manager.GetHandbookHashes()
		if err == nil {
			t.Fatal("Expected error for uninitialized state, got nil")
		}
	})
}

func TestGetLastSync(t *testing.T) {
	t.Run("returns last sync timestamp", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		expectedTime := time.Now().Truncate(time.Millisecond)
		hashes := map[string]string{
			"docs/readme.md": "abc123",
		}

		if err := manager.UpdateHandbookSync(hashes, expectedTime); err != nil {
			t.Fatalf("UpdateHandbookSync failed: %v", err)
		}

		lastSync, err := manager.GetLastSync()
		if err != nil {
			t.Fatalf("GetLastSync failed: %v", err)
		}

		// Truncate to millisecond for JSON precision
		lastSync = lastSync.Truncate(time.Millisecond)

		if !lastSync.Equal(expectedTime) {
			t.Errorf("Expected last sync time %v, got %v", expectedTime, lastSync)
		}
	})

	t.Run("returns zero time when no syncs exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		lastSync, err := manager.GetLastSync()
		if err != nil {
			t.Fatalf("GetLastSync failed: %v", err)
		}

		if !lastSync.IsZero() {
			t.Errorf("Expected zero time, got %v", lastSync)
		}
	})

	t.Run("returns error when state not initialized", func(t *testing.T) {
		manager := NewManager()

		_, err := manager.GetLastSync()
		if err == nil {
			t.Fatal("Expected error for uninitialized state, got nil")
		}
	})
}

func TestUpdateServerState(t *testing.T) {
	t.Run("is a no-op (server state not stored in JSON)", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		status := interfaces.ServerStatus{
			Running:       true,
			Healthy:       true,
			ContainerName: "onemcp-test-123",
		}

		// Should not error
		err := manager.UpdateServerState(status)
		if err != nil {
			t.Fatalf("UpdateServerState should be no-op, got error: %v", err)
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("is a no-op for JSON storage", func(t *testing.T) {
		tmpDir := t.TempDir()
		statePath := filepath.Join(tmpDir, "test.json")

		manager := NewManager()

		if err := manager.Initialize(statePath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		if err := manager.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify we can still read the file after Close (no exclusive locking)
		_, err := os.ReadFile(statePath)
		if err != nil {
			t.Error("File should still be accessible after Close")
		}
	})

	t.Run("handles closing uninitialized manager", func(t *testing.T) {
		manager := NewManager()

		err := manager.Close()
		if err != nil {
			t.Errorf("Close on uninitialized manager should not error, got: %v", err)
		}
	})
}
