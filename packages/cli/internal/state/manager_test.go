package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onemcp/cli/internal/interfaces"
)

func TestInitialize(t *testing.T) {
	t.Run("creates database with correct schema", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		err := manager.Initialize(dbPath)
		if err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// Verify database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Fatal("Database file was not created")
		}

		// Verify tables exist by querying them
		tables := []string{"handbook_files", "server_state", "sync_history"}
		for _, table := range tables {
			var count int
			err := manager.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
			if err != nil {
				t.Errorf("Table %s does not exist or is not accessible: %v", table, err)
			}
		}
	})

	t.Run("handles corrupted database file", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "corrupted.db")

		// Create a corrupted database file
		if err := os.WriteFile(dbPath, []byte("not a valid sqlite database"), 0600); err != nil {
			t.Fatalf("Failed to create corrupted file: %v", err)
		}

		manager := NewManager()
		defer manager.Close()

		err := manager.Initialize(dbPath)
		if err == nil {
			t.Fatal("Expected error for corrupted database, got nil")
		}

		if !contains(err.Error(), "corrupted") && !contains(err.Error(), "not a database") {
			t.Errorf("Expected corruption error message, got: %v", err)
		}
	})
}

func TestUpdateHandbookSync(t *testing.T) {
	t.Run("stores file hashes and timestamps", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		hashes := map[string]string{
			"docs/readme.md":     "abc123",
			"apis/api.yaml":      "def456",
			"regression/test.md": "ghi789",
		}
		timestamp := time.Now()

		err := manager.UpdateHandbookSync(hashes, timestamp)
		if err != nil {
			t.Fatalf("UpdateHandbookSync failed: %v", err)
		}

		// Verify files were stored
		var count int
		err = manager.db.QueryRow("SELECT COUNT(*) FROM handbook_files").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query handbook_files: %v", err)
		}

		if count != len(hashes) {
			t.Errorf("Expected %d files, got %d", len(hashes), count)
		}

		// Verify specific file hash
		var hash string
		err = manager.db.QueryRow("SELECT hash FROM handbook_files WHERE path = ?", "docs/readme.md").Scan(&hash)
		if err != nil {
			t.Fatalf("Failed to query specific file: %v", err)
		}

		if hash != "abc123" {
			t.Errorf("Expected hash 'abc123', got '%s'", hash)
		}

		// Verify sync history was recorded
		err = manager.db.QueryRow("SELECT COUNT(*) FROM sync_history WHERE operation = 'handbook_sync'").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query sync_history: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 sync history entry, got %d", count)
		}
	})

	t.Run("replaces existing hashes on update", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
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
		var count int
		err := manager.db.QueryRow("SELECT COUNT(*) FROM handbook_files").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query handbook_files: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 file after replacement, got %d", count)
		}

		// Verify the correct file exists
		var path string
		err = manager.db.QueryRow("SELECT path FROM handbook_files").Scan(&path)
		if err != nil {
			t.Fatalf("Failed to query file path: %v", err)
		}

		if path != "docs/guide.md" {
			t.Errorf("Expected path 'docs/guide.md', got '%s'", path)
		}
	})

	t.Run("returns error when database not initialized", func(t *testing.T) {
		manager := NewManager()

		err := manager.UpdateHandbookSync(map[string]string{}, time.Now())
		if err == nil {
			t.Fatal("Expected error for uninitialized database, got nil")
		}
	})
}

func TestUpdateServerState(t *testing.T) {
	t.Run("stores server status", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		status := interfaces.ServerStatus{
			Running:       true,
			Healthy:       true,
			ContainerName: "onemcp-test-123",
		}

		err := manager.UpdateServerState(status)
		if err != nil {
			t.Fatalf("UpdateServerState failed: %v", err)
		}

		// Verify server state was stored
		var running, healthy, containerName string
		err = manager.db.QueryRow("SELECT value FROM server_state WHERE key = 'running'").Scan(&running)
		if err != nil {
			t.Fatalf("Failed to query running state: %v", err)
		}

		err = manager.db.QueryRow("SELECT value FROM server_state WHERE key = 'healthy'").Scan(&healthy)
		if err != nil {
			t.Fatalf("Failed to query healthy state: %v", err)
		}

		err = manager.db.QueryRow("SELECT value FROM server_state WHERE key = 'container_name'").Scan(&containerName)
		if err != nil {
			t.Fatalf("Failed to query container name: %v", err)
		}

		if running != "true" {
			t.Errorf("Expected running='true', got '%s'", running)
		}

		if healthy != "true" {
			t.Errorf("Expected healthy='true', got '%s'", healthy)
		}

		if containerName != "onemcp-test-123" {
			t.Errorf("Expected container_name='onemcp-test-123', got '%s'", containerName)
		}
	})

	t.Run("updates existing server state", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		// First update
		status1 := interfaces.ServerStatus{
			Running:       true,
			Healthy:       true,
			ContainerName: "onemcp-test-123",
		}
		if err := manager.UpdateServerState(status1); err != nil {
			t.Fatalf("First UpdateServerState failed: %v", err)
		}

		// Second update
		status2 := interfaces.ServerStatus{
			Running:       false,
			Healthy:       false,
			ContainerName: "onemcp-test-456",
		}
		if err := manager.UpdateServerState(status2); err != nil {
			t.Fatalf("Second UpdateServerState failed: %v", err)
		}

		// Verify updated state
		var running string
		err := manager.db.QueryRow("SELECT value FROM server_state WHERE key = 'running'").Scan(&running)
		if err != nil {
			t.Fatalf("Failed to query running state: %v", err)
		}

		if running != "false" {
			t.Errorf("Expected running='false', got '%s'", running)
		}
	})

	t.Run("returns error when database not initialized", func(t *testing.T) {
		manager := NewManager()

		status := interfaces.ServerStatus{
			Running:       true,
			Healthy:       true,
			ContainerName: "test",
		}

		err := manager.UpdateServerState(status)
		if err == nil {
			t.Fatal("Expected error for uninitialized database, got nil")
		}
	})
}

func TestGetLastSync(t *testing.T) {
	t.Run("returns last sync timestamp", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		expectedTime := time.Now().Truncate(time.Second)
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

		// Truncate to second for comparison (SQLite stores with second precision)
		lastSync = lastSync.Truncate(time.Second)

		if !lastSync.Equal(expectedTime) {
			t.Errorf("Expected last sync time %v, got %v", expectedTime, lastSync)
		}
	})

	t.Run("returns zero time when no syncs exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()
		defer manager.Close()

		if err := manager.Initialize(dbPath); err != nil {
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

	t.Run("returns error when database not initialized", func(t *testing.T) {
		manager := NewManager()

		_, err := manager.GetLastSync()
		if err == nil {
			t.Fatal("Expected error for uninitialized database, got nil")
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("closes database connection", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		manager := NewManager()

		if err := manager.Initialize(dbPath); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		if err := manager.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify database is closed by attempting an operation
		err := manager.UpdateHandbookSync(map[string]string{}, time.Now())
		if err == nil {
			t.Fatal("Expected error after closing database, got nil")
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
