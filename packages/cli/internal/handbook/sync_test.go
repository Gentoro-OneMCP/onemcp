package handbook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onemcp/cli/internal/interfaces"
)

// TestSyncIfNeeded_NoChanges tests that no sync happens when everything matches
func TestSyncIfNeeded_NoChanges(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "handbook-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")
	content := map[string]string{
		"docs/README.md": "test content",
		"Agent.yaml":     "agent config",
	}

	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Calculate hashes
	manager := NewManager(nil)
	hashes, err := manager.(*Manager).calculateFileHashes(handbookDir)
	if err != nil {
		t.Fatalf("Failed to calculate hashes: %v", err)
	}

	// Create mock server that returns the same hashes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mng/handbook/hashes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hashes)
		}
	}))
	defer server.Close()

	// Create mock state manager that returns the same hashes
	mockState := &mockStateManagerWithHashes{hashes: hashes}
	manager = NewManager(mockState)

	// Run sync
	ctx := context.Background()
	err = manager.SyncIfNeeded(ctx, server.URL, handbookDir)
	if err != nil {
		t.Errorf("SyncIfNeeded failed: %v", err)
	}

	// Verify no push or pull happened (would be tracked in a more complete mock)
}

// TestSyncIfNeeded_LocalChangedOnly tests that local changes are pushed
func TestSyncIfNeeded_LocalChangedOnly(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "handbook-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")
	content := map[string]string{
		"docs/README.md": "new content",
		"Agent.yaml":     "agent config",
	}

	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Old hashes (what was last synced)
	oldHashes := map[string]string{
		"docs/README.md": "old_hash",
		"Agent.yaml":     "agent_hash",
	}

	// Create mock server that returns old hashes and accepts push
	pushCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mng/handbook/hashes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oldHashes)
		} else if r.URL.Path == "/mng/handbook" && r.Method == "PUT" {
			pushCalled = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Create mock state manager that returns old hashes
	mockState := &mockStateManagerWithHashes{hashes: oldHashes}
	manager := NewManager(mockState)

	// Run sync
	ctx := context.Background()
	err = manager.SyncIfNeeded(ctx, server.URL, handbookDir)
	if err != nil {
		t.Errorf("SyncIfNeeded failed: %v", err)
	}

	if !pushCalled {
		t.Error("Expected push to be called when local changed")
	}
}

// TestSyncIfNeeded_ServerChangedOnly tests that server changes are pulled
func TestSyncIfNeeded_ServerChangedOnly(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "handbook-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")
	content := map[string]string{
		"docs/README.md": "old content",
		"Agent.yaml":     "agent config",
	}

	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Calculate current hashes (which match last sync)
	manager := NewManager(nil)
	currentHashes, err := manager.(*Manager).calculateFileHashes(handbookDir)
	if err != nil {
		t.Fatalf("Failed to calculate hashes: %v", err)
	}

	// Server has different hashes
	serverHashes := map[string]string{
		"docs/README.md": "new_server_hash",
		"Agent.yaml":     "agent_hash",
	}

	// Server content (what will be pulled)
	serverContent := map[string]string{
		"docs/README.md": "new server content",
		"Agent.yaml":     "agent config",
	}

	// Create mock server with proper tar.gz
	pullCalled := false
	server := createMockHandbookServer(serverContent)
	defer server.Close()

	// Wrap the server to track hashes endpoint
	wrappedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mng/handbook/hashes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(serverHashes)
		} else if r.URL.Path == "/mng/handbook" && r.Method == "GET" {
			pullCalled = true
			// Forward to the real mock server
			resp, err := http.Get(server.URL + "/mng/handbook")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()
			w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
			w.WriteHeader(resp.StatusCode)
			// Copy response body
			buf := make([]byte, 4096)
			for {
				n, err := resp.Body.Read(buf)
				if n > 0 {
					w.Write(buf[:n])
				}
				if err != nil {
					break
				}
			}
		}
	}))
	defer wrappedServer.Close()

	// Create mock state manager that returns current hashes (matching local)
	mockState := &mockStateManagerWithHashes{hashes: currentHashes}
	manager = NewManager(mockState)

	// Run sync
	ctx := context.Background()
	err = manager.SyncIfNeeded(ctx, wrappedServer.URL, handbookDir)
	if err != nil {
		t.Errorf("SyncIfNeeded failed: %v", err)
	}

	if !pullCalled {
		t.Error("Expected pull to be called when server changed")
	}
}

// TestSyncIfNeeded_BothChanged tests that conflicts are detected
func TestSyncIfNeeded_BothChanged(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "handbook-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")
	content := map[string]string{
		"docs/README.md": "local new content",
		"Agent.yaml":     "agent config",
	}

	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Last synced hashes (different from both local and server)
	lastSyncHashes := map[string]string{
		"docs/README.md": "old_hash",
		"Agent.yaml":     "agent_hash",
	}

	// Server has different hashes (also changed)
	serverHashes := map[string]string{
		"docs/README.md": "server_new_hash",
		"Agent.yaml":     "agent_hash",
	}

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mng/handbook/hashes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(serverHashes)
		}
	}))
	defer server.Close()

	// Create mock state manager that returns last sync hashes
	mockState := &mockStateManagerWithHashes{hashes: lastSyncHashes}
	manager := NewManager(mockState)

	// Run sync - should detect conflict
	ctx := context.Background()
	err = manager.SyncIfNeeded(ctx, server.URL, handbookDir)
	if err == nil {
		t.Error("Expected conflict error when both local and server changed")
	}

	// Check that error message mentions conflict
	if err != nil {
		errMsg := err.Error()
		hasConflict := false
		for i := 0; i <= len(errMsg)-8; i++ {
			if errMsg[i:i+8] == "conflict" {
				hasConflict = true
				break
			}
		}
		if !hasConflict {
			t.Errorf("Expected conflict error, got: %v", err)
		}
	}
}

// TestSyncIfNeeded_ServerNotFound tests that handbook is pushed when server doesn't have it
func TestSyncIfNeeded_ServerNotFound(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "handbook-sync-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")
	content := map[string]string{
		"docs/README.md": "content",
		"Agent.yaml":     "agent config",
	}

	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Create mock server that returns 404 for hashes
	pushCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mng/handbook/hashes" {
			w.WriteHeader(http.StatusNotFound)
		} else if r.URL.Path == "/mng/handbook" && r.Method == "PUT" {
			pushCalled = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Create mock state manager
	mockState := &mockStateManagerWithHashes{hashes: make(map[string]string)}
	manager := NewManager(mockState)

	// Run sync
	ctx := context.Background()
	err = manager.SyncIfNeeded(ctx, server.URL, handbookDir)
	if err != nil {
		t.Errorf("SyncIfNeeded failed: %v", err)
	}

	if !pushCalled {
		t.Error("Expected push to be called when server doesn't have handbook")
	}
}

// mockStateManagerWithHashes is a mock that returns specific hashes
type mockStateManagerWithHashes struct {
	hashes       map[string]string
	updateCalled bool
}

func (m *mockStateManagerWithHashes) Initialize(dbPath string) error {
	return nil
}

func (m *mockStateManagerWithHashes) UpdateHandbookSync(hashes map[string]string, timestamp time.Time) error {
	m.updateCalled = true
	return nil
}

func (m *mockStateManagerWithHashes) UpdateServerState(status interfaces.ServerStatus) error {
	return nil
}

func (m *mockStateManagerWithHashes) GetLastSync() (time.Time, error) {
	return time.Time{}, nil
}

func (m *mockStateManagerWithHashes) GetHandbookHashes() (map[string]string, error) {
	return m.hashes, nil
}

func (m *mockStateManagerWithHashes) Close() error {
	return nil
}

// createMockHandbookServerWithHashes creates a mock server that returns specific hashes
func createMockHandbookServerWithHashes(content map[string]string, hashes map[string]string, pullCalled *bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/handbook/hashes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hashes)
		} else if r.URL.Path == "/handbook" && r.Method == "GET" {
			*pullCalled = true
			// Return a tar.gz archive
			w.Header().Set("Content-Type", "application/gzip")
			// For simplicity, just return empty archive
			w.WriteHeader(http.StatusOK)
		}
	}))
}
