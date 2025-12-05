package handbook

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/onemcp/cli/internal/interfaces"
)

// **Feature: onemcp-cli, Property 7: Handbook pull overwrites local content**
func TestProperty_HandbookPullOverwritesLocalContent(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("handbook pull overwrites local content",
		prop.ForAll(
			func(initialContent map[string]string, serverContent map[string]string) bool {
				// Create temporary directory for test
				tmpDir, err := os.MkdirTemp("", "handbook-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				handbookDir := filepath.Join(tmpDir, "handbook")

				// Create initial handbook content
				if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
					t.Logf("Failed to create initial handbook: %v", err)
					return false
				}

				// Create mock server that returns server content
				server := createMockHandbookServer(serverContent)
				defer server.Close()

				// Create manager (without state manager for this test)
				manager := NewManager(nil)

				// Pull handbook from server
				ctx := context.Background()
				err = manager.Pull(ctx, server.URL, handbookDir)
				if err != nil {
					t.Logf("Pull failed: %v", err)
					return false
				}

				// Verify that local content matches server content exactly
				return verifyHandbookContent(handbookDir, serverContent)
			},
			genHandbookContent(),
			genHandbookContent(),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// genHandbookContent generates random handbook content
func genHandbookContent() gopter.Gen {
	return gen.MapOf(
		gen.OneConstOf(
			"docs/README.md",
			"docs/guide.md",
			"apis/api1.yaml",
			"apis/api2.yaml",
			"regression-suite/test1.yaml",
			"regression-suite/test2.yaml",
			"Agent.yaml",
		),
		gen.AlphaString().SuchThat(func(s string) bool {
			return len(s) > 0 && len(s) < 100
		}),
	).SuchThat(func(m map[string]string) bool {
		// Ensure we have at least the required structure
		_, hasAgent := m["Agent.yaml"]
		return hasAgent && len(m) > 0 && len(m) < 10
	})
}

// createHandbookWithContent creates a handbook directory with the given content
func createHandbookWithContent(handbookDir string, content map[string]string) error {
	// Create required directories
	dirs := []string{
		filepath.Join(handbookDir, "docs"),
		filepath.Join(handbookDir, "apis"),
		filepath.Join(handbookDir, "regression-suite"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	// Create files
	for path, fileContent := range content {
		fullPath := filepath.Join(handbookDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(fullPath, []byte(fileContent), 0644); err != nil {
			return err
		}
	}

	return nil
}

// createMockHandbookServer creates a mock HTTP server that returns a handbook tar.gz
func createMockHandbookServer(content map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mng/handbook" {
			http.NotFound(w, r)
			return
		}

		// Create tar.gz archive
		w.Header().Set("Content-Type", "application/gzip")

		gzw := gzip.NewWriter(w)
		defer gzw.Close()

		tw := tar.NewWriter(gzw)
		defer tw.Close()

		// Add files to archive
		for path, fileContent := range content {
			header := &tar.Header{
				Name: path,
				Mode: 0644,
				Size: int64(len(fileContent)),
			}

			if err := tw.WriteHeader(header); err != nil {
				return
			}

			if _, err := tw.Write([]byte(fileContent)); err != nil {
				return
			}
		}
	}))
}

// verifyHandbookContent checks if the handbook directory matches the expected content
func verifyHandbookContent(handbookDir string, expectedContent map[string]string) bool {
	for path, expectedData := range expectedContent {
		fullPath := filepath.Join(handbookDir, path)
		actualData, err := os.ReadFile(fullPath)
		if err != nil {
			return false
		}

		if string(actualData) != expectedData {
			return false
		}
	}

	// Also verify that no extra files exist (beyond what's expected)
	actualFiles := make(map[string]bool)
	filepath.Walk(handbookDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		relPath, _ := filepath.Rel(handbookDir, path)
		actualFiles[relPath] = true
		return nil
	})

	for path := range actualFiles {
		if _, expected := expectedContent[path]; !expected {
			return false
		}
	}

	return true
}

// TestInstallAcmeTemplate tests the Acme template installation
func TestInstallAcmeTemplate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "acme-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewManager(nil)
	if err := manager.InstallAcmeTemplate(tmpDir); err != nil {
		t.Fatalf("Failed to install Acme template: %v", err)
	}

	// Verify required structure exists
	requiredPaths := []string{
		filepath.Join(tmpDir, "docs"),
		filepath.Join(tmpDir, "apis"),
		filepath.Join(tmpDir, "regression-suite"),
		filepath.Join(tmpDir, "Agent.yaml"),
	}

	for _, path := range requiredPaths {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Required path %s does not exist: %v", path, err)
		}
	}
}

// TestValidateStructure tests handbook structure validation
func TestValidateStructure(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(string) error
		wantError bool
	}{
		{
			name: "valid structure",
			setup: func(dir string) error {
				dirs := []string{"docs", "apis", "regression-suite"}
				for _, d := range dirs {
					if err := os.MkdirAll(filepath.Join(dir, d), 0755); err != nil {
						return err
					}
				}
				return os.WriteFile(filepath.Join(dir, "Agent.yaml"), []byte("test"), 0644)
			},
			wantError: false,
		},
		{
			name: "missing docs directory",
			setup: func(dir string) error {
				dirs := []string{"apis", "regression-suite"}
				for _, d := range dirs {
					if err := os.MkdirAll(filepath.Join(dir, d), 0755); err != nil {
						return err
					}
				}
				return os.WriteFile(filepath.Join(dir, "Agent.yaml"), []byte("test"), 0644)
			},
			wantError: true,
		},
		{
			name: "missing Agent.yaml",
			setup: func(dir string) error {
				dirs := []string{"docs", "apis", "regression-suite"}
				for _, d := range dirs {
					if err := os.MkdirAll(filepath.Join(dir, d), 0755); err != nil {
						return err
					}
				}
				return nil
			},
			wantError: true,
		},
		{
			name: "Agent.yaml is a directory",
			setup: func(dir string) error {
				dirs := []string{"docs", "apis", "regression-suite", "Agent.yaml"}
				for _, d := range dirs {
					if err := os.MkdirAll(filepath.Join(dir, d), 0755); err != nil {
						return err
					}
				}
				return nil
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "validate-test-*")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			if err := tt.setup(tmpDir); err != nil {
				t.Fatalf("Failed to setup test: %v", err)
			}

			manager := NewManager(nil)
			err = manager.ValidateStructure(tmpDir, true)

			if tt.wantError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestPullWithBackupRestore tests that backup is restored on failure
func TestPullWithBackupRestore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-backup-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create initial content
	initialContent := map[string]string{
		"Agent.yaml":     "initial",
		"docs/README.md": "initial docs",
	}
	if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
		t.Fatalf("Failed to create initial handbook: %v", err)
	}

	// Create a server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	manager := NewManager(nil)
	ctx := context.Background()

	// Pull should fail
	err = manager.Pull(ctx, server.URL, handbookDir)
	if err == nil {
		t.Fatal("Expected pull to fail but it succeeded")
	}

	// Verify that initial content is still there (backup was restored)
	for path, expectedContent := range initialContent {
		fullPath := filepath.Join(handbookDir, path)
		actualContent, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("Failed to read %s after failed pull: %v", path, err)
			continue
		}
		if string(actualContent) != expectedContent {
			t.Errorf("Content mismatch for %s: expected %q, got %q", path, expectedContent, string(actualContent))
		}
	}
}

// mockStateManager is a simple mock for testing
type mockStateManager struct {
	updateCalled bool
	updateError  error
}

func (m *mockStateManager) Initialize(dbPath string) error {
	return nil
}

func (m *mockStateManager) UpdateHandbookSync(hashes map[string]string, timestamp time.Time) error {
	m.updateCalled = true
	return m.updateError
}

func (m *mockStateManager) UpdateServerState(status interfaces.ServerStatus) error {
	return nil
}

func (m *mockStateManager) GetLastSync() (time.Time, error) {
	return time.Time{}, nil
}

func (m *mockStateManager) GetHandbookHashes() (map[string]string, error) {
	return make(map[string]string), nil
}

func (m *mockStateManager) Close() error {
	return nil
}

// TestPullUpdatesStateDatabase tests that state database is updated after successful pull
func TestPullUpdatesStateDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create initial content
	initialContent := map[string]string{
		"Agent.yaml": "initial",
	}
	if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
		t.Fatalf("Failed to create initial handbook: %v", err)
	}

	// Create server with new content
	serverContent := map[string]string{
		"Agent.yaml":     "updated",
		"docs/README.md": "new docs",
	}
	server := createMockHandbookServer(serverContent)
	defer server.Close()

	// Create mock state manager
	mockState := &mockStateManager{}

	manager := NewManager(mockState)
	ctx := context.Background()

	// Pull should succeed
	err = manager.Pull(ctx, server.URL, handbookDir)
	if err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	// Verify state manager was updated
	if !mockState.updateCalled {
		t.Error("State manager UpdateHandbookSync was not called")
	}
}

// TestPullNetworkError tests handling of network errors
func TestPullNetworkError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-network-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create initial content
	initialContent := map[string]string{
		"Agent.yaml": "initial",
	}
	if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
		t.Fatalf("Failed to create initial handbook: %v", err)
	}

	manager := NewManager(nil)
	ctx := context.Background()

	// Use invalid URL to trigger network error
	err = manager.Pull(ctx, "http://invalid-server-that-does-not-exist.local", handbookDir)
	if err == nil {
		t.Fatal("Expected pull to fail with network error but it succeeded")
	}

	// Verify initial content is preserved
	content, err := os.ReadFile(filepath.Join(handbookDir, "Agent.yaml"))
	if err != nil {
		t.Fatalf("Failed to read Agent.yaml: %v", err)
	}
	if string(content) != "initial" {
		t.Errorf("Expected initial content to be preserved, got %q", string(content))
	}
}

// **Feature: onemcp-cli, Property 9: Acme reset requires confirmation**
func TestProperty_AcmeResetRequiresConfirmation(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("Acme reset only proceeds with 'y' confirmation",
		prop.ForAll(
			func(confirmation string, initialContent map[string]string) bool {
				tmpDir, err := os.MkdirTemp("", "acme-reset-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				handbookDir := filepath.Join(tmpDir, "handbook")

				// Create initial handbook content
				if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
					t.Logf("Failed to create initial handbook: %v", err)
					return false
				}

				// Store initial content for verification
				initialFiles := make(map[string]string)
				filepath.Walk(handbookDir, func(path string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return nil
					}
					relPath, _ := filepath.Rel(handbookDir, path)
					content, _ := os.ReadFile(path)
					initialFiles[relPath] = string(content)
					return nil
				})

				manager := NewManager(nil)
				err = manager.ResetToAcme(handbookDir, confirmation)

				if confirmation == "y" {
					// Should succeed and install Acme template
					if err != nil {
						t.Logf("Reset with 'y' confirmation failed: %v", err)
						return false
					}

					// Verify Acme template was installed (check for required structure)
					requiredPaths := []string{
						filepath.Join(handbookDir, "docs"),
						filepath.Join(handbookDir, "apis"),
						filepath.Join(handbookDir, "regression-suite"),
						filepath.Join(handbookDir, "Agent.yaml"),
					}

					for _, path := range requiredPaths {
						if _, err := os.Stat(path); err != nil {
							t.Logf("Required path %s does not exist after reset", path)
							return false
						}
					}

					// Verify content changed (not the same as initial)
					agentContent, err := os.ReadFile(filepath.Join(handbookDir, "Agent.yaml"))
					if err != nil {
						return false
					}
					initialAgent, hadInitialAgent := initialFiles["Agent.yaml"]
					// If there was an initial Agent.yaml, it should be different now
					if hadInitialAgent && string(agentContent) == initialAgent {
						t.Logf("Agent.yaml content unchanged after reset")
						return false
					}

					return true
				} else {
					// Should fail with any other input
					if err == nil {
						t.Logf("Reset with '%s' confirmation should have failed", confirmation)
						return false
					}

					// Verify initial content is preserved
					for path, expectedContent := range initialFiles {
						fullPath := filepath.Join(handbookDir, path)
						actualContent, err := os.ReadFile(fullPath)
						if err != nil {
							t.Logf("Failed to read %s after cancelled reset: %v", path, err)
							return false
						}
						if string(actualContent) != expectedContent {
							t.Logf("Content changed for %s despite cancelled reset", path)
							return false
						}
					}

					return true
				}
			},
			gen.OneConstOf("y", "n", "yes", "no", "Y", "N", ""),
			genHandbookContent(),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 8: State database updates after sync operations**
func TestProperty_StateDatabaseUpdatesAfterSyncOperations(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("state database updates after pull",
		prop.ForAll(
			func(serverContent map[string]string) bool {
				tmpDir, err := os.MkdirTemp("", "state-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				handbookDir := filepath.Join(tmpDir, "handbook")

				// Create initial content
				initialContent := map[string]string{"Agent.yaml": "initial"}
				if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
					t.Logf("Failed to create initial handbook: %v", err)
					return false
				}

				// Create mock server
				server := createMockHandbookServer(serverContent)
				defer server.Close()

				// Create mock state manager
				mockState := &mockStateManager{}
				manager := NewManager(mockState)

				// Pull handbook
				ctx := context.Background()
				err = manager.Pull(ctx, server.URL, handbookDir)
				if err != nil {
					t.Logf("Pull failed: %v", err)
					return false
				}

				// Verify state manager was updated
				return mockState.updateCalled
			},
			genHandbookContent(),
		))

	properties.Property("state database updates after push",
		prop.ForAll(
			func(handbookContent map[string]string) bool {
				tmpDir, err := os.MkdirTemp("", "state-push-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				handbookDir := filepath.Join(tmpDir, "handbook")

				// Create handbook content
				if err := createHandbookWithContent(handbookDir, handbookContent); err != nil {
					t.Logf("Failed to create handbook: %v", err)
					return false
				}

				// Create mock server that accepts pushes
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "PUT" && r.URL.Path == "/mng/handbook" {
						w.WriteHeader(http.StatusOK)
						return
					}
					http.NotFound(w, r)
				}))
				defer server.Close()

				// Create mock state manager
				mockState := &mockStateManager{}
				manager := NewManager(mockState)

				// Push handbook
				ctx := context.Background()
				err = manager.Push(ctx, server.URL, handbookDir)
				if err != nil {
					t.Logf("Push failed: %v", err)
					return false
				}

				// Verify state manager was updated
				return mockState.updateCalled
			},
			genHandbookContent(),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// TestPushValidatesStructure tests that push validates handbook structure
func TestPushValidatesStructure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "push-validate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create invalid handbook (missing required directories)
	if err := os.MkdirAll(handbookDir, 0755); err != nil {
		t.Fatalf("Failed to create handbook dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(handbookDir, "Agent.yaml"), []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create Agent.yaml: %v", err)
	}

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	manager := NewManager(nil)
	ctx := context.Background()

	// Push should fail due to invalid structure
	err = manager.Push(ctx, server.URL, handbookDir)
	if err == nil {
		t.Fatal("Expected push to fail with invalid structure but it succeeded")
	}
}

// TestPushCreatesValidArchive tests that push creates a valid tar.gz archive
func TestPushCreatesValidArchive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "push-archive-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create valid handbook
	content := map[string]string{
		"Agent.yaml":     "agent config",
		"docs/README.md": "documentation",
		"apis/api.yaml":  "api spec",
	}
	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Create mock server that captures the uploaded data
	var uploadedData []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && r.URL.Path == "/mng/handbook" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			uploadedData = data
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	manager := NewManager(nil)
	ctx := context.Background()

	// Push should succeed
	err = manager.Push(ctx, server.URL, handbookDir)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Verify data was uploaded
	if len(uploadedData) == 0 {
		t.Fatal("No data was uploaded")
	}

	// Verify it's a valid gzip archive
	gzr, err := gzip.NewReader(strings.NewReader(string(uploadedData)))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzr.Close()

	// Verify it's a valid tar archive
	tr := tar.NewReader(gzr)
	filesFound := make(map[string]bool)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read tar: %v", err)
		}
		filesFound[header.Name] = true
	}

	// Verify all expected files are in the archive
	for path := range content {
		if !filesFound[path] {
			t.Errorf("Expected file %s not found in archive", path)
		}
	}
}

// TestPushHandlesServerError tests that push handles server errors gracefully
func TestPushHandlesServerError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "push-error-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create valid handbook
	content := map[string]string{
		"Agent.yaml": "agent config",
	}
	if err := createHandbookWithContent(handbookDir, content); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Create mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	manager := NewManager(nil)
	ctx := context.Background()

	// Push should fail
	err = manager.Push(ctx, server.URL, handbookDir)
	if err == nil {
		t.Fatal("Expected push to fail with server error but it succeeded")
	}
}

// TestBackupAndRestore tests the backup and restore mechanism
func TestBackupAndRestore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "backup-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create initial content
	initialContent := map[string]string{
		"Agent.yaml":     "original",
		"docs/README.md": "original docs",
	}
	if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	manager := &Manager{}

	// Create backup
	backup, err := manager.createBackup(handbookDir)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}
	defer backup.Cleanup()

	// Modify original content
	if err := os.WriteFile(filepath.Join(handbookDir, "Agent.yaml"), []byte("modified"), 0644); err != nil {
		t.Fatalf("Failed to modify file: %v", err)
	}

	// Verify content was modified
	content, err := os.ReadFile(filepath.Join(handbookDir, "Agent.yaml"))
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(content) != "modified" {
		t.Errorf("Expected modified content, got %q", string(content))
	}

	// Restore backup
	if err := backup.Restore(); err != nil {
		t.Fatalf("Failed to restore backup: %v", err)
	}

	// Verify original content was restored
	content, err = os.ReadFile(filepath.Join(handbookDir, "Agent.yaml"))
	if err != nil {
		t.Fatalf("Failed to read file after restore: %v", err)
	}
	if string(content) != "original" {
		t.Errorf("Expected original content after restore, got %q", string(content))
	}
}

// TestCalculateFileHashes tests file hash calculation
func TestCalculateFileHashes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hash-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	files := map[string]string{
		"file1.txt":     "content1",
		"file2.txt":     "content2",
		"dir/file3.txt": "content3",
	}

	for path, content := range files {
		fullPath := filepath.Join(tmpDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	manager := &Manager{}
	hashes, err := manager.calculateFileHashes(tmpDir)
	if err != nil {
		t.Fatalf("Failed to calculate hashes: %v", err)
	}

	// Verify all files have hashes
	if len(hashes) != len(files) {
		t.Errorf("Expected %d hashes, got %d", len(files), len(hashes))
	}

	for path := range files {
		if _, ok := hashes[path]; !ok {
			t.Errorf("Missing hash for file %s", path)
		}
	}

	// Verify hashes are consistent
	hashes2, err := manager.calculateFileHashes(tmpDir)
	if err != nil {
		t.Fatalf("Failed to calculate hashes second time: %v", err)
	}

	for path, hash := range hashes {
		if hashes2[path] != hash {
			t.Errorf("Hash mismatch for %s: %s != %s", path, hash, hashes2[path])
		}
	}
}

// TestResetToAcmeWithStateUpdate tests that reset updates state database
func TestResetToAcmeWithStateUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reset-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handbookDir := filepath.Join(tmpDir, "handbook")

	// Create initial content
	initialContent := map[string]string{
		"Agent.yaml": "initial",
	}
	if err := createHandbookWithContent(handbookDir, initialContent); err != nil {
		t.Fatalf("Failed to create handbook: %v", err)
	}

	// Create mock state manager
	mockState := &mockStateManager{}
	manager := NewManager(mockState)

	// Reset to Acme
	err = manager.ResetToAcme(handbookDir, "y")
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify state manager was updated
	if !mockState.updateCalled {
		t.Error("State manager UpdateHandbookSync was not called")
	}
}

// Benchmark for handbook pull operation
func BenchmarkHandbookPull(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench-pull-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create server content
	serverContent := make(map[string]string)
	for i := 0; i < 50; i++ {
		serverContent[fmt.Sprintf("docs/file%d.md", i)] = fmt.Sprintf("content %d", i)
	}
	serverContent["Agent.yaml"] = "agent config"

	server := createMockHandbookServer(serverContent)
	defer server.Close()

	manager := NewManager(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handbookDir := filepath.Join(tmpDir, fmt.Sprintf("handbook-%d", i))
		ctx := context.Background()
		if err := manager.Pull(ctx, server.URL, handbookDir); err != nil {
			b.Fatalf("Pull failed: %v", err)
		}
	}
}
