package project

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/onemcp/cli/internal/interfaces"
)

// **Feature: onemcp-cli, Property 11: Invalid directory structure aborts initialization**
func TestProperty_InvalidDirectoryStructureAbortsInitialization(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("invalid directory structure returns DirectoryInvalid",
		prop.ForAll(
			func(fileCount int, dirCount int) bool {
				// Create a temporary directory with random files and directories
				tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				// Create random files (not matching project structure)
				for i := 0; i < fileCount; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					filename := filepath.Join(tmpDir, name.(string)+".txt")
					if err := os.WriteFile(filename, []byte("test content"), 0644); err != nil {
						t.Logf("Failed to create file: %v", err)
						return false
					}
				}

				// Create random directories (not matching handbook structure)
				for i := 0; i < dirCount; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					dirname := filepath.Join(tmpDir, name.(string))
					if err := os.MkdirAll(dirname, 0755); err != nil {
						t.Logf("Failed to create directory: %v", err)
						return false
					}
				}

				// Validate directory
				manager := NewManager()
				state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

				// Should return DirectoryInvalid for non-empty directories without valid structure
				if fileCount > 0 || dirCount > 0 {
					return state == interfaces.DirectoryInvalid && err == nil
				}

				// Empty directory should return DirectoryEmpty
				return state == interfaces.DirectoryEmpty && err == nil
			},
			gen.IntRange(0, 5),
			gen.IntRange(0, 5),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 4: Project context detection finds nearest config**
func TestProperty_ProjectContextDetectionFindsNearestConfig(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("finds project root from any subdirectory depth",
		prop.ForAll(
			func(depth int) bool {
				// Create a temporary project structure
				tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				// Create .onemcp directory and config file
				oneMcpDir := filepath.Join(tmpDir, ".onemcp")
				if err := os.MkdirAll(oneMcpDir, 0755); err != nil {
					t.Logf("Failed to create .onemcp dir: %v", err)
					return false
				}

				configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
				configContent := `version: 1
server:
  mode: local
  docker_container: test-container
  port: 8080
handbook:
  source: acme
  created_at: 2024-01-01T00:00:00Z
`
				if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
					t.Logf("Failed to create config file: %v", err)
					return false
				}

				// Create nested subdirectories
				currentDir := tmpDir
				for i := 0; i < depth; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					currentDir = filepath.Join(currentDir, name.(string))
					if err := os.MkdirAll(currentDir, 0755); err != nil {
						t.Logf("Failed to create subdirectory: %v", err)
						return false
					}
				}

				// Find project root from the deepest subdirectory
				manager := NewManager()
				foundRoot, err := manager.FindProjectRoot(currentDir)

				if err != nil {
					t.Logf("FindProjectRoot returned error: %v", err)
					return false
				}

				// Should find the original tmpDir as the project root
				return foundRoot == tmpDir
			},
			gen.IntRange(0, 10),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// Unit Tests for Project Manager

func TestValidateDirectory_EmptyDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if state != interfaces.DirectoryEmpty {
		t.Errorf("Expected DirectoryEmpty, got: %v", state)
	}
}

func TestValidateDirectory_ValidHandbookStructure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create valid handbook structure
	handbookDir := filepath.Join(tmpDir, "handbook")
	os.MkdirAll(filepath.Join(handbookDir, "docs"), 0755)
	os.MkdirAll(filepath.Join(handbookDir, "apis"), 0755)
	os.MkdirAll(filepath.Join(handbookDir, "regression-suite"), 0755)
	os.WriteFile(filepath.Join(handbookDir, "Agent.yaml"), []byte("test"), 0644)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if state != interfaces.DirectoryValidStructure {
		t.Errorf("Expected DirectoryValidStructure, got: %v", state)
	}
}

func TestValidateDirectory_InvalidStructure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create some random files
	os.WriteFile(filepath.Join(tmpDir, "random.txt"), []byte("test"), 0644)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if state != interfaces.DirectoryInvalid {
		t.Errorf("Expected DirectoryInvalid, got: %v", state)
	}
}

func TestValidateDirectory_ExistingProject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create .onemcp directory
	os.MkdirAll(filepath.Join(tmpDir, ".onemcp"), 0755)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err == nil {
		t.Error("Expected error for existing project")
	}
	if state != interfaces.DirectoryInvalid {
		t.Errorf("Expected DirectoryInvalid, got: %v", state)
	}
}

func TestValidateDirectory_HiddenFilesIgnored(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create hidden files
	os.WriteFile(filepath.Join(tmpDir, ".hidden"), []byte("test"), 0644)
	os.MkdirAll(filepath.Join(tmpDir, ".hidden_dir"), 0755)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if state != interfaces.DirectoryEmpty {
		t.Errorf("Expected DirectoryEmpty (hidden files ignored), got: %v", state)
	}
}

func TestValidateDirectory_IncompleteHandbook(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create incomplete handbook structure (missing Agent.yaml)
	handbookDir := filepath.Join(tmpDir, "handbook")
	os.MkdirAll(filepath.Join(handbookDir, "docs"), 0755)
	os.MkdirAll(filepath.Join(handbookDir, "apis"), 0755)
	os.MkdirAll(filepath.Join(handbookDir, "regression-suite"), 0755)

	manager := NewManager()
	state, err := manager.ValidateDirectory(tmpDir, interfaces.InitModeLocal)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if state != interfaces.DirectoryInvalid {
		t.Errorf("Expected DirectoryInvalid for incomplete handbook, got: %v", state)
	}
}

func TestFindProjectRoot_CurrentDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config file
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	os.WriteFile(configPath, []byte("version: 1"), 0644)

	manager := NewManager()
	root, err := manager.FindProjectRoot(tmpDir)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if root != tmpDir {
		t.Errorf("Expected root to be %s, got: %s", tmpDir, root)
	}
}

func TestFindProjectRoot_ParentDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config file in root
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	os.WriteFile(configPath, []byte("version: 1"), 0644)

	// Create subdirectory
	subDir := filepath.Join(tmpDir, "subdir", "nested")
	os.MkdirAll(subDir, 0755)

	manager := NewManager()
	root, err := manager.FindProjectRoot(subDir)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if root != tmpDir {
		t.Errorf("Expected root to be %s, got: %s", tmpDir, root)
	}
}

func TestFindProjectRoot_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	manager := NewManager()
	_, err = manager.FindProjectRoot(tmpDir)

	if err == nil {
		t.Error("Expected error when project root not found")
	}
}

func TestLoadConfig_ValidLocalConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create valid local config
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	configContent := `version: 1
server:
  mode: local
  docker_container: test-container
  port: 8080
handbook:
  source: acme
  created_at: 2024-01-01T00:00:00Z
`
	os.WriteFile(configPath, []byte(configContent), 0644)

	manager := NewManager()
	config, err := manager.LoadConfig(tmpDir)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if config == nil {
		t.Fatal("Expected config to be non-nil")
	}
	if config.Version != 1 {
		t.Errorf("Expected version 1, got: %d", config.Version)
	}
	if config.Server.Mode != interfaces.ModeLocal {
		t.Errorf("Expected mode local, got: %s", config.Server.Mode)
	}
	if config.Server.DockerContainer != "test-container" {
		t.Errorf("Expected docker_container test-container, got: %s", config.Server.DockerContainer)
	}
	if config.Server.Port != 8080 {
		t.Errorf("Expected port 8080, got: %d", config.Server.Port)
	}
}

func TestLoadConfig_ValidRemoteConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create valid remote config
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	configContent := `version: 1
server:
  mode: remote
  url: https://example.com
handbook:
  source: remote
  created_at: 2024-01-01T00:00:00Z
`
	os.WriteFile(configPath, []byte(configContent), 0644)

	manager := NewManager()
	config, err := manager.LoadConfig(tmpDir)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if config == nil {
		t.Fatal("Expected config to be non-nil")
	}
	if config.Server.Mode != interfaces.ModeRemote {
		t.Errorf("Expected mode remote, got: %s", config.Server.Mode)
	}
	if config.Server.URL != "https://example.com" {
		t.Errorf("Expected url https://example.com, got: %s", config.Server.URL)
	}
}

func TestLoadConfig_MalformedYAML(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create malformed config
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	os.WriteFile(configPath, []byte("invalid: yaml: content: ["), 0644)

	manager := NewManager()
	_, err = manager.LoadConfig(tmpDir)

	if err == nil {
		t.Error("Expected error for malformed YAML")
	}
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config missing required fields
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	configContent := `version: 1
server:
  mode: local
handbook:
  source: acme
`
	os.WriteFile(configPath, []byte(configContent), 0644)

	manager := NewManager()
	_, err = manager.LoadConfig(tmpDir)

	if err == nil {
		t.Error("Expected error for missing required fields")
	}
}

func TestLoadConfig_InvalidMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create config with invalid mode
	oneMcpDir := filepath.Join(tmpDir, ".onemcp")
	os.MkdirAll(oneMcpDir, 0755)
	configPath := filepath.Join(oneMcpDir, "onemcp.yaml")
	configContent := `version: 1
server:
  mode: invalid
handbook:
  source: acme
`
	os.WriteFile(configPath, []byte(configContent), 0644)

	manager := NewManager()
	_, err = manager.LoadConfig(tmpDir)

	if err == nil {
		t.Error("Expected error for invalid mode")
	}
}

// Mock managers for testing initialization

type mockServerManager struct {
	startCalled bool
	startError  error
}

func (m *mockServerManager) Start(ctx context.Context, config interfaces.ServerConfig) error {
	m.startCalled = true
	return m.startError
}

func (m *mockServerManager) Stop(ctx context.Context, containerName string) error {
	return nil
}

func (m *mockServerManager) Status(ctx context.Context, containerName string) (interfaces.ServerStatus, error) {
	return interfaces.ServerStatus{}, nil
}

func (m *mockServerManager) HealthCheck(ctx context.Context, endpoint string) error {
	return nil
}

func (m *mockServerManager) AutoStart(ctx context.Context, config interfaces.ServerConfig) error {
	return nil
}

type mockHandbookManager struct {
	installCalled bool
	installError  error
}

func (m *mockHandbookManager) Pull(ctx context.Context, serverURL string, targetDir string) error {
	return nil
}

func (m *mockHandbookManager) Push(ctx context.Context, serverURL string, sourceDir string) error {
	return nil
}

func (m *mockHandbookManager) InstallAcmeTemplate(targetDir string) error {
	m.installCalled = true
	return m.installError
}

func (m *mockHandbookManager) ValidateStructure(handbookDir string) error {
	return nil
}

func (m *mockHandbookManager) ResetToAcme(handbookDir string, confirmation string) error {
	return nil
}

func (m *mockHandbookManager) SyncIfNeeded(ctx context.Context, serverURL string, sourceDir string) error {
	return nil
}

type mockStateManager struct {
	initCalled bool
	initError  error
}

func (m *mockStateManager) Initialize(dbPath string) error {
	m.initCalled = true
	return m.initError
}

func (m *mockStateManager) UpdateHandbookSync(hashes map[string]string, timestamp time.Time) error {
	return nil
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

// **Feature: onemcp-cli, Property 1: Empty directory initialization creates complete structure**
func TestProperty_EmptyDirectoryInitializationCreatesCompleteStructure(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("empty directory initialization creates all required directories and config",
		prop.ForAll(
			func(dirName string) bool {
				// Create a temporary empty directory
				tmpBase, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp base dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpBase)

				targetDir := filepath.Join(tmpBase, dirName)

				// Create mock managers
				serverMgr := &mockServerManager{}
				handbookMgr := &mockHandbookManager{}
				stateMgr := &mockStateManager{}

				// Initialize project
				manager := NewManager()
				opts := interfaces.InitOptions{
					TargetDirectory: targetDir,
					ServerURL:       "",
					ServerManager:   serverMgr,
					HandbookManager: handbookMgr,
					StateManager:    stateMgr,
				}

				err = manager.Initialize(context.Background(), opts)
				if err != nil {
					t.Logf("Initialize failed: %v", err)
					return false
				}

				// Verify all required directories exist
				requiredDirs := []string{
					filepath.Join(targetDir, "handbook"),
					filepath.Join(targetDir, "logs"),
					filepath.Join(targetDir, "reports"),
					filepath.Join(targetDir, ".onemcp"),
				}

				for _, dir := range requiredDirs {
					info, err := os.Stat(dir)
					if err != nil {
						t.Logf("Directory %s does not exist: %v", dir, err)
						return false
					}
					if !info.IsDir() {
						t.Logf("%s is not a directory", dir)
						return false
					}
				}

				// Verify config file exists
				configPath := filepath.Join(targetDir, ".onemcp", "onemcp.yaml")
				info, err := os.Stat(configPath)
				if err != nil {
					t.Logf("Config file does not exist: %v", err)
					return false
				}
				if info.IsDir() {
					t.Logf("Config path is a directory, not a file")
					return false
				}

				// Verify managers were called
				if !serverMgr.startCalled {
					t.Logf("Server manager Start was not called")
					return false
				}
				if !handbookMgr.installCalled {
					t.Logf("Handbook manager InstallAcmeTemplate was not called")
					return false
				}
				if !stateMgr.initCalled {
					t.Logf("State manager Initialize was not called")
					return false
				}

				// Verify config content
				config, err := manager.LoadConfig(targetDir)
				if err != nil {
					t.Logf("Failed to load config: %v", err)
					return false
				}

				if config.Version != 1 {
					t.Logf("Expected version 1, got %d", config.Version)
					return false
				}
				if config.Server.Mode != interfaces.ModeLocal {
					t.Logf("Expected mode local, got %s", config.Server.Mode)
					return false
				}
				if config.Server.DockerContainer == "" {
					t.Logf("Docker container name is empty")
					return false
				}
				if config.Server.Port != 8080 {
					t.Logf("Expected port 8080, got %d", config.Server.Port)
					return false
				}
				if config.Handbook.Source != "acme" {
					t.Logf("Expected handbook source 'acme', got '%s'", config.Handbook.Source)
					return false
				}

				return true
			},
			gen.Identifier().SuchThat(func(s string) bool {
				return len(s) > 0 && len(s) < 50
			}),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 2: Local initialization with existing handbook preserves content**
func TestProperty_LocalInitializationWithExistingHandbookPreservesContent(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("existing handbook content is preserved during initialization",
		prop.ForAll(
			func(fileCount int) bool {
				// Create a temporary directory with valid handbook structure
				tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				// Create valid handbook structure
				handbookDir := filepath.Join(tmpDir, "handbook")
				os.MkdirAll(filepath.Join(handbookDir, "docs"), 0755)
				os.MkdirAll(filepath.Join(handbookDir, "apis"), 0755)
				os.MkdirAll(filepath.Join(handbookDir, "regression-suite"), 0755)
				os.WriteFile(filepath.Join(handbookDir, "Agent.yaml"), []byte("original: content"), 0644)

				// Create random files in handbook to verify preservation
				originalFiles := make(map[string]string)
				for i := 0; i < fileCount; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					filename := filepath.Join(handbookDir, "docs", name.(string)+".md")
					content := "original content " + name.(string)
					if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
						t.Logf("Failed to create file: %v", err)
						return false
					}
					originalFiles[filename] = content
				}

				// Create mock managers
				serverMgr := &mockServerManager{}
				handbookMgr := &mockHandbookManager{}
				stateMgr := &mockStateManager{}

				// Initialize project with existing handbook
				manager := NewManager()
				opts := interfaces.InitOptions{
					TargetDirectory: tmpDir,
					ServerURL:       "",
					ServerManager:   serverMgr,
					HandbookManager: handbookMgr,
					StateManager:    stateMgr,
				}

				err = manager.Initialize(context.Background(), opts)
				if err != nil {
					t.Logf("Initialize failed: %v", err)
					return false
				}

				// Verify handbook content is preserved
				for filename, expectedContent := range originalFiles {
					content, err := os.ReadFile(filename)
					if err != nil {
						t.Logf("Failed to read file %s: %v", filename, err)
						return false
					}
					if string(content) != expectedContent {
						t.Logf("File %s content changed. Expected: %s, Got: %s", filename, expectedContent, string(content))
						return false
					}
				}

				// Verify Agent.yaml is preserved
				agentContent, err := os.ReadFile(filepath.Join(handbookDir, "Agent.yaml"))
				if err != nil {
					t.Logf("Failed to read Agent.yaml: %v", err)
					return false
				}
				if string(agentContent) != "original: content" {
					t.Logf("Agent.yaml content changed")
					return false
				}

				// Verify InstallAcmeTemplate was NOT called (since handbook exists)
				if handbookMgr.installCalled {
					t.Logf("InstallAcmeTemplate should not be called when handbook exists")
					return false
				}

				// Verify other directories were created
				requiredDirs := []string{
					filepath.Join(tmpDir, "logs"),
					filepath.Join(tmpDir, "reports"),
					filepath.Join(tmpDir, ".onemcp"),
				}

				for _, dir := range requiredDirs {
					info, err := os.Stat(dir)
					if err != nil {
						t.Logf("Directory %s does not exist: %v", dir, err)
						return false
					}
					if !info.IsDir() {
						t.Logf("%s is not a directory", dir)
						return false
					}
				}

				// Verify config has "existing" source
				config, err := manager.LoadConfig(tmpDir)
				if err != nil {
					t.Logf("Failed to load config: %v", err)
					return false
				}

				if config.Handbook.Source != "existing" {
					t.Logf("Expected handbook source 'existing', got '%s'", config.Handbook.Source)
					return false
				}

				return true
			},
			gen.IntRange(1, 10),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 3: Remote initialization rejects non-empty directories**
func TestProperty_RemoteInitializationRejectsNonEmptyDirectories(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("remote initialization fails for non-empty directories",
		prop.ForAll(
			func(fileCount int, dirCount int) bool {
				// Create a temporary directory with some content
				tmpDir, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				// Create random files
				for i := 0; i < fileCount; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					filename := filepath.Join(tmpDir, name.(string)+".txt")
					if err := os.WriteFile(filename, []byte("test content"), 0644); err != nil {
						t.Logf("Failed to create file: %v", err)
						return false
					}
				}

				// Create random directories
				for i := 0; i < dirCount; i++ {
					name, ok := gen.Identifier().Sample()
					if !ok {
						t.Logf("Failed to generate identifier")
						return false
					}
					dirname := filepath.Join(tmpDir, name.(string))
					if err := os.MkdirAll(dirname, 0755); err != nil {
						t.Logf("Failed to create directory: %v", err)
						return false
					}
				}

				// Create mock managers
				serverMgr := &mockServerManager{}
				handbookMgr := &mockHandbookManager{}
				stateMgr := &mockStateManager{}

				// Try to initialize remote project in non-empty directory
				manager := NewManager()
				opts := interfaces.InitOptions{
					TargetDirectory: tmpDir,
					ServerURL:       "https://example.com",
					ServerManager:   serverMgr,
					HandbookManager: handbookMgr,
					StateManager:    stateMgr,
				}

				err = manager.Initialize(context.Background(), opts)

				// Should fail for non-empty directories
				if fileCount > 0 || dirCount > 0 {
					if err == nil {
						t.Logf("Expected error for non-empty directory, but got nil")
						return false
					}

					// Verify no project files were created
					configPath := filepath.Join(tmpDir, ".onemcp", "onemcp.yaml")
					if _, err := os.Stat(configPath); err == nil {
						t.Logf("Config file should not exist after failed initialization")
						return false
					}

					return true
				}

				// Empty directory should succeed (but we're not testing that here)
				return true
			},
			gen.IntRange(1, 5),
			gen.IntRange(0, 5),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 14: Target directory creation succeeds when possible**
func TestProperty_TargetDirectoryCreationSucceedsWhenPossible(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("non-existent target directory is created successfully",
		prop.ForAll(
			func(dirName string) bool {
				// Create a temporary base directory
				tmpBase, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp base dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpBase)

				// Target directory that doesn't exist yet
				targetDir := filepath.Join(tmpBase, dirName)

				// Verify target doesn't exist
				if _, err := os.Stat(targetDir); err == nil {
					t.Logf("Target directory already exists")
					return false
				}

				// Create mock managers
				serverMgr := &mockServerManager{}
				handbookMgr := &mockHandbookManager{}
				stateMgr := &mockStateManager{}

				// Initialize project in non-existent directory
				manager := NewManager()
				opts := interfaces.InitOptions{
					TargetDirectory: targetDir,
					ServerURL:       "",
					ServerManager:   serverMgr,
					HandbookManager: handbookMgr,
					StateManager:    stateMgr,
				}

				err = manager.Initialize(context.Background(), opts)
				if err != nil {
					t.Logf("Initialize failed: %v", err)
					return false
				}

				// Verify target directory was created
				info, err := os.Stat(targetDir)
				if err != nil {
					t.Logf("Target directory was not created: %v", err)
					return false
				}
				if !info.IsDir() {
					t.Logf("Target path is not a directory")
					return false
				}

				// Verify project structure was created inside
				requiredDirs := []string{
					filepath.Join(targetDir, "handbook"),
					filepath.Join(targetDir, "logs"),
					filepath.Join(targetDir, "reports"),
					filepath.Join(targetDir, ".onemcp"),
				}

				for _, dir := range requiredDirs {
					info, err := os.Stat(dir)
					if err != nil {
						t.Logf("Directory %s does not exist: %v", dir, err)
						return false
					}
					if !info.IsDir() {
						t.Logf("%s is not a directory", dir)
						return false
					}
				}

				return true
			},
			gen.Identifier().SuchThat(func(s string) bool {
				return len(s) > 0 && len(s) < 50
			}),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// **Feature: onemcp-cli, Property 15: Configuration file reflects initialization mode**
func TestProperty_ConfigurationFileReflectsInitializationMode(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("config file accurately reflects initialization parameters",
		prop.ForAll(
			func(useRemote bool, dirName string) bool {
				// Create a temporary base directory
				tmpBase, err := os.MkdirTemp("", "onemcp-test-*")
				if err != nil {
					t.Logf("Failed to create temp base dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpBase)

				targetDir := filepath.Join(tmpBase, dirName)

				// Create mock managers
				serverMgr := &mockServerManager{}
				handbookMgr := &mockHandbookManager{}
				stateMgr := &mockStateManager{}

				// Prepare initialization options
				serverURL := ""
				if useRemote {
					serverURL = "https://example.com"
				}

				opts := interfaces.InitOptions{
					TargetDirectory: targetDir,
					ServerURL:       serverURL,
					ServerManager:   serverMgr,
					HandbookManager: handbookMgr,
					StateManager:    stateMgr,
				}

				// Initialize project
				manager := NewManager()
				err = manager.Initialize(context.Background(), opts)
				if err != nil {
					t.Logf("Initialize failed: %v", err)
					return false
				}

				// Load and verify configuration
				config, err := manager.LoadConfig(targetDir)
				if err != nil {
					t.Logf("Failed to load config: %v", err)
					return false
				}

				// Verify mode matches initialization
				if useRemote {
					if config.Server.Mode != interfaces.ModeRemote {
						t.Logf("Expected remote mode, got %s", config.Server.Mode)
						return false
					}
					if config.Server.URL != serverURL {
						t.Logf("Expected URL %s, got %s", serverURL, config.Server.URL)
						return false
					}
					if config.Handbook.Source != "remote" {
						t.Logf("Expected handbook source 'remote', got '%s'", config.Handbook.Source)
						return false
					}
				} else {
					if config.Server.Mode != interfaces.ModeLocal {
						t.Logf("Expected local mode, got %s", config.Server.Mode)
						return false
					}
					if config.Server.DockerContainer == "" {
						t.Logf("Docker container name should not be empty for local mode")
						return false
					}
					if config.Server.Port != 8080 {
						t.Logf("Expected port 8080, got %d", config.Server.Port)
						return false
					}
					if config.Handbook.Source != "acme" {
						t.Logf("Expected handbook source 'acme', got '%s'", config.Handbook.Source)
						return false
					}
				}

				// Verify common fields
				if config.Version != 1 {
					t.Logf("Expected version 1, got %d", config.Version)
					return false
				}

				if config.Handbook.CreatedAt.IsZero() {
					t.Logf("CreatedAt should not be zero")
					return false
				}

				return true
			},
			gen.Bool(),
			gen.Identifier().SuchThat(func(s string) bool {
				return len(s) > 0 && len(s) < 50
			}),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}
