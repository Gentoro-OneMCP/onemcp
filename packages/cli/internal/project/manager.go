package project

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/spf13/viper"
)

// Manager implements the ProjectManager interface
type Manager struct{}

// NewManager creates a new ProjectManager instance
func NewManager() interfaces.ProjectManager {
	return &Manager{}
}

// ValidateDirectory checks the state of a directory for initialization
func (m *Manager) ValidateDirectory(dir string, mode interfaces.InitMode) (interfaces.DirectoryState, error) {
	// Check if directory exists
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return interfaces.DirectoryEmpty, nil
		}
		return interfaces.DirectoryInvalid, errors.NewGenericError("failed to access directory", err)
	}

	if !info.IsDir() {
		return interfaces.DirectoryInvalid, errors.NewValidationError("path is not a directory")
	}

	// Check if directory is empty
	entries, err := os.ReadDir(dir)
	if err != nil {
		return interfaces.DirectoryInvalid, errors.NewGenericError("failed to read directory", err)
	}

	// Check if directory contains .onemcp/ - indicates existing project
	for _, entry := range entries {
		if entry.Name() == ".onemcp" && entry.IsDir() {
			return interfaces.DirectoryInvalid, errors.NewValidationError("directory already contains an onemcp project")
		}
	}

	// Filter out hidden files for empty check
	visibleEntries := filterVisibleEntries(entries)

	if len(visibleEntries) == 0 {
		return interfaces.DirectoryEmpty, nil
	}

	// Check if directory has valid handbook structure
	hasValidHandbook, err := m.validateHandbookStructure(dir)
	if err != nil {
		return interfaces.DirectoryInvalid, err
	}

	if hasValidHandbook {
		return interfaces.DirectoryValidStructure, nil
	}

	// Directory has content but doesn't match project structure
	return interfaces.DirectoryInvalid, nil
}

// validateHandbookStructure checks if a directory contains a valid handbook structure
func (m *Manager) validateHandbookStructure(dir string) (bool, error) {
	handbookPath := filepath.Join(dir, "handbook")

	// Check if handbook directory exists
	info, err := os.Stat(handbookPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.NewGenericError("failed to access handbook directory", err)
	}

	// Follow symlinks
	if info.Mode()&os.ModeSymlink != 0 {
		realPath, err := filepath.EvalSymlinks(handbookPath)
		if err != nil {
			return false, errors.NewGenericError("failed to resolve handbook symlink", err)
		}
		info, err = os.Stat(realPath)
		if err != nil {
			return false, errors.NewGenericError("failed to access handbook symlink target", err)
		}
	}

	if !info.IsDir() {
		return false, nil
	}

	// Check for required subdirectories and files
	requiredPaths := []string{
		filepath.Join(handbookPath, "docs"),
		filepath.Join(handbookPath, "apis"),
		filepath.Join(handbookPath, "regression-suite"),
		filepath.Join(handbookPath, "Agent.yaml"),
	}

	for _, path := range requiredPaths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			// Permission errors should be reported
			if os.IsPermission(err) {
				return false, errors.NewGenericError(fmt.Sprintf("permission denied accessing %s", path), err)
			}
			return false, errors.NewGenericError(fmt.Sprintf("failed to access %s", path), err)
		}

		// For directories, follow symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			realPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				return false, errors.NewGenericError(fmt.Sprintf("failed to resolve symlink %s", path), err)
			}
			info, err = os.Stat(realPath)
			if err != nil {
				return false, errors.NewGenericError(fmt.Sprintf("failed to access symlink target %s", path), err)
			}
		}

		// Check that directories are directories and Agent.yaml is a file
		if path == filepath.Join(handbookPath, "Agent.yaml") {
			if info.IsDir() {
				return false, nil
			}
		} else {
			if !info.IsDir() {
				return false, nil
			}
		}
	}

	return true, nil
}

// filterVisibleEntries filters out hidden files (starting with .)
func filterVisibleEntries(entries []os.DirEntry) []os.DirEntry {
	visible := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if len(entry.Name()) > 0 && entry.Name()[0] != '.' {
			visible = append(visible, entry)
		}
	}
	return visible
}

// FindProjectRoot searches for .onemcp/onemcp.yaml in current and parent directories
func (m *Manager) FindProjectRoot(startDir string) (string, error) {
	// Get absolute path
	absPath, err := filepath.Abs(startDir)
	if err != nil {
		return "", errors.NewGenericError("failed to resolve absolute path", err)
	}

	// Search current and parent directories
	currentDir := absPath
	for {
		configPath := filepath.Join(currentDir, ".onemcp", "onemcp.yaml")

		// Check if config file exists
		info, err := os.Stat(configPath)
		if err == nil && !info.IsDir() {
			// Found the config file
			return currentDir, nil
		}

		// Check for permission errors
		if err != nil && !os.IsNotExist(err) {
			if os.IsPermission(err) {
				return "", errors.NewGenericError("permission denied accessing .onemcp/onemcp.yaml", err)
			}
		}

		// Move to parent directory
		parentDir := filepath.Dir(currentDir)

		// Check if we've reached the root
		if parentDir == currentDir {
			break
		}

		currentDir = parentDir
	}

	return "", errors.NewContextError("not in an onemcp project directory")
}

// LoadConfig parses the onemcp.yaml configuration file
func (m *Manager) LoadConfig(projectRoot string) (*interfaces.ProjectConfig, error) {
	configPath := filepath.Join(projectRoot, ".onemcp", "onemcp.yaml")

	// Check if config file exists
	if _, err := os.Stat(configPath); err != nil {
		if os.IsNotExist(err) {
			return nil, errors.NewContextError("configuration file not found")
		}
		return nil, errors.NewGenericError("failed to access configuration file", err)
	}

	// Create a new viper instance for this config
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.NewGenericError("failed to parse configuration file", err)
	}

	// Unmarshal into ProjectConfig struct
	var config interfaces.ProjectConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, errors.NewGenericError("failed to parse configuration structure", err)
	}

	// Validate required fields
	if config.Version == 0 {
		return nil, errors.NewGenericError("invalid configuration: missing version", nil)
	}

	if config.Server.Mode != interfaces.ModeLocal && config.Server.Mode != interfaces.ModeRemote {
		return nil, errors.NewGenericError("invalid configuration: mode must be 'local' or 'remote'", nil)
	}

	if config.Server.Mode == interfaces.ModeLocal {
		if config.Server.DockerContainer == "" {
			return nil, errors.NewGenericError("invalid configuration: local mode requires docker_container", nil)
		}
		if config.Server.Port == 0 {
			return nil, errors.NewGenericError("invalid configuration: local mode requires port", nil)
		}
	}

	if config.Server.Mode == interfaces.ModeRemote {
		if config.Server.URL == "" {
			return nil, errors.NewGenericError("invalid configuration: remote mode requires url", nil)
		}
	}

	if config.Handbook.Source == "" {
		return nil, errors.NewGenericError("invalid configuration: missing handbook source", nil)
	}

	return &config, nil
}

// Initialize creates a new onemcp project
func (m *Manager) Initialize(ctx context.Context, opts interfaces.InitOptions) error {
	// Resolve target directory
	targetDir := opts.TargetDirectory
	if targetDir == "" {
		var err error
		targetDir, err = os.Getwd()
		if err != nil {
			return errors.NewGenericError("failed to get current directory", err)
		}
	}

	// Convert to absolute path
	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		return errors.NewGenericError("failed to resolve absolute path", err)
	}
	targetDir = absTargetDir

	// Create target directory if it doesn't exist
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return errors.NewGenericError("failed to create target directory", err)
		}
	} else if err != nil {
		return errors.NewGenericError("failed to access target directory", err)
	}

 // Infer InitMode if not explicitly provided
 if opts.InitMode == "" {
     if strings.TrimSpace(opts.ServerURL) != "" {
         opts.InitMode = interfaces.InitModeRemote
     } else {
         opts.InitMode = interfaces.InitModeLocal
     }
 }

 // Validate directory state
 dirState, err := m.ValidateDirectory(targetDir, opts.InitMode)
	if err != nil {
		return err
	}

	// Handle different directory states and modes
	if opts.InitMode == interfaces.InitModeRemote {
		if dirState != interfaces.DirectoryEmpty {
			return errors.NewValidationError("remote projects require an empty directory")
		}
		return m.initializeRemote(ctx, targetDir, opts)
	}

	// Local mode initialization
	switch dirState {
	case interfaces.DirectoryEmpty:
		return m.initializeLocalEmpty(ctx, targetDir, opts)
	case interfaces.DirectoryValidStructure:
		return m.initializeLocalExisting(ctx, targetDir, opts)
	case interfaces.DirectoryInvalid:
		return errors.NewValidationError("directory does not match onemcp project structure")
	default:
		return errors.NewValidationError("unknown directory state")
	}
}

// initializeLocalEmpty initializes a local project in an empty directory
func (m *Manager) initializeLocalEmpty(ctx context.Context, targetDir string, opts interfaces.InitOptions) error {
	// Track created resources for rollback
	var createdDirs []string
	var stateDB string

	rollback := func() {
		// Remove created directories in reverse order
		for i := len(createdDirs) - 1; i >= 0; i-- {
			os.RemoveAll(createdDirs[i])
		}
		// Remove state database
		if stateDB != "" {
			os.Remove(stateDB)
		}
	}

	// Create project structure directories
	dirs := []string{
		filepath.Join(targetDir, "handbook"),
		filepath.Join(targetDir, "logs"),
		filepath.Join(targetDir, "reports"),
		filepath.Join(targetDir, ".onemcp"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			rollback()
			return errors.NewGenericError(fmt.Sprintf("failed to create directory %s", dir), err)
		}
		createdDirs = append(createdDirs, dir)
	}

	// Generate container name
	containerName := fmt.Sprintf("onemcp-%s", generateRandomString(8))

	// Start Docker container and verify health
	serverMgr := opts.ServerManager
	if serverMgr == nil {
		rollback()
		return errors.NewGenericError("server manager not provided", nil)
	}

	// Determine port, default to 8080 if not provided
	port := opts.TcpPort
	if port == 0 {
		port = 8080
	}

	serverConfig := interfaces.ServerConfig{
		Mode:            interfaces.ModeLocal,
		DockerContainer: containerName,
		Port:            port,
		Model:           opts.Model,
		ModelAPIKey:     opts.ModelAPIKey,
	}

	if err := serverMgr.Start(ctx, serverConfig); err != nil {
		rollback()
		return errors.NewServerError("failed to start Docker container", err)
	}

	// Install Acme template
	handbookMgr := opts.HandbookManager
	if handbookMgr == nil {
		rollback()
		return errors.NewGenericError("handbook manager not provided", nil)
	}

	handbookDir := filepath.Join(targetDir, "handbook")
	if err := handbookMgr.InstallAcmeTemplate(handbookDir); err != nil {
		rollback()
		return errors.NewGenericError("failed to install Acme template", err)
	}

	// Initialize state database
	stateMgr := opts.StateManager
	if stateMgr == nil {
		rollback()
		return errors.NewGenericError("state manager not provided", nil)
	}

	stateDBPath := filepath.Join(targetDir, ".onemcp", "state.db")
	stateDB = stateDBPath
	if err := stateMgr.Initialize(stateDBPath); err != nil {
		rollback()
		return errors.NewGenericError("failed to initialize state database", err)
	}

	// Push handbook to server after container is healthy
	serverURL := fmt.Sprintf("http://localhost:%d", serverConfig.Port)
	if err := handbookMgr.Push(ctx, serverURL, handbookDir); err != nil {
		rollback()
		return errors.NewGenericError("failed to push handbook to server", err)
	}

	// Write configuration file
	config := interfaces.ProjectConfig{
		Version: 1,
		Server: interfaces.ServerConfig{
			Mode:            interfaces.ModeLocal,
			DockerContainer: containerName,
			Port:            port,
			Model:           opts.Model,
			ModelAPIKey:     opts.ModelAPIKey,
		},
		Handbook: interfaces.HandbookConfig{
			Source:    "acme",
			CreatedAt: time.Now(),
		},
	}

	if err := m.writeConfig(targetDir, &config); err != nil {
		rollback()
		return errors.NewGenericError("failed to write configuration file", err)
	}

	return nil
}

// initializeLocalExisting initializes a local project with existing handbook
func (m *Manager) initializeLocalExisting(ctx context.Context, targetDir string, opts interfaces.InitOptions) error {
	// Track created resources for rollback
	var createdDirs []string
	var stateDB string

	rollback := func() {
		// Remove created directories in reverse order
		for i := len(createdDirs) - 1; i >= 0; i-- {
			os.RemoveAll(createdDirs[i])
		}
		// Remove state database
		if stateDB != "" {
			os.Remove(stateDB)
		}
	}

	// Create remaining project structure (handbook already exists)
	dirs := []string{
		filepath.Join(targetDir, "logs"),
		filepath.Join(targetDir, "reports"),
		filepath.Join(targetDir, ".onemcp"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			rollback()
			return errors.NewGenericError(fmt.Sprintf("failed to create directory %s", dir), err)
		}
		createdDirs = append(createdDirs, dir)
	}

	// Generate container name
	containerName := fmt.Sprintf("onemcp-%s", generateRandomString(8))

	// Start Docker container and verify health
	serverMgr := opts.ServerManager
	if serverMgr == nil {
		rollback()
		return errors.NewGenericError("server manager not provided", nil)
	}

 // Determine port, default to 8080 if not provided
    port := opts.TcpPort
    if port == 0 {
        port = 8080
    }

    serverConfig := interfaces.ServerConfig{
        Mode:            interfaces.ModeLocal,
        DockerContainer: containerName,
        Port:            port,
        Model:           opts.Model,
        ModelAPIKey:     opts.ModelAPIKey,
    }

	if err := serverMgr.Start(ctx, serverConfig); err != nil {
		rollback()
		return errors.NewServerError("failed to start Docker container", err)
	}

	// Initialize state database
	stateMgr := opts.StateManager
	if stateMgr == nil {
		rollback()
		return errors.NewGenericError("state manager not provided", nil)
	}

	stateDBPath := filepath.Join(targetDir, ".onemcp", "state.db")
	stateDB = stateDBPath
	if err := stateMgr.Initialize(stateDBPath); err != nil {
		rollback()
		return errors.NewGenericError("failed to initialize state database", err)
	}

	// Push handbook to server after container is healthy
	handbookMgr := opts.HandbookManager
	if handbookMgr == nil {
		rollback()
		return errors.NewGenericError("handbook manager not provided", nil)
	}

 handbookDir := filepath.Join(targetDir, "handbook")
 serverURL := fmt.Sprintf("http://localhost:%d", serverConfig.Port)
 if err := handbookMgr.Push(ctx, serverURL, handbookDir); err != nil {
     rollback()
     return errors.NewGenericError("failed to push handbook to server", err)
 }

	// Write configuration file with "existing" source
 config := interfaces.ProjectConfig{
        Version: 1,
        Server: interfaces.ServerConfig{
            Mode:            interfaces.ModeLocal,
            DockerContainer: containerName,
            Port:            port,
            Model:           opts.Model,
            ModelAPIKey:     opts.ModelAPIKey,
        },
        Handbook: interfaces.HandbookConfig{
            Source:    "existing",
			CreatedAt: time.Now(),
		},
	}

	if err := m.writeConfig(targetDir, &config); err != nil {
		rollback()
		return errors.NewGenericError("failed to write configuration file", err)
	}

	return nil
}

// initializeRemote initializes a remote project
func (m *Manager) initializeRemote(ctx context.Context, targetDir string, opts interfaces.InitOptions) error {
	// Track created resources for rollback
	var createdDirs []string
	var stateDB string

	rollback := func() {
		// Remove created directories in reverse order
		for i := len(createdDirs) - 1; i >= 0; i-- {
			os.RemoveAll(createdDirs[i])
		}
		// Remove state database
		if stateDB != "" {
			os.Remove(stateDB)
		}
	}

	// Validate remote server health
	serverMgr := opts.ServerManager
	if serverMgr == nil {
		return errors.NewGenericError("server manager not provided", nil)
	}

	healthEndpoint := strings.TrimSuffix(opts.ServerURL, "/") + "/actuator/health"
	if err := serverMgr.HealthCheck(ctx, healthEndpoint); err != nil {
		return errors.NewServerError("remote server health check failed", err)
	}

	// Create project structure directories
	dirs := []string{
		filepath.Join(targetDir, "handbook"),
		filepath.Join(targetDir, "logs"),
		filepath.Join(targetDir, "reports"),
		filepath.Join(targetDir, ".onemcp"),
		filepath.Join(targetDir, "handbook/docs"),
		filepath.Join(targetDir, "handbook/apis"),
		filepath.Join(targetDir, "handbook/regression-suite"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			rollback()
			return errors.NewGenericError(fmt.Sprintf("failed to create directory %s", dir), err)
		}
		createdDirs = append(createdDirs, dir)
	}

	// Pull handbook from remote server
	handbookMgr := opts.HandbookManager
	if handbookMgr == nil {
		rollback()
		return errors.NewGenericError("handbook manager not provided", nil)
	}

	// Initialize state database
	stateMgr := opts.StateManager
	if stateMgr == nil {
		rollback()
		return errors.NewGenericError("state manager not provided", nil)
	}

	stateDBPath := filepath.Join(targetDir, ".onemcp", "state.db")
	stateDB = stateDBPath
	if err := stateMgr.Initialize(stateDBPath); err != nil {
		rollback()
		return errors.NewGenericError("failed to initialize state database", err)
	}

	handbookDir := filepath.Join(targetDir, "handbook")
	if err := handbookMgr.Pull(ctx, opts.ServerURL, handbookDir); err != nil {
		rollback()
		return errors.NewSyncError("failed to pull handbook from remote server", err)
	}

	// Write configuration file
	config := interfaces.ProjectConfig{
		Version: 1,
		Server: interfaces.ServerConfig{
			Mode: interfaces.ModeRemote,
			URL:  opts.ServerURL,
		},
		Handbook: interfaces.HandbookConfig{
			Source:    "remote",
			CreatedAt: time.Now(),
		},
	}

	if err := m.writeConfig(targetDir, &config); err != nil {
		rollback()
		return errors.NewGenericError("failed to write configuration file", err)
	}

	return nil
}

// writeConfig writes the project configuration to onemcp.yaml
func (m *Manager) writeConfig(targetDir string, config *interfaces.ProjectConfig) error {
	configPath := filepath.Join(targetDir, ".onemcp", "onemcp.yaml")

	// Create viper instance
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// Set values
	v.Set("version", config.Version)
	v.Set("server.mode", config.Server.Mode)

	if config.Server.Mode == interfaces.ModeLocal {
		v.Set("server.docker_container", config.Server.DockerContainer)
		v.Set("server.port", config.Server.Port)
		if config.Server.Model != "" {
			v.Set("server.model", config.Server.Model)
		}
		if config.Server.ModelAPIKey != "" {
			v.Set("server.model_api_key", config.Server.ModelAPIKey)
		}
	} else {
		v.Set("server.url", config.Server.URL)
	}

	v.Set("handbook.source", config.Handbook.Source)
	v.Set("handbook.created_at", config.Handbook.CreatedAt)
	if config.Handbook.LastSync != nil {
		v.Set("handbook.last_sync", config.Handbook.LastSync)
	}

	// Write config file
	if err := v.WriteConfig(); err != nil {
		return errors.NewGenericError("failed to write config file", err)
	}

	return nil
}

// generateRandomString generates a random alphanumeric string of given length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}
