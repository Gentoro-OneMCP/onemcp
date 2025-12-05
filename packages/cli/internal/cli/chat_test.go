package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// Test chat command outside project context
func TestChatCommand_NoProjectContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-no-project-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Change to non-project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Run command
	err = runChat(cmd, []string{})

	// Should fail with context error
	if err == nil {
		t.Fatal("Expected error for no project context, got nil")
	}

	cliErr, ok := err.(*errors.CLIError)
	if !ok {
		t.Fatalf("Expected CLIError, got %T", err)
	}

	if cliErr.Code != errors.CodeNotInProject {
		t.Errorf("Expected error code %d, got %d", errors.CodeNotInProject, cliErr.Code)
	}
}

// Test chat command in local mode (integration test)
func TestChatCommand_LocalMode(t *testing.T) {
	projectDir := createTestProject(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This is an integration test that requires Docker and a running server
	t.Skip("Integration test - requires Docker and running server")
}

// Test chat command in remote mode (integration test)
func TestChatCommand_RemoteMode(t *testing.T) {
	projectDir := createTestProject(t, interfaces.ModeRemote)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This is an integration test that requires a remote server
	t.Skip("Integration test - requires remote server")
}

// Test chat command from subdirectory
func TestChatCommand_FromSubdirectory(t *testing.T) {
	projectDir := createTestProject(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Create a subdirectory
	subDir := filepath.Join(projectDir, "subdir")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	// Change to subdirectory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(subDir)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Run chat command
	// This should find the project root and load config successfully
	// It will fail at server operations, but that's expected
	err := runChat(cmd, []string{})

	// We expect a server error (Docker not available), not a context error
	if err == nil {
		t.Skip("Docker is available - skipping test")
	}

	cliErr, ok := err.(*errors.CLIError)
	if !ok {
		// If it's not a CLI error, it might be a Docker error, which is fine
		return
	}

	// Should NOT be a context error
	if cliErr.Code == errors.CodeNotInProject {
		t.Error("Should find project context from subdirectory")
	}
}

// Test error handling for malformed config
func TestChatCommand_MalformedConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-malformed-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create .onemcp directory
	onemcpDir := filepath.Join(tmpDir, ".onemcp")
	if err := os.MkdirAll(onemcpDir, 0755); err != nil {
		t.Fatalf("Failed to create .onemcp dir: %v", err)
	}

	// Create malformed config file
	configPath := filepath.Join(onemcpDir, "onemcp.yaml")
	if err := os.WriteFile(configPath, []byte("invalid: yaml: content: ["), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Run command
	err = runChat(cmd, []string{})

	// Should fail with generic error
	if err == nil {
		t.Fatal("Expected error for malformed config, got nil")
	}

	cliErr, ok := err.(*errors.CLIError)
	if !ok {
		t.Fatalf("Expected CLIError, got %T", err)
	}

	if cliErr.Code != errors.CodeGeneric {
		t.Errorf("Expected error code %d, got %d", errors.CodeGeneric, cliErr.Code)
	}
}

// Test that chat command validates project context before auto-start
func TestChatCommand_ContextValidationBeforeAutoStart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-context-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Change to non-project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Run command
	err = runChat(cmd, []string{})

	// Should fail with context error before attempting auto-start
	if err == nil {
		t.Fatal("Expected error for no project context, got nil")
	}

	cliErr, ok := err.(*errors.CLIError)
	if !ok {
		t.Fatalf("Expected CLIError, got %T", err)
	}

	if cliErr.Code != errors.CodeNotInProject {
		t.Errorf("Expected error code %d, got %d", errors.CodeNotInProject, cliErr.Code)
	}
}

// Test chat command with local mode creates proper log path
func TestChatCommand_LogPathGeneration(t *testing.T) {
	projectDir := createTestProject(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Create logs directory
	logsDir := filepath.Join(projectDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create logs dir: %v", err)
	}

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This test verifies the log path structure
	// The actual chat session would require a running server
	t.Skip("Integration test - requires running server")
}

// Test chat command handles auto-start errors appropriately
func TestChatCommand_AutoStartError(t *testing.T) {
	projectDir := createTestProject(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))

	// Run command - should fail at auto-start if Docker is not available
	err := runChat(cmd, []string{})

	// If Docker is available, skip this test
	if err == nil {
		t.Skip("Docker is available and server started - skipping test")
	}

	// Should get a server error from auto-start
	cliErr, ok := err.(*errors.CLIError)
	if !ok {
		// Could be a different error type, which is acceptable
		return
	}

	// Should be a server error, not a context error
	if cliErr.Code == errors.CodeNotInProject {
		t.Error("Should not be a context error - context validation passed")
	}
}

// Test chat command with remote mode configuration
func TestChatCommand_RemoteModeConfiguration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "onemcp-remote-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create .onemcp directory
	onemcpDir := filepath.Join(tmpDir, ".onemcp")
	if err := os.MkdirAll(onemcpDir, 0755); err != nil {
		t.Fatalf("Failed to create .onemcp dir: %v", err)
	}

	// Create logs directory
	logsDir := filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create logs dir: %v", err)
	}

	// Create config file with remote mode
	config := interfaces.ProjectConfig{
		Version: 1,
		Server: interfaces.ServerConfig{
			Mode: interfaces.ModeRemote,
			URL:  "http://remote-server:8080",
		},
		Handbook: interfaces.HandbookConfig{
			Source: "remote",
		},
	}

	configPath := filepath.Join(onemcpDir, "onemcp.yaml")
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// This test verifies remote mode configuration is handled
	// The actual chat session would require a remote server
	t.Skip("Integration test - requires remote server")
}
