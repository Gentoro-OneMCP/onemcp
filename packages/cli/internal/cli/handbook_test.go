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

// Helper function to create a test project with handbook
func createTestProjectWithHandbook(t *testing.T, mode interfaces.ProjectMode) string {
	tmpDir, err := os.MkdirTemp("", "onemcp-handbook-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create .onemcp directory
	onemcpDir := filepath.Join(tmpDir, ".onemcp")
	if err := os.MkdirAll(onemcpDir, 0755); err != nil {
		t.Fatalf("Failed to create .onemcp dir: %v", err)
	}

	// Create handbook directory structure
	handbookDir := filepath.Join(tmpDir, "handbook")
	dirs := []string{
		filepath.Join(handbookDir, "docs"),
		filepath.Join(handbookDir, "apis"),
		filepath.Join(handbookDir, "regression-suite"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create handbook dir: %v", err)
		}
	}

	// Create Agent.yaml
	agentYaml := filepath.Join(handbookDir, "Agent.yaml")
	if err := os.WriteFile(agentYaml, []byte("name: test-agent\n"), 0644); err != nil {
		t.Fatalf("Failed to create Agent.yaml: %v", err)
	}

	// Create config file
	config := interfaces.ProjectConfig{
		Version: 1,
		Server: interfaces.ServerConfig{
			Mode:            mode,
			DockerContainer: "test-container",
			Port:            8080,
		},
		Handbook: interfaces.HandbookConfig{
			Source: "acme",
		},
	}

	if mode == interfaces.ModeRemote {
		config.Server.URL = "http://remote-server:8080"
		config.Handbook.Source = "remote"
	}

	configPath := filepath.Join(onemcpDir, "onemcp.yaml")
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	return tmpDir
}

// Test handbook pull command outside project context
func TestHandbookPullCommand_NoProjectContext(t *testing.T) {
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
	err = runHandbookPull(cmd, []string{})

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

// Test handbook push command outside project context
func TestHandbookPushCommand_NoProjectContext(t *testing.T) {
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
	err = runHandbookPush(cmd, []string{})

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

// Test handbook acme command outside project context
func TestHandbookAcmeCommand_NoProjectContext(t *testing.T) {
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
	err = runHandbookAcme(cmd, []string{})

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

// Test handbook pull command in local mode (integration test)
func TestHandbookPullCommand_LocalMode(t *testing.T) {
	projectDir := createTestProjectWithHandbook(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This is an integration test that requires Docker and a running server
	t.Skip("Integration test - requires Docker and running server")
}

// Test handbook push command in local mode (integration test)
func TestHandbookPushCommand_LocalMode(t *testing.T) {
	projectDir := createTestProjectWithHandbook(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This is an integration test that requires Docker and a running server
	t.Skip("Integration test - requires Docker and running server")
}

// Test handbook acme command with confirmation
func TestHandbookAcmeCommand_WithConfirmation(t *testing.T) {
	projectDir := createTestProjectWithHandbook(t, interfaces.ModeLocal)
	defer os.RemoveAll(projectDir)

	// Change to project directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(projectDir)

	// This test requires user input simulation
	// The actual acme reset logic is tested in the handbook manager tests
	t.Skip("Integration test - requires user input simulation")
}

// Test context validation across all handbook subcommands
func TestHandbookCommand_ContextValidation(t *testing.T) {
	tests := []struct {
		name    string
		runFunc func(*cobra.Command, []string) error
	}{
		{"pull", runHandbookPull},
		{"push", runHandbookPush},
		{"acme", runHandbookAcme},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			err = tt.runFunc(cmd, []string{})

			// Should fail with context error
			if err == nil {
				t.Fatalf("Expected error for %s command, got nil", tt.name)
			}

			cliErr, ok := err.(*errors.CLIError)
			if !ok {
				t.Fatalf("Expected CLIError for %s, got %T", tt.name, err)
			}

			if cliErr.Code != errors.CodeNotInProject {
				t.Errorf("Expected error code %d for %s, got %d", errors.CodeNotInProject, tt.name, cliErr.Code)
			}
		})
	}
}

// Test that handbook commands work from subdirectories
func TestHandbookCommand_FromSubdirectory(t *testing.T) {
	projectDir := createTestProjectWithHandbook(t, interfaces.ModeLocal)
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

	// Run acme command (doesn't require server)
	// This should find the project root and load config successfully
	// It will fail at user input, but that's expected
	err := runHandbookAcme(cmd, []string{})

	// We expect either success (if input simulation works) or a different error
	// Should NOT be a context error
	if err != nil {
		cliErr, ok := err.(*errors.CLIError)
		if ok && cliErr.Code == errors.CodeNotInProject {
			t.Error("Should find project context from subdirectory")
		}
	}
}

// Test error handling for malformed config
func TestHandbookCommand_MalformedConfig(t *testing.T) {
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
	err = runHandbookPull(cmd, []string{})

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
