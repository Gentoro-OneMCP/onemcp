package cli

import (
	"fmt"
	"path/filepath"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/spf13/cobra"
)

var handbookCmd = &cobra.Command{
	Use:   "handbook",
	Short: "Manage project handbook",
	Long:  `Pull, push, or reset the project handbook.`,
}

var handbookPullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Pull handbook from server",
	RunE:  runHandbookPull,
}

var handbookPushCmd = &cobra.Command{
	Use:   "push",
	Short: "Push handbook to server",
	RunE:  runHandbookPush,
}

var handbookAcmeCmd = &cobra.Command{
	Use:   "acme",
	Short: "Reset handbook to Acme template",
	RunE:  runHandbookAcme,
}

func init() {
	rootCmd.AddCommand(handbookCmd)
	handbookCmd.AddCommand(handbookPullCmd)
	handbookCmd.AddCommand(handbookPushCmd)
	handbookCmd.AddCommand(handbookAcmeCmd)
}

func runHandbookPull(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Resolve project directory
	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	// Validate project context
	projectMgr := getProjectManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err
	}

	// Load project configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Auto-start local server if stopped
	serverMgr := getServerManager()
	if err := serverMgr.AutoStart(ctx, config.Server); err != nil {
		return errors.NewServerError("failed to auto-start server", err)
	}

	// Determine server URL
	serverURL := config.Server.URL
	if config.Server.Mode == interfaces.ModeLocal {
		serverURL = fmt.Sprintf("http://localhost:%d", config.Server.Port)
	}

	stateMgr := getStateManager()
	dbPath := fmt.Sprintf("%s/.onemcp/state.db", projectRoot)
	if err := stateMgr.Initialize(dbPath); err != nil {
		return errors.NewGenericError("Could not initialize state management", nil)
	} else {
		defer stateMgr.Close()
		stateMgr.Initialize(dbPath)
	}

	// Fetch handbook from server
	handbookMgr := getHandbookManager(stateMgr)
	handbookDir := filepath.Join(projectRoot, "handbook")

	if err := handbookMgr.Pull(ctx, serverURL, handbookDir); err != nil {
		return errors.NewSyncError("failed to pull handbook", err)
	}

	cmd.Println("Successfully pulled handbook from server")
	return nil
}

func runHandbookPush(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Resolve project directory
	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	// Validate project context
	projectMgr := getProjectManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err
	}

	// Load project configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Auto-start local server if stopped
	serverMgr := getServerManager()
	if err := serverMgr.AutoStart(ctx, config.Server); err != nil {
		return errors.NewServerError("failed to auto-start server", err)
	}

	// Determine server URL
	serverURL := config.Server.URL
	if config.Server.Mode == interfaces.ModeLocal {
		serverURL = fmt.Sprintf("http://localhost:%d", config.Server.Port)
	}

	stateMgr := getStateManager()
	dbPath := fmt.Sprintf("%s/.onemcp/state.db", projectRoot)
	if err := stateMgr.Initialize(dbPath); err != nil {
		return errors.NewGenericError("Could not initialize state management", nil)
	} else {
		defer stateMgr.Close()
		stateMgr.Initialize(dbPath)
	}

	// Upload handbook to server
	handbookMgr := getHandbookManager(stateMgr)
	handbookDir := filepath.Join(projectRoot, "handbook")

	if err := handbookMgr.Push(ctx, serverURL, handbookDir); err != nil {
		return errors.NewSyncError("failed to push handbook", err)
	}

	cmd.Println("Successfully pushed handbook to server")
	return nil
}

func runHandbookAcme(cmd *cobra.Command, args []string) error {
	// Resolve project directory
	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	// Validate project context
	projectMgr := getProjectManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err
	}

	// Prompt user for confirmation
	cmd.Print("This will overwrite your current handbook with the Acme default. Continue? [y/N]: ")

	var confirmation string
	if _, err := fmt.Scanln(&confirmation); err != nil {
		// If there's an error reading (e.g., EOF), treat as "n"
		confirmation = "n"
	}

	stateMgr := getStateManager()
	dbPath := fmt.Sprintf("%s/.onemcp/state.db", projectRoot)
	if err := stateMgr.Initialize(dbPath); err != nil {
		return errors.NewGenericError("Could not initialize state management", nil)
	} else {
		defer stateMgr.Close()
		stateMgr.Initialize(dbPath)
	}

	// Reset to Acme template with confirmation
	handbookMgr := getHandbookManager(stateMgr)
	handbookDir := filepath.Join(projectRoot, "handbook")

	if err := handbookMgr.ResetToAcme(handbookDir, confirmation); err != nil {
		// Check if user cancelled
		if confirmation != "y" {
			cmd.Println("Operation cancelled")
			return nil
		}
		return errors.NewGenericError("failed to reset handbook to Acme template", err)
	}

	cmd.Println("Successfully reset handbook to Acme template")
	return nil
}
