package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/handbook"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/onemcp/cli/internal/project"
	"github.com/onemcp/cli/internal/server"
	"github.com/onemcp/cli/internal/state"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Manage local MCP server",
	Long:  `Start, stop, or check the status of the local MCP server.`,
}

var serverStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the local MCP server",
	RunE:  runServerStart,
}

var serverStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the local MCP server",
	RunE:  runServerStop,
}

var serverStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check the status of the local MCP server",
	RunE:  runServerStatus,
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverStartCmd)
	serverCmd.AddCommand(serverStopCmd)
	serverCmd.AddCommand(serverStatusCmd)
}

func runServerStart(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	projectMgr := project.NewManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err // Already a context error
	}

	// Load configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Check for local mode
	if config.Server.Mode != interfaces.ModeLocal {
		return errors.NewServerError("server commands are only available for local deployments", nil)
	}

	// Create server manager
	serverMgr, err := server.NewManager()
	if err != nil {
		return errors.NewServerError("failed to create server manager", err)
	}
	defer serverMgr.Close()

	// Start the server
	cmd.Println("Starting local MCP server...")
	if err := serverMgr.Start(ctx, config.Server); err != nil {
		return err
	}

	cmd.Printf("Server started successfully: %s\n", config.Server.DockerContainer)
	cmd.Printf("Server is running on port %d\n", config.Server.Port)

	// Initialize state manager
	stateMgr := state.NewManager()
	dbPath := fmt.Sprintf("%s/.onemcp/state.json", projectRoot)
	if err := stateMgr.Initialize(dbPath); err != nil {
		cmd.PrintErrf("Warning: failed to initialize state database: %v\n", err)
	} else {
		defer stateMgr.Close()

		// Create handbook manager and sync if needed
		handbookMgr := handbook.NewManager(stateMgr)
		handbookDir := filepath.Join(projectRoot, "handbook")
		serverURL := fmt.Sprintf("http://localhost:%d", config.Server.Port)

		cmd.Println("Checking handbook sync status...")
		if err := handbookMgr.SyncIfNeeded(ctx, serverURL, handbookDir); err != nil {
			cmd.PrintErrf("Warning: failed to sync handbook: %v\n", err)
		} else {
			cmd.Println("Handbook is in sync with server")
		}
	}

	return nil
}

func runServerStop(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	projectMgr := project.NewManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err // Already a context error
	}

	// Load configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Check for local mode
	if config.Server.Mode != interfaces.ModeLocal {
		return errors.NewServerError("server commands are only available for local deployments", nil)
	}

	// Create server manager
	serverMgr, err := server.NewManager()
	if err != nil {
		return errors.NewServerError("failed to create server manager", err)
	}
	defer serverMgr.Close()

	// Stop the server
	cmd.Println("Stopping local MCP server...")
	if err := serverMgr.Stop(ctx, config.Server.DockerContainer); err != nil {
		return err
	}

	// Update state database
	stateMgr := state.NewManager()
	dbPath := fmt.Sprintf("%s/.onemcp/state.json", projectRoot)
	if err := stateMgr.Initialize(dbPath); err != nil {
		// Log warning but don't fail the command
		cmd.PrintErrf("Warning: failed to update state database: %v\n", err)
	} else {
		defer stateMgr.Close()
		status := interfaces.ServerStatus{
			Running:       false,
			Healthy:       false,
			ContainerName: config.Server.DockerContainer,
		}
		if err := stateMgr.UpdateServerState(status); err != nil {
			cmd.PrintErrf("Warning: failed to update server state: %v\n", err)
		}
	}

	cmd.Printf("Server stopped: %s\n", config.Server.DockerContainer)

	return nil
}

func runServerStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	projectMgr := project.NewManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err // Already a context error
	}

	// Load configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Check for local mode
	if config.Server.Mode != interfaces.ModeLocal {
		return errors.NewServerError("server commands are only available for local deployments", nil)
	}

	// Create server manager
	serverMgr, err := server.NewManager()
	if err != nil {
		return errors.NewServerError("failed to create server manager", err)
	}
	defer serverMgr.Close()

	// Get server status
	status, err := serverMgr.Status(ctx, config.Server.DockerContainer)
	if err != nil {
		return err
	}

	// Display status
	cmd.Printf("Container: %s\n", status.ContainerName)
	if status.Running {
		cmd.Println("Status: Running")
		if status.Healthy {
			cmd.Println("Health: Healthy")
		} else {
			cmd.Println("Health: Unhealthy")
		}
	} else {
		cmd.Println("Status: Stopped")
	}

	return nil
}
