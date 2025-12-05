package cli

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/onemcp/cli/internal/chat"
	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/spf13/cobra"
)

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Start interactive chat session with MCP server",
	Long:  `Launch an interactive terminal REPL session to communicate with the MCP server.`,
	RunE:  runChat,
}

func init() {
	rootCmd.AddCommand(chatCmd)
}

func runChat(cmd *cobra.Command, args []string) error {
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

	// Create log file path with timestamp
	timestamp := time.Now().Format("20060102-150405")
	logPath := filepath.Join(projectRoot, "logs", fmt.Sprintf("chat-%s.log", timestamp))

	// Start chat session
	chatMgr := getChatManager(config, projectDir)
	if err := chatMgr.Start(ctx, serverURL, logPath); err != nil {
		return errors.NewGenericError("failed to start chat session", err)
	}

	return nil
}

// getChatManager returns a new chat manager instance
func getChatManager(config *interfaces.ProjectConfig, projectDir string) interfaces.ChatManager {
	return chat.NewManager(config, projectDir)
}
