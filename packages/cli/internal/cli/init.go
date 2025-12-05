package cli

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/handbook"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/onemcp/cli/internal/project"
	"github.com/onemcp/cli/internal/server"
	"github.com/onemcp/cli/internal/state"
	"github.com/spf13/cobra"
)

var (
	serverURL   string
	modelType   string
	modelAPIKey string
	tcpPort     int

	initCmd = &cobra.Command{
		Use:   "init [target_directory]",
		Short: "Initialize a new OneMCP project",
		Long: `
Initialize a new OneMCP project in the specified directory.
If no directory is specified, the current directory is used.

For local mode, you can specify the LLM model and API key:
  --model=OPENAI --model-apikey=sk-...
  --model=GEMINI --model-apikey=...
  --model=ANTHROPIC --model-apikey=...
  --model=OLLAMA (no API key needed)

If model flags are not provided, interactive mode will prompt for them.

If local mode, you can also specify a TCP port where the container will listen:

Use --server flag to initialize a remote project (model flags not applicable).`,
		Args: cobra.MaximumNArgs(1),
		RunE: runInit,
	}
)

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().StringVar(&serverURL, "server", "", "Remote server URL for remote mode initialization")
	initCmd.Flags().StringVar(&modelType, "model", "", "LLM model to use (OPENAI, GEMINI, ANTHROPIC, OLLAMA)")
	initCmd.Flags().StringVar(&modelAPIKey, "model-apikey", "", "API key for the selected model")
	initCmd.Flags().IntVar(&tcpPort, "tcp-port", 8080, "Tcp Port when running local mode")
}

func runInit(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Determine target directory with priority:
	// 1. Positional argument (if provided)
	// 2. --project-dir flag (if provided)
	// 3. Current directory (default)
	var targetDir string
	if len(args) > 0 {
		targetDir = args[0]
	} else if projectDir != "" {
		targetDir = projectDir
	} else {
		// Will be resolved to the current directory by Initialize
		targetDir = ""
	}

	// Determine if this is a remote mode
	isRemoteMode := serverURL != ""
	mode := ModeLocal
	if !isRemoteMode {
		var err error
		mode, err = selectMode(mode)
		if err != nil {
			return errors.NewGenericError("could not select execution mode properly", err)
		}

		if mode == ModeRemote {
			isRemoteMode = true
			serverURL, err = provideInput("Provide OneMCP endpoint URL:", "http://localhost:8080")
			if err != nil {
				return errors.NewGenericError("could not retrieve server url", err)
			}
		} else {
			tcpPort, err = selectTcpPort(tcpPort)
			if err != nil {
				return errors.NewGenericError("could not select tcp port", err)
			}
		}
	}

	// Handle model configuration for local mode
	var model interfaces.LLMModel
	var apiKey string
	interactive := false

	if !isRemoteMode {
		// Local mode - handle model configuration
		if modelType == "" {
			// Interactive mode
			interactive = true
			var err error
			model, apiKey, err = promptForModelConfig(cmd)
			if err != nil {
				return err
			}
		} else {
			// Explicit mode
			var err error
			model, err = parseModelType(modelType)
			if err != nil {
				return err
			}

			// Validate API key for models that require it
			if requiresAPIKey(model) {
				if modelAPIKey == "" {
					return errors.NewValidationError("--model-apikey is required for " + string(model))
				}
				apiKey = modelAPIKey
			}
		}
	}

	// Create managers
	projectMgr := getProjectManager()
	serverMgr := getServerManager()
	stateMgr := getStateManager()
	handbookMgr := getHandbookManager(stateMgr)

	SetInitMode := func() interfaces.InitMode {
		if mode == ModeLocal {
			return interfaces.InitModeLocal
		}
		return interfaces.InitModeRemote
	}

	// Prepare initialization options
	opts := interfaces.InitOptions{
		TargetDirectory: targetDir,
		ServerURL:       serverURL,
		Model:           model,
		ModelAPIKey:     apiKey,
		Interactive:     interactive,
		ServerManager:   serverMgr,
		HandbookManager: handbookMgr,
		StateManager:    stateMgr,
		InitMode:        SetInitMode(),
		TcpPort:         tcpPort,
	}

	// Initialize project
	if err := projectMgr.Initialize(ctx, opts); err != nil {
		return err
	}

	if mode == ModeRemote {
		handbookDir := filepath.Join(targetDir, "handbook")
		if err := handbookMgr.Pull(ctx, serverURL, handbookDir); err != nil {
			return errors.NewSyncError("failed to pull current handbook in server", err)
		}
	} else {
		handbookDir := filepath.Join(targetDir, "handbook")
		if err := handbookMgr.InstallAcmeTemplate(handbookDir); err != nil {
			return errors.NewSyncError("failed to install acme handbook", err)
		}
	}

	// Success message
	if targetDir == "" {
		var err error
		targetDir, err = getCurrentDir()
		if err != nil {
			targetDir = "{current_dir}"
		}
	}

	cmd.Println()
	cmd.Printf("✅ Successfully initialized %s project in %s\n", defaultShortLabel(mode), targetDir)
	if !isRemoteMode && model != interfaces.ModelUnset {
		cmd.Printf("Configured with %s model\n", model)
	}

	mcpEndpoint := strings.TrimSuffix(serverURL, "/") + "/mcp"
	cmd.Printf("Handbook directory is located at: %s/handbook/\n", targetDir)
	cmd.Println("📝 Next Steps (Provide Handbook Content):")
	cmd.Printf("   1). Add OpenAPI definitions to: %s/handbook/apis/\n", targetDir)
	cmd.Println("       OneMCP supports OpenAPI 3.0.x and 3.1.x, in both YAML and JSON.")
	cmd.Println()
	cmd.Printf("   2). Add documentation to: %s/handbook/docs/\n", targetDir)
	cmd.Println("       Express any file structure you need, using Markdown. OneMCP will recursively load all *.md files in this directory.")
	cmd.Println()
	cmd.Println("   3). Push your handbook: onemcp handbook push")
	cmd.Println("       Once Handbook is complete, push to OneMCP. Its content will be:")
	cmd.Println("       - Validate: Assert that your handbook is valid and can be served by OneMCP.")
	cmd.Println("       - Indexed: Content will be ingested and populated into our Graph Contextual Search Engine.")
	cmd.Println("       - Tested (Optional): In case regression-suite was configured, OneMCP will run tests against your handbook.")
	cmd.Println()
	cmd.Println("   4. Start chatting: onemcp chat")
	cmd.Println("       Use our integrated interactive chat to send requests to OneMCP.")
	cmd.Printf("       Use your preferred MCP Client and connect with OneMCP Server URL: %s\n", mcpEndpoint)
	cmd.Println()
	return nil
}

// Helper functions to get manager instances
func getProjectManager() interfaces.ProjectManager {
	return project.NewManager()
}

func getServerManager() interfaces.ServerManager {
	mgr, err := server.NewManager()
	if err != nil {
		// In a real scenario, we'd handle this error properly
		// For now, return nil and let the caller handle it
		return nil
	}
	return mgr
}

func getHandbookManager(stateMgr interfaces.StateManager) interfaces.HandbookManager {
	return handbook.NewManager(stateMgr)
}

func getStateManager() interfaces.StateManager {
	return state.NewManager()
}

// promptForModelConfig prompts the user to select a model and provide API key
func promptForModelConfig(cmd *cobra.Command) (interfaces.LLMModel, string, error) {
	modelInput, err := selectOption("Which model should be used? Here is the list of models currently supported by OneMCP:",
		[]string{"OpenAI", "Gemini", "Anthropic", "Ollama"},
		"OpenAI")
	if err != nil {
		return interfaces.ModelUnset, "", errors.NewGenericError("failed to read model input", err)
	}

	modelInput = strings.TrimSpace(strings.ToUpper(modelInput))
	model, err := parseModelType(modelInput)
	if err != nil {
		return interfaces.ModelUnset, "", err
	}

	// Prompt for API key if needed
	var apiKey string
	if requiresAPIKey(model) {
		apiKey, err = provideSensitiveInput("Enter the API Key for " + string(model) + ": ")
		if err != nil {
			return interfaces.ModelUnset, "", errors.NewGenericError("failed to read API key input", err)
		}
		apiKey = strings.TrimSpace(apiKey)
	}

	return model, apiKey, nil
}

// parseModelType converts a string to LLMModel type
func parseModelType(modelStr string) (interfaces.LLMModel, error) {
	modelStr = strings.TrimSpace(strings.ToUpper(modelStr))

	switch modelStr {
	case "OPENAI":
		return interfaces.ModelOpenAI, nil
	case "GEMINI":
		return interfaces.ModelGemini, nil
	case "ANTHROPIC":
		return interfaces.ModelAnthropic, nil
	case "OLLAMA":
		return interfaces.ModelOllama, nil
	default:
		return interfaces.ModelUnset, errors.NewValidationError(
			fmt.Sprintf("invalid model type: %s. Must be one of: OPENAI, GEMINI, ANTHROPIC, OLLAMA", modelStr),
		)
	}
}

// requiresAPIKey checks if a model requires an API key
func requiresAPIKey(model interfaces.LLMModel) bool {
	switch model {
	case interfaces.ModelOpenAI, interfaces.ModelGemini, interfaces.ModelAnthropic:
		return true
	case interfaces.ModelOllama:
		return false
	default:
		return false
	}
}
