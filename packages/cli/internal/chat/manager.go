package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/handbook"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/onemcp/cli/internal/project"
)

// Manager implements the ChatManager interface
type Manager struct {
	projectDir    string
	projectConfig *interfaces.ProjectConfig
	serverURL     string
	logPath       string
	logFile       *os.File
	session       *mcp.ClientSession
	progressChan  chan *mcp.ProgressNotificationParams
	uiStop        chan struct{}
}

// NewManager creates a new chat manager
func NewManager(projectConfig *interfaces.ProjectConfig, projectDir string) *Manager {
	return &Manager{
		projectDir:    projectDir,
		projectConfig: projectConfig,
		progressChan:  make(chan *mcp.ProgressNotificationParams, 20),
		uiStop:        make(chan struct{}),
	}
}

// OneMCPResponse represents the structured response from OneMCP server
type OneMCPResponse struct {
	Parts []struct {
		IsSupported bool   `json:"isSupported"`
		Assignment  string `json:"assignment"`
		IsError     bool   `json:"isError"`
		Content     string `json:"content,omitempty"`
	} `json:"parts"`
	Statistics struct {
		PromptTokens     int      `json:"promptTokens"`
		CompletionTokens int      `json:"completionTokens"`
		TotalTokens      int      `json:"totalTokens"`
		TotalTimeMs      int      `json:"totalTimeMs"`
		Operations       []string `json:"operations"`
	} `json:"statistics"`
}

// Start initializes the chat session with readline interface
func (m *Manager) Start(ctx context.Context, serverURL string, logPath string) error {
	// Ensure we always have a valid context (tests may pass nil)
	if ctx == nil {
		ctx = context.Background()
	}
	m.serverURL = serverURL
	m.logPath = logPath

	// Create log directory if it doesn't exist
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return errors.NewGenericError("failed to create log directory", err)
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return errors.NewGenericError("failed to open log file", err)
	}
	m.logFile = logFile

	// Log session start
	m.logToFile(fmt.Sprintf("=== Chat session started at %s ===\n", time.Now().Format(time.RFC3339)))
	m.logToFile(fmt.Sprintf("Server URL: %s\n\n", serverURL))

	// Run readline-based chat interface
	if err := m.runReadlineMode(ctx); err != nil {
		m.Close()
		return err
	}

	return nil
}

// Register MCP Progress Handler
func (m *Manager) handleProgress(ctx context.Context, req *mcp.ProgressNotificationClientRequest) {
	if req == nil || req.Params == nil {
		return
	}
	m.progressChan <- req.Params
}

// initMCPSession initializes the MCP client session
func (m *Manager) initMCPSession(ctx context.Context) error {
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "onemcp-cli",
		Version: "0.1.0",
	}, &mcp.ClientOptions{
		KeepAlive:                   5 * time.Second,
		ProgressNotificationHandler: m.handleProgress,
	})
	// Construct MCP endpoint (serverURL + /mcp)
	mcpEndpoint := strings.TrimSuffix(m.serverURL, "/") + "/mcp"

	// Use the built-in StreamableClientTransport
	transport := &mcp.StreamableClientTransport{
		Endpoint: mcpEndpoint,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DisableCompression: true,
			},
		},
	}

	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return err
	}

	m.session = session
	return nil
}

// SendMessage logs the message (kept for interface compatibility)
// In the new MCP implementation, actual sending happens in sendAndReceive
func (m *Manager) SendMessage(message string) error {
	if message == "" {
		return nil
	}

	// Log user message
	m.logToFile(fmt.Sprintf("[%s] User: %s\n", time.Now().Format("15:04:05"), message))

	return nil
}

// ReceiveResponse is kept for interface compatibility
// In the new MCP implementation, actual communication happens in sendAndReceive
func (m *Manager) ReceiveResponse() (string, error) {
	return "", nil
}

//
// ──────────────────────────────────────────────────────────────────────
// UI Renderer (Spinner + Progress Bar)
// ──────────────────────────────────────────────────────────────────────
//

// Print progress bar
func (m *Manager) renderProgress(p *mcp.ProgressNotificationParams) {
	pct := p.Progress
	total := p.Total

	var percent float64
	if total > 0 {
		percent = pct / total * 100
	} else {
		percent = pct
	}

	barWidth := 40
	filled := int((percent / 100) * float64(barWidth))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	fmt.Printf("[%s] %3.0f%%  %s", bar, percent, p.Message)
}

// Unified renderer goroutine
func (m *Manager) startUIRenderer() {
	go func() {
		spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		si := 0
		var lastProgress *mcp.ProgressNotificationParams

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-m.uiStop:
				return

			case p := <-m.progressChan:
				lastProgress = p

			case <-ticker.C:
				// Start with spinner + Thinking...
				spinnerPart := fmt.Sprintf("Agent: %s Thinking...", spinner[si%len(spinner)])
				si++

				line := spinnerPart

				// Add progress bar if available
				if lastProgress != nil {
					line += "   " + renderUnifiedProgress(lastProgress)
				}

				// Clean line then print the unified status
				fmt.Print("\r" + strings.Repeat(" ", 120) + "\r")
				fmt.Print(line)
			}
		}
	}()
}

func renderUnifiedProgress(p *mcp.ProgressNotificationParams) string {
	pct := p.Progress
	total := p.Total

	var percent float64
	if total > 0 {
		percent = (pct / total) * 100
	} else {
		percent = pct
	}

	barWidth := 22
	filled := int((percent / 100) * float64(barWidth))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	// Show: [██████░░░░░░]  42%  message
	return fmt.Sprintf("[%s] %3.0f%%  %s", bar, percent, p.Message)
}

// sendAndReceive sends a message to the MCP server using the onemcp.run tool
func (m *Manager) sendAndReceive(message string) (string, string, error) {

	// 1. Long-lived session
	if m.session == nil {
		sessionCtx := context.Background() // stays alive for the entire CLI
		if err := m.initMCPSession(sessionCtx); err != nil {
			return "", "", errors.NewGenericError("failed to initialize MCP session", err)
		}
	}

	// Log user message
	m.logToFile(fmt.Sprintf("[%s] User: %s\n", time.Now().Format("15:04:05"), message))

	// 2. Short-lived tool call context
	toolCtx, toolCancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer toolCancel()

	requestContext := "{}"

	// Path to Agent.yaml
	agentYamlFile := m.projectDir + "/handbook/Agent.yaml"

	// Load Agent.yaml only if it exists
	if _, err := os.Stat(agentYamlFile); err == nil {

		agentYamlConfig, err := project.LoadConfig(agentYamlFile)
		if err != nil {
			return "", "", errors.NewGenericError("failed to load Agent.yaml", err)
		}

		// Only proceed if APIs are defined
		if len(agentYamlConfig.APIs) > 0 {

			// Path to context.yaml
			contextFile := m.projectDir + "/.onemcp/context.yaml"

			// Load context.yaml only if it exists
			if _, err := os.Stat(contextFile); err == nil {

				projectContext, err := project.LoadContext(contextFile)
				if err != nil {
					return "", "", errors.NewGenericError("failed to load .onemcp/context.yaml", err)
				}

				// Match API→context entries
				apiContexts := projectContext.GetContextsForAPIs(agentYamlConfig.APIs)
				if len(apiContexts) > 0 {

					requestContext, err = BuildContextJSON(apiContexts)
					if err != nil {
						return "", "", errors.NewGenericError("unable to marshal context", err)
					}
				}
			}
		}
	}

	// Generate progress token
	token := fmt.Sprintf("progress-%d", time.Now().UnixNano())
	if strings.TrimSpace(requestContext) == "" {
		requestContext = "{}"
	}

	result, err := m.session.CallTool(toolCtx, &mcp.CallToolParams{
		Meta: mcp.Meta{
			"progressToken": token,
		},
		Name: "onemcp.run",
		Arguments: map[string]interface{}{
			"prompt":  message,
			"context": requestContext,
		},
	})

	if err != nil {
		// Check if timeout occurred
		if toolCtx.Err() == context.DeadlineExceeded {
			// Reset session on timeout to prevent server-side resource conflicts
			m.session = nil
			return "", "", errors.NewGenericError("request timed out after 240 seconds - session reset for next query", nil)
		}
		return "", "", errors.NewGenericError("failed to call tool", err)
	}

	if result.IsError {
		return "", "", errors.NewGenericError("tool execution error", nil)
	}

	// Extract response from tool result
	responseText, reportFile, err := m.extractResponse(result)
	if err != nil {
		return "", "", errors.NewGenericError("failed to extract response from tool result", err)
	}

	// Log server response
	if responseText != "" {
		m.logToFile(fmt.Sprintf("[%s] Server: %s\n", time.Now().Format("15:04:05"), responseText))
		m.logToFile(fmt.Sprintf("[%s] Execution report: %s\n", time.Now().Format("15:04:05"), reportFile))
	}

	return responseText, reportFile, nil
}

// extractResponse extracts the response text from MCP tool result
func (m *Manager) extractResponse(result *mcp.CallToolResult) (string, string, error) {
	fileName := fmt.Sprintf("report-%d.txt", time.Now().UnixNano())
	filePath, err := filepath.Abs(filepath.Join(m.logPath, "..", "..", "reports", fileName))
	if err != nil {
		return "", "", errors.NewGenericError("failed to define log file path", err)
	}
	file, _ := os.Create(filePath)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}(file)

	for _, content := range result.Content {
		if tc, ok := content.(*mcp.TextContent); ok {
			// Try to parse as OneMCP structured response
			var resp MCPResponse
			if err := json.Unmarshal([]byte(tc.Text), &resp); err == nil {

				PrettyPrintTrace(file, resp.Statistics.Trace)
				// Extract only the supported, non-error content
				var results []string
				for _, part := range resp.Parts {
					if part.IsSupported && !part.IsError && part.Content != "" {
						results = append(results, part.Content)
					}
				}
				if len(results) > 0 {
					return strings.Join(results, "\n"), filePath, nil
				}
			}
			// Fallback: return raw text if JSON parsing fails
			return tc.Text, filePath, nil
		}
	}
	return "", filePath, nil
}

// Close closes the chat session and log file
func (m *Manager) Close() error {
	// Close MCP session if it exists
	if m.session != nil {
		// Note: The MCP SDK doesn't expose a Close method on ClientSession
		// The connection will be cleaned up when the session is garbage collected
		m.session = nil
	}

	if m.logFile != nil {
		m.logToFile(fmt.Sprintf("\n=== Chat session ended at %s ===\n", time.Now().Format(time.RFC3339)))
		return m.logFile.Close()
	}
	return nil
}

// logToFile writes a message to the log file
func (m *Manager) logToFile(message string) {
	if m.logFile != nil {
		m.logFile.WriteString(message)
	}
}

// runReadlineMode runs the chat interface with readline support
func (m *Manager) runReadlineMode(ctx context.Context) error {
	// Configure readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     filepath.Join(os.TempDir(), ".onemcp_history"),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return errors.NewGenericError("failed to initialize readline", err)
	}
	defer rl.Close()

	ModeInfo := func() string {
		if m.projectConfig.Server.Mode == interfaces.ModeRemote {
			return fmt.Sprintf("Remote (URL: %s)", m.projectConfig.Server.URL)
		} else {
			return fmt.Sprintf("Local (Container ID: %s)", m.projectConfig.Server.DockerContainer)
		}
	}

	ModelInfo := func() string {
		if m.projectConfig.Server.Model == "" {
			return "UnSet"
		} else {
			if m.projectConfig.Server.ModelAPIKey != "" {
				return fmt.Sprintf("%s (API Key: %s)", m.projectConfig.Server.Model, m.projectConfig.Server.ModelAPIKey)
			} else {
				return fmt.Sprintf("%s", m.projectConfig.Server.Model)
			}
		}
	}

	regressionTests, err := handbook.LoadProjectRegressionSuite(m.projectDir)
	if err != nil {
		return err
	}

	// Clear screen and show header with new handbook
	fmt.Print("\033[H\033[2J")
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║    Gentoro OneMCP - Chat Mode        ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Server:   %s\n", m.serverURL)
	fmt.Printf("Handbook: %s/handbook\n", m.projectDir)
	fmt.Printf("Mode:     %s\n", ModeInfo())
	fmt.Printf("Model:    %s\n", ModelInfo())

	if len(regressionTests) > 0 {
		fmt.Println()
		fmt.Println("Here are some examples:")

		maxNum := 5
		if len(regressionTests) < maxNum {
			maxNum = len(regressionTests)
		}

		for _, test := range regressionTests[:maxNum] {
			fmt.Printf(" > %s\n", test.Prompt)
		}
	}

	fmt.Println()
	fmt.Println("Type your messages and press Enter to send.")
	fmt.Println("Press Ctrl+C or type 'exit' to quit.")
	fmt.Println()

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Read input with readline
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				// Ctrl+C
				fmt.Println("\nGoodbye!")
				return nil
			} else if err == io.EOF {
				// Ctrl+D or EOF
				fmt.Println("\nGoodbye!")
				return nil
			}
			return errors.NewGenericError("error reading input", err)
		}

		message := strings.TrimSpace(line)
		if message == "" {
			continue
		}

		// Handle exit command
		if message == "exit" || message == "quit" {
			fmt.Println("Goodbye!")
			return nil
		}

		// Start spinner+progress renderer
		m.uiStop = make(chan struct{})
		m.startUIRenderer()

		// Send and receive
		response, reportFile, err := m.sendAndReceive(message)

		// Stop UI renderer and clear thinking/progress line
		close(m.uiStop)
		time.Sleep(50 * time.Millisecond)
		fmt.Print("\r" + strings.Repeat(" ", 80) + "\r")

		if err != nil {
			fmt.Printf("Error: %v\n\n", err)
			continue
		}

		if response != "" {
			fmt.Printf("\n%s\n\n", response)
		}
		if reportFile != "" {
			fmt.Printf("\nExecution report: %s\n\n", reportFile)
		}
	}
}
