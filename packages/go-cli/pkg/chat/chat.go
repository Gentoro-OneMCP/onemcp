package chat

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/chzyer/readline"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/gentoro/onemcp/go-cli/pkg/handbook"
	"github.com/gentoro/onemcp/go-cli/pkg/mcp"
)

type Mode struct {
	client *mcp.Client
	config *config.GlobalConfig
}

func NewMode(cfg *config.GlobalConfig) *Mode {
	baseURL := fmt.Sprintf("http://localhost:%d/mcp", cfg.DefaultPort)
	client := mcp.NewClient(baseURL)
	client.Config = cfg // Set config for timeout support
	return &Mode{
		client: client,
		config: cfg,
	}
}

func (m *Mode) Start() error {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║    Gentoro OneMCP - Chat Mode        ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Handbook: %s\n", m.config.CurrentHandbook)
	fmt.Printf("Provider: %s\n", m.config.Provider)
	fmt.Println("Type 'exit' to quit, 'clear' to clear history, 'switch' to change handbook")
	fmt.Println()

	// Initialize readline with history support
	rl, err := readline.New("You: ")
	if err != nil {
		return err
	}
	defer rl.Close()
	defer m.client.Close() // Ensure connection is properly closed

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt || err == io.EOF {
				fmt.Println("\n👋 Goodbye!")
				return nil
			}
			return err
		}

		input := strings.TrimSpace(line)
		if input == "" {
			continue
		}

		// Handle special commands
		switch strings.ToLower(input) {
		case "exit", "quit":
			fmt.Println("\n👋 Goodbye!")
			return nil

		case "help":
			m.showHelp()
			continue

		case "clear":
			// Clear screen and show header again
			fmt.Print("\033[H\033[2J")
			fmt.Println()
			fmt.Println("╔══════════════════════════════════════╗")
			fmt.Println("║    Gentoro OneMCP - Chat Mode        ║")
			fmt.Println("╚══════════════════════════════════════╝")
			fmt.Println()
			fmt.Printf("Handbook: %s\n", m.config.CurrentHandbook)
			fmt.Printf("Provider: %s\n", m.config.Provider)
			fmt.Println("🧹 Chat history cleared.")
			fmt.Println()
			continue

		case "switch":
			oldHandbook := m.config.CurrentHandbook
			newHandbook, err := m.switchHandbook()
			if err != nil {
				fmt.Printf("Failed to switch handbook: %v\n", err)
				fmt.Println()
				fmt.Println("💡 Continuing with current handbook...")
				fmt.Println()

				// Recreate MCP client after survey interaction (fixes terminal state)
				baseURL := fmt.Sprintf("http://localhost:%d/mcp", m.config.DefaultPort)
				m.client = mcp.NewClient(baseURL)
				m.client.Config = m.config

				continue // Don't do anything - keep current setup
			}

			// Only restart if handbook actually changed
			if newHandbook == oldHandbook {
				fmt.Println("✅ No change needed - already using this handbook")
				continue
			}

			// If switched to a different handbook, restart Docker container
			if newHandbook != m.config.CurrentHandbook {
				fmt.Println()
				fmt.Println("🔄 Restarting server with new handbook...")

				ctx := context.Background()

				// Create docker manager
				dockerMgr, err := docker.NewManager()
				if err != nil {
					fmt.Printf("❌ Failed to initialize Docker: %v\n", err)
					fmt.Println("   Please restart chat mode manually: ./onemcp chat")
					os.Exit(1)
				}

				// Stop current container
				if err := dockerMgr.StopServer(ctx); err != nil {
					fmt.Printf("⚠️  Warning: Failed to stop server: %v\n", err)
				}

				// Update config
				m.config.CurrentHandbook = newHandbook
				cm := config.GetManager()
				if err := cm.SaveGlobalConfig(m.config); err != nil {
					fmt.Printf("❌ Failed to save config: %v\n", err)
					os.Exit(1)
				}

				// Determine handbook path
				var handbookPath string
				if newHandbook == "acme-analytics" {
					handbookPath = "acme-analytics" // Built-in
				} else {
					handbookPath = cm.GetHandbookPath(newHandbook)
				}

				// Start new container with new handbook
				if err := dockerMgr.StartServer(ctx, m.config, handbookPath); err != nil {
					fmt.Printf("❌ Failed to start server with new handbook: %v\n", err)
					fmt.Println("   Please restart chat mode manually: ./onemcp chat")
					os.Exit(1)
				}

				// Wait for server to be ready
				fmt.Println("Waiting for server to be ready...")
				time.Sleep(3 * time.Second) // Give server time to start

				// Recreate MCP client to connect to new server instance
				baseURL := fmt.Sprintf("http://localhost:%d/mcp", m.config.DefaultPort)
				m.client = mcp.NewClient(baseURL)
				m.client.Config = m.config

				fmt.Println("✅ Server restarted successfully!")
				fmt.Printf("✅ Switched to handbook: %s\n", newHandbook)
				fmt.Printf("Provider: %s\n", m.config.Provider)
			}

			// Clear screen and show header with new handbook
			fmt.Print("\033[H\033[2J")
			fmt.Println()
			fmt.Println("╔══════════════════════════════════════╗")
			fmt.Println("║    Gentoro OneMCP - Chat Mode        ║")
			fmt.Println("╚══════════════════════════════════════╝")
			fmt.Println()
			fmt.Printf("Handbook: %s\n", m.config.CurrentHandbook)
			fmt.Printf("Provider: %s\n", m.config.Provider)
			fmt.Println("Chat history cleared.")
			fmt.Println()
			continue
		}
		// Run spinner in background
		done := make(chan bool)
		go func() {
			spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
			i := 0
			start := time.Now()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					elapsed := time.Since(start).Seconds()
					fmt.Print("\r" + strings.Repeat(" ", 80) + "\r")
					fmt.Printf("Agent: %s Thinking... (%.1fs)", spinner[i%len(spinner)], elapsed)
					i++
				}
			}
		}()

		// Send message
		response, err := m.client.SendMessage(input)

		// Stop spinner
		done <- true
		time.Sleep(50 * time.Millisecond) // Give spinner goroutine time to clean up

		// Clear thinking/progress line
		fmt.Print("\r" + strings.Repeat(" ", 80) + "\r")

		if err != nil {
			fmt.Printf("Agent Error: %v\n", err)
		} else {
			fmt.Printf("Agent: %s\n", response)
		}
		fmt.Println()
	}

	return nil
}

func (m *Mode) showHelp() {
	fmt.Println()
	fmt.Println("Chat Mode Commands:")
	fmt.Println()
	fmt.Println("  help   - Show this help message")
	fmt.Println("  clear  - Clear chat history")
	fmt.Println("  switch - Switch to a different handbook")
	fmt.Println("  exit   - Exit chat mode")
	fmt.Println()
}

func (m *Mode) switchHandbook() (string, error) {
	cm := config.GetManager()
	hm := handbook.NewManager()

	// Get list of available handbooks (returns []HandbookConfig)
	handbookList, err := hm.List()
	if err != nil {
		return "", err
	}

	// Build list including ACME and all other handbooks
	var choices []string

	// Always offer ACME
	if m.config.CurrentHandbook != "acme-analytics" {
		choices = append(choices, "acme-analytics (built-in)")
	}

	// Add other handbooks
	for _, hb := range handbookList {
		if hb.Name != m.config.CurrentHandbook {
			choices = append(choices, hb.Name)
		}
	}

	if len(choices) == 0 {
		fmt.Println("⚠️  No other handbooks available to switch to.")
		return m.config.CurrentHandbook, nil // Return current, no change
	}

	// Show selection
	var selectedHandbook string
	prompt := &survey.Select{
		Message: "Switch to handbook:",
		Options: choices,
	}
	if err := survey.AskOne(prompt, &selectedHandbook); err != nil {
		return "", err
	}

	// Extract handbook name (remove " (built-in)" suffix if present)
	selectedHandbook = strings.TrimSuffix(selectedHandbook, " (built-in)")

	// Validate the selected handbook (skip for ACME)
	if selectedHandbook != "acme-analytics" {
		handbookPath := cm.GetHandbookPath(selectedHandbook)
		valid, errors := hm.Validate(handbookPath)
		if !valid {
			fmt.Println("❌ Selected handbook is invalid:")
			for _, e := range errors {
				fmt.Printf("   - %s\n", e)
			}
			fmt.Println()
			fmt.Println("Please fix the handbook before switching.")
			return "", fmt.Errorf("handbook validation failed")
		}
	}

	// Don't update config here - let the caller decide after successful Docker restart
	// This prevents the bug where m.config.CurrentHandbook is already updated
	// when we check if we need to restart Docker

	fmt.Printf("Selected handbook: %s\n", selectedHandbook)

	return selectedHandbook, nil
}
