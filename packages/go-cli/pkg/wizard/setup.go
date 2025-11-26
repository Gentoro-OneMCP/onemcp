package wizard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/handbook"
)

type Wizard struct {
	configManager *config.Manager
	hbManager     *handbook.Manager
}

func NewWizard() *Wizard {
	return &Wizard{
		configManager: config.GetManager(),
		hbManager:     handbook.NewManager(),
	}
}

func (w *Wizard) Run() error {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║        Gentoro OneMCP Setup          ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()

	// Step 1: Select Provider (returns lowercase for config)
	provider, err := w.selectProvider()
	if err != nil {
		return err
	}

	// Step 2: API Key (capitalize first letter for display)
	providerDisplay := strings.ToUpper(string(provider[0])) + provider[1:]
	if provider == "openai" {
		providerDisplay = "OpenAI"
	}
	apiKey, err := w.getAPIKey(providerDisplay)
	if err != nil {
		return err
	}

	// Step 3: Mode
	mode, err := w.selectMode()
	if err != nil {
		return err
	}

	var currentHandbook string

	if mode == "own" {
		path, err := w.configureHandbook()
		if err != nil {
			return err
		}
		currentHandbook = filepath.Base(path)
		// Simplified: skip service config for now to save time/space

		// Save config first
		cfg := &config.GlobalConfig{
			Provider: config.ModelProvider(provider),
			APIKeys: map[string]string{
				provider: apiKey,
			},
			DefaultPort:     8080,
			HandbookDir:     filepath.Join(os.Getenv("HOME"), "onemcp-handbooks"),
			CurrentHandbook: currentHandbook,
			ChatTimeout:     240, // 240 seconds (4 minutes)
		}

		if err := w.configManager.SaveGlobalConfig(cfg); err != nil {
			return err
		}

		fmt.Println("✅ Configuration saved successfully!")
		fmt.Println()
		fmt.Println("📝 Next Steps:")
		fmt.Printf("   1. Add OpenAPI definitions to: %s/openapi/\n", path)
		fmt.Printf("   2. Add documentation to: %s/docs/\n", path)
		fmt.Printf("   3. Validate handbook: onemcp handbook validate %s\n", currentHandbook)
		fmt.Println("   4. Start chatting: onemcp chat")
		fmt.Println()

		// Return special sentinel error to indicate we should exit without starting server
		return fmt.Errorf("SETUP_COMPLETE_NO_START")
	} else {
		// Example mode
		// Get current handbook choice
		currentHandbook = "acme-analytics" // For now, only ACME is the example
		fmt.Println("Using built-in ACME Analytics example...")
		// Don't create local folder - ACME is considered built into the Docker image or CLI
	}

	// Save (convert provider to lowercase for config)
	cfg := &config.GlobalConfig{
		Provider: config.ModelProvider(provider),
		APIKeys: map[string]string{
			provider: apiKey,
		},
		DefaultPort:     8080,
		HandbookDir:     filepath.Join(os.Getenv("HOME"), "onemcp-handbooks"),
		CurrentHandbook: currentHandbook,
		ChatTimeout:     240, // 240 seconds (4 minutes)
	}

	if err := w.configManager.SaveGlobalConfig(cfg); err != nil {
		return err
	}

	fmt.Println("✅ Configuration saved successfully!")
	return nil
}

func (w *Wizard) selectProvider() (string, error) {
	var provider string
	prompt := &survey.Select{
		Message: "Step 1 — Choose your AI provider:",
		Options: []string{"OpenAI", "Gemini", "Anthropic"},
	}
	if err := survey.AskOne(prompt, &provider); err != nil {
		return "", err
	}
	// Convert to lowercase for config (server expects lowercase)
	return strings.ToLower(provider), nil
}

func (w *Wizard) getAPIKey(providerDisplay string) (string, error) {
	var apiKey string
	prompt := &survey.Password{
		Message: fmt.Sprintf("Enter your %s API key:", providerDisplay),
	}
	if err := survey.AskOne(prompt, &apiKey, survey.WithValidator(survey.Required)); err != nil {
		return "", err
	}
	return apiKey, nil
}

func (w *Wizard) selectMode() (string, error) {
	var mode string
	prompt := &survey.Select{
		Message: "Step 2 — Choose how to start:",
		Options: []string{
			"Start with sample Acme Analytics handbook (example)",
			"Connect your own API service (own)",
		},
	}
	if err := survey.AskOne(prompt, &mode); err != nil {
		return "", err
	}

	if strings.Contains(mode, "example") {
		return "example", nil
	}
	return "own", nil
}

func (w *Wizard) configureHandbook() (string, error) {
	var name string
	prompt := &survey.Input{
		Message: "Step 3 — Handbook name:",
		Default: "my-handbook",
		Help:    "Press Enter to use 'my-handbook' or type a custom name",
	}
	if err := survey.AskOne(prompt, &name); err != nil {
		return "", err
	}

	// survey handles empty input by using Default
	if name == "" {
		name = "my-handbook"
	}

	path := w.configManager.GetHandbookPath(name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		var create bool
		confirmPrompt := &survey.Confirm{
			Message: "Handbook directory doesn't exist. Create it?",
			Default: true,
		}
		if err := survey.AskOne(confirmPrompt, &create); err != nil {
			return "", fmt.Errorf("setup aborted")
		}

		if create {
			w.hbManager.Init(name, path)
		} else {
			return "", fmt.Errorf("setup aborted")
		}
	}
	return path, nil
}
