package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/spf13/cobra"
)

var providerCmd = &cobra.Command{
	Use:   "provider",
	Short: "Manage AI model provider settings",
	Long:  `Configure and switch between AI model providers (OpenAI, Gemini, Anthropic).`,
}

var providerListCmd = &cobra.Command{
	Use:   "list",
	Short: "List configured providers",
	Long:  `Show all configured AI providers and which one is currently active.`,
	Run: func(cmd *cobra.Command, args []string) {
		runProviderList()
	},
}

var providerSwitchCmd = &cobra.Command{
	Use:   "switch",
	Short: "Switch between configured providers",
	Long:  `Interactively switch between configured AI providers.`,
	Run: func(cmd *cobra.Command, args []string) {
		runProviderSwitch()
	},
}

var providerSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set provider and API key",
	Long:  `Configure a new AI provider and its API key.`,
	Run: func(cmd *cobra.Command, args []string) {
		runProviderSet()
	},
}

func init() {
	providerCmd.AddCommand(providerListCmd)
	providerCmd.AddCommand(providerSwitchCmd)
	providerCmd.AddCommand(providerSetCmd)
	rootCmd.AddCommand(providerCmd)
}

func runProviderList() {
	cm := config.GetManager()

	if !cm.HasConfiguration() {
		fmt.Println("No configuration found. Run 'onemcp chat' to set up.")
		return
	}

	cfg, err := cm.LoadGlobalConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("Provider Configuration:")
	fmt.Println()

	providerNames := map[string]string{
		"openai":    "OpenAI",
		"gemini":    "Google Gemini",
		"anthropic": "Anthropic Claude",
	}

	for provider, name := range providerNames {
		apiKey, exists := cfg.APIKeys[provider]
		status := ""
		if exists && apiKey != "" {
			status = "✅ Configured"
		} else {
			status = "  Not configured"
		}

		current := ""
		if string(cfg.Provider) == provider {
			current = " (current)"
		}

		fmt.Printf("%s%s: %s\n", name, current, status)
	}

	fmt.Println()
	fmt.Println("Use 'onemcp provider set' to configure a provider")
	fmt.Println("Use 'onemcp provider switch' to switch between providers")
}

func runProviderSwitch() {
	cm := config.GetManager()

	if !cm.HasConfiguration() {
		fmt.Println("No configuration found. Run 'onemcp chat' to set up.")
		return
	}

	cfg, err := cm.LoadGlobalConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	currentProvider := string(cfg.Provider)

	// Get list of configured providers (those with API keys)
	var configuredProviders []string
	for provider := range cfg.APIKeys {
		if provider != currentProvider {
			configuredProviders = append(configuredProviders, provider)
		}
	}

	if len(configuredProviders) == 0 {
		fmt.Println("No other providers configured.")
		fmt.Println("Use 'onemcp provider set' to add more providers.")
		return
	}

	// Build choices with current indicator
	var choices []string
	var defaultChoice string

	// Add current provider first
	currentChoice := currentProvider + " ← current"
	choices = append(choices, currentChoice)
	defaultChoice = currentChoice

	// Add other configured providers
	for _, p := range configuredProviders {
		choices = append(choices, p)
	}

	var selected string
	prompt := &survey.Select{
		Message: fmt.Sprintf("Select AI provider to switch to (current: %s):", currentProvider),
		Options: choices,
		Default: defaultChoice,
	}
	if err := survey.AskOne(prompt, &selected); err != nil {
		fmt.Println("Selection cancelled.")
		return
	}

	// Extract provider name (remove suffix)
	selected = strings.TrimSuffix(selected, " ← current")

	if selected == currentProvider {
		fmt.Printf("Already using %s\n", currentProvider)
		return
	}

	cfg.Provider = config.ModelProvider(selected)
	if err := cm.SaveGlobalConfig(cfg); err != nil {
		fmt.Printf("Failed to save config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Switched to %s\n", selected)
}

func runProviderSet() {
	cm := config.GetManager()

	var cfg *config.GlobalConfig
	var err error

	if cm.HasConfiguration() {
		cfg, err = cm.LoadGlobalConfig()
		if err != nil {
			fmt.Printf("Failed to load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Initialize a new config if none exists
		homeDir, _ := os.UserHomeDir()
		cfg = &config.GlobalConfig{
			APIKeys:         make(map[string]string),
			DefaultPort:     8080,
			HandbookDir:     filepath.Join(homeDir, "onemcp-handbooks"),
			CurrentHandbook: "",
			ChatTimeout:     240,
		}
	}

	currentProvider := string(cfg.Provider)
	providers := []string{"OpenAI", "Gemini", "Anthropic"}

	// Build choices with current indicator
	var choices []string
	var defaultChoice string
	for _, p := range providers {
		pLower := strings.ToLower(p)
		if pLower == currentProvider {
			choice := p + " ← current"
			choices = append(choices, choice)
			defaultChoice = choice
		} else {
			choices = append(choices, p)
		}
	}

	var providerChoice string
	prompt := &survey.Select{
		Message: fmt.Sprintf("Select which provider to configure (current: %s):", currentProvider),
		Options: choices,
		Default: defaultChoice,
	}
	if err := survey.AskOne(prompt, &providerChoice); err != nil {
		fmt.Println("Selection cancelled.")
		return
	}

	// Extract provider name (remove suffix)
	providerChoice = strings.TrimSuffix(providerChoice, " ← current")
	provider := strings.ToLower(providerChoice)

	var apiKey string
	keyPrompt := &survey.Password{
		Message: fmt.Sprintf("Enter your %s API key:", providerChoice),
	}
	if err := survey.AskOne(keyPrompt, &apiKey, survey.WithValidator(survey.Required)); err != nil {
		fmt.Println("Setup cancelled.")
		return
	}

	cfg.APIKeys[provider] = apiKey
	cfg.Provider = config.ModelProvider(provider)

	if err := cm.SaveGlobalConfig(cfg); err != nil {
		fmt.Printf("Failed to save config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Provider set to %s\n", provider)
}
