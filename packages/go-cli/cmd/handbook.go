package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/handbook"
	"github.com/spf13/cobra"
)

var handbookCmd = &cobra.Command{
	Use:   "handbook",
	Short: "Manage handbook directories",
}

var initCmd = &cobra.Command{
	Use:   "init [name]",
	Short: "Initialize a new handbook",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		hm := handbook.NewManager()
		path, err := hm.Init(name, "")
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ Handbook initialized at %s\n", path)
	},
}

var validateCmd = &cobra.Command{
	Use:   "validate [name]",
	Short: "Validate handbook structure",
	Long:  `Validate a handbook's structure. If no name is provided, validates the current handbook.`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cm := config.GetManager()
		hm := handbook.NewManager()

		var path string
		var handbookName string

		if len(args) > 0 {
			// If argument looks like a path (contains /), use as-is
			// Otherwise treat as handbook name and resolve to full path
			if strings.Contains(args[0], "/") || args[0] == "." {
				path = args[0]
				handbookName = filepath.Base(path)
			} else {
				handbookName = args[0]
				path = cm.GetHandbookPath(handbookName)
			}
		} else {
			// No argument - use current handbook
			cfg, err := cm.LoadGlobalConfig()
			if err != nil || cfg == nil || cfg.CurrentHandbook == "" {
				fmt.Println("❌ No handbook specified and no current handbook set.")
				fmt.Println("   Use: onemcp handbook validate <name>")
				fmt.Println("   Or: onemcp handbook use <name>")
				os.Exit(1)
			}
			handbookName = cfg.CurrentHandbook

			// Skip validation for built-in ACME
			if handbookName == "acme-analytics" {
				fmt.Println("✅ ACME Analytics is a built-in handbook (always valid)")
				return
			}

			path = cm.GetHandbookPath(handbookName)
		}

		fmt.Printf("Validating handbook: %s\n", handbookName)
		valid, errors := hm.Validate(path)
		if valid {
			fmt.Println("✅ Handbook is valid")
		} else {
			fmt.Println("❌ Handbook is invalid:")
			for _, e := range errors {
				fmt.Printf("  - %s\n", e)
			}
			os.Exit(1)
		}
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all handbooks",
	Run: func(cmd *cobra.Command, args []string) {
		hm := handbook.NewManager()
		list, err := hm.List()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("📚 Available Handbooks:")
		for _, h := range list {
			fmt.Printf("  - %s (v%s)\n", h.Name, h.Version)
		}
	},
}

var useCmd = &cobra.Command{
	Use:   "use [name]",
	Short: "Set the current handbook",
	Long:  `Set the current handbook. If no name is provided, shows an interactive list to choose from.`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cm := config.GetManager()
		hm := handbook.NewManager()

		var name string

		if len(args) > 0 {
			name = args[0]
		} else {
			// No argument - show interactive selection
			cfg, _ := cm.LoadGlobalConfig()
			currentHandbook := ""
			if cfg != nil {
				currentHandbook = cfg.CurrentHandbook
			}

			list, err := hm.List()
			if err != nil {
				fmt.Printf("Error loading handbooks: %v\n", err)
				os.Exit(1)
			}

			if len(list) == 0 && currentHandbook != "acme-analytics" {
				fmt.Println("❌ No handbooks available.")
				fmt.Println("   Create one with: onemcp handbook init <name>")
				os.Exit(1)
			}

			// Build list of handbook names with current indicator
			var choices []string
			var defaultChoice string

			// Add ACME
			if currentHandbook == "acme-analytics" {
				choices = append(choices, "acme-analytics (built-in) ← current")
				defaultChoice = "acme-analytics (built-in) ← current"
			} else {
				choices = append(choices, "acme-analytics (built-in)")
			}

			// Add other handbooks
			for _, h := range list {
				if h.Name != "acme-analytics" {
					if h.Name == currentHandbook {
						choice := h.Name + " ← current"
						choices = append(choices, choice)
						defaultChoice = choice
					} else {
						choices = append(choices, h.Name)
					}
				}
			}

			prompt := &survey.Select{
				Message: fmt.Sprintf("Select handbook to use (current: %s):", currentHandbook),
				Options: choices,
				Default: defaultChoice,
			}

			var selected string
			if err := survey.AskOne(prompt, &selected); err != nil {
				fmt.Println("Selection cancelled.")
				os.Exit(1)
			}

			// Extract handbook name (remove suffixes)
			name = strings.TrimSuffix(selected, " ← current")
			name = strings.TrimSuffix(name, " (built-in)")
		}

		cfg, err := cm.LoadGlobalConfig()
		if err != nil {
			cfg = &config.GlobalConfig{
				Provider:        config.ProviderGemini,
				APIKeys:         make(map[string]string),
				DefaultPort:     8080,
				HandbookDir:     filepath.Join(os.Getenv("HOME"), "onemcp-handbooks"),
				CurrentHandbook: name,
				ChatTimeout:     240,
			}
		} else {
			cfg.CurrentHandbook = name
		}

		if err := cm.SaveGlobalConfig(cfg); err != nil {
			fmt.Printf("Failed to save config: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✅ Current handbook set to: %s\n", name)
	},
}

var currentCmd = &cobra.Command{
	Use:   "current",
	Short: "Show the currently active handbook",
	Run: func(cmd *cobra.Command, args []string) {
		cm := config.GetManager()
		cfg, err := cm.LoadGlobalConfig()
		if err != nil || cfg == nil {
			fmt.Println("No configuration found. Run `onemcp chat` to set up.")
			os.Exit(1)
		}

		if cfg.CurrentHandbook == "" {
			fmt.Println("No handbook currently selected.")
			return
		}

		fmt.Printf("Current handbook: %s\n", cfg.CurrentHandbook)

		// Special handling for ACME (built into Docker image)
		if cfg.CurrentHandbook == "acme-analytics" {
			fmt.Println("Type: Built-in example handbook")
		} else {
			path := cm.GetHandbookPath(cfg.CurrentHandbook)
			fmt.Printf("Path: %s\n", path)
		}
	},
}

func init() {
	handbookCmd.AddCommand(initCmd)
	handbookCmd.AddCommand(validateCmd)
	handbookCmd.AddCommand(listCmd)
	handbookCmd.AddCommand(useCmd)
	handbookCmd.AddCommand(currentCmd)
	rootCmd.AddCommand(handbookCmd)
}
