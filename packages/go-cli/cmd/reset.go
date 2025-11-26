package cmd

import (
	"fmt"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/wizard"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset all OneMCP configuration",
	Long:  `Delete all configuration files, API keys, and service settings. This action cannot be undone.`,
	Run: func(cmd *cobra.Command, args []string) {
		runReset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)
}

func runReset() {
	cm := config.GetManager()

	// Confirm reset
	fmt.Println("⚠️  This will delete ALL configuration and API keys")
	fmt.Println("   This action cannot be undone.")
	fmt.Println()

	var confirm bool
	prompt := &survey.Confirm{
		Message: "Continue with reset?",
		Default: false,
	}
	if err := survey.AskOne(prompt, &confirm); err != nil {
		fmt.Println("Reset cancelled.")
		return
	}

	if !confirm {
		fmt.Println("Reset cancelled.")
		return
	}

	fmt.Println("Resetting configuration...")

	// Delete config directory
	if err := cm.ResetConfiguration(); err != nil {
		fmt.Printf("Failed to reset: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Configuration reset successfully!")
	fmt.Println()
	fmt.Println("🔄 Running setup wizard...")
	fmt.Println()

	// Re-run setup wizard directly
	wiz := wizard.NewWizard()
	if err := wiz.Run(); err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("✅ Setup complete! Run 'onemcp chat' to start.")
}
