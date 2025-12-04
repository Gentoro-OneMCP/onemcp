package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update OneMCP Docker image and show CLI update instructions",
	Long:  `Updates the OneMCP Docker image to the latest version and displays instructions for updating the CLI binary.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Show CLI update instructions based on installation method
		showUpdateInstructions()

		fmt.Println()
		fmt.Println("📦 Updating OneMCP Docker image...")

		// Update Docker image
		ctx := context.Background()
		dm, err := docker.NewManager()
		if err != nil {
			fmt.Printf("❌ Failed to connect to Docker: %v\n", err)
			fmt.Println()
			fmt.Println("Please ensure Docker is running and try again.")
			os.Exit(1)
		}

		if err := dm.EnsureImage(ctx, true); err != nil {
			fmt.Printf("❌ Update failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("✅ Docker image updated successfully!")
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}

// showUpdateInstructions detects installation method and shows appropriate update instructions
func showUpdateInstructions() {
	installMethod := detectInstallationMethod()

	fmt.Println("📌 To update the OneMCP CLI:")
	fmt.Println()

	switch installMethod {
	case "homebrew":
		fmt.Println("  Homebrew:")
		fmt.Println("    brew upgrade onemcp")
	case "scoop":
		fmt.Println("  Scoop:")
		fmt.Println("    scoop update onemcp")
	default:
		// Manual installation
		fmt.Println("  Download the latest version:")
		fmt.Println("    https://github.com/Gentoro-OneMCP/onemcp/releases/latest")
		fmt.Println()
		fmt.Println("  Or use a package manager:")
		fmt.Println("    Homebrew: brew upgrade onemcp")
		fmt.Println("    Scoop:    scoop update onemcp")
	}
}

// detectInstallationMethod attempts to detect how the CLI was installed
func detectInstallationMethod() string {
	// Get the path to current executable
	exePath, err := os.Executable()
	if err != nil {
		return "manual"
	}

	// Check if installed via Homebrew (macOS/Linux)
	if strings.Contains(exePath, "/Cellar/onemcp/") || strings.Contains(exePath, "/Homebrew/") {
		return "homebrew"
	}

	// Check if installed via Scoop (Windows)
	if runtime.GOOS == "windows" && strings.Contains(exePath, "\\scoop\\apps\\onemcp\\") {
		return "scoop"
	}

	// Check if Homebrew is available and onemcp is installed via it
	if runtime.GOOS != "windows" {
		if output, err := exec.Command("brew", "list", "onemcp").CombinedOutput(); err == nil {
			if strings.Contains(string(output), "onemcp") {
				return "homebrew"
			}
		}
	}

	// Check if Scoop is available and onemcp is installed via it
	if runtime.GOOS == "windows" {
		if output, err := exec.Command("scoop", "list", "onemcp").CombinedOutput(); err == nil {
			if strings.Contains(string(output), "onemcp") {
				return "scoop"
			}
		}
	}

	return "manual"
}
