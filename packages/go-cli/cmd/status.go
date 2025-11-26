package cmd

import (
	"context"
	"fmt"

	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show OneMCP status",
	Long:  `Display the current status of the OneMCP server and configuration.`,
	Run: func(cmd *cobra.Command, args []string) {
		runStatus()
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

func runStatus() {
	ctx := context.Background()
	cm := config.GetManager()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║      OneMCP Status                   ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()

	// Check configuration
	if !cm.HasConfiguration() {
		fmt.Println("⚠️  No configuration found")
		fmt.Println("   Run 'onemcp chat' to set up")
		fmt.Println()
		return
	}

	cfg, err := cm.LoadGlobalConfig()
	if err != nil {
		fmt.Printf("⚠️  Failed to load config: %v\n", err)
		fmt.Println()
		return
	}

	// Show configuration
	fmt.Printf("📋 Configuration:\n")
	fmt.Printf("   Provider: %s\n", cfg.Provider)
	fmt.Printf("   Handbook: %s\n", cfg.CurrentHandbook)
	fmt.Printf("   Port: %d\n", cfg.DefaultPort)
	fmt.Println()

	// Check Docker
	dm, err := docker.NewManager()
	if err != nil {
		fmt.Printf("⚠️  Docker: Not available (%v)\n", err)
		fmt.Println()
		return
	}

	// Check container status
	inspect, err := dm.GetContainerInfo(ctx, docker.ContainerName)
	if err != nil {
		fmt.Println("⚠️  Server: Not running")
		fmt.Println("   Run 'onemcp chat' to start")
	} else {
		if inspect.State.Running {
			fmt.Println("✅ Server: Running")
			fmt.Printf("   Container ID: %s\n", inspect.ID[:12])
			fmt.Printf("   Started: %s\n", inspect.State.StartedAt)
			if inspect.State.Health != nil {
				fmt.Printf("   Health: %s\n", inspect.State.Health.Status)
			}
		} else {
			fmt.Println("⚠️  Server: Stopped")
			fmt.Println("   Run 'onemcp chat' to start")
		}
	}

	fmt.Println()
}
