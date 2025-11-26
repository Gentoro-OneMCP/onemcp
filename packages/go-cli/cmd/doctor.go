package cmd

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check system requirements and configuration",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("🔍 One MCP Doctor")
		fmt.Println()

		// Check Docker
		fmt.Print("Docker: ")
		if _, err := exec.LookPath("docker"); err != nil {
			fmt.Println("❌ Not found (Required)")
		} else {
			ctx := context.Background()
			dm, err := docker.NewManager()
			if err != nil || !dm.IsRunning(ctx) {
				// IsRunning checks specific container, but NewManager checks connection
				if err != nil {
					fmt.Println("❌ Daemon not reachable")
				} else {
					fmt.Println("✅ Installed & Running")
				}
			} else {
				fmt.Println("✅ Installed & Running")
			}
		}

		// Check Config
		cm := config.GetManager()
		fmt.Print("Configuration: ")
		if cm.HasConfiguration() {
			fmt.Println("✅ Found")
		} else {
			fmt.Println("❌ Not configured (Run 'onemcp chat' to setup)")
		}

		// Check Server Status (Container)
		ctx := context.Background()
		dm, err := docker.NewManager()
		if err == nil {
			fmt.Print("Server Container: ")
			if dm.IsRunning(ctx) {
				fmt.Println("✅ Running")
			} else {
				fmt.Println("⚠️  Stopped")
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(doctorCmd)
}

