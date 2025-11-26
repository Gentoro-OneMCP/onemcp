package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update One MCP to the latest version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Checking for updates...")
		ctx := context.Background()
		dm, err := docker.NewManager()
		if err != nil {
			fmt.Printf("Failed to connect to Docker: %v\n", err)
			os.Exit(1)
		}

		if err := dm.EnsureImage(ctx, true); err != nil {
			fmt.Printf("Update failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("✅ Update complete! You are running the latest version.")
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}

