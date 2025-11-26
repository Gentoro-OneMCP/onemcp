package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the OneMCP server",
	Long:  `Stop the running OneMCP Docker container.`,
	Run: func(cmd *cobra.Command, args []string) {
		runStop()
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
}

func runStop() {
	ctx := context.Background()

	dm, err := docker.NewManager()
	if err != nil {
		fmt.Printf("Failed to initialize Docker client: %v\n", err)
		fmt.Println("Is Docker running?")
		os.Exit(1)
	}

	fmt.Println("Stopping OneMCP server...")

	if err := dm.StopServer(ctx); err != nil {
		fmt.Printf("Failed to stop server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ OneMCP server stopped successfully")
}
