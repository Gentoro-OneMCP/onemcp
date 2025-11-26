package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/spf13/cobra"
)

var (
	logsLines  int
	logsFollow bool
)

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Show OneMCP server logs",
	Long:  `Display logs from the OneMCP Docker container.`,
	Run: func(cmd *cobra.Command, args []string) {
		runLogs()
	},
}

func init() {
	logsCmd.Flags().IntVarP(&logsLines, "lines", "n", 50, "Number of lines to show")
	logsCmd.Flags().BoolVarP(&logsFollow, "follow", "f", false, "Follow log output")
	rootCmd.AddCommand(logsCmd)
}

func runLogs() {
	ctx := context.Background()

	dm, err := docker.NewManager()
	if err != nil {
		fmt.Printf("Failed to initialize Docker client: %v\n", err)
		fmt.Println("Is Docker running?")
		os.Exit(1)
	}

	logs, err := dm.GetLogs(ctx, docker.ContainerName, logsLines, logsFollow)
	if err != nil {
		fmt.Printf("Failed to get logs: %v\n", err)
		os.Exit(1)
	}

	// Stream logs to stdout
	io.Copy(os.Stdout, logs)
}
