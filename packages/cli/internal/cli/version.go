package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version metadata. These are intended to be overridden at build time via -ldflags.
var (
	// Version is the semantic version of the CLI (e.g., 0.0.4).
	Version = "0.0.1"
	// Commit is the git commit hash of the build.
	Commit = "47584018"
	// Date is the build timestamp (e.g., 2025-12-05T13:45:00Z).
	Date = "2025-12-05T13:45:00Z"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the onemcp CLI version",
	RunE: func(cmd *cobra.Command, args []string) error {
		out := cmd.OutOrStdout()
		fmt.Fprintf(out, "onemcp %s\n", Version)
		fmt.Fprintf(out, "  commit: %s\n", Commit)
		fmt.Fprintf(out, "  built:  %s\n", Date)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}