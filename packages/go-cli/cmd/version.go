package cmd

import (
	"fmt"

	"github.com/gentoro/onemcp/go-cli/pkg/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show OneMCP CLI version",
	Long:  `Display the version of the OneMCP CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("OneMCP CLI v%s\n", version.Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
