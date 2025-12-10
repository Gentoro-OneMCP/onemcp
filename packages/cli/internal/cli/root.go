package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile    string
	projectDir string
	rootCmd    = &cobra.Command{
		Use:   "onemcp",
		Short: "OneMCP CLI - Manage MCP server projects",
		Long: `OneMCP is a project-based command-line interface that enables developers 
to interact with MCP (Machine Collaboration Protocol) servers. 

The CLI supports both local Docker-hosted server instances and remote server 
connections. Each project maintains a local editable handbook, logs, reports, 
and private state within a structured directory hierarchy.`,
	}
)

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is .onemcp/onemcp.yaml)")
	rootCmd.PersistentFlags().StringVar(&projectDir, "project-dir", "", "project directory (default is current directory)")
}

// initConfig reads in config file and ENV variables if set
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Search for config in .onemcp directory
		viper.AddConfigPath(".onemcp")
		viper.SetConfigType("yaml")
		viper.SetConfigName("onemcp")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

// resolveProjectDir resolves the project directory from flag or current directory
// Returns the absolute path to the project root
func resolveProjectDir() (string, error) {
	dir := projectDir
	if dir == "" {
		return getCurrentDir()
	}

	// Expand tilde to home directory
	if strings.HasPrefix(dir, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		dir = filepath.Join(homeDir, dir[2:])
	} else if dir == "~" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		dir = homeDir
	}

	// Convert to absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	return absDir, nil
}

func getCurrentDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current directory: %w", err)
	}
	return cwd, nil
}
