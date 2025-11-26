package cmd

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/spf13/cobra"
)

var serviceCmd = &cobra.Command{
	Use:   "service",
	Short: "Manage service configurations",
	Long:  `Configure authentication and settings for external services that your handbook APIs interact with.`,
}

var serviceAuthCmd = &cobra.Command{
	Use:   "auth <service-name>",
	Short: "Configure service authentication",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runServiceAuth(args[0])
	},
}

var serviceRenewCmd = &cobra.Command{
	Use:   "renew <service-name>",
	Short: "Renew service token",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runServiceRenew(args[0])
	},
}

var serviceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List configured services",
	Run: func(cmd *cobra.Command, args []string) {
		runServiceList()
	},
}

func init() {
	rootCmd.AddCommand(serviceCmd)
	serviceCmd.AddCommand(serviceAuthCmd)
	serviceCmd.AddCommand(serviceRenewCmd)
	serviceCmd.AddCommand(serviceListCmd)
}

func runServiceAuth(serviceName string) {
	cm := config.GetManager()

	// Base URL
	var baseUrl string
	baseUrlPrompt := &survey.Input{
		Message: "Service base URL:",
	}
	if err := survey.AskOne(baseUrlPrompt, &baseUrl, survey.WithValidator(func(ans interface{}) error {
		str := ans.(string)
		if str == "" {
			return fmt.Errorf("base URL is required")
		}
		_, err := url.Parse(str)
		if err != nil {
			return fmt.Errorf("please enter a valid URL")
		}
		return nil
	})); err != nil {
		fmt.Printf("Setup cancelled: %v\n", err)
		os.Exit(1)
	}

	// Header name
	var header string
	headerPrompt := &survey.Input{
		Message: "Header name:",
		Default: "Authorization",
		Help:    "The HTTP header name for authentication",
	}
	if err := survey.AskOne(headerPrompt, &header); err != nil {
		fmt.Printf("Setup cancelled: %v\n", err)
		os.Exit(1)
	}

	// Pattern
	var pattern string
	patternPrompt := &survey.Input{
		Message: "Pattern:",
		Default: "Bearer {token}",
		Help:    "Must include {token} placeholder",
	}
	if err := survey.AskOne(patternPrompt, &pattern, survey.WithValidator(func(ans interface{}) error {
		str := ans.(string)
		if !strings.Contains(str, "{token}") {
			return fmt.Errorf("pattern must include {token} placeholder")
		}
		return nil
	})); err != nil {
		fmt.Printf("Setup cancelled: %v\n", err)
		os.Exit(1)
	}

	// Access token (password input)
	var token string
	tokenPrompt := &survey.Password{
		Message: "Access token:",
	}
	if err := survey.AskOne(tokenPrompt, &token, survey.WithValidator(survey.Required)); err != nil {
		fmt.Printf("Setup cancelled: %v\n", err)
		os.Exit(1)
	}

	// Save service config
	serviceConfig := config.ServiceConfig{
		Service: serviceName,
		BaseURL: baseUrl,
		Header:  header,
		Pattern: pattern,
		Token:   token,
	}

	if err := cm.SaveServiceConfig(serviceName, &serviceConfig); err != nil {
		fmt.Printf("Failed to save service config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Service %s configured successfully\n", serviceName)
}

func runServiceRenew(serviceName string) {
	cm := config.GetManager()

	// Load existing config
	serviceConfig, err := cm.LoadServiceConfig(serviceName)
	if err != nil {
		fmt.Printf("Service %s not found\n", serviceName)
		os.Exit(1)
	}

	// Get new token
	var token string
	tokenPrompt := &survey.Password{
		Message: fmt.Sprintf("Enter new token for %s:", serviceName),
	}
	if err := survey.AskOne(tokenPrompt, &token, survey.WithValidator(survey.Required)); err != nil {
		fmt.Printf("Setup cancelled: %v\n", err)
		os.Exit(1)
	}

	// Update and save
	serviceConfig.Token = token
	serviceConfig.ExpiresAt = "" // Clear expiration

	if err := cm.SaveServiceConfig(serviceName, serviceConfig); err != nil {
		fmt.Printf("Failed to save service config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Token renewed for %s\n", serviceName)
}

func runServiceList() {
	cm := config.GetManager()

	services, err := cm.ListServices()
	if err != nil {
		fmt.Printf("Failed to list services: %v\n", err)
		os.Exit(1)
	}

	if len(services) == 0 {
		fmt.Println("⚠️  No services configured")
		return
	}

	fmt.Println()
	fmt.Println("📋 Configured Services:")
	fmt.Println()

	for _, serviceName := range services {
		serviceConfig, err := cm.LoadServiceConfig(serviceName)
		if err != nil {
			fmt.Printf("  %s (error loading config)\n", serviceName)
			continue
		}

		fmt.Printf("  %s\n", serviceName)
		if serviceConfig.BaseURL != "" {
			fmt.Printf("    Base URL: %s\n", serviceConfig.BaseURL)
		}
		fmt.Printf("    Header: %s\n", serviceConfig.Header)
		fmt.Printf("    Pattern: %s\n", serviceConfig.Pattern)
		fmt.Println()
	}
}
