package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gentoro/onemcp/go-cli/pkg/chat"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/docker"
	"github.com/gentoro/onemcp/go-cli/pkg/handbook"
	"github.com/gentoro/onemcp/go-cli/pkg/wizard"
	"github.com/spf13/cobra"
)

var (
	localMode   bool
	customImage string
)

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Open interactive chat mode",
	Run: func(cmd *cobra.Command, args []string) {
		runChat()
	},
}

func init() {
	chatCmd.Flags().BoolVar(&localMode, "local", false, "Connect to local server (skip Docker)")
	chatCmd.Flags().StringVar(&customImage, "image", "", "Custom Docker image to use")
	rootCmd.AddCommand(chatCmd)
}

func runChat() {
	cm := config.GetManager()

	// Run wizard if no config
	if !cm.HasConfiguration() {
		fmt.Println("No configuration found. Running setup wizard...")
		wizard := wizard.NewWizard()
		if err := wizard.Run(); err != nil {
			// Check if this is the special "setup complete, don't start server" case
			if err.Error() == "SETUP_COMPLETE_NO_START" {
				// Exit gracefully - config was saved but server shouldn't start
				return
			}
			fmt.Printf("Setup failed: %v\n", err)
			os.Exit(1)
		}
	}

	cfg, err := cm.LoadGlobalConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if cfg.CurrentHandbook == "" {
		fmt.Println("No current handbook selected. Please run setup or select a handbook.")
		os.Exit(1)
	}

	// Validate handbook before starting server (skip for built-in ACME)
	if cfg.CurrentHandbook != "acme-analytics" {
		handbookPath := cm.GetHandbookPath(cfg.CurrentHandbook)
		hm := handbook.NewManager()
		valid, errors := hm.Validate(handbookPath)

		if !valid {
			fmt.Println("❌ Handbook validation failed:")
			for _, e := range errors {
				fmt.Printf("   - %s\n", e)
			}
			fmt.Println()
			fmt.Println("📝 Please fix the handbook issues:")
			fmt.Printf("   Handbook location: %s\n", handbookPath)
			fmt.Println("   Required:")
			fmt.Println("   - Add OpenAPI YAML files to: openapi/")
			fmt.Println("   - Ensure instructions.md exists")
			fmt.Println()
			fmt.Printf("   Validate with: onemcp handbook validate %s\n", cfg.CurrentHandbook)
			os.Exit(1)
		}
		fmt.Println("✅ Handbook validated successfully")
	}

	// Local mode - skip Docker entirely
	if localMode {
		fmt.Println("🔌 Connecting to local server...")
		// Wait for server to be healthy
		fmt.Println("Waiting for server to be ready...")
		ctx := context.Background()
		dm, _ := docker.NewManager() // Create manager just for health check
		if err := dm.WaitForHealthy(ctx, cfg.DefaultPort, 60*time.Second); err != nil {
			fmt.Printf("Server failed to start: %v\n", err)
			fmt.Println("\nMake sure the local server is running first.")
			os.Exit(1)
		}
		fmt.Println("✅ Server is ready!")
	} else {
		// Docker & Server checks
		ctx := context.Background()
		dm, err := docker.NewManager()
		if err != nil {
			fmt.Println("❌ Error: Docker daemon not reachable")
			fmt.Println()
			fmt.Println("   Is Docker running? You can:")
			fmt.Println("   • Start Docker Desktop")
			fmt.Println("   • Check installation: docker --version")
			fmt.Println()
			os.Exit(1)
		}

		// Determine which image to use
		imageName := customImage
		if imageName == "" {
			imageName = docker.ImageName // Default
			// Ensure Image (always pull to be safe/fresh for default image)
			if err := dm.EnsureImage(ctx, true); err != nil {
				fmt.Printf("Failed to pull image: %v\n", err)
				os.Exit(1)
			}
		} else {
			// For custom images, don't pull - assume already built
			fmt.Printf("Using custom image: %s\n", imageName)
		}

		// Pull and start server
		handbookPath := cm.GetHandbookPath(cfg.CurrentHandbook)

		// Show appropriate message based on handbook type
		if cfg.CurrentHandbook == "acme-analytics" {
			fmt.Println("Starting OneMCP server with built-in ACME Analytics handbook...")
		} else {
			fmt.Printf("Starting OneMCP server with handbook: %s\n", handbookPath)
		}

		if err := dm.StartServer(ctx, cfg, handbookPath, imageName); err != nil {
			fmt.Printf("Failed to start server: %v\n", err)
			os.Exit(1)
		}

		// Setup signal handler for graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Goroutine to handle shutdown
		go func() {
			<-sigChan
			fmt.Println("\n\nShutting down gracefully...")
			// Stop container in background - don't wait
			go dm.StopServer(context.Background())
			// Give it a moment to start stopping, then exit
			time.Sleep(100 * time.Millisecond)
			os.Exit(0)
		}()

		// Also ensure we stop on normal exit
		defer func() {
			// Stop in background so we don't block
			go dm.StopServer(context.Background())
			time.Sleep(100 * time.Millisecond)
		}()

		// Wait for server to be healthy
		fmt.Println("Waiting for server to be ready...")
		if err := dm.WaitForHealthy(ctx, cfg.DefaultPort, 60*time.Second); err != nil {
			fmt.Printf("Server failed to start: %v\n", err)
			// Don't exit, maybe logs will show why, but usually we should
			// os.Exit(1)
		} else {
			fmt.Println("✅ Server is ready!")
		}
	}

	// 4. Start Chat
	mode := chat.NewMode(cfg)
	mode.ShowReports = localMode // Show reports only in local/dev mode
	if err := mode.Start(); err != nil {
		fmt.Printf("Chat error: %v\n", err)
	}
}
