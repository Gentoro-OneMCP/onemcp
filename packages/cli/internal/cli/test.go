package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"github.com/onemcp/cli/internal/test"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run regression tests against the handbook",
	Long: `Execute regression test suites defined in the handbook.

The test command runs all regression tests by default, or specific test files
if specified with the --files flag. Progress is displayed in real-time, and
a detailed report is saved to the reports/ directory.

Press Ctrl+C to cancel running tests.

Examples:
  onemcp test                              # Run all regression tests
  onemcp test --files=api.yaml,auth.yaml   # Run specific test suites`,
	RunE: runTest,
}

func init() {
	rootCmd.AddCommand(testCmd)
	testCmd.Flags().StringSlice("files", nil, "Specific test suite files to run (comma-separated)")
}

func runTest(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Resolve project directory
	targetDir, err := resolveProjectDir()
	if err != nil {
		return errors.NewGenericError("failed to resolve project directory", err)
	}

	// Validate project context
	projectMgr := getProjectManager()
	projectRoot, err := projectMgr.FindProjectRoot(targetDir)
	if err != nil {
		return err
	}

	// Load project configuration
	config, err := projectMgr.LoadConfig(projectRoot)
	if err != nil {
		return err
	}

	// Auto-start local server if stopped
	serverMgr := getServerManager()
	if err := serverMgr.AutoStart(ctx, config.Server); err != nil {
		return errors.NewServerError("failed to auto-start server", err)
	}

	// Determine server URL
	serverURL := config.Server.URL
	if config.Server.Mode == interfaces.ModeLocal {
		serverURL = fmt.Sprintf("http://localhost:%d", config.Server.Port)
	}

	// Get files flag
	files, _ := cmd.Flags().GetStringSlice("files")

	// Prefix files with regression-suites/ if not already
	for i, f := range files {
		if !strings.HasPrefix(f, "regression-suites/") {
			files[i] = "regression-suites/" + f
		}
	}

	// Create test manager
	testMgr := test.NewManager()

	// Start tests
	fmt.Println()
	if len(files) > 0 {
		fmt.Printf("Running tests: %s\n", strings.Join(files, ", "))
	} else {
		fmt.Println("Running all regression tests...")
	}

	jobId, err := testMgr.RunTests(ctx, serverURL, files)
	if err != nil {
		return errors.NewGenericError("failed to start tests", err)
	}

	// Setup CTRL+C handler
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	cancelled := false
	go func() {
		<-sigChan
		fmt.Println("\n\nCancelling tests...")
		_ = testMgr.CancelJob(context.Background(), serverURL, jobId)
		fmt.Println("Tests cancelled.")
		cancelled = true
		cancel()
	}()

	// Poll for status and show progress
	var lastMessage string
	for {
		select {
		case <-cancelCtx.Done():
			return nil
		default:
		}

		status, err := testMgr.GetStatus(cancelCtx, serverURL, jobId)
		if err != nil {
			// Context cancelled - exit gracefully
			if cancelCtx.Err() != nil {
				return nil
			}
			return errors.NewGenericError("failed to get test status", err)
		}

		// Update progress line (overwrite previous)
		if status.Message != "" && status.Message != lastMessage {
			// Clear line and print new status
			fmt.Printf("\r\033[K%s", status.Message)
			lastMessage = status.Message
		}

		if status.Status == "DONE" {
			fmt.Println() // New line after progress
			break
		}

		if status.Status == "FAILED" {
			fmt.Println()
			return errors.NewGenericError("tests failed: "+status.Message, nil)
		}

		if status.Status == "CANCELLED" {
			fmt.Println()
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Check if cancelled during final status check
	if cancelled {
		return nil
	}

	// Get results
	results, err := testMgr.GetResults(ctx, serverURL, jobId)
	if err != nil {
		return errors.NewGenericError("failed to get test results", err)
	}

	// Display summary
	fmt.Println()
	fmt.Println("Testing complete:")
	fmt.Printf("   ✓ Success: %d\n", results.Passed)
	fmt.Printf("   ✗ Failed: %d\n", results.Failed)
	fmt.Printf("   Duration: %dms\n", results.DurationMs)

	// Save report
	reportPath, err := saveTestReport(projectRoot, results)
	if err != nil {
		fmt.Printf("\nWarning: Failed to save report: %v\n", err)
	} else {
		fmt.Printf("\nFull report available at: file://%s\n", reportPath)
	}

	// Exit with non-zero if tests failed
	if results.Failed > 0 {
		os.Exit(1)
	}

	return nil
}

// saveTestReport saves the test results to a file
func saveTestReport(projectRoot string, results *interfaces.TestResults) (string, error) {
	reportsDir := filepath.Join(projectRoot, "reports")
	if err := os.MkdirAll(reportsDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create reports directory: %w", err)
	}

	timestamp := time.Now().Format("2006-01-02-150405")
	reportPath := filepath.Join(reportsDir, fmt.Sprintf("regression-%s.txt", timestamp))

	var sb strings.Builder
	sb.WriteString("=== OneMCP Regression Test Report ===\n")
	sb.WriteString(fmt.Sprintf("Date: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("Duration: %dms\n", results.DurationMs))
	sb.WriteString("\n")

	sb.WriteString("Summary:\n")
	sb.WriteString(fmt.Sprintf("  ✓ Passed: %d\n", results.Passed))
	sb.WriteString(fmt.Sprintf("  ✗ Failed: %d\n", results.Failed))
	sb.WriteString("\n")

	// Failed tests
	if results.Failed > 0 {
		sb.WriteString("Failed Tests:\n")
		for _, tc := range results.TestCases {
			if !tc.Passed {
				sb.WriteString(fmt.Sprintf("  ✗ %s (%dms)\n", tc.Name, tc.DurationMs))
				if tc.Details != "" {
					// Indent details
					lines := strings.Split(tc.Details, "\n")
					for _, line := range lines {
						sb.WriteString(fmt.Sprintf("      %s\n", line))
					}
				}
			}
		}
		sb.WriteString("\n")
	}

	// Passed tests
	if results.Passed > 0 {
		sb.WriteString("Passed Tests:\n")
		for _, tc := range results.TestCases {
			if tc.Passed {
				sb.WriteString(fmt.Sprintf("  ✓ %s (%dms)\n", tc.Name, tc.DurationMs))
			}
		}
	}

	if err := os.WriteFile(reportPath, []byte(sb.String()), 0644); err != nil {
		return "", fmt.Errorf("failed to write report: %w", err)
	}

	return reportPath, nil
}
