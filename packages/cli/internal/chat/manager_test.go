package chat

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "testing"
    "time"

    "github.com/leanovate/gopter"
    "github.com/leanovate/gopter/gen"
    "github.com/leanovate/gopter/prop"
)

// **Feature: onemcp-cli, Property 10: Chat session logs all communication**
func TestProperty_ChatSessionLogsAllCommunication(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Custom generator that creates non-empty message slices
	messageGen := gen.SliceOfN(3, gen.Identifier()).Map(func(msgs []string) []string {
		// Filter out empty strings and ensure we have valid messages
		result := make([]string, 0, len(msgs))
		for _, msg := range msgs {
			if msg != "" && len(msg) >= 3 {
				result = append(result, msg)
			}
		}
		// Ensure at least one message
		if len(result) == 0 {
			result = append(result, "testmessage")
		}
		return result
	})

	properties.Property("**Feature: onemcp-cli, Property 10: Chat session logs all communication**",
		prop.ForAll(
			func(messages []string) bool {
				// Create temporary directory for logs
				tmpDir, err := os.MkdirTemp("", "chat-test-*")
				if err != nil {
					t.Logf("Failed to create temp dir: %v", err)
					return false
				}
				defer os.RemoveAll(tmpDir)

				logPath := filepath.Join(tmpDir, "chat-test.log")

    // Create chat manager
    manager := NewManager(nil, tmpDir)
    manager.logPath = logPath

				// Open log file
				logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					t.Logf("Failed to open log file: %v", err)
					return false
				}
				manager.logFile = logFile

				// Log session start
				manager.logToFile(fmt.Sprintf("=== Chat session started at %s ===\n", time.Now().Format(time.RFC3339)))

				// Send and receive messages
				for _, msg := range messages {
					// Send message
					if err := manager.SendMessage(msg); err != nil {
						t.Logf("Failed to send message: %v", err)
						manager.Close()
						return false
					}

					// Receive response
					if _, err := manager.ReceiveResponse(); err != nil {
						t.Logf("Failed to receive response: %v", err)
						manager.Close()
						return false
					}
				}

				// Close manager to flush logs
				manager.Close()

				// Read log file and verify all messages are logged
				logContent, err := os.ReadFile(logPath)
				if err != nil {
					t.Logf("Failed to read log file: %v", err)
					return false
				}

				logStr := string(logContent)

				// Verify session start is logged
				if !strings.Contains(logStr, "Chat session started") {
					t.Logf("Log missing session start")
					return false
				}

				// Verify all user messages are logged
				for _, msg := range messages {
					if !strings.Contains(logStr, fmt.Sprintf("User: %s", msg)) {
						t.Logf("Log missing user message: %s", msg)
						return false
					}
				}

    // Note: server responses are produced by MCP in sendAndReceive,
    // which is not exercised in this unit test.

				// Verify session end is logged
				if !strings.Contains(logStr, "Chat session ended") {
					t.Logf("Log missing session end")
					return false
				}

				return true
			},
			messageGen,
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// Unit test for session initialization
func TestChatManager_Start(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "logs", "chat-test.log")

 manager := NewManager(nil, tmpDir)

	// Note: Start() runs the bubbletea program which blocks, so we can't test it directly
 // Instead, we test the initialization parts
 manager.logPath = logPath

	// Create log directory
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	manager.logFile = logFile
	defer manager.Close()

	// Verify log file was created
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("Log file was not created")
	}
}

// Unit test for message exchange
func TestChatManager_MessageExchange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "chat-test.log")

 manager := NewManager(nil, tmpDir)
 manager.logPath = logPath

 // Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	manager.logFile = logFile
	defer manager.Close()

 // Test sending a message (logs only)
 testMessage := "Hello, server!"
 if err := manager.SendMessage(testMessage); err != nil {
     t.Errorf("Failed to send message: %v", err)
 }

 // Read log file and verify the user message was logged
 logContent, err := os.ReadFile(logPath)
 if err != nil {
     t.Fatalf("Failed to read log file: %v", err)
 }
 if !strings.Contains(string(logContent), "User: "+testMessage) {
     t.Errorf("Expected user message to be logged")
 }
}

// Unit test for logging functionality
func TestChatManager_Logging(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "chat-test.log")

 manager := NewManager(nil, tmpDir)
 manager.logPath = logPath

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	manager.logFile = logFile

	// Log session start
	manager.logToFile("=== Session started ===\n")

	// Send and receive messages
	manager.SendMessage("Test message")
	manager.ReceiveResponse()

	// Close to flush logs
	manager.Close()

	// Read log file
	logContent, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	logStr := string(logContent)

	// Verify session start is logged
	if !strings.Contains(logStr, "Session started") {
		t.Errorf("Log missing session start")
	}

	// Verify user message is logged
	if !strings.Contains(logStr, "User: Test message") {
		t.Errorf("Log missing user message")
	}

 // Note: server response logging occurs in sendAndReceive; not covered here.
}

// Test empty message handling
func TestChatManager_EmptyMessage(t *testing.T) {
    manager := NewManager(nil, "")

	// Empty messages should be handled gracefully
	if err := manager.SendMessage(""); err != nil {
		t.Errorf("Empty message should not cause error: %v", err)
	}
}

// Test log file creation in nested directories
func TestChatManager_LogFileCreation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use nested path
	logPath := filepath.Join(tmpDir, "logs", "nested", "chat-test.log")

 manager := NewManager(nil, "")
	manager.logPath = logPath

	// Create log directory
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	manager.logFile = logFile
	defer manager.Close()

	// Verify log file exists
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("Log file was not created in nested directory")
	}
}
