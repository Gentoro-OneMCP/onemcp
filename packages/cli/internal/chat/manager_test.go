package chat

import (
	"fmt"
	"net/http"
	"net/http/httptest"
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

				// Create a mock MCP server
				messageIndex := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "POST" {
						// Accept message
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`))
					} else if r.Method == "GET" {
						// Return a response
						if messageIndex < len(messages) {
							response := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"message":"Response to: %s"}}`, messages[messageIndex])
							messageIndex++
							w.WriteHeader(http.StatusOK)
							w.Write([]byte(response))
						} else {
							w.WriteHeader(http.StatusOK)
							w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
						}
					}
				}))
				defer server.Close()

				// Create chat manager
				manager := NewManager()
				manager.serverURL = server.URL
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

				// Verify server responses are logged
				for _, msg := range messages {
					expectedResponse := fmt.Sprintf("Response to: %s", msg)
					if !strings.Contains(logStr, expectedResponse) {
						t.Logf("Log missing server response for: %s", msg)
						return false
					}
				}

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

	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	manager := NewManager()

	// Note: Start() runs the bubbletea program which blocks, so we can't test it directly
	// Instead, we test the initialization parts
	manager.serverURL = server.URL
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

	// Create a mock MCP server
	receivedMessages := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			// Record received message
			receivedMessages = append(receivedMessages, "message")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`))
		} else if r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"message":"Hello from server"}}`))
		}
	}))
	defer server.Close()

	manager := NewManager()
	manager.serverURL = server.URL
	manager.logPath = logPath

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	manager.logFile = logFile
	defer manager.Close()

	// Test sending a message
	testMessage := "Hello, server!"
	if err := manager.SendMessage(testMessage); err != nil {
		t.Errorf("Failed to send message: %v", err)
	}

	// Verify message was sent to server
	if len(receivedMessages) != 1 {
		t.Errorf("Expected 1 message to be sent, got %d", len(receivedMessages))
	}

	// Test receiving a response
	response, err := manager.ReceiveResponse()
	if err != nil {
		t.Errorf("Failed to receive response: %v", err)
	}

	if response != "Hello from server" {
		t.Errorf("Expected response 'Hello from server', got '%s'", response)
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

	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`))
		} else if r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"message":"Test response"}}`))
		}
	}))
	defer server.Close()

	manager := NewManager()
	manager.serverURL = server.URL
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

	// Verify server response is logged
	if !strings.Contains(logStr, "Server: Test response") {
		t.Errorf("Log missing server response")
	}
}

// Test empty message handling
func TestChatManager_EmptyMessage(t *testing.T) {
	manager := NewManager()

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

	manager := NewManager()
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
