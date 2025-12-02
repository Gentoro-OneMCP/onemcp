package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/gentoro/onemcp/go-cli/pkg/version"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Client struct {
	BaseURL    string
	Session    *mcp.ClientSession
	Config     *config.GlobalConfig
	connCtx    context.Context
	connCancel context.CancelFunc
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
	}
}

// OneMCPResponse represents the structured response from OneMCP server
type OneMCPResponse struct {
	Parts []struct {
		IsSupported bool   `json:"isSupported"`
		Assignment  string `json:"assignment"`
		IsError     bool   `json:"isError"`
		Content     string `json:"content,omitempty"`
	} `json:"parts"`
	Statistics struct {
		PromptTokens     int      `json:"promptTokens"`
		CompletionTokens int      `json:"completionTokens"`
		TotalTokens      int      `json:"totalTokens"`
		TotalTimeMs      int      `json:"totalTimeMs"`
		Operations       []string `json:"operations"`
	} `json:"statistics"`
}

// SendMessage sends a message to the MCP agent and returns the response
func (c *Client) SendMessage(prompt string) (string, error) {
	// Get timeout from config (default 240 seconds)
	timeout := 240 * time.Second
	if c.Config != nil && c.Config.ChatTimeout > 0 {
		timeout = time.Duration(c.Config.ChatTimeout) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Check if we need to (re)connect
	if c.Session == nil {
		if err := c.connect(ctx); err != nil {
			return "", err
		}
	}

	// Call Tool
	result, err := c.Session.CallTool(ctx, &mcp.CallToolParams{
		Name: "onemcp.run",
		Arguments: map[string]interface{}{
			"prompt": prompt,
		},
	})

	if err != nil {
		// Check if timeout occurred
		if ctx.Err() == context.DeadlineExceeded {
			// Reset session on timeout to prevent server-side resource conflicts
			c.Session = nil
			return "", fmt.Errorf("request timed out after %d seconds - session reset for next query", c.Config.ChatTimeout)
		}

		// Check if connection was closed - try to reconnect once
		if strings.Contains(err.Error(), "connection closed") ||
			strings.Contains(err.Error(), "client is closing") ||
			strings.Contains(err.Error(), "connection failed") {
			c.Session = nil

			// Try to reconnect
			if reconnectErr := c.connect(ctx); reconnectErr != nil {
				return "", fmt.Errorf("connection lost and reconnect failed: %v", reconnectErr)
			}

			// Retry the request once
			result, err = c.Session.CallTool(ctx, &mcp.CallToolParams{
				Name: "onemcp.run",
				Arguments: map[string]interface{}{
					"prompt": prompt,
				},
			})

			if err != nil {
				return "", fmt.Errorf("request failed after reconnection: %v", err)
			}
		} else {
			return "", err
		}
	}

	if result.IsError {
		return "", fmt.Errorf("tool execution error")
	}

	// Try to parse as OneMCP structured response
	for _, content := range result.Content {
		if tc, ok := content.(*mcp.TextContent); ok {
			var resp OneMCPResponse
			if err := json.Unmarshal([]byte(tc.Text), &resp); err == nil {
				// Extract only the supported, non-error content
				var results []string
				for _, part := range resp.Parts {
					if part.IsSupported && !part.IsError && part.Content != "" {
						results = append(results, part.Content)
					}
				}

				if len(results) > 0 {
					return strings.Join(results, "\n"), nil
				}
			}

			// Fallback: return raw text if JSON parsing fails
			return tc.Text, nil
		}
	}

	return "", fmt.Errorf("no text content in response")
}

// connect establishes a new MCP session with a long-lived connection context
func (c *Client) connect(ctx context.Context) error {
	// Create long-lived context for the SSE connection
	// This prevents the connection from being cancelled when individual requests complete
	c.connCtx, c.connCancel = context.WithCancel(context.Background())

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "go-cli",
		Version: version.Version,
	}, nil)

	// Use the built-in StreamableClientTransport
	transport := &mcp.StreamableClientTransport{
		Endpoint: c.BaseURL,
	}

	// Use connection context (long-lived), not the request context (short-lived)
	session, err := client.Connect(c.connCtx, transport, nil)
	if err != nil {
		c.connCancel() // Clean up context if connection fails
		return fmt.Errorf("failed to connect: %v", err)
	}

	c.Session = session
	return nil
}

// Close closes the MCP session and cancels the connection context
func (c *Client) Close() {
	if c.connCancel != nil {
		c.connCancel()
	}
	c.Session = nil
}
