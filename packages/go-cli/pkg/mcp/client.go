package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Client struct {
	BaseURL string
	Session *mcp.ClientSession
	Config  *config.GlobalConfig
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

	if c.Session == nil {
		client := mcp.NewClient(&mcp.Implementation{
			Name:    "go-cli",
			Version: "0.1.0",
		}, nil)

		// Use the built-in StreamableClientTransport
		transport := &mcp.StreamableClientTransport{
			Endpoint: c.BaseURL,
		}

		session, err := client.Connect(ctx, transport, nil)
		if err != nil {
			return "", fmt.Errorf("failed to connect: %v", err)
		}
		c.Session = session
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
		return "", err
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
