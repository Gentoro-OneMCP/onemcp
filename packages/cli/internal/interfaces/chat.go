package interfaces

import "context"

// ChatManager handles interactive terminal session with MCP server
type ChatManager interface {
	Start(ctx context.Context, serverURL string, logPath string) error
	SendMessage(message string) error
	ReceiveResponse() (string, error)
	Close() error
}
