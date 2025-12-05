package interfaces

import "context"

// ServerManager handles Docker container lifecycle and health monitoring
type ServerManager interface {
	Start(ctx context.Context, config ServerConfig) error
	Stop(ctx context.Context, containerName string) error
	Status(ctx context.Context, containerName string) (ServerStatus, error)
	HealthCheck(ctx context.Context, endpoint string) error
	AutoStart(ctx context.Context, config ServerConfig) error
}

// ServerStatus represents the current status of a server
type ServerStatus struct {
	Running       bool
	Healthy       bool
	ContainerName string
}
