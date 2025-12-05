package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
)

// Manager implements the ServerManager interface
type Manager struct {
	dockerClient *client.Client
	httpClient   *http.Client
}

// SKIP_DOCKER Meant for testing purposes only, bypasses Docker operations
const SKIP_DOCKER = true

// NewManager creates a new ServerManager instance
func NewManager() (*Manager, error) {
	var dockerClient *client.Client = nil
	if !SKIP_DOCKER {
		var err error
		dockerClient, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			host := os.Getenv("DOCKER_HOST")
			if host == "" {
				host = "unix:///var/run/docker.sock (default)"
			}

			return nil, errors.NewServerError(
				fmt.Sprintf(
					"Failed to create Docker client.\n\n"+
						"Troubleshooting tips:\n"+
						"  • Ensure Docker Desktop is running.\n"+
						"  • Check your Docker socket: DOCKER_HOST=%s\n"+
						"  • On macOS, the socket may be located at:\n"+
						"       ~/Library/Containers/com.docker.docker/Data/docker-cli.sock\n"+
						"    Export it manually:\n"+
						"       export DOCKER_HOST=unix://$HOME/Library/Containers/com.docker.docker/Data/docker-cli.sock\n\n"+
						"Original error: %v",
					host, err,
				),
				err,
			)
		}
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	return &Manager{
		dockerClient: dockerClient,
		httpClient:   httpClient,
	}, nil
}

// Start creates and starts a Docker container
func (m *Manager) Start(ctx context.Context, config interfaces.ServerConfig) error {
	if SKIP_DOCKER {
		return nil
	}

	// Validate that we're in local mode
	if config.Mode != interfaces.ModeLocal {
		return errors.NewServerError("Cannot start server in remote mode.", nil)
	}

	// Pull the image if it doesn't exist
	imageName := "admingentoro/gentoro:latest"
	if err := m.pullImageIfNeeded(ctx, imageName); err != nil {
		help := fmt.Sprintf(
			`Failed to pull Docker image: %s

			Troubleshooting steps:

			• If you are on macOS with Docker Desktop, Docker may be using a non-standard socket.
				Try exporting the correct socket:
					export DOCKER_HOST=unix://$HOME/Library/Containers/com.docker.docker/Data/docker-cli.sock

			• Check internet connectivity (image pulls require network access).
			• Ensure the image exists and is public:
					docker pull %s

			• If behind a proxy, verify the Docker system settings:
					Settings → Resources → Proxies

			• Check if you can pull manually from the terminal:
					docker pull %s

			Original error:
			%v
			`, imageName, imageName, imageName, err)

		return errors.NewServerError(help, err)
	}

	// Configure port bindings
	hostPort := fmt.Sprintf("%d", config.Port)
	containerPort := "8080/tcp"
	portBindings := nat.PortMap{
		nat.Port(containerPort): []nat.PortBinding{
			{
				HostIP:   "0.0.0.0",
				HostPort: hostPort,
			},
		},
	}

	// Prepare environment variables
	env := []string{}
	if config.Model != "" {
		env = append(env, fmt.Sprintf("LLM_ACTIVE_PROFILE=%s", strings.ToLower(string(config.Model))))
	}
	if config.ModelAPIKey != "" {
		env = append(env, fmt.Sprintf("%s_API_KEY=%s", config.Model, config.ModelAPIKey))
	}

	// Create container configuration
	containerConfig := &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			nat.Port(containerPort): struct{}{},
		},
		Env: env,
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		AutoRemove:   false,
	}

	// Create the container
	resp, err := m.dockerClient.ContainerCreate(
		ctx,
		containerConfig,
		hostConfig,
		nil,
		nil,
		config.DockerContainer,
	)
	if err != nil {
		return errors.NewServerError("failed to create Docker container", err)
	}

	// Start the container
	if err := m.dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		// Clean up the container if start fails
		_ = m.dockerClient.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return errors.NewServerError("failed to start Docker container", err)
	}

	// Wait for health check to pass
	healthURL := fmt.Sprintf("http://localhost:%d/actuator/health", config.Port)
	if err := m.waitForHealth(ctx, healthURL); err != nil {
		// Clean up the container if health check fails
		_ = m.dockerClient.ContainerStop(ctx, resp.ID, container.StopOptions{})
		_ = m.dockerClient.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return errors.NewServerError("server failed health check", err)
	}

	return nil
}

// Stop stops and removes a Docker container
func (m *Manager) Stop(ctx context.Context, containerName string) error {
	// Stop the container
	timeout := 10
	stopOptions := container.StopOptions{
		Timeout: &timeout,
	}
	if err := m.dockerClient.ContainerStop(ctx, containerName, stopOptions); err != nil {
		return errors.NewServerError("failed to stop Docker container", err)
	}

	// Remove the container
	if err := m.dockerClient.ContainerRemove(ctx, containerName, container.RemoveOptions{}); err != nil {
		return errors.NewServerError("failed to remove Docker container", err)
	}

	return nil
}

// Status queries the current status of a Docker container
func (m *Manager) Status(ctx context.Context, containerName string) (interfaces.ServerStatus, error) {
	status := interfaces.ServerStatus{
		Running:       false,
		Healthy:       false,
		ContainerName: containerName,
	}

	// Inspect the container
	containerJSON, err := m.dockerClient.ContainerInspect(ctx, containerName)
	if err != nil {
		// Container doesn't exist or can't be inspected
		if client.IsErrNotFound(err) {
			return status, nil
		}
		return status, errors.NewServerError("failed to inspect Docker container", err)
	}

	// Check if container is running
	status.Running = containerJSON.State.Running

	// If running, check health
	if status.Running {
		// Try to get the port
		if bindings, ok := containerJSON.NetworkSettings.Ports["8080/tcp"]; ok && len(bindings) > 0 {
			// Port is bound, we can check health
			healthURL := fmt.Sprintf("http://localhost:%s/actuator/health", bindings[0].HostPort)
			if err := m.HealthCheck(ctx, healthURL); err == nil {
				status.Healthy = true
			}
		}
	}

	return status, nil
}

// HealthCheck performs a health check on the given endpoint
func (m *Manager) HealthCheck(ctx context.Context, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return errors.NewServerError("failed to create health check request", err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return errors.NewServerError("health check request failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.NewServerError(fmt.Sprintf("health check returned status %d", resp.StatusCode), nil)
	}

	return nil
}

// AutoStart automatically starts a stopped local server
func (m *Manager) AutoStart(ctx context.Context, config interfaces.ServerConfig) error {
	if SKIP_DOCKER {
		return nil
	}
	// Check if we're in local mode
	if config.Mode != interfaces.ModeLocal {
		return nil // Nothing to do for remote mode
	}

	// Check current status
	status, err := m.Status(ctx, config.DockerContainer)
	if err != nil {
		return err
	}

	// If already running and healthy, nothing to do
	if status.Running && status.Healthy {
		return nil
	}

	// If container exists but not running, remove it first
	if !status.Running {
		// Try to remove the container (ignore errors if it doesn't exist)
		_ = m.dockerClient.ContainerRemove(ctx, config.DockerContainer, container.RemoveOptions{Force: true})
	}

	// Start the server
	return m.Start(ctx, config)
}

// pullImageIfNeeded pulls a Docker image if it doesn't exist locally
func (m *Manager) pullImageIfNeeded(ctx context.Context, imageName string) error {
	// Check if image exists
	_, _, err := m.dockerClient.ImageInspectWithRaw(ctx, imageName)
	if err == nil {
		// Image exists
		return nil
	}

	// Image doesn't exist, pull it
	reader, err := m.dockerClient.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()

	// Wait for pull to complete
	_, err = io.Copy(io.Discard, reader)
	return err
}

// waitForHealth waits for the health check to pass with exponential backoff
func (m *Manager) waitForHealth(ctx context.Context, healthURL string) error {
	maxAttempts := 10
	baseDelay := 500 * time.Millisecond
	maxDelay := 5 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := m.HealthCheck(ctx, healthURL); err == nil {
			return nil
		}

		// Calculate delay with exponential backoff
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}

		// Wait before next attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return errors.NewServerError(fmt.Sprintf("health check failed after %d attempts", maxAttempts), nil)
}

// Close closes the Docker client connection
func (m *Manager) Close() error {
	if m.dockerClient != nil {
		return m.dockerClient.Close()
	}
	return nil
}
