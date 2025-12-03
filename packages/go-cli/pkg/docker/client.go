package docker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/gentoro/onemcp/go-cli/pkg/config"
)

const (
	ImageName     = "admingentoro/gentoro:latest"
	ContainerName = "onemcp-server"
)

type Manager struct {
	cli *client.Client
}

func NewManager() (*Manager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &Manager{cli: cli}, nil
}

func (m *Manager) EnsureImage(ctx context.Context, forcePull bool) error {
	// Check if image exists
	_, _, err := m.cli.ImageInspectWithRaw(ctx, ImageName)
	if err == nil && !forcePull {
		return nil
	}

	fmt.Printf("Pulling image %s...\n", ImageName)
	// Use types.ImagePullOptions
	reader, err := m.cli.ImagePull(ctx, ImageName, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()
	io.Copy(os.Stdout, reader) // Pipe pull output to stdout
	return nil
}

func (m *Manager) StartServer(ctx context.Context, cfg *config.GlobalConfig, handbookPath string, imageName string) error {
	// Use default if not provided
	if imageName == "" {
		imageName = ImageName
	}

	// Check if container exists
	inspect, err := m.cli.ContainerInspect(ctx, ContainerName)
	if err == nil {
		// Container exists - clean it up to ensure fresh config
		if inspect.State.Running {
			fmt.Printf("Stopping existing container %s...\n", ContainerName)
			timeout := 10
			if err := m.cli.ContainerStop(ctx, ContainerName, container.StopOptions{Timeout: &timeout}); err != nil {
				// ignore error if already stopped
			}
		}
		// Always remove so we can recreate with fresh config
		m.cli.ContainerRemove(ctx, ContainerName, container.RemoveOptions{Force: true})
	}

	// Prepare Env
	env := []string{
		fmt.Sprintf("SERVER_PORT=%d", cfg.DefaultPort),
		fmt.Sprintf("INFERENCE_DEFAULT_PROVIDER=%s", cfg.Provider),
	}

	// Add API Keys
	for k, v := range cfg.APIKeys {
		if k == "openai" {
			env = append(env, fmt.Sprintf("OPENAI_API_KEY=%s", v))
		} else if k == "gemini" {
			env = append(env, fmt.Sprintf("GEMINI_API_KEY=%s", v))
		} else if k == "anthropic" {
			env = append(env, fmt.Sprintf("ANTHROPIC_API_KEY=%s", v))
		}
	}

	// Mount handbook only for custom handbooks (not built-in ACME)
	var mounts []mount.Mount
	if cfg.CurrentHandbook != "acme-analytics" {
		// For custom handbooks, set HANDBOOK_DIR and mount the handbook directory
		env = append(env, "HANDBOOK_DIR=/app/handbook")
		mounts = []mount.Mount{{
			Type:   mount.TypeBind,
			Source: handbookPath,
			Target: "/app/handbook",
		}}
	}
	// For ACME, don't set HANDBOOK_DIR - server uses default: classpath:acme-handbook

	// Host Config
	portBinding := nat.PortBinding{
		HostIP:   "0.0.0.0",
		HostPort: fmt.Sprintf("%d", cfg.DefaultPort),
	}
	portMap := nat.PortMap{
		nat.Port("8080/tcp"): []nat.PortBinding{portBinding},
	}

	resp, err := m.cli.ContainerCreate(ctx, &container.Config{
		Image: imageName, // Use provided image name
		Env:   env,
		ExposedPorts: nat.PortSet{
			nat.Port("8080/tcp"): struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: portMap,
		Mounts:       mounts,
	}, nil, nil, ContainerName)

	if err != nil {
		return err
	}

	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return err
	}

	fmt.Printf("Container %s started. ID: %s\n", ContainerName, resp.ID)
	return nil
}

func (m *Manager) StopServer(ctx context.Context) error {
	timeout := 10
	return m.cli.ContainerStop(ctx, ContainerName, container.StopOptions{Timeout: &timeout})
}

func (m *Manager) IsRunning(ctx context.Context) bool {
	inspect, err := m.cli.ContainerInspect(ctx, ContainerName)
	if err != nil {
		return false
	}
	return inspect.State.Running
}

func (m *Manager) WaitForHealthy(ctx context.Context, port int, timeout time.Duration) error {
	url := fmt.Sprintf("http://localhost:%d/actuator/health", port)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			continue
		}
	}

	return fmt.Errorf("server did not become healthy within %s", timeout)
}

// GetContainerInfo returns container inspection details
func (m *Manager) GetContainerInfo(ctx context.Context, name string) (*types.ContainerJSON, error) {
	inspect, err := m.cli.ContainerInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	return &inspect, nil
}

// GetLogs retrieves logs from a container
func (m *Manager) GetLogs(ctx context.Context, name string, tail int, follow bool) (io.ReadCloser, error) {
	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
		Tail:       fmt.Sprintf("%d", tail),
	}

	return m.cli.ContainerLogs(ctx, name, options)
}
