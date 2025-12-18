package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/onemcp/cli/internal/interfaces"
)

// Manager implements the TestManager interface for regression testing
type Manager struct {
	httpClient *http.Client
}

// NewManager creates a new test manager
func NewManager() *Manager {
	return &Manager{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// runRequest is a helper for the regression run endpoint request
type runRequest struct {
	Paths []string `json:"paths,omitempty"`
	// Note: context is intentionally omitted - the server handles null context
	// Sending an empty object {} causes NPE in ToolCallContext.getContext().stream()
}

// runResponse is the response from the run endpoint
type runResponse struct {
	JobId  string `json:"jobId"`
	Status string `json:"status"`
}

// RunTests starts regression tests on the server
func (m *Manager) RunTests(ctx context.Context, serverURL string, files []string) (string, error) {
	url := fmt.Sprintf("%s/mng/handbook/regression/run", serverURL)

	reqBody := runRequest{
		Paths: files,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	var result runResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return result.JobId, nil
}

// GetStatus returns the current status of a test job
func (m *Manager) GetStatus(ctx context.Context, serverURL string, jobId string) (*interfaces.JobStatus, error) {
	url := fmt.Sprintf("%s/mng/jobs/status/%s", serverURL, jobId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("job not found: %s", jobId)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	var status interfaces.JobStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

// GetResults returns the test results once complete
func (m *Manager) GetResults(ctx context.Context, serverURL string, jobId string) (*interfaces.TestResults, error) {
	url := fmt.Sprintf("%s/mng/handbook/regression/summary?id=%s", serverURL, jobId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 202 means still processing
	if resp.StatusCode == http.StatusAccepted {
		return nil, fmt.Errorf("tests still running")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	var results interfaces.TestResults
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &results, nil
}

// CancelJob cancels a running test job
func (m *Manager) CancelJob(ctx context.Context, serverURL string, jobId string) error {
	url := fmt.Sprintf("%s/mng/jobs/%s", serverURL, jobId)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("job not found: %s", jobId)
	}

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
