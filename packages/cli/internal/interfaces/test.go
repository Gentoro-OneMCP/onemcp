package interfaces

import "context"

// TestManager handles regression test execution
type TestManager interface {
	// RunTests starts regression tests and returns job ID
	RunTests(ctx context.Context, serverURL string, files []string) (string, error)

	// GetStatus returns the current status of a test job
	GetStatus(ctx context.Context, serverURL string, jobId string) (*JobStatus, error)

	// GetResults returns the test results once complete
	GetResults(ctx context.Context, serverURL string, jobId string) (*TestResults, error)

	// CancelJob cancels a running test job
	CancelJob(ctx context.Context, serverURL string, jobId string) error
}

// JobStatus represents the status of a running test job
type JobStatus struct {
	JobId     string `json:"jobId"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
	StartedAt string `json:"startedAt,omitempty"`
}

// TestResults represents the results of a completed test run
type TestResults struct {
	Passed     int        `json:"passed"`
	Failed     int        `json:"failed"`
	DurationMs int64      `json:"durationMs"`
	TestCases  []TestCase `json:"testCases"`
}

// TestCase represents a single test case result
type TestCase struct {
	Name       string `json:"name"`
	Passed     bool   `json:"passed"`
	Details    string `json:"details,omitempty"`
	DurationMs int64  `json:"durationMs"`
}
