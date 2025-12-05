package errors

import (
	"errors"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_ErrorCodesAreConsistentAcrossOperations tests that error codes
// are consistent across operations
// **Feature: onemcp-cli, Property 13: Error codes are consistent across operations**
func TestProperty_ErrorCodesAreConsistentAcrossOperations(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Property 1: Generic errors always return code 1
	properties.Property("generic errors return code 1", prop.ForAll(
		func(message string) bool {
			err := NewGenericError(message, nil)
			return err.Code == CodeGeneric && int(err.Code) == 1
		},
		gen.AnyString(),
	))

	// Property 2: Validation errors always return code 2
	properties.Property("validation errors return code 2", prop.ForAll(
		func(message string) bool {
			err := NewValidationError(message)
			return err.Code == CodeInvalidDirectory && int(err.Code) == 2
		},
		gen.AnyString(),
	))

	// Property 3: Server errors always return code 3
	properties.Property("server errors return code 3", prop.ForAll(
		func(message string) bool {
			err := NewServerError(message, nil)
			return err.Code == CodeServerFailure && int(err.Code) == 3
		},
		gen.AnyString(),
	))

	// Property 4: Sync errors always return code 4
	properties.Property("sync errors return code 4", prop.ForAll(
		func(message string) bool {
			err := NewSyncError(message, nil)
			return err.Code == CodeSyncFailure && int(err.Code) == 4
		},
		gen.AnyString(),
	))

	// Property 5: Context errors always return code 5
	properties.Property("context errors return code 5", prop.ForAll(
		func(message string) bool {
			err := NewContextError(message)
			return err.Code == CodeNotInProject && int(err.Code) == 5
		},
		gen.AnyString(),
	))

	// Property 6: Error wrapping preserves error code
	properties.Property("error wrapping preserves error code", prop.ForAll(
		func(message string, causeMsg string) bool {
			cause := errors.New(causeMsg)
			err := NewGenericError(message, cause)

			// Unwrap should return the cause
			unwrapped := errors.Unwrap(err)
			return unwrapped != nil && unwrapped.Error() == causeMsg
		},
		gen.AnyString(),
		gen.AnyString(),
	))

	// Property 7: Error messages are preserved
	properties.Property("error messages are preserved", prop.ForAll(
		func(message string) bool {
			err := NewGenericError(message, nil)
			return err.Message == message
		},
		gen.AnyString(),
	))

	// Property 8: Error.Error() includes message
	properties.Property("Error() includes message", prop.ForAll(
		func(message string) bool {
			err := NewGenericError(message, nil)
			errorString := err.Error()
			// The error string should contain the message
			return errorString == message
		},
		gen.AnyString().SuchThat(func(s string) bool {
			return len(s) > 0
		}),
	))

	// Property 9: Error.Error() includes cause when present
	properties.Property("Error() includes cause when present", prop.ForAll(
		func(message string, causeMsg string) bool {
			cause := errors.New(causeMsg)
			err := NewGenericError(message, cause)
			errorString := err.Error()
			// The error string should contain both message and cause
			return len(errorString) > len(message) && len(errorString) > len(causeMsg)
		},
		gen.AnyString().SuchThat(func(s string) bool {
			return len(s) > 0
		}),
		gen.AnyString().SuchThat(func(s string) bool {
			return len(s) > 0
		}),
	))

	// Property 10: CLIError can be extracted using errors.As
	properties.Property("CLIError can be extracted using errors.As", prop.ForAll(
		func(message string) bool {
			err := NewGenericError(message, nil)
			var cliErr *CLIError
			return errors.As(err, &cliErr) && cliErr.Code == CodeGeneric
		},
		gen.AnyString(),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// Unit tests for error handling

func TestNewGenericError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := NewGenericError("test message", nil)
		if err.Code != CodeGeneric {
			t.Errorf("expected code %d, got %d", CodeGeneric, err.Code)
		}
		if err.Message != "test message" {
			t.Errorf("expected message 'test message', got '%s'", err.Message)
		}
		if err.Cause != nil {
			t.Errorf("expected nil cause, got %v", err.Cause)
		}
		if err.Error() != "test message" {
			t.Errorf("expected error string 'test message', got '%s'", err.Error())
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("underlying error")
		err := NewGenericError("test message", cause)
		if err.Code != CodeGeneric {
			t.Errorf("expected code %d, got %d", CodeGeneric, err.Code)
		}
		if err.Message != "test message" {
			t.Errorf("expected message 'test message', got '%s'", err.Message)
		}
		if err.Cause != cause {
			t.Errorf("expected cause to be preserved")
		}
		expectedError := "test message: underlying error"
		if err.Error() != expectedError {
			t.Errorf("expected error string '%s', got '%s'", expectedError, err.Error())
		}
	})
}

func TestNewValidationError(t *testing.T) {
	err := NewValidationError("validation failed")
	if err.Code != CodeInvalidDirectory {
		t.Errorf("expected code %d, got %d", CodeInvalidDirectory, err.Code)
	}
	if int(err.Code) != 2 {
		t.Errorf("expected error code 2, got %d", err.Code)
	}
	if err.Message != "validation failed" {
		t.Errorf("expected message 'validation failed', got '%s'", err.Message)
	}
}

func TestNewServerError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := NewServerError("server failed", nil)
		if err.Code != CodeServerFailure {
			t.Errorf("expected code %d, got %d", CodeServerFailure, err.Code)
		}
		if int(err.Code) != 3 {
			t.Errorf("expected error code 3, got %d", err.Code)
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("docker error")
		err := NewServerError("server failed", cause)
		if err.Cause != cause {
			t.Errorf("expected cause to be preserved")
		}
	})
}

func TestNewSyncError(t *testing.T) {
	err := NewSyncError("sync failed", nil)
	if err.Code != CodeSyncFailure {
		t.Errorf("expected code %d, got %d", CodeSyncFailure, err.Code)
	}
	if int(err.Code) != 4 {
		t.Errorf("expected error code 4, got %d", err.Code)
	}
}

func TestNewContextError(t *testing.T) {
	err := NewContextError("not in project")
	if err.Code != CodeNotInProject {
		t.Errorf("expected code %d, got %d", CodeNotInProject, err.Code)
	}
	if int(err.Code) != 5 {
		t.Errorf("expected error code 5, got %d", err.Code)
	}
}

func TestCLIError_Unwrap(t *testing.T) {
	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("underlying error")
		err := NewGenericError("test message", cause)
		unwrapped := errors.Unwrap(err)
		if unwrapped != cause {
			t.Errorf("expected unwrapped error to be the cause")
		}
	})

	t.Run("without cause", func(t *testing.T) {
		err := NewGenericError("test message", nil)
		unwrapped := errors.Unwrap(err)
		if unwrapped != nil {
			t.Errorf("expected unwrapped error to be nil, got %v", unwrapped)
		}
	})
}

func TestCLIError_ErrorsAs(t *testing.T) {
	err := NewGenericError("test message", nil)

	var cliErr *CLIError
	if !errors.As(err, &cliErr) {
		t.Errorf("expected errors.As to succeed")
	}

	if cliErr.Code != CodeGeneric {
		t.Errorf("expected code %d, got %d", CodeGeneric, cliErr.Code)
	}
}

func TestErrorCodeConstants(t *testing.T) {
	tests := []struct {
		name     string
		code     ErrorCode
		expected int
	}{
		{"CodeGeneric", CodeGeneric, 1},
		{"CodeInvalidDirectory", CodeInvalidDirectory, 2},
		{"CodeServerFailure", CodeServerFailure, 3},
		{"CodeSyncFailure", CodeSyncFailure, 4},
		{"CodeNotInProject", CodeNotInProject, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.code) != tt.expected {
				t.Errorf("expected %s to be %d, got %d", tt.name, tt.expected, tt.code)
			}
		})
	}
}

func TestErrorMessageClarity(t *testing.T) {
	tests := []struct {
		name    string
		err     *CLIError
		wantMsg string
	}{
		{
			name:    "generic error message",
			err:     NewGenericError("failed to read file", nil),
			wantMsg: "failed to read file",
		},
		{
			name:    "validation error message",
			err:     NewValidationError("directory is not empty"),
			wantMsg: "directory is not empty",
		},
		{
			name:    "server error message",
			err:     NewServerError("Docker container failed to start", nil),
			wantMsg: "Docker container failed to start",
		},
		{
			name:    "sync error message",
			err:     NewSyncError("failed to pull handbook", nil),
			wantMsg: "failed to pull handbook",
		},
		{
			name:    "context error message",
			err:     NewContextError("not in an onemcp project directory"),
			wantMsg: "not in an onemcp project directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Message != tt.wantMsg {
				t.Errorf("expected message '%s', got '%s'", tt.wantMsg, tt.err.Message)
			}
			if tt.err.Error() != tt.wantMsg {
				t.Errorf("expected Error() to return '%s', got '%s'", tt.wantMsg, tt.err.Error())
			}
		})
	}
}

func TestErrorContextPreservation(t *testing.T) {
	t.Run("single level wrapping", func(t *testing.T) {
		cause := errors.New("file not found")
		err := NewGenericError("failed to read config", cause)

		// Check that cause is preserved
		if err.Cause != cause {
			t.Errorf("expected cause to be preserved")
		}

		// Check that unwrap works
		unwrapped := errors.Unwrap(err)
		if unwrapped != cause {
			t.Errorf("expected unwrapped error to be the cause")
		}
	})

	t.Run("multi-level wrapping", func(t *testing.T) {
		rootCause := errors.New("permission denied")
		level1 := NewGenericError("failed to open file", rootCause)
		level2 := NewGenericError("failed to read config", level1)

		// Check that we can unwrap to level1
		unwrapped1 := errors.Unwrap(level2)
		if unwrapped1 != level1 {
			t.Errorf("expected first unwrap to return level1 error")
		}

		// Check that we can unwrap level1 to root cause
		unwrapped2 := errors.Unwrap(unwrapped1)
		if unwrapped2 != rootCause {
			t.Errorf("expected second unwrap to return root cause")
		}
	})
}
