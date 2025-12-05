package errors

import "fmt"

// ErrorCode represents the CLI error codes
type ErrorCode int

const (
	// CodeGeneric represents a generic failure (code 1)
	CodeGeneric ErrorCode = 1
	// CodeInvalidDirectory represents invalid directory conditions (code 2)
	CodeInvalidDirectory ErrorCode = 2
	// CodeServerFailure represents Docker or server startup failures (code 3)
	CodeServerFailure ErrorCode = 3
	// CodeSyncFailure represents handbook synchronization failures (code 4)
	CodeSyncFailure ErrorCode = 4
	// CodeNotInProject represents commands executed outside project context (code 5)
	CodeNotInProject ErrorCode = 5
)

// CLIError represents a CLI error with a specific error code
type CLIError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *CLIError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *CLIError) Unwrap() error {
	return e.Cause
}

// NewGenericError creates a new generic error (code 1)
func NewGenericError(message string, cause error) *CLIError {
	return &CLIError{
		Code:    CodeGeneric,
		Message: message,
		Cause:   cause,
	}
}

// NewValidationError creates a new validation error (code 2)
func NewValidationError(message string) *CLIError {
	return &CLIError{
		Code:    CodeInvalidDirectory,
		Message: message,
	}
}

// NewServerError creates a new server error (code 3)
func NewServerError(message string, cause error) *CLIError {
	return &CLIError{
		Code:    CodeServerFailure,
		Message: message,
		Cause:   cause,
	}
}

// NewSyncError creates a new sync error (code 4)
func NewSyncError(message string, cause error) *CLIError {
	return &CLIError{
		Code:    CodeSyncFailure,
		Message: message,
		Cause:   cause,
	}
}

// NewContextError creates a new context error (code 5)
func NewContextError(message string) *CLIError {
	return &CLIError{
		Code:    CodeNotInProject,
		Message: message,
	}
}
