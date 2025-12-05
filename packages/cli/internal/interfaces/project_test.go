package interfaces_test

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/onemcp/cli/internal/interfaces"
)

// TestPropertyBasedTestingSetup verifies that gopter is properly configured
func TestPropertyBasedTestingSetup(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("sample property: string concatenation is associative",
		prop.ForAll(
			func(a, b, c string) bool {
				return (a+b)+c == a+(b+c)
			},
			gen.AlphaString(),
			gen.AlphaString(),
			gen.AlphaString(),
		))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

// TestProjectModeConstants verifies project mode constants
func TestProjectModeConstants(t *testing.T) {
	if interfaces.ModeLocal != "local" {
		t.Errorf("ModeLocal should be 'local', got %s", interfaces.ModeLocal)
	}
	if interfaces.ModeRemote != "remote" {
		t.Errorf("ModeRemote should be 'remote', got %s", interfaces.ModeRemote)
	}
}

// TestDirectoryStateConstants verifies directory state constants
func TestDirectoryStateConstants(t *testing.T) {
	if interfaces.DirectoryEmpty != 0 {
		t.Errorf("DirectoryEmpty should be 0, got %d", interfaces.DirectoryEmpty)
	}
	if interfaces.DirectoryValidStructure != 1 {
		t.Errorf("DirectoryValidStructure should be 1, got %d", interfaces.DirectoryValidStructure)
	}
	if interfaces.DirectoryInvalid != 2 {
		t.Errorf("DirectoryInvalid should be 2, got %d", interfaces.DirectoryInvalid)
	}
}
