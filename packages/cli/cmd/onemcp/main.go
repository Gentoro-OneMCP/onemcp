package main

import (
	"errors"
	"os"

	"github.com/onemcp/cli/internal/cli"
	clierrors "github.com/onemcp/cli/internal/errors"
)

func main() {
	if err := cli.Execute(); err != nil {
		// Extract error code from CLIError if present
		var cliErr *clierrors.CLIError
		if errors.As(err, &cliErr) {
			os.Exit(int(cliErr.Code))
		}
		// Default to exit code 1 for non-CLI errors
		os.Exit(1)
	}
}
