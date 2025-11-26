package handbook

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gentoro/onemcp/go-cli/pkg/config"
	"gopkg.in/yaml.v3"
)

type Manager struct {
	configManager *config.Manager
	baseDir       string
}

func NewManager() *Manager {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return &Manager{
		configManager: config.GetManager(),
		baseDir:       filepath.Join(home, "onemcp-handbooks"),
	}
}

func (m *Manager) Init(name, source string) (string, error) {
	dir := filepath.Join(m.baseDir, name)

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return "", fmt.Errorf("handbook directory already exists: %s", dir)
	}

	fmt.Printf("Creating handbook at %s...\n", dir)

	dirs := []string{
		dir,
		filepath.Join(dir, "openapi"),
		filepath.Join(dir, "docs"),
		filepath.Join(dir, "data"),
		filepath.Join(dir, "config"),
	}

	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return "", err
		}
	}

	// Create instructions.md (required by server)
	instructionsMd := `# Agent Instructions

This is your OneMCP handbook. This file guides your AI agent's behavior and understanding.

## Purpose

Describe what your agent does and its primary responsibilities.

## Available Services

List the APIs and services your agent can interact with. These should have corresponding OpenAPI definitions in the openapi/ directory.

## Behavior Guidelines

Define how your agent should respond to requests, handle errors, and interact with users.

## Data Sources

If you have data files (CSVs, etc.), describe them here and place them in the data/ directory.
`
	if err := os.WriteFile(filepath.Join(dir, "instructions.md"), []byte(instructionsMd), 0644); err != nil {
		return "", err
	}

	// Create README.md
	readmeMd := fmt.Sprintf(`# %s Handbook

Welcome to your OneMCP handbook! This handbook guides your AI agent in understanding and interacting with your services.

## Quick Start

1. **Add API Definitions**: Place your OpenAPI YAML files in `+"`openapi/`"+`
2. **Add Documentation**: Add guides and documentation in `+"`docs/`"+`
3. **Add Data**: Place any CSV or data files in `+"`data/`"+`
4. **Configure Settings**: Update handbook config in `+"`config/handbook.yaml`"+`

## Structure

- `+"`instructions.md`"+` - Main agent instructions
- `+"`openapi/`"+` - OpenAPI specifications for your services
- `+"`docs/`"+` - Additional documentation and guides
- `+"`data/`"+` - Data files (CSV, JSON, etc.)
- `+"`config/`"+` - Handbook configuration

## Validation

Validate your handbook structure:
`+"```bash\nonemcp handbook validate %s\n```"+`

## Next Steps

1. Add your OpenAPI YAML files to the `+"`openapi/`"+` directory
2. Update `+"`instructions.md`"+` with your agent's specific instructions
3. Add relevant documentation to `+"`docs/`"+`
4. Run validation to ensure everything is set up correctly
5. Start chatting: `+"`onemcp chat`"+`
`, name, name)
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(readmeMd), 0644); err != nil {
		return "", err
	}

	// Create example documentation
	exampleDoc := `# Getting Started

## Overview

Add your service documentation here.

## Authentication

Describe how authentication works with your services.

## Common Operations

Document common operations and use cases.

## Examples

Provide example requests and responses.
`
	if err := os.WriteFile(filepath.Join(dir, "docs", "getting-started.md"), []byte(exampleDoc), 0644); err != nil {
		return "", err
	}

	// Create openapi directory placeholder
	openApiReadme := `# OpenAPI Definitions

Place your OpenAPI YAML files here.

## File Naming

- Use descriptive names: ` + "`my-api.yaml`" + `
- One service per file
- Must have ` + "`.yaml`" + ` extension

## Example Structure

` + "```yaml\nopenapi: 3.0.0\ninfo:\n  title: My API\n  version: 1.0.0\npaths:\n  /example:\n    get:\n      summary: Example endpoint\n      responses:\n        '200':\n          description: Success\n```" + `

## Resources

- [OpenAPI Specification](https://swagger.io/specification/)
- [OpenAPI Examples](https://github.com/OAI/OpenAPI-Specification/tree/main/examples)
`
	if err := os.WriteFile(filepath.Join(dir, "openapi", "README.md"), []byte(openApiReadme), 0644); err != nil {
		return "", err
	}

	// Create config
	defaultConfig := config.HandbookConfig{
		Name:    name,
		Version: "1.0.0",
	}
	configData, _ := yaml.Marshal(defaultConfig)
	if err := os.WriteFile(filepath.Join(dir, "config", "handbook.yaml"), configData, 0644); err != nil {
		return "", err
	}

	return dir, nil
}

func (m *Manager) Validate(dir string) (bool, []string) {
	var errors []string

	// Check for instructions.md (required by server)
	if _, err := os.Stat(filepath.Join(dir, "instructions.md")); os.IsNotExist(err) {
		errors = append(errors, "Missing required file: instructions.md")
	}

	// Check for openapi/ directory (required by server)
	openapiDir := filepath.Join(dir, "openapi")
	if stat, err := os.Stat(openapiDir); os.IsNotExist(err) || !stat.IsDir() {
		errors = append(errors, "Missing required directory: openapi/")
	} else {
		// Check if openapi/ has at least one .yaml file
		entries, _ := os.ReadDir(openapiDir)
		hasYaml := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".yaml") {
				hasYaml = true
				break
			}
		}
		if !hasYaml {
			errors = append(errors, "openapi/ directory must contain at least one .yaml file")
		}
	}

	return len(errors) == 0, errors
}

func (m *Manager) List() ([]config.HandbookConfig, error) {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		return nil, err
	}

	var handbooks []config.HandbookConfig
	for _, e := range entries {
		if e.IsDir() {
			hbConfig, err := m.configManager.LoadHandbookConfig(e.Name())
			if err == nil {
				handbooks = append(handbooks, *hbConfig)
			} else {
				// minimal config if load fails
				handbooks = append(handbooks, config.HandbookConfig{Name: e.Name(), Version: "unknown"})
			}
		}
	}
	return handbooks, nil
}

func defaultAgentInstructions(name string) string {
	return fmt.Sprintf(`# %s Agent Instructions

## Purpose
Define your agent's purpose and primary responsibilities here.

## Scope
Describe what your agent can and cannot do.
`, name)
}

func defaultReadme(name string) string {
	return fmt.Sprintf(`# %s
OneMCP handbook for %s.
`, name, name)
}
