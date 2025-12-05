package interfaces

import (
	"context"
	"time"
)

// ProjectManager handles project lifecycle, directory structure validation, and context detection
type ProjectManager interface {
	Initialize(ctx context.Context, opts InitOptions) error
	FindProjectRoot(startDir string) (string, error)
	LoadConfig(projectRoot string) (*ProjectConfig, error)
	ValidateDirectory(dir string, mode InitMode) (DirectoryState, error)
}

// InitOptions contains parameters for project initialization
type InitOptions struct {
	TargetDirectory string
	ServerURL       string
	Mode            ProjectMode
	Model           LLMModel
	ModelAPIKey     string
	Interactive     bool
	ServerManager   ServerManager
	HandbookManager HandbookManager
	StateManager    StateManager
	TcpPort         int
	InitMode        InitMode
}

// LLMModel represents the LLM model type
type LLMModel string

const (
	ModelOpenAI    LLMModel = "OPENAI"
	ModelGemini    LLMModel = "GEMINI"
	ModelAnthropic LLMModel = "ANTHROPIC"
	ModelOllama    LLMModel = "OLLAMA"
	ModelUnset     LLMModel = ""
)

// DirectoryState represents the state of a directory during validation
type DirectoryState int

const (
	DirectoryEmpty DirectoryState = iota
	DirectoryValidStructure
	DirectoryInvalid
)

// ProjectMode represents the operational mode of a project
type ProjectMode string

const (
	ModeLocal  ProjectMode = "local"
	ModeRemote ProjectMode = "remote"
)

// InitMode represents the initialization mode
type InitMode string

const (
	InitModeLocal  InitMode = "local"
	InitModeRemote InitMode = "remote"
)

// ProjectConfig represents the project configuration
type ProjectConfig struct {
	Version  int            `yaml:"version" mapstructure:"version"`
	Server   ServerConfig   `yaml:"server" mapstructure:"server"`
	Handbook HandbookConfig `yaml:"handbook" mapstructure:"handbook"`
}

// ServerConfig contains server-related configuration
type ServerConfig struct {
	Mode            ProjectMode `yaml:"mode" mapstructure:"mode"`
	URL             string      `yaml:"url,omitempty" mapstructure:"url"`
	DockerContainer string      `yaml:"docker_container,omitempty" mapstructure:"docker_container"`
	Port            int         `yaml:"port,omitempty" mapstructure:"port"`
	Model           LLMModel    `yaml:"model,omitempty" mapstructure:"model"`
	ModelAPIKey     string      `yaml:"model_api_key,omitempty" mapstructure:"model_api_key"`
}

// HandbookConfig contains handbook-related configuration
type HandbookConfig struct {
	Source    string     `yaml:"source" mapstructure:"source"`
	CreatedAt time.Time  `yaml:"created_at" mapstructure:"created_at"`
	LastSync  *time.Time `yaml:"last_sync,omitempty" mapstructure:"last_sync"`
}
