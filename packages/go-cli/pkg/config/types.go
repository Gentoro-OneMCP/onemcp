package config

// ModelProvider represents the AI model provider
type ModelProvider string

const (
	ProviderOpenAI    ModelProvider = "openai"
	ProviderGemini    ModelProvider = "gemini"
	ProviderAnthropic ModelProvider = "anthropic"
)

// GlobalConfig represents the global configuration in ~/.onemcp/config.yaml
type GlobalConfig struct {
	Provider        ModelProvider     `mapstructure:"provider" yaml:"provider"`
	APIKeys         map[string]string `mapstructure:"apikeys" yaml:"apikeys"`
	DefaultPort     int               `mapstructure:"defaultport" yaml:"defaultport"`
	HandbookDir     string            `mapstructure:"handbookdir" yaml:"handbookdir"`
	CurrentHandbook string            `mapstructure:"currenthandbook" yaml:"currenthandbook"`
	ChatTimeout     int               `mapstructure:"chattimeout" yaml:"chattimeout"` // timeout in seconds for MCP requests
}

// ServiceConfig represents a service configuration
type ServiceConfig struct {
	Service   string `yaml:"service"`
	Header    string `yaml:"header"`
	Pattern   string `yaml:"pattern"`
	Token     string `yaml:"token"`
	ExpiresAt string `yaml:"expiresAt,omitempty"`
	BaseURL   string `yaml:"baseUrl,omitempty"`
}

// HandbookConfig represents a handbook's configuration
type HandbookConfig struct {
	Name        string             `yaml:"name"`
	Version     string             `yaml:"version"`
	Description string             `yaml:"description,omitempty"`
	Services    []ServiceReference `yaml:"services"`
	Provider    ModelProvider      `yaml:"provider,omitempty"`
	APIKeys     map[string]string  `yaml:"apiKeys,omitempty"`
	ModelName   string             `yaml:"modelName,omitempty"`
	BaseURL     string             `yaml:"baseUrl,omitempty"`
	ChatTimeout int                `yaml:"chatTimeout,omitempty"`
}

// ServiceReference represents a service reference in a handbook
type ServiceReference struct {
	Name        string `yaml:"name"`
	BaseURL     string `yaml:"baseUrl"`
	AuthType    string `yaml:"authType"` // bearer, api-key, custom
	OpenAPISpec string `yaml:"openApiSpec,omitempty"`
}
