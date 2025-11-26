package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Manager struct {
	globalConfig *GlobalConfig
	configDir    string
}

var instance *Manager

func GetManager() *Manager {
	if instance == nil {
		homeDir, _ := os.UserHomeDir()
		instance = &Manager{
			configDir: filepath.Join(homeDir, ".onemcp"),
		}
	}
	return instance
}

func (m *Manager) LoadGlobalConfig() (*GlobalConfig, error) {
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return nil, nil
		}
		return nil, err
	}

	var config GlobalConfig
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}
	m.globalConfig = &config
	return &config, nil
}

func (m *Manager) SaveGlobalConfig(config *GlobalConfig) error {
	viper.SetDefault("provider", "gemini")
	viper.SetDefault("defaultport", 8080)
	viper.SetDefault("chattimeout", 240) // 240 seconds timeout

	viper.Set("provider", config.Provider)
	viper.Set("apiKeys", config.APIKeys)
	viper.Set("defaultPort", config.DefaultPort)
	viper.Set("handbookDir", config.HandbookDir)
	viper.Set("currentHandbook", config.CurrentHandbook)
	viper.Set("chatTimeout", config.ChatTimeout)

	// Ensure directory exists
	configPath := viper.ConfigFileUsed()
	if configPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		configDir := filepath.Join(home, ".onemcp")
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return err
		}
		configPath = filepath.Join(configDir, "config.yaml")
	}

	return viper.WriteConfigAs(configPath)
}

func (m *Manager) GetHandbookPath(name string) string {
	cfg, err := m.LoadGlobalConfig()
	if err != nil || cfg == nil {
		// Fallback to default
		homeDir, _ := os.UserHomeDir()
		return filepath.Join(homeDir, "onemcp-handbooks", name)
	}
	return filepath.Join(cfg.HandbookDir, name)
}

// Service Configuration Methods

func (m *Manager) LoadServiceConfig(serviceName string) (*ServiceConfig, error) {
	servicesDir := filepath.Join(m.configDir, "services")
	configPath := filepath.Join(servicesDir, serviceName+".yaml")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("service %s not found", serviceName)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config ServiceConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func (m *Manager) SaveServiceConfig(serviceName string, config *ServiceConfig) error {
	servicesDir := filepath.Join(m.configDir, "services")

	// Ensure services directory exists
	if err := os.MkdirAll(servicesDir, 0755); err != nil {
		return err
	}

	configPath := filepath.Join(servicesDir, serviceName+".yaml")

	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	return os.WriteFile(configPath, data, 0600) // 0600 for security (contains tokens)
}

func (m *Manager) ListServices() ([]string, error) {
	servicesDir := filepath.Join(m.configDir, "services")

	// Check if services directory exists
	if _, err := os.Stat(servicesDir); os.IsNotExist(err) {
		return []string{}, nil
	}

	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, err
	}

	var services []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			serviceName := strings.TrimSuffix(entry.Name(), ".yaml")
			services = append(services, serviceName)
		}
	}

	return services, nil
}

func (m *Manager) LoadHandbookConfig(name string) (*HandbookConfig, error) {
	path := m.GetHandbookPath(name)
	configPath := filepath.Join(path, "config", "handbook.yaml")

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config HandbookConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// ResetConfiguration deletes the entire config directory
func (m *Manager) ResetConfiguration() error {
	// Use the manager's configDir
	return os.RemoveAll(m.configDir)
}

func (m *Manager) HasConfiguration() bool {
	if err := viper.ReadInConfig(); err != nil {
		_, ok := err.(viper.ConfigFileNotFoundError)
		return !ok
	}
	return true
}
