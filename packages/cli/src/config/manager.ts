/**
 * Configuration manager for OneMCP CLI
 */
import fs from 'fs-extra';
import YAML from 'yaml';
import { GlobalConfig, ServiceConfig, HandbookConfig, HandbookInfo } from '../types.js';
import { paths } from './paths.js';

export class ConfigManager {
  private static instance: ConfigManager;
  private globalConfig?: GlobalConfig;

  private constructor() {}

  static getInstance(): ConfigManager {
    if (!ConfigManager.instance) {
      ConfigManager.instance = new ConfigManager();
    }
    return ConfigManager.instance;
  }

  /**
   * Load global configuration
   */
  async loadGlobalConfig(): Promise<GlobalConfig | null> {
    if (!(await paths.configExists())) {
      return null;
    }

    try {
      const content = await fs.readFile(paths.configFile, 'utf-8');
      const config = YAML.parse(content) as any;

      // Migrate old apiKey field to new apiKeys structure
      if (config.apiKey && !config.apiKeys) {
        config.apiKeys = {
          [config.provider]: config.apiKey
        };
        delete config.apiKey;

        // Save the migrated config
        await this.saveGlobalConfig(config);
      }

      this.globalConfig = config as GlobalConfig;
      return this.globalConfig;
    } catch (error) {
      throw new Error(`Failed to load config: ${error}`);
    }
  }

  /**
   * Save global configuration
   */
  async saveGlobalConfig(config: GlobalConfig): Promise<void> {
    await paths.ensureDirectories();
    
    const yamlContent = YAML.stringify(config);
    await fs.writeFile(paths.configFile, yamlContent, 'utf-8');
    
    this.globalConfig = config;
  }

  /**
   * Get cached global config or load it
   */
  async getGlobalConfig(): Promise<GlobalConfig | null> {
    if (this.globalConfig) {
      return this.globalConfig;
    }
    return this.loadGlobalConfig();
  }

  /**
   * Get the current API key for the configured provider
   */
  async getCurrentApiKey(): Promise<string | null> {
    const config = await this.getGlobalConfig();
    if (!config) return null;
    return config.apiKeys[config.provider] || null;
  }

  /**
   * Get API key for a specific provider
   */
  async getApiKeyForProvider(provider: string): Promise<string | null> {
    const config = await this.getGlobalConfig();
    if (!config || !config.apiKeys) return null;
    return config.apiKeys[provider as keyof typeof config.apiKeys] || null;
  }

  /**
   * Set API key for a specific provider
   */
  async setApiKeyForProvider(provider: string, apiKey: string): Promise<void> {
    const config = await this.getGlobalConfig();
    if (!config) {
      throw new Error('No configuration found. Please run setup first.');
    }

    const updated = {
      ...config,
      apiKeys: {
        ...config.apiKeys,
        [provider]: apiKey
      }
    };
    await this.saveGlobalConfig(updated);
  }

  /**
   * Update specific global config fields
   */
  async updateGlobalConfig(updates: Partial<GlobalConfig>): Promise<void> {
    const current = await this.getGlobalConfig();
    if (!current) {
      throw new Error('No configuration found. Please run setup first.');
    }

    const updated = { ...current, ...updates };
    await this.saveGlobalConfig(updated);
  }

  /**
   * Load service configuration
   */
  async loadServiceConfig(serviceName: string): Promise<ServiceConfig | null> {
    const configPath = paths.getServiceConfigPath(serviceName);
    
    if (!(await fs.pathExists(configPath))) {
      return null;
    }

    try {
      const content = await fs.readFile(configPath, 'utf-8');
      return YAML.parse(content) as ServiceConfig;
    } catch (error) {
      throw new Error(`Failed to load service config for ${serviceName}: ${error}`);
    }
  }

  /**
   * Save service configuration
   */
  async saveServiceConfig(serviceName: string, config: ServiceConfig): Promise<void> {
    await paths.ensureDirectories();
    
    const configPath = paths.getServiceConfigPath(serviceName);
    const yamlContent = YAML.stringify(config);
    await fs.writeFile(configPath, yamlContent, 'utf-8');
  }

  /**
   * List all configured services
   */
  async listServices(): Promise<string[]> {
    await paths.ensureDirectories();
    
    const files = await fs.readdir(paths.servicesDir);
    return files
      .filter((f) => f.endsWith('.yaml'))
      .map((f) => f.replace('.yaml', ''));
  }

  /**
   * Delete service configuration
   */
  async deleteServiceConfig(serviceName: string): Promise<void> {
    const configPath = paths.getServiceConfigPath(serviceName);
    if (await fs.pathExists(configPath)) {
      await fs.remove(configPath);
    }
  }

  /**
   * Check if any configuration exists
   */
  async hasConfiguration(): Promise<boolean> {
    return paths.configExists();
  }

  /**
   * Get default configuration
   */
  getDefaultConfig(): Partial<GlobalConfig> {
    return {
      provider: 'openai',
      apiKeys: {},
      defaultPort: 8080,
      handbookDir: paths.handbooksDir, // Parent directory containing all handbooks
      currentHandbook: undefined, // No current handbook set initially
      logDir: paths.logDir,
      chatTimeout: 240000, // 4 minutes default timeout for chat requests
    };
  }

  /**
   * Validate global configuration
   */
  validateGlobalConfig(config: GlobalConfig): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!config.provider) {
      errors.push('Provider is required');
    }

    if (!['openai', 'gemini', 'anthropic'].includes(config.provider)) {
      errors.push('Provider must be one of: openai, gemini, anthropic');
    }

    // Check that the current provider has an API key
    const currentApiKey = config.apiKeys?.[config.provider];
    if (!currentApiKey || currentApiKey.trim() === '') {
      errors.push(`API key for ${config.provider} is required`);
    }

    if (config.defaultPort && (config.defaultPort < 1 || config.defaultPort > 65535)) {
      errors.push('Port must be between 1 and 65535');
    }

    if (config.chatTimeout !== undefined && (config.chatTimeout < 1000 || config.chatTimeout > 600000)) {
      errors.push('Chat timeout must be between 1000ms (1 second) and 600000ms (10 minutes)');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  /**
   * Reset all configuration (for re-running setup wizard)
   */
  async resetConfiguration(): Promise<void> {
    try {
      // Remove global config file
      if (await fs.pathExists(paths.configFile)) {
        await fs.remove(paths.configFile);
      }

      // Remove all service configs
      if (await fs.pathExists(paths.servicesDir)) {
        await fs.emptyDir(paths.servicesDir);
      }

      // Clear cached config
      this.globalConfig = undefined;
    } catch (error) {
      throw new Error(`Failed to reset configuration: ${error}`);
    }
  }

  /**
   * Validate service configuration
   */
  validateServiceConfig(config: ServiceConfig): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!config.service) {
      errors.push('Service name is required');
    }

    if (!config.header) {
      errors.push('Header name is required');
    }

    if (!config.pattern) {
      errors.push('Pattern is required');
    }

    if (!config.token) {
      errors.push('Token is required');
    }

    if (config.pattern && !config.pattern.includes('{token}')) {
      errors.push('Pattern must include {token} placeholder');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  /**
   * Load handbook configuration
   */
  async loadHandbookConfig(handbookName: string): Promise<HandbookConfig | null> {
    const handbookPath = paths.getHandbookPath(handbookName);
    const configPath = `${handbookPath}/config/handbook.yaml`;

    if (!(await fs.pathExists(configPath))) {
      return null;
    }

    try {
      const content = await fs.readFile(configPath, 'utf-8');
      const config = YAML.parse(content) as HandbookConfig;
      return config;
    } catch (error) {
      throw new Error(`Failed to load handbook config for ${handbookName}: ${error}`);
    }
  }

  /**
   * Save handbook configuration
   */
  async saveHandbookConfig(handbookName: string, config: HandbookConfig): Promise<void> {
    const handbookPath = paths.getHandbookPath(handbookName);
    const configPath = `${handbookPath}/config/handbook.yaml`;

    await fs.ensureDir(`${handbookPath}/config`);

    const yamlContent = YAML.stringify(config);
    await fs.writeFile(configPath, yamlContent, 'utf-8');
  }

  /**
   * Get handbook configuration with defaults from global config
   */
  async getEffectiveHandbookConfig(handbookName: string): Promise<HandbookConfig | null> {
    const handbookConfig = await this.loadHandbookConfig(handbookName);
    if (!handbookConfig) {
      return null;
    }

    const globalConfig = await this.getGlobalConfig();
    if (!globalConfig) {
      return handbookConfig;
    }

    // Merge global defaults with handbook-specific overrides
    return {
      ...handbookConfig,
      provider: handbookConfig.provider || globalConfig.provider,
      apiKeys: handbookConfig.apiKeys || globalConfig.apiKeys,
      modelName: handbookConfig.modelName || globalConfig.modelName,
      baseUrl: handbookConfig.baseUrl || globalConfig.baseUrl,
      chatTimeout: handbookConfig.chatTimeout || globalConfig.chatTimeout,
    };
  }

  /**
   * Validate handbook configuration
   */
  validateHandbookConfig(config: HandbookConfig): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!config.name) {
      errors.push('Handbook name is required');
    }

    if (!config.version) {
      errors.push('Version is required');
    }

    if (config.provider && !['openai', 'gemini', 'anthropic'].includes(config.provider)) {
      errors.push('Provider must be one of: openai, gemini, anthropic');
    }

    // Check that the current provider has an API key if configured
    if (config.provider && config.apiKeys) {
      const currentApiKey = config.apiKeys[config.provider];
      if (!currentApiKey || currentApiKey.trim() === '') {
        errors.push(`API key for ${config.provider} is required`);
      }
    }

    if (config.chatTimeout !== undefined && (config.chatTimeout < 1000 || config.chatTimeout > 600000)) {
      errors.push('Chat timeout must be between 1000ms (1 second) and 600000ms (10 minutes)');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  /**
   * Create default handbook configuration
   */
  createDefaultHandbookConfig(name: string): HandbookConfig {
    return {
      name,
      version: '1.0.0',
      description: `Handbook configuration for ${name}`,
      services: [],
    };
  }
}

export const configManager = ConfigManager.getInstance();

