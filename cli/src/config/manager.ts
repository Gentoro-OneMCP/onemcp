/**
 * Configuration manager for OneMCP CLI
 */
import fs from 'fs-extra';
import YAML from 'yaml';
import { GlobalConfig, ServiceConfig } from '../types.js';
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
      this.globalConfig = YAML.parse(content) as GlobalConfig;
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
      defaultPort: 8080,
      handbookDir: paths.handbooksDir,
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

    if (!config.apiKey || config.apiKey.trim() === '') {
      errors.push('API key is required');
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
}

export const configManager = ConfigManager.getInstance();

