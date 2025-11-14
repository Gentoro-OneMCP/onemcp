/**
 * Path management for CLI configuration and data
 */
import { homedir } from 'os';
import { join } from 'path';
import fs from 'fs-extra';

export class PathManager {
  private static instance: PathManager;
  
  readonly configRoot: string;
  readonly configFile: string;
  readonly servicesDir: string;
  readonly handbooksDir: string;
  readonly logDir: string;
  readonly stateDir: string;
  readonly cacheDir: string;

  private constructor() {
    this.configRoot = join(homedir(), '.onemcp');
    this.configFile = join(this.configRoot, 'config.yaml');
    this.servicesDir = join(this.configRoot, 'services');
    this.handbooksDir = join(homedir(), 'handbooks');
    this.logDir = join(this.configRoot, 'logs');
    this.stateDir = join(this.configRoot, 'state');
    this.cacheDir = join(this.configRoot, 'cache');
  }

  static getInstance(): PathManager {
    if (!PathManager.instance) {
      PathManager.instance = new PathManager();
    }
    return PathManager.instance;
  }

  /**
   * Ensure all required directories exist
   */
  async ensureDirectories(): Promise<void> {
    await fs.ensureDir(this.configRoot);
    await fs.ensureDir(this.servicesDir);
    await fs.ensureDir(this.handbooksDir);
    await fs.ensureDir(this.logDir);
    await fs.ensureDir(this.stateDir);
    await fs.ensureDir(this.cacheDir);
  }

  /**
   * Get service config file path
   */
  getServiceConfigPath(serviceName: string): string {
    return join(this.servicesDir, `${serviceName}.yaml`);
  }

  /**
   * Get handbook directory path
   */
  getHandbookPath(name: string): string {
    return join(this.handbooksDir, name);
  }

  /**
   * Get log file path for a service
   */
  getLogPath(service: string): string {
    return join(this.logDir, `${service}.log`);
  }

  /**
   * Get archive directory for logs
   */
  getLogArchivePath(): string {
    return join(this.logDir, 'archive');
  }

  /**
   * Get PID file path for a service
   */
  getPidFilePath(service: string): string {
    return join(this.stateDir, `${service}.pid`);
  }

  /**
   * Get socket file path for a service
   */
  getSocketPath(service: string): string {
    return join(this.stateDir, `${service}.sock`);
  }

  /**
   * Check if config file exists
   */
  async configExists(): Promise<boolean> {
    return fs.pathExists(this.configFile);
  }

  /**
   * Check if a service config exists
   */
  async serviceConfigExists(serviceName: string): Promise<boolean> {
    return fs.pathExists(this.getServiceConfigPath(serviceName));
  }

  /**
   * Check if handbook directory exists
   */
  async handbookExists(name: string): Promise<boolean> {
    return fs.pathExists(this.getHandbookPath(name));
  }
}

export const paths = PathManager.getInstance();

