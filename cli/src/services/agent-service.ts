/**
 * OneMCP service manager
 */
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { homedir } from 'os';
import chalk from 'chalk';
import { processManager, ProcessConfig } from './process-manager.js';
import { configManager } from '../config/manager.js';
import { paths } from '../config/paths.js';
import { AgentStatus } from '../types.js';
import fs from 'fs-extra';

export interface StartOptions {
  port?: number;
  handbookDir?: string;
  provider?: string;
  apiKey?: string;
}

export class AgentService {
  private initialized = false;

  /**
   * Find the project root directory
   */
  private findProjectRoot(): string {
    // Check if ONEMCP_ROOT environment variable is set
    if (process.env.ONEMCP_ROOT) {
      return process.env.ONEMCP_ROOT;
    }

    // Try to find project root by looking for known markers
    const cwd = process.cwd();
    const possibleRoots = [
      cwd, // Current directory (when running from repo root)
      join(cwd, '..'), // Parent directory (if running from cli/)
      join(homedir(), '.onemcp-src'), // Install script location
    ];

    // Add installed path derived from this module location (works for install.sh)
    try {
      const thisFile = fileURLToPath(import.meta.url); // .../cli/dist/services/agent-service.js
      const servicesDir = dirname(thisFile); // .../cli/dist/services
      const distDir = dirname(servicesDir);  // .../cli/dist
      const cliDir = dirname(distDir);       // .../cli
      const repoRoot = dirname(cliDir);      // ... (project root)
      possibleRoots.push(repoRoot);
    } catch {
      // ignore if import.meta.url is unavailable
    }

    // Also check ONEMCP_SRC if it's set (for development)
    if (process.env.ONEMCP_SRC) {
      possibleRoots.unshift(process.env.ONEMCP_SRC);
    }

    for (const root of possibleRoots) {
      const pomPath = join(root, 'src/onemcp/pom.xml');
      if (fs.existsSync(pomPath)) {
        return root;
      }
    }

    throw new Error(
      'Could not find OneMCP installation. Please ensure you are running from the project directory or set ONEMCP_ROOT environment variable.'
    );
  }

  /**
   * Initialize service definitions
   */
  private async initialize(): Promise<void> {
    if (this.initialized) return;

    // Get configuration
    const config = await configManager.getGlobalConfig();
    const port = config?.defaultPort || 8080;

    // Find project root
    const projectRoot = this.findProjectRoot();

    const onemcpJar = await this.resolveOnemcpJar(projectRoot);
    const activeProfile = this.resolveActiveProfile(config?.provider);
    const javaArgs = this.buildJavaArgs(onemcpJar, activeProfile);

    processManager.register({
      name: 'app',
      command: 'java',
      args: javaArgs,
      env: {
        SERVER_PORT: port.toString(),
        FOUNDATION_DIR: config?.handbookDir || paths.handbooksDir,
        OPENAI_API_KEY: config?.apiKeys?.openai || '',
        GEMINI_API_KEY: config?.apiKeys?.gemini || '',
        ANTHROPIC_API_KEY: config?.apiKeys?.anthropic || '',
        INFERENCE_DEFAULT_PROVIDER: config?.provider || 'openai',
        LLM_ACTIVE_PROFILE: activeProfile,
      },
      port,
      healthCheckUrl: `http://localhost:${port}/mcp`,
    });

    this.initialized = true;
  }

  /**
   * Resolve the built OneMCP jar path regardless of version or packaging plugin.
   */
  private async resolveOnemcpJar(projectRoot: string): Promise<string> {
    const targetDir = join(projectRoot, 'src/onemcp/target');

    let artifacts: string[];
    try {
      artifacts = await fs.readdir(targetDir);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : String(error);
      throw new Error(
        `Could not read ${targetDir}: ${message}. Ensure the Java build step completed successfully.`
      );
    }

    const patterns = [
      /^onemcp-.*-jar-with-dependencies\.jar$/,
      /^onemcp-.*\.jar$/,
    ];

    for (const pattern of patterns) {
      const match = artifacts.find((file) => pattern.test(file));
      if (match) {
        return join(targetDir, match);
      }
    }

    throw new Error(
      `Could not find a OneMCP jar in ${targetDir}. Run "mvn clean package -DskipTests" and try again.`
    );
  }

  /**
   * Start OneMCP with all required services
   */
  async start(options: StartOptions = {}): Promise<void> {
    await this.initialize();

    const config = await configManager.getGlobalConfig();
    if (!config) {
      throw new Error('No configuration found. Please run setup first.');
    }

    // Validate environment before starting
    const validation = await this.validateEnvironment();
    if (!validation.valid) {
      const missingList = validation.missing.join(', ');
      throw new Error(
        `Missing required dependencies: ${missingList}\n` +
        'Please install the required dependencies:\n' +
        '  - Java 21+: https://adoptium.net/\n' +
        '  - Node.js 20+: https://nodejs.org/\n' +
        '  - Maven: https://maven.apache.org/install.html\n' +
        'Then run "onemcp doctor" to verify your installation.'
      );
    }

    // Update environment for processes based on current handbook
    await this.updateEnvironmentForCurrentHandbook(options);

    // Update environment for processes if options provided
    if (options.port || options.handbookDir || options.provider || options.apiKey) {
      await this.updateProcessEnvironments(options);
    }

    // Validate handbook configuration before starting services
    await this.validateHandbookConfiguration();

    // Clean up any stale processes that might be using our ports
    await this.cleanupStaleProcesses();

    console.log(chalk.dim('  • Starting OneMCP core service...'));
    try {
      await processManager.start('app');
      console.log(chalk.dim('    ✓ OneMCP started, waiting for health check...'));

      // Wait for OneMCP to be fully healthy
      const appConfig = processManager.getConfig('app');
      if (appConfig?.healthCheckUrl) {
        try {
          await this.waitForServiceHealthy('app', appConfig, 60000);
          console.log(chalk.dim('    ✓ OneMCP ready'));
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          console.log(chalk.red('    ❌ OneMCP failed to become healthy'));
          console.log(chalk.dim(`      Error: ${errorMessage}`));
          throw error;
        }
      } else {
        console.log(chalk.dim('    ✓ OneMCP ready'));
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      if (errorMessage.includes('Foundation dir not found') ||
          errorMessage.includes('Agent.md') ||
          errorMessage.includes('handbook')) {
        console.log(chalk.red('    ❌ OneMCP failed to start - handbook configuration issue'));
        console.log(chalk.dim('      This usually happens when the handbook directory or Agent.md file is missing.'));
        console.log(chalk.dim('      Try running the setup wizard again or check your handbook directory.'));
        console.log(chalk.dim(`      Handbook directory: ${config?.handbookDir || 'not configured'}`));
      } else {
        console.log(chalk.red('    ❌ OneMCP failed to start'));
        console.log(chalk.dim(`      Error: ${errorMessage}`));
      }

      throw error;
    }

    console.log(chalk.dim('  • OneMCP service started successfully!'));
  }

  /**
   * Stop all OneMCP services
   */
  async stop(): Promise<void> {
    await processManager.stopAll();
  }

  /**
   * Get status of all services
   */
  async getStatus(): Promise<AgentStatus> {
    await this.initialize();

    const services = await processManager.getAllStatus();
    const config = await configManager.getGlobalConfig();
    const appRunning = services.find((s) => s.name === 'app')?.running || false;

    const port = config?.defaultPort || 8080;

    return {
      running: appRunning,
      services,
      mcpUrl: appRunning ? `http://localhost:${port}/mcp` : undefined,
      handbookDir: config?.handbookDir,
      currentHandbook: config?.currentHandbook,
    };
  }

  /**
   * Restart a specific service or all services
   */
  async restart(serviceName?: string): Promise<void> {
    await this.initialize();

    if (serviceName) {
      console.log(`Restarting ${serviceName}...`);
      await processManager.stop(serviceName);
      await processManager.start(serviceName);
    } else {
      console.log('Restarting all services...');
      await this.stop();
      await this.start();
    }
  }

  /**
   * Get logs for a service
   */
  async getLogs(serviceName: string, lines = 50): Promise<string> {
    return processManager.getLogs(serviceName, lines);
  }

  /**
   * Clean up any stale processes that might be using our ports
   */
  private async cleanupStaleProcesses(): Promise<void> {
    const { execa } = await import('execa');

    // Get the expected ports from registered configs
    const portsToCheck: Array<{ name: string; port: number }> = [];
    // We need to access all configs - this is a bit hacky but necessary for cleanup
    const configs = (processManager as unknown as { configs: Map<string, ProcessConfig> }).configs;
    for (const [name, config] of configs.entries()) {
      if (config.port) {
        portsToCheck.push({ name, port: config.port });
      }
    }

    // Check each port and kill any processes using them
    for (const { name, port } of portsToCheck) {
      try {
        const { stdout } = await execa('lsof', ['-i', `:${port}`, '-t'], {
          reject: false,
          timeout: 1000,
        });

        if (stdout.trim()) {
          const pids = stdout.trim().split('\n').map(p => parseInt(p, 10)).filter(p => !isNaN(p));
          for (const pid of pids) {
            try {
              // Check if this is our own service
              if (await processManager.isRunning(name)) {
                continue; // Don't kill our own running service
              }

              // Kill the stale process
              console.log(chalk.dim(`    • Cleaning up stale process on port ${port} (PID: ${pid})`));
              process.kill(pid, 'SIGTERM');

              // Wait a bit for it to die
              await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
              // Process might have already died or we don't have permission
            }
          }
        }
      } catch (error) {
        // lsof failed, continue
      }
    }
  }

  /**
   * Wait for a service to become healthy
   */
  private async waitForServiceHealthy(serviceName: string, config: ProcessConfig, timeoutMs: number): Promise<void> {
    if (!config.healthCheckUrl) {
      throw new Error(`Service ${serviceName} has no health check URL configured`);
    }

    const startTime = Date.now();
    const interval = 1000; // Check every 1 second

    while (Date.now() - startTime < timeoutMs) {
      try {
        const axios = (await import('axios')).default;
        const response = await axios.get(config.healthCheckUrl, {
          timeout: 5000,
          validateStatus: () => true, // Accept any status code
          maxRedirects: 0,
        });

        // For health endpoints, any response (even error) means the service is responding
        if (response.status >= 200 && response.status < 500) {
          return; // Service is healthy
        }
      } catch (error) {
        // Connection failed, service not ready yet
      }

      // Wait before next check
      await new Promise(resolve => setTimeout(resolve, interval));
    }

    throw new Error(`Service ${serviceName} failed to become healthy within ${timeoutMs}ms`);
  }

  /**
   * Update process environments based on current handbook configuration
   */
  private async updateEnvironmentForCurrentHandbook(options: StartOptions): Promise<void> {
    const config = await configManager.getGlobalConfig();
    const currentHandbook = config?.currentHandbook;

    let handbookConfig;
    let handbookPath = config?.handbookDir || paths.handbooksDir;

    if (currentHandbook) {
      handbookConfig = await configManager.getEffectiveHandbookConfig(currentHandbook);
      handbookPath = paths.getHandbookPath(currentHandbook);
    }

    const appConfig = processManager.getConfig('app');
    if (appConfig) {
      // Set foundation directory
      appConfig.env = {
        ...appConfig.env,
        FOUNDATION_DIR: handbookPath,
      };

      // Set API keys and provider from handbook config
      if (handbookConfig) {
        const provider = handbookConfig.provider || config?.provider || 'openai';
        const activeProfile = this.resolveActiveProfile(provider);
        const apiKeys = handbookConfig.apiKeys || config?.apiKeys || {};

        appConfig.env = {
          ...appConfig.env,
          OPENAI_API_KEY: apiKeys.openai || '',
          GEMINI_API_KEY: apiKeys.gemini || '',
          ANTHROPIC_API_KEY: apiKeys.anthropic || '',
          INFERENCE_DEFAULT_PROVIDER: provider,
          LLM_ACTIVE_PROFILE: activeProfile,
        };
        this.applyActiveProfileArgs(appConfig, activeProfile);
      } else {
        const fallbackProfile = this.resolveActiveProfile(config?.provider);
        this.applyActiveProfileArgs(appConfig, fallbackProfile);
      }
    }
  }

  /**
   * Update process environments based on options
   */
  private async updateProcessEnvironments(options: StartOptions): Promise<void> {
    // This would update the registered process configs with new environment variables
    // For now, we'll need to re-register with updated configs
    if (options.port) {
      const appConfig = processManager.getConfig('app');
      if (appConfig) {
        appConfig.env = {
          ...appConfig.env,
          SERVER_PORT: options.port.toString(),
        };
        appConfig.port = options.port;
        appConfig.healthCheckUrl = `http://localhost:${options.port}/actuator/health`;
      }
    }

    if (options.handbookDir) {
      const appConfig = processManager.getConfig('app');
      if (appConfig) {
        appConfig.env = {
          ...appConfig.env,
          FOUNDATION_DIR: options.handbookDir,
        };
      }
    }

    if (options.provider && options.apiKey) {
      const appConfig = processManager.getConfig('app');
      if (appConfig) {
        const keyEnvVar =
          options.provider === 'openai'
            ? 'OPENAI_API_KEY'
            : options.provider === 'gemini'
            ? 'GEMINI_API_KEY'
            : 'ANTHROPIC_API_KEY';
        const activeProfile = this.resolveActiveProfile(options.provider);

        appConfig.env = {
          ...appConfig.env,
          [keyEnvVar]: options.apiKey,
          INFERENCE_DEFAULT_PROVIDER: options.provider,
          LLM_ACTIVE_PROFILE: activeProfile,
        };
        this.applyActiveProfileArgs(appConfig, activeProfile);
      }
    }
  }

  private resolveActiveProfile(provider?: string): string {
    const normalized = (provider || 'openai').toLowerCase();
    switch (normalized) {
      case 'gemini':
        return 'gemini-flash';
      case 'anthropic':
        return 'anthropic-sonnet';
      default:
        return 'openai';
    }
  }

  private buildJavaArgs(jarPath: string, activeProfile: string): string[] {
    return [`-Dllm.active-profile=${activeProfile}`, '-jar', jarPath];
  }

  private applyActiveProfileArgs(appConfig: ProcessConfig, activeProfile: string): void {
    if (!appConfig.args || appConfig.args.length === 0) {
      return;
    }

    let jarPath: string | undefined;
    const jarFlagIndex = appConfig.args.findIndex((arg) => arg === '-jar');
    if (jarFlagIndex >= 0 && jarFlagIndex + 1 < appConfig.args.length) {
      jarPath = appConfig.args[jarFlagIndex + 1];
    } else {
      jarPath = appConfig.args[appConfig.args.length - 1];
    }

    if (!jarPath) {
      return;
    }

    appConfig.args = this.buildJavaArgs(jarPath, activeProfile);
  }

  /**
   * Validate handbook configuration before starting services
   */
  private async validateHandbookConfiguration(): Promise<void> {
    const config = await configManager.getGlobalConfig();
    const currentHandbook = config?.currentHandbook;

    let handbookPath: string;

    if (currentHandbook) {
      handbookPath = paths.getHandbookPath(currentHandbook);
    } else {
      // Fallback to legacy handbookDir
      handbookPath = config?.handbookDir || paths.handbooksDir;
    }

    if (!handbookPath) {
      throw new Error('No handbook directory configured. Please run setup first or set a current handbook.');
    }

    // Check if handbook directory exists
    if (!fs.existsSync(handbookPath)) {
      const handbookName = currentHandbook ? `handbook '${currentHandbook}'` : 'configured handbook directory';
      throw new Error(
        `Handbook directory not found: ${handbookPath}\n` +
        `Please ensure the ${handbookName} exists and contains the required Agent.md file.\n` +
        'Try running the setup wizard again: onemcp setup'
      );
    }

    // Check for required Agent.md file
    const agentMdPath = `${handbookPath}/Agent.md`;
    if (!fs.existsSync(agentMdPath)) {
      const handbookName = currentHandbook ? `handbook '${currentHandbook}'` : 'handbook directory';
      throw new Error(
        `Required Agent.md file not found in ${handbookName}: ${handbookPath}\n` +
        'The handbook must contain an Agent.md file with agent instructions.\n' +
        'Try running the setup wizard again: onemcp setup'
      );
    }

    // Additional validation could be added here for other required files/directories
  }

  /**
   * Validate that required binaries are available
   */
  async validateEnvironment(): Promise<{ valid: boolean; missing: string[] }> {
    const required = ['java', 'node', 'mvn'];
    const missing: string[] = [];

    for (const binary of required) {
      try {
        const { execa } = await import('execa');
        await execa(binary, ['--version'], { timeout: 5000 });
      } catch {
        missing.push(binary);
      }
    }

    return {
      valid: missing.length === 0,
      missing,
    };
  }

  /**
   * Build the project if needed
   */
  async buildProject(): Promise<void> {
    const { execa } = await import('execa');
    const projectRoot = this.findProjectRoot();
    
    console.log('Building OneMCP...');
    
    // Build Java application
    console.log('Building Java application...');
    await execa('mvn', ['clean', 'package', '-DskipTests'], {
      cwd: join(projectRoot, 'src/onemcp'),
      stdio: 'inherit',
    });

    // Build CLI
    console.log('Building CLI...');
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'cli'),
      stdio: 'inherit',
    });

    console.log('Build completed successfully!');
  }

  /**
   * Get current version/commit hash
   */
  async getCurrentVersion(): Promise<string | null> {
    try {
      const projectRoot = this.findProjectRoot();
      const { execa } = await import('execa');
      const { stdout } = await execa('git', ['rev-parse', 'HEAD'], { cwd: projectRoot });
      return stdout.trim();
    } catch {
      return null;
    }
  }

  /**
   * Update repository to latest version
   */
  async updateRepository(): Promise<void> {
    const projectRoot = this.findProjectRoot();
    const { execa } = await import('execa');

    // Fetch latest changes
    await execa('git', ['fetch', 'origin'], { cwd: projectRoot });

    // Reset to remote main branch
    await execa('git', ['reset', '--hard', 'origin/main'], { cwd: projectRoot });
  }

  /**
   * Rebuild all components
   */
  async rebuildAll(): Promise<void> {
    const projectRoot = this.findProjectRoot();
    const { execa } = await import('execa');

    // Build Java app
    await execa('mvn', ['clean', 'package', '-DskipTests', '-q'], {
      cwd: join(projectRoot, 'src/onemcp')
    });

    // Build CLI
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'cli')
    });
  }

  /**
   * Update CLI symlink
   */
  async updateCliSymlink(): Promise<void> {
    const projectRoot = this.findProjectRoot();
    const cliDistPath = join(projectRoot, 'cli/dist/index.js');
    const symlinkPath = join(homedir(), '.local/bin/onemcp');

    // Ensure directory exists
    await fs.ensureDir(dirname(symlinkPath));

    // Remove existing symlink if it exists
    try {
      await fs.unlink(symlinkPath);
    } catch {
      // Ignore if symlink doesn't exist
    }

    // Create new symlink
    await fs.symlink(cliDistPath, symlinkPath);
    await fs.chmod(cliDistPath, 0o755);
  }
}

export const agentService = new AgentService();

