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
  mockMode?: boolean;
  provider?: string;
  apiKey?: string;
  disableServices?: string[];
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
      const tsRuntimePath = join(root, 'src/typescript-runtime/dist/server.js');
      const onemcpPath = join(root, 'src/onemcp/target/onemcp-0.1.0-SNAPSHOT.jar');
      
      if (fs.existsSync(tsRuntimePath) && fs.existsSync(onemcpPath)) {
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

    // Register OpenTelemetry Collector
    processManager.register({
      name: 'otel',
      command: 'otelcol',
      args: ['--config', '/tmp/onemcp-otel-config.yaml'],
      env: {
        OTEL_SERVICE_NAME: 'onemcp',
      },
      port: 4317,
      healthCheckUrl: undefined, // OTEL doesn't have a simple health endpoint
    });

    // Register TypeScript Runtime
    const tsRuntimeServer = join(projectRoot, 'src/typescript-runtime/dist/server.js');
    const tsRuntimeDir = join(projectRoot, 'src/typescript-runtime');
    
    processManager.register({
      name: 'ts-runtime',
      command: 'node',
      args: [tsRuntimeServer],
      env: {
        PORT: '7070',
        NODE_ENV: 'production',
      },
      cwd: tsRuntimeDir,
      port: 7070,
      healthCheckUrl: 'http://localhost:7070/health',
      dependsOn: [],
    });

    // Register OneMCP (Java application)
    const onemcpJar = join(projectRoot, 'src/onemcp/target/onemcp-0.1.0-SNAPSHOT.jar');
    
    processManager.register({
      name: 'app',
      command: 'java',
      args: ['-jar', onemcpJar],
      env: {
        SERVER_PORT: port.toString(),
        FOUNDATION_DIR: config?.handbookDir || paths.handbooksDir,
        OPENAI_API_KEY: config?.provider === 'openai' ? config.apiKey : '',
        GEMINI_API_KEY: config?.provider === 'gemini' ? config.apiKey : '',
        ANTHROPIC_API_KEY: config?.provider === 'anthropic' ? config.apiKey : '',
        INFERENCE_DEFAULT_PROVIDER: config?.provider || 'openai',
        TS_RUNTIME_URL: 'http://localhost:7070',
        OTEL_EXPORTER_OTLP_ENDPOINT: 'http://localhost:4317',
      },
      port,
      healthCheckUrl: `http://localhost:${port}/mcp`,
      dependsOn: ['ts-runtime'],
    });

    // Register Mock Server (ACME Analytics)
    const mockServerJar = join(projectRoot, 'src/acme-analytics-server/server/target/acme-analytics-server-1.0.0.jar');
    
    processManager.register({
      name: 'mock',
      command: 'java',
      args: ['-jar', mockServerJar, '8082'],
      port: 8082,
      healthCheckUrl: 'http://localhost:8082/health',
    });

    this.initialized = true;
  }

  /**
   * Start OneMCP with all required services
   */
  async start(options: StartOptions = {}): Promise<void> {
    await this.initialize();

    const config = await configManager.getGlobalConfig();
    if (!config && !options.mockMode) {
      throw new Error('No configuration found. Please run setup first or use --mock flag.');
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

    // Update environment for processes if options provided
    if (options.port || options.handbookDir || options.provider || options.apiKey) {
      await this.updateProcessEnvironments(options);
    }

    const disabledServices = options.disableServices || [];

    // Validate handbook configuration before starting services
    if (!disabledServices.includes('app') && !options.mockMode) {
      await this.validateHandbookConfiguration(config || undefined);
    }

    // Clean up any stale processes that might be using our ports
    await this.cleanupStaleProcesses();

    // Start services in order
    if (!disabledServices.includes('otel')) {
      console.log(chalk.dim('  • Starting OpenTelemetry Collector for observability...'));
      try {
        await processManager.start('otel');
        // Wait a bit for the process to either start or fail
        await new Promise(resolve => setTimeout(resolve, 500));
        // Check if the service is actually running
        if (await processManager.isRunning('otel')) {
          console.log(chalk.dim('    ✓ OTEL Collector ready'));
        } else {
          console.log(chalk.dim('    ⚠ OTEL Collector not available (optional) - continuing without'));
        }
      } catch (error: unknown) {
        console.log(chalk.dim('    ⚠ OTEL Collector not available (optional) - continuing without'));
      }
    }

    console.log(chalk.dim('  • Starting TypeScript Runtime for tool execution...'));
    if (!disabledServices.includes('ts-runtime')) {
      await processManager.start('ts-runtime');
      console.log(chalk.dim('    ✓ TypeScript Runtime started, waiting for health check...'));

      // Wait for TypeScript runtime to be fully healthy before starting dependent services
      const tsConfig = processManager.getConfig('ts-runtime');
      if (tsConfig?.healthCheckUrl) {
        try {
          await this.waitForServiceHealthy('ts-runtime', tsConfig, 30000); // 30 second timeout
          console.log(chalk.dim('    ✓ TypeScript Runtime ready'));
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          console.log(chalk.yellow('    ⚠ TypeScript Runtime health check failed, but continuing...'));
          console.log(chalk.dim(`      Error: ${errorMessage}`));
        }
      } else {
        console.log(chalk.dim('    ✓ TypeScript Runtime ready'));
      }
    }

    console.log(chalk.dim('  • Starting OneMCP core service...'));
    if (!disabledServices.includes('app')) {
      try {
        await processManager.start('app');
        console.log(chalk.dim('    ✓ OneMCP started, waiting for health check...'));

        // Wait for OneMCP to be fully healthy
        const appConfig = processManager.getConfig('app');
        if (appConfig?.healthCheckUrl) {
          try {
            await this.waitForServiceHealthy('app', appConfig, 60000); // 60 second timeout for Java app
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

        // Check if this is a handbook-related error
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

        // Re-throw the error to fail the entire startup
        throw error;
      }
    }

    if (options.mockMode) {
      console.log(chalk.dim('  • Starting ACME Analytics mock server for testing...'));
      if (!disabledServices.includes('mock')) {
        await processManager.start('mock');
        console.log(chalk.dim('    ✓ Mock server ready'));
      }
    }

    console.log(chalk.dim('  • All services started successfully!'));
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
      if (config.port && !['otel'].includes(name)) { // Skip otel as it's optional
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

        appConfig.env = {
          ...appConfig.env,
          [keyEnvVar]: options.apiKey,
          INFERENCE_DEFAULT_PROVIDER: options.provider,
        };
      }
    }
  }

  /**
   * Validate handbook configuration before starting services
   */
  private async validateHandbookConfiguration(config?: { handbookDir?: string }): Promise<void> {
    if (!config?.handbookDir) {
      throw new Error('No handbook directory configured. Please run setup first.');
    }

    // Check if handbook directory exists
    if (!fs.existsSync(config.handbookDir)) {
      throw new Error(
        `Handbook directory not found: ${config.handbookDir}\n` +
        'Please ensure the handbook directory exists and contains the required Agent.md file.\n' +
        'Try running the setup wizard again: onemcp setup'
      );
    }

    // Check for required Agent.md file
    const agentMdPath = `${config.handbookDir}/Agent.md`;
    if (!fs.existsSync(agentMdPath)) {
      throw new Error(
        `Required Agent.md file not found in handbook directory: ${config.handbookDir}\n` +
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

    // Check for optional otelcol
    try {
      const { execa } = await import('execa');
      await execa('otelcol', ['--version'], { timeout: 5000 });
    } catch {
      console.warn('OpenTelemetry Collector not found (optional)');
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

    // Build TypeScript runtime
    console.log('Building TypeScript runtime...');
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'src/typescript-runtime'),
      stdio: 'inherit',
    });

    // Build Mock server
    console.log('Building Mock server...');
    await execa('mvn', ['clean', 'package', '-DskipTests'], {
      cwd: join(projectRoot, 'src/acme-analytics-server/server'),
      stdio: 'inherit',
    });

    console.log('Build completed successfully!');
  }

  /**
   * Create default OTEL config if it doesn't exist
   */
  async ensureOtelConfig(): Promise<void> {
    const otelConfigPath = '/tmp/onemcp-otel-config.yaml';
    
    if (await fs.pathExists(otelConfigPath)) {
      return;
    }

    const defaultConfig = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  file:
    path: ${paths.logDir}/otel-collector.json
  logging:
    loglevel: info

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file, logging]
    metrics:
      receivers: [otlp]
      exporters: [file, logging]
    logs:
      receivers: [otlp]
      exporters: [file, logging]
`;

    await fs.writeFile(otelConfigPath, defaultConfig, 'utf-8');
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
    await execa('mvn', ['clean', 'package', 'spring-boot:repackage', '-DskipTests', '-q'], {
      cwd: join(projectRoot, 'src/onemcp')
    });

    // Build TypeScript runtime
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'src/typescript-runtime')
    });

    // Build mock server (optional)
    try {
      await execa('mvn', ['clean', 'package', '-DskipTests', '-q'], {
        cwd: join(projectRoot, 'src/acme-analytics-server/server')
      });
    } catch {
      // Mock server build is optional, ignore errors
    }

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

