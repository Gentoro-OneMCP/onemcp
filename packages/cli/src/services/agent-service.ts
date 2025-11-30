/**
 * OneMCP service manager
 */
import { dirname, join, delimiter } from 'path';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import { homedir } from 'os';
import chalk from 'chalk';
import { processManager, ProcessConfig } from './process-manager.js';
import { configManager } from '../config/manager.js';
import { paths } from '../config/paths.js';
import { AgentStatus } from '../types.js';

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
  findProjectRoot(): string {
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
      const thisFile = fileURLToPath(import.meta.url); // .../packages/cli/dist/services/agent-service.js
      const servicesDir = dirname(thisFile); // .../packages/cli/dist/services
      const distDir = dirname(servicesDir);  // .../packages/cli/dist
      const cliDir = dirname(distDir);       // .../packages/cli
      const packagesDir = dirname(cliDir);    // .../packages
      const repoRoot = dirname(packagesDir);  // ... (project root)
      possibleRoots.push(repoRoot);
    } catch {
      // ignore if import.meta.url is unavailable
    }

    // Also check ONEMCP_SRC if it's set (for development)
    if (process.env.ONEMCP_SRC) {
      possibleRoots.unshift(process.env.ONEMCP_SRC);
    }

    for (const root of possibleRoots) {
      const pomPath = join(root, 'packages/server/pom.xml');
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

    // CLI always runs from repo, so use compiled classes (no JAR needed)
    await this.ensureServerCompiled(projectRoot);
    const activeProfile = this.resolveActiveProfile(config?.provider);
    const javaArgs = await this.buildJavaArgs(projectRoot, activeProfile, port);

    const initialEnv = {
      SERVER_PORT: port.toString(),
      FOUNDATION_DIR: config?.handbookDir || paths.handbooksDir,
      OPENAI_API_KEY: config?.apiKeys?.openai || '',
      GEMINI_API_KEY: config?.apiKeys?.gemini || '',
      ANTHROPIC_API_KEY: config?.apiKeys?.anthropic || '',
      INFERENCE_DEFAULT_PROVIDER: config?.provider || 'openai',
      LLM_ACTIVE_PROFILE: activeProfile,
      ONEMCP_HOME_DIR: paths.homeDir,
      ONEMCP_CACHE_DIR: paths.cacheDir,
      // Set OrientDB root directory to ONEMCP_HOME_DIR/orient
      GRAPH_V2_ORIENT_ROOT_DIR: join(paths.homeDir, 'orient'),
    };

    processManager.register({
      name: 'app',
      command: 'java',
      args: javaArgs,
      env: initialEnv,
      cwd: projectRoot, // Set working directory to project root, not handbook directory
      port,
      healthCheckUrl: `http://localhost:${port}/mcp`,
    });

    this.initialized = true;
  }

  /**
   * Check if server classes are compiled.
   * CLI always runs from repo, so we use compiled classes instead of JAR.
   */
  async ensureServerCompiled(projectRoot: string): Promise<void> {
    const serverDir = join(projectRoot, 'packages/server');
    const classesDir = join(serverDir, 'target/classes');
    const mainClass = join(classesDir, 'com/gentoro/onemcp/OneMcpApp.class');

    if (!(await fs.pathExists(mainClass))) {
      throw new Error(
        `Server classes not found. Run "mvn compile" in packages/server first.`
      );
    }
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

    // Show log file location
    const logPath = paths.getLogPath('app');
    console.log(chalk.dim(`  • Logs: ${logPath}`));

    console.log(chalk.dim('  • Starting OneMCP core service...'));
    try {
      // Log position is captured in processManager.start() BEFORE spawn
      await processManager.start('app');
      console.log(chalk.dim('    ✓ OneMCP started, waiting for health check...'));

      // Wait for OneMCP to be fully healthy
      const appConfig = processManager.getConfig('app');
      
      if (appConfig?.healthCheckUrl) {
        try {
          // Use the log position captured when "Starting One MCP service..." was printed
          // This is the earliest possible capture point
          const logStartPosition = processManager.getLogStartPosition('app');
          
          await this.waitForServiceHealthy('app', appConfig, 60000, logStartPosition);
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
    // Access process manager configs for cleanup
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
  private async waitForServiceHealthy(serviceName: string, config: ProcessConfig, timeoutMs: number, logStartPosition: number = 0): Promise<void> {
    if (!config.healthCheckUrl) {
      throw new Error(`Service ${serviceName} has no health check URL configured`);
    }

    const startTime = Date.now();
    const healthCheckInterval = 1000; // Check health every 1 second
    const logCheckInterval = 500; // Check for new logs every 500ms
    
    let lastLogPosition = logStartPosition;
    let lastLogCheckTime = 0; // Start at 0 to check immediately
    let lastHealthCheckTime = 0; // Start at 0 to check immediately
    let hasDisplayedLogs = false;

    while (Date.now() - startTime < timeoutMs) {
      const currentTime = Date.now();
      
      // Check for new logs every 500ms (no initial delay - start immediately)
      if (currentTime - lastLogCheckTime >= logCheckInterval) {
        try {
          // Get new log entries since last check
          const { content, newPosition } = await processManager.getLogsSince(serviceName, lastLogPosition);
          
          // If position changed, there's new content (even if it's just whitespace)
          if (newPosition !== lastLogPosition) {
            if (!hasDisplayedLogs) {
              // First time displaying logs - show from the start position
              console.log(chalk.dim('\n--- Server logs (from startup) ---'));
              hasDisplayedLogs = true;
            }
            
            // Display new content immediately (preserve original formatting, including whitespace)
            if (content) {
              process.stdout.write(content);
            }
            
            // Always update position, even if content is empty/whitespace
            lastLogPosition = newPosition;
          }
          lastLogCheckTime = currentTime;
        } catch (error) {
          // Silently continue on error - log file might not exist yet
          lastLogCheckTime = currentTime;
        }
      }

      // Check health every 1 second
      if (currentTime - lastHealthCheckTime >= healthCheckInterval) {
        try {
          const axios = (await import('axios')).default;
          const response = await axios.get(config.healthCheckUrl, {
            timeout: 5000,
            validateStatus: () => true, // Accept any status code
            maxRedirects: 0,
          });

          // For health endpoints, any response (even error) means the service is responding
          if (response.status >= 200 && response.status < 500) {
            // Server is healthy - stop tailing logs
            if (hasDisplayedLogs) {
              console.log(chalk.dim('\n--- Server logs end ---\n'));
            }
            return; // Service is healthy
          }
          lastHealthCheckTime = currentTime;
        } catch (error) {
          // Connection failed, service not ready yet - continue waiting
          lastHealthCheckTime = currentTime;
        }
      }

      // Wait a short time before next iteration (100ms for responsive tailing)
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Service ${serviceName} failed to become healthy within ${timeoutMs}ms`);
  }

  /**
   * Update process environments based on current handbook configuration
   */
  private async updateEnvironmentForCurrentHandbook(_options: StartOptions): Promise<void> {
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
      // Set foundation directory and handbook directory
      // HANDBOOK_DIR is used by application.yaml to set handbook.location
      // This ensures the server uses the actual handbook path, not a temp copy
      appConfig.env = {
        ...appConfig.env,
        FOUNDATION_DIR: handbookPath,
        HANDBOOK_DIR: handbookPath,
      };

      // Set logging directory for reports to ONEMCP_HOME_DIR/logs
      const logsDir = paths.logDir;
      const cacheDir = paths.cacheDir;
      await fs.ensureDir(logsDir);
      await fs.ensureDir(cacheDir);
      appConfig.env = {
        ...appConfig.env,
        ONEMCP_LOG_DIR: logsDir,
        ONEMCP_CACHE_DIR: cacheDir,
        // Ensure ONEMCP_HOME_DIR is set
        ONEMCP_HOME_DIR: paths.homeDir,
        // Set OrientDB root directory to ONEMCP_HOME_DIR/orient
        GRAPH_V2_ORIENT_ROOT_DIR: join(paths.homeDir, 'orient'),
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
        await this.applyActiveProfileArgs(appConfig, activeProfile);
      } else {
        const fallbackProfile = this.resolveActiveProfile(config?.provider);
        await this.applyActiveProfileArgs(appConfig, fallbackProfile);
      }
    }
  }

  /**
   * Update process environments based on options
   */
  private async updateProcessEnvironments(options: StartOptions): Promise<void> {
    const config = await configManager.getGlobalConfig();

    // This would update the registered process configs with new environment variables
    // For now, we'll need to re-register with updated configs
    const appConfig = processManager.getConfig('app');
    if (appConfig) {
      // Always ensure ONEMCP_HOME_DIR and related directories are set
      appConfig.env = {
        ...appConfig.env,
        ONEMCP_HOME_DIR: paths.homeDir,
        GRAPH_V2_ORIENT_ROOT_DIR: join(paths.homeDir, 'orient'),
        ONEMCP_LOG_DIR: paths.logDir,
        ONEMCP_CACHE_DIR: paths.cacheDir,
      };

      if (options.port) {
        appConfig.env = {
          ...appConfig.env,
          SERVER_PORT: options.port.toString(),
        };
        appConfig.port = options.port;
        appConfig.healthCheckUrl = `http://localhost:${options.port}/actuator/health`;
        const provider =
          appConfig.env?.INFERENCE_DEFAULT_PROVIDER ||
          options.provider ||
          config?.provider ||
          'openai';
        const activeProfile = this.resolveActiveProfile(provider);
        await this.applyActiveProfileArgs(appConfig, activeProfile);
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
        await this.applyActiveProfileArgs(appConfig, activeProfile);
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

  private async buildJavaArgs(projectRoot: string, activeProfile: string, port: number): Promise<string[]> {
    const serverDir = join(projectRoot, 'packages/server');
    const classesDir = join(serverDir, 'target/classes');
    
    // Get Maven classpath (dependencies)
    let mavenClasspath = '';
    try {
      const { execa } = await import('execa');
      // Use -Dmdep.outputFile to get classpath in a file, then read it
      // This is more reliable than parsing stdout which may have other output
      const tempClasspathFile = join(serverDir, 'target', '.classpath.tmp');
      await execa('mvn', [
        'dependency:build-classpath',
        `-Dmdep.outputFile=${tempClasspathFile}`,
        '-q'
      ], {
        cwd: serverDir,
        stdio: 'pipe',
      });
      
      // Read the classpath from the file
      if (await fs.pathExists(tempClasspathFile)) {
        mavenClasspath = (await fs.readFile(tempClasspathFile, 'utf-8')).trim();
        // Clean up temp file
        await fs.remove(tempClasspathFile);
        
        // Verify classpath contains SLF4J (critical dependency)
        if (!mavenClasspath.includes('slf4j-api')) {
          throw new Error(`Maven classpath is missing SLF4J. Classpath preview: ${mavenClasspath.substring(0, 200)}`);
        }
      } else {
        throw new Error(`Maven classpath file was not created at ${tempClasspathFile}`);
      }
      
      if (!mavenClasspath) {
        throw new Error('Maven classpath is empty. Run "mvn dependency:resolve" first.');
      }
    } catch (error) {
      throw new Error(
        'Failed to build Maven classpath. Make sure Maven is installed and dependencies are resolved. ' +
        'Run "mvn dependency:resolve" in packages/server first. ' +
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    }
    
    // Build full classpath: classes (which already includes resources) + Maven dependencies
    // Note: Maven copies resources to target/classes during compilation, so we don't need src/main/resources
    // Use platform-specific path delimiter (':' on Unix, ';' on Windows)
    const classpath = [
      classesDir,
      mavenClasspath,
    ].filter(Boolean).join(delimiter);
    
    // Verify classpath is not empty and contains required entries
    if (!classpath || classpath.trim().length === 0) {
      throw new Error('Classpath is empty. Cannot start server.');
    }
    
    const classpathEntries = classpath.split(delimiter);
    if (classpathEntries.length < 2) {
      throw new Error(`Classpath has too few entries (${classpathEntries.length}). Expected at least classes and dependencies.`);
    }
    
    // Log the actual command that will be run (for debugging)
    const javaCommand = [
      'java',
      '-cp',
      classpath,
      'com.gentoro.onemcp.OneMcpApp',
      '--mode',
      'server',
    ];
    
    // Verify classpath is actually set
    if (!classpath || classpath.trim().length === 0) {
      throw new Error('FATAL: Classpath is empty! Cannot start server.');
    }
    
    // Check if SLF4J is in classpath
    if (!classpath.includes('slf4j-api')) {
      throw new Error(`FATAL: SLF4J not found in classpath! Classpath entries: ${classpath.split(delimiter).slice(0, 5).join(', ')}...`);
    }
    
    return javaCommand.slice(1); // Return args without 'java' command
  }

  private async applyActiveProfileArgs(appConfig: ProcessConfig, activeProfile: string): Promise<void> {
    if (!appConfig.args || appConfig.args.length === 0) {
      return;
    }

    const projectRoot = this.findProjectRoot();
    const port =
      typeof appConfig.port === 'number'
        ? appConfig.port
        : parseInt(appConfig.env?.SERVER_PORT ?? '', 10) || 8080;

    appConfig.args = await this.buildJavaArgs(projectRoot, activeProfile, port);
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
        `Please ensure the ${handbookName} exists.\n` +
        'Try running the setup wizard again: onemcp setup'
      );
    }

    // Let the server validate Agent.md - don't check here
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
      cwd: join(projectRoot, 'packages/server'),
      stdio: 'inherit',
    });

    // Build CLI
    console.log('Building CLI...');
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'packages/cli'),
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
      cwd: join(projectRoot, 'packages/server')
    });

    // Build CLI
    await execa('npm', ['run', 'build'], {
      cwd: join(projectRoot, 'packages/cli')
    });
  }

  /**
   * Update CLI symlink
   */
  async updateCliSymlink(): Promise<void> {
    const projectRoot = this.findProjectRoot();
    const cliDistPath = join(projectRoot, 'packages/cli/dist/index.js');
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

