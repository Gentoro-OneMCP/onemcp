/**
 * Process manager for running OneMCP services
 */
import { ChildProcess, spawn } from 'child_process';
import { EventEmitter } from 'events';
import fs from 'fs-extra';
import kill from 'tree-kill';
import { paths } from '../config/paths.js';
import { ServiceStatus } from '../types.js';

export interface ProcessConfig {
  name: string;
  command: string;
  args: string[];
  env?: Record<string, string>;
  cwd?: string;
  port?: number;
  healthCheckUrl?: string;
  healthCheckInterval?: number;
  dependsOn?: string[];
}

export class ProcessManager extends EventEmitter {
  private processes = new Map<string, ChildProcess>();
  private configs = new Map<string, ProcessConfig>();
  private startTimes = new Map<string, Date>();

  constructor() {
    super();
  }

  /**
   * Register a process configuration
   */
  register(config: ProcessConfig): void {
    this.configs.set(config.name, config);
  }

  /**
   * Start a process
   */
  async start(name: string): Promise<void> {
    const config = this.configs.get(name);
    if (!config) {
      throw new Error(`Process ${name} not registered`);
    }

    // Check if already running
    if (await this.isRunning(name)) {
      console.log(`Process ${name} is already running`);
      return; // Don't throw error, just return
    }

    // Check dependencies - ensure they are both running and healthy
    if (config.dependsOn) {
      for (const dep of config.dependsOn) {
        if (!(await this.isRunning(dep))) {
          throw new Error(`Dependency ${dep} is not running`);
        }

        // Also check if dependency has a health check and is healthy
        const depConfig = this.configs.get(dep);
        if (depConfig?.healthCheckUrl) {
          try {
            await this.waitForHealthy(depConfig);
          } catch (error: any) {
            throw new Error(`Dependency ${dep} is not healthy: ${error.message}`);
          }
        }
      }
    }

    // Ensure directories exist
    await paths.ensureDirectories();

    // Prepare environment
    const env = {
      ...process.env,
      ...config.env,
    };

    // Start process (detached so parent can exit)
    const proc = spawn(config.command, config.args, {
      env,
      cwd: config.cwd || process.cwd(),
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: true,
      shell: false, // Don't use shell to avoid quoting issues
    });
    
    // Unref so parent process can exit
    proc.unref();

    // Setup logging with unbuffered writes
    const logPath = paths.getLogPath(name);
    const logStream = fs.createWriteStream(logPath, {
      flags: 'a',
      highWaterMark: 0 // Disable buffering
    });

    proc.stdout?.pipe(logStream);
    proc.stderr?.pipe(logStream);

    // Handle process events
    proc.on('error', (error: NodeJS.ErrnoException) => {
      // Enhance error message with helpful context
      let enhancedError = error;
      if (error.code === 'ENOENT') {
        const errorMsg = `[${error.code}] Command '${config.command}' not found. ` +
          `Please ensure it is installed and available in your PATH.\n` +
          `Command: ${config.command}\n` +
          `Args: ${config.args.join(' ')}`;
        enhancedError = new Error(errorMsg) as NodeJS.ErrnoException;
        enhancedError.code = error.code;
      }

      // Emit error for required services only
      const optionalServices: string[] = ['otel'];
      if (!optionalServices.includes(config.name || '')) {
        this.emit('error', { name, error: enhancedError });
      } else {
        console.warn(`Optional service ${name} failed to start: ${error.message}`);
      }
      // Clean up for any service
      this.processes.delete(name);
      this.startTimes.delete(name);
      const pidPath = paths.getPidFilePath(name);
      fs.remove(pidPath).catch(() => {});
    });

    proc.on('exit', (code, signal) => {
      this.processes.delete(name);
      this.startTimes.delete(name);
      this.emit('exit', { name, code, signal });
      logStream.end();
    });

    // Store process info
    this.processes.set(name, proc);
    this.startTimes.set(name, new Date());

    // Save PID
    if (proc.pid) {
      const pidPath = paths.getPidFilePath(name);
      await fs.writeFile(pidPath, proc.pid.toString(), 'utf-8');
    }

    this.emit('start', { name, pid: proc.pid });

    // Wait for health check if configured
    if (config.healthCheckUrl) {
      await this.waitForHealthy(config);
    }
  }

  /**
   * Stop a process
   */
  async stop(name: string, force = false): Promise<void> {
    const pidPath = paths.getPidFilePath(name);
    
    // Try to get PID from in-memory process first
    let pid: number | undefined;
    const proc = this.processes.get(name);
    if (proc && proc.pid) {
      pid = proc.pid;
    } else {
      // Process not in memory, try to read from PID file
      try {
        if (await fs.pathExists(pidPath)) {
          const pidStr = await fs.readFile(pidPath, 'utf-8');
          pid = parseInt(pidStr.trim(), 10);
          if (isNaN(pid)) {
            pid = undefined;
          }
        }
      } catch (error) {
        // PID file doesn't exist or can't be read
        console.warn(`Could not read PID file for ${name}`);
      }
    }

    // If no PID found, just clean up and return
    if (!pid) {
      await fs.remove(pidPath);
      return;
    }

    // Check if process actually exists
    try {
      process.kill(pid, 0);
    } catch (e: any) {
      if (e.code === 'ESRCH') {
        // Process doesn't exist, just clean up PID file
        await fs.remove(pidPath);
        this.processes.delete(name);
        this.startTimes.delete(name);
        return;
      }
    }

    // Kill the process
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Timeout stopping process ${name}`));
      }, 10000);

      kill(pid!, force ? 'SIGKILL' : 'SIGTERM', (err) => {
        clearTimeout(timeout);
        if (err) {
          // If we get ESRCH (process not found), that's okay
          if ((err as any).code === 'ESRCH') {
            this.processes.delete(name);
            this.startTimes.delete(name);
            fs.remove(pidPath).then(() => resolve()).catch(() => resolve());
          } else {
            reject(err);
          }
        } else {
          this.processes.delete(name);
          this.startTimes.delete(name);
          
          // Clean up PID file
          fs.remove(pidPath).then(() => resolve()).catch(() => resolve());
        }
      });
    });
  }

  /**
   * Check if a process is running by checking PID file, validating process, and checking port
   */
  async isRunning(name: string): Promise<boolean> {
    // First check in-memory processes
    const proc = this.processes.get(name);
    if (proc !== undefined && proc.pid !== undefined && !proc.killed) {
      return true;
    }

    // If not in memory, check PID file (for cross-process detection)
    try {
      const pidPath = paths.getPidFilePath(name);
      const config = this.configs.get(name);

      // Check if PID file exists and process is running
      if (fs.existsSync(pidPath)) {
        const pidStr = fs.readFileSync(pidPath, 'utf-8').trim();
        const pid = parseInt(pidStr, 10);

        if (!isNaN(pid)) {
          try {
            // Sending signal 0 checks if process exists without killing it
            process.kill(pid, 0);
            return true;
          } catch (e: any) {
            // ESRCH means process doesn't exist
            if (e.code === 'ESRCH') {
              // Clean up stale PID file
              fs.removeSync(pidPath);
            }
            // EPERM means process exists but we don't have permission (still running)
            else if (e.code === 'EPERM') {
              return true;
            }
          }
        }
      }

      // If no valid PID file or process not found, check if port is listening
      // This handles cases where process crashed but restarted or kept running
      if (config?.port) {
        try {
          const { execa } = await import('execa');
          const { stdout } = await execa('lsof', ['-i', `:${config.port}`, '-t'], {
            timeout: 1000,
            reject: false
          });
          if (stdout.trim()) {
            // Port is listening, service is running
            // Try to create/update PID file for future checks
            const pids = stdout.trim().split('\n').map(p => parseInt(p, 10)).filter(p => !isNaN(p));
            if (pids.length > 0) {
              // Save the first PID we find
              fs.writeFileSync(pidPath, pids[0].toString(), 'utf-8');
            }
            return true;
          }
        } catch {
          // lsof failed or timed out, continue with other checks
        }
      }

      return false;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get process status
   */
  async getStatus(name: string): Promise<ServiceStatus> {
    const config = this.configs.get(name);
    const proc = this.processes.get(name);
    const running = await this.isRunning(name);
    const startTime = this.startTimes.get(name);

    let uptime: string | undefined;
    if (running && startTime) {
      const seconds = Math.floor((Date.now() - startTime.getTime()) / 1000);
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      const secs = seconds % 60;
      uptime = `${hours}h ${minutes}m ${secs}s`;
    }

    let healthy: boolean | undefined;
    if (running && config?.healthCheckUrl) {
      healthy = await this.checkHealth(config);
    }

    return {
      name,
      running,
      pid: proc?.pid,
      port: config?.port,
      uptime,
      healthy,
    };
  }

  /**
   * Get all process statuses
   */
  async getAllStatus(): Promise<ServiceStatus[]> {
    const statuses: ServiceStatus[] = [];
    for (const name of this.configs.keys()) {
      statuses.push(await this.getStatus(name));
    }
    return statuses;
  }

  /**
   * Get configuration for a service
   */
  getConfig(name: string): ProcessConfig | undefined {
    return this.configs.get(name);
  }

  /**
   * Stop all processes
   */
  async stopAll(): Promise<void> {
    // Get all service names from both in-memory processes and PID files
    const names = new Set<string>();
    
    // Add in-memory processes
    for (const name of this.processes.keys()) {
      names.add(name);
    }
    
    // Add registered configs (may not be running but need to be checked)
    for (const name of this.configs.keys()) {
      names.add(name);
    }
    
    // Also check for PID files in the state directory
    try {
      const stateDir = paths.stateDir;
      if (await fs.pathExists(stateDir)) {
        const files = await fs.readdir(stateDir);
        for (const file of files) {
          if (file.endsWith('.pid')) {
            const serviceName = file.replace('.pid', '');
            names.add(serviceName);
          }
        }
      }
    } catch (error) {
      // Ignore errors reading state directory
      console.warn('Error reading state directory:', error);
    }
    
    // Stop all services
    for (const name of names) {
      try {
        await this.stop(name);
      } catch (error) {
        console.error(`Failed to stop ${name}:`, error);
      }
    }
  }

  /**
   * Check health of a service
   */
  private async checkHealth(config: ProcessConfig): Promise<boolean> {
    if (!config.healthCheckUrl) {
      return true;
    }

    try {
      const axios = (await import('axios')).default;
      const response = await axios.get(config.healthCheckUrl, {
        timeout: 3000,
        validateStatus: () => true, // Accept any status code
        maxRedirects: 0,
      });
      // For MCP endpoints, any response (even empty) means it's alive
      return response.status >= 200 && response.status < 500;
    } catch (error: any) {
      // Connection refused or timeout means not healthy
      return false;
    }
  }

  /**
   * Wait for service to become healthy
   */
  private async waitForHealthy(
    config: ProcessConfig,
    maxAttempts = 60,
    interval = 2000
  ): Promise<void> {
    if (!config.healthCheckUrl) {
      return;
    }

    let lastError: string = '';
    for (let i = 0; i < maxAttempts; i++) {
      try {
        if (await this.checkHealth(config)) {
          return;
        }
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
      }
      await new Promise((resolve) => setTimeout(resolve, interval));
    }

    // Provide more specific error messages based on service type
    let errorMessage = `Service ${config.name} failed to become healthy after ${maxAttempts * interval / 1000} seconds`;

    if (config.name === 'app') {
      errorMessage += '\n\nPossible causes:';
      errorMessage += '\n  • Handbook directory or Agent.md file missing';
      errorMessage += '\n  • Java application failed to start due to configuration issues';
      errorMessage += '\n  • Required dependencies not available';
      errorMessage += '\n\nCheck the app service logs with: onemcp logs app';
      if (lastError) {
        errorMessage += `\n\nLast health check error: ${lastError}`;
      }
    }

    throw new Error(errorMessage);
  }

  /**
   * Get log output for a service
   */
  async getLogs(name: string, lines = 50): Promise<string> {
    const logPath = paths.getLogPath(name);
    
    if (!(await fs.pathExists(logPath))) {
      return 'No logs available';
    }

    try {
      const content = await fs.readFile(logPath, 'utf-8');
      const allLines = content.split('\n');
      const lastLines = allLines.slice(-lines);
      return lastLines.join('\n');
    } catch (error) {
      return `Error reading logs: ${error}`;
    }
  }


  /**
   * Clear logs for a service
   */
  async clearLogs(name: string): Promise<void> {
    const logPath = paths.getLogPath(name);
    if (await fs.pathExists(logPath)) {
      await fs.remove(logPath);
    }
  }

  /**
   * Archive logs
   */
  async archiveLogs(): Promise<void> {
    const archivePath = paths.getLogArchivePath();
    await fs.ensureDir(archivePath);

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    for (const name of this.configs.keys()) {
      const logPath = paths.getLogPath(name);
      if (await fs.pathExists(logPath)) {
        const archiveName = `${name}-${timestamp}.log`;
        await fs.move(logPath, `${archivePath}/${archiveName}`);
      }
    }
  }
}

export const processManager = new ProcessManager();

