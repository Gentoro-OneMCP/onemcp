import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals';
import { ProcessManager, type ProcessConfig } from '../process-manager.js';
import { EventEmitter } from 'events';

// Create mock spawn function
const mockSpawn = jest.fn();

// Mock child_process
jest.mock('child_process', () => ({
  spawn: mockSpawn,
}));

// Mock fs-extra
jest.mock('fs-extra');

// Mock config paths
jest.mock('../../config/paths.js', () => ({
  paths: {
    ensureDirectories: jest.fn(async () => undefined),
    getLogPath: jest.fn(() => '/tmp/test.log'),
    getPidFilePath: jest.fn(() => '/tmp/test.pid'),
  },
}));

describe('ProcessManager', () => {
  let processManager: ProcessManager;
  let mockProcess: any;

  beforeEach(async () => {
    jest.clearAllMocks();
    
    // Create a mock child process
    mockProcess = new EventEmitter();
    mockProcess.pid = 12345;
    mockProcess.killed = false;
    mockProcess.stdout = new EventEmitter();
    mockProcess.stderr = new EventEmitter();
    mockProcess.unref = jest.fn();
    
    // Mock spawn to return our mock process
    mockSpawn.mockReturnValue(mockProcess);
    
    // Dynamically import fs-extra to set up mocks
    const fs = await import('fs-extra');
    jest.spyOn(fs.default, 'createWriteStream').mockReturnValue({
      write: jest.fn(),
      end: jest.fn(),
      on: jest.fn(),
    } as any);
    jest.spyOn(fs.default, 'writeFile').mockResolvedValue(undefined as never);
    jest.spyOn(fs.default, 'readFile').mockResolvedValue('12345' as never);
    jest.spyOn(fs.default, 'pathExists').mockResolvedValue(false as never);
    jest.spyOn(fs.default, 'remove').mockResolvedValue(undefined as never);
    
    // Create a new instance for each test
    processManager = new ProcessManager();
  });

  afterEach(async () => {
    // Clean up any running processes
    try {
      await processManager.stopAll();
    } catch (e) {
      // Ignore errors during cleanup
    }
    processManager.removeAllListeners();
  });

  describe('register', () => {
    it('should register a process configuration', () => {
      const config: ProcessConfig = {
        name: 'unique-test-service-1',
        command: 'node',
        args: ['test.js'],
        env: { NODE_ENV: 'test' },
        port: 3000,
      };

      processManager.register(config);
      
      // Register should not throw, it overwrites existing configs
      expect(() => processManager.register(config)).not.toThrow();
    });

    it('should allow re-registering a process (overwrites)', () => {
      const config: ProcessConfig = {
        name: 'unique-test-service-2',
        command: 'node',
        args: ['test.js'],
      };

      processManager.register(config);
      
      // Re-registering should not throw
      expect(() => processManager.register(config)).not.toThrow();
    });
  });

  describe('isRunning', () => {
    it('should return false for unregistered process', async () => {
      expect(await processManager.isRunning('non-existent')).toBe(false);
    });

    it('should return false for registered but not started process', async () => {
      processManager.register({
        name: 'test',
        command: 'node',
        args: ['test.js'],
      });

      expect(await processManager.isRunning('test')).toBe(false);
    });
  });

  describe('process spawning', () => {
    it('should spawn process with correct command and args', async () => {
      const config: ProcessConfig = {
        name: 'spawn-test-1',
        command: 'sleep',
        args: ['10'],
        env: { NODE_ENV: 'test' },
      };

      processManager.register(config);
      
      await processManager.start('spawn-test-1');
      
      // Wait a bit for process to be registered
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Verify the process is running
      expect(await processManager.isRunning('spawn-test-1')).toBe(true);
    });

    it('should track running processes', async () => {
      processManager.register({
        name: 'spawn-test-2',
        command: 'sleep',
        args: ['10'],
      });

      await processManager.start('spawn-test-2');
      
      // Wait a bit for process to be registered
      await new Promise(resolve => setTimeout(resolve, 100));
      
      expect(await processManager.isRunning('spawn-test-2')).toBe(true);
    });

    it('should emit start event with PID', async () => {
      processManager.register({
        name: 'spawn-test-3',
        command: 'echo',
        args: ['test'],
      });

      const promise = new Promise<{name: string, pid: number}>((resolve) => {
        processManager.once('start', ({ name, pid }) => {
          resolve({ name, pid });
        });
      });

      await processManager.start('spawn-test-3');
      const result = await promise;
      
      expect(result.name).toBe('spawn-test-3');
      expect(result.pid).toBeDefined();
      expect(typeof result.pid).toBe('number');
    });
  });

  describe('process error handling', () => {
    it('should emit error event for non-existent commands', async () => {
      processManager.register({
        name: 'error-test-required',
        command: 'non-existent-command-12345',
        args: [],
      });

      const errorPromise = new Promise((resolve) => {
        processManager.once('error', ({ name, error }) => {
          resolve({ name, error });
        });
      });

      // Start will fail but shouldn't throw
      await processManager.start('error-test-required').catch(() => {});
      
      const result: any = await errorPromise;
      expect(result.name).toBe('error-test-required');
      expect(result.error).toBeDefined();
      expect(result.error.message).toContain('ENOENT');
    });

    it('should not emit error for optional services like otel', async () => {
      processManager.register({
        name: 'otel',
        command: 'non-existent-otelcol',
        args: [],
      });

      let errorEmitted = false;
      processManager.on('error', () => {
        errorEmitted = true;
      });

      // Start will fail silently for optional service
      await processManager.start('otel').catch(() => {});
      
      // Wait a bit to ensure no error event was emitted
      await new Promise(resolve => setTimeout(resolve, 100));
      
      expect(errorEmitted).toBe(false);
    });
  });

  describe('getStatus', () => {
    it('should return status for registered process', async () => {
      processManager.register({
        name: 'status-test-1',
        command: 'node',
        args: ['test.js'],
        port: 3000,
      });

      const status = await processManager.getStatus('status-test-1');
      
      expect(status).toEqual({
        name: 'status-test-1',
        running: false,
        pid: undefined,
        port: 3000,
        uptime: undefined,
        healthy: undefined,
      });
    });

    it('should return status with PID for running process', async () => {
      processManager.register({
        name: 'status-test-2',
        command: 'node',
        args: ['test.js'],
        port: 3000,
      });

      await processManager.start('status-test-2');
      
      const status = await processManager.getStatus('status-test-2');
      
      expect(status.running).toBe(true);
      expect(status.pid).toBeDefined();
    });
  });

  describe('getAllStatus', () => {
    it('should return status for all registered processes', async () => {
      processManager.register({
        name: 'all-status-service1',
        command: 'node',
        args: ['test1.js'],
      });
      
      processManager.register({
        name: 'all-status-service2',
        command: 'node',
        args: ['test2.js'],
      });

      const statuses = await processManager.getAllStatus();
      
      expect(statuses).toHaveLength(2);
      expect(statuses.some(s => s.name === 'all-status-service1')).toBe(true);
      expect(statuses.some(s => s.name === 'all-status-service2')).toBe(true);
    });

    it('should return empty array if no processes registered', async () => {
      const statuses = await processManager.getAllStatus();
      
      expect(statuses).toEqual([]);
    });
  });
});

