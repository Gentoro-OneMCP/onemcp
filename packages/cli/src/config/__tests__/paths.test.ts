import { describe, it, expect, beforeEach } from '@jest/globals';
import { PathManager } from '../paths.js';
import { homedir } from 'os';
import { join } from 'path';

describe('PathManager', () => {
  let pathManager: PathManager;

  beforeEach(() => {
    pathManager = PathManager.getInstance();
  });

  describe('getInstance', () => {
    it('should return a singleton instance', () => {
      const instance1 = PathManager.getInstance();
      const instance2 = PathManager.getInstance();
      expect(instance1).toBe(instance2);
    });
  });

  describe('path properties', () => {
    it('should have correct configRoot path', () => {
      expect(pathManager.configRoot).toBe(join(homedir(), '.onemcp'));
    });

    it('should have correct configFile path', () => {
      expect(pathManager.configFile).toBe(join(homedir(), '.onemcp', 'config.yaml'));
    });

    it('should have correct servicesDir path', () => {
      expect(pathManager.servicesDir).toBe(join(homedir(), '.onemcp', 'services'));
    });

    it('should have correct handbooksDir path', () => {
      expect(pathManager.handbooksDir).toBe(join(homedir(), 'handbooks'));
    });

    it('should have correct logDir path', () => {
      expect(pathManager.logDir).toBe(join(homedir(), '.onemcp', 'logs'));
    });

    it('should have correct stateDir path', () => {
      expect(pathManager.stateDir).toBe(join(homedir(), '.onemcp', 'state'));
    });

    it('should have correct cacheDir path', () => {
      expect(pathManager.cacheDir).toBe(join(homedir(), '.onemcp', 'cache'));
    });
  });

  describe('getServiceConfigPath', () => {
    it('should return correct service config path', () => {
      const serviceName = 'test-service';
      const expected = join(homedir(), '.onemcp', 'services', `${serviceName}.yaml`);
      expect(pathManager.getServiceConfigPath(serviceName)).toBe(expected);
    });
  });

  describe('getHandbookPath', () => {
    it('should return correct handbook path', () => {
      const handbookName = 'my-handbook';
      const expected = join(homedir(), 'handbooks', handbookName);
      expect(pathManager.getHandbookPath(handbookName)).toBe(expected);
    });
  });

  describe('getLogPath', () => {
    it('should return correct log file path', () => {
      const serviceName = 'app';
      const expected = join(homedir(), '.onemcp', 'logs', `${serviceName}.log`);
      expect(pathManager.getLogPath(serviceName)).toBe(expected);
    });
  });

  describe('getLogArchivePath', () => {
    it('should return correct log archive path', () => {
      const expected = join(homedir(), '.onemcp', 'logs', 'archive');
      expect(pathManager.getLogArchivePath()).toBe(expected);
    });
  });

  describe('getPidFilePath', () => {
    it('should return correct PID file path', () => {
      const serviceName = 'app';
      const expected = join(homedir(), '.onemcp', 'state', `${serviceName}.pid`);
      expect(pathManager.getPidFilePath(serviceName)).toBe(expected);
    });
  });

  describe('getSocketPath', () => {
    it('should return correct socket file path', () => {
      const serviceName = 'app';
      const expected = join(homedir(), '.onemcp', 'state', `${serviceName}.sock`);
      expect(pathManager.getSocketPath(serviceName)).toBe(expected);
    });
  });
});

