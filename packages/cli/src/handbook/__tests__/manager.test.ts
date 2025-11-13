import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { HandbookManager } from '../manager.js';
import fs from 'fs-extra';

// Mock fs-extra
jest.mock('fs-extra');

describe('HandbookManager', () => {
  let handbookManager: HandbookManager;

  beforeEach(() => {
    jest.clearAllMocks();
    handbookManager = new HandbookManager();
    // Mock console.log to avoid output during tests
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('init', () => {
    it('should create handbook directory structure', async () => {
      jest.spyOn(fs, 'pathExists').mockResolvedValue(false as never);
      jest.spyOn(fs, 'ensureDir').mockResolvedValue(undefined as never);
      jest.spyOn(fs, 'writeFile').mockResolvedValue(undefined as never);

      const result = await handbookManager.init('test-handbook');

      expect(fs.ensureDir).toHaveBeenCalled();
      expect(fs.writeFile).toHaveBeenCalledWith(
        expect.stringContaining('Agent.md'),
        expect.any(String),
        'utf-8'
      );
      expect(result).toContain('test-handbook');
    });

    it('should throw error if handbook already exists', async () => {
      jest.spyOn(fs, 'pathExists').mockResolvedValue(true as never);

      await expect(
        handbookManager.init('existing-handbook')
      ).rejects.toThrow('Handbook directory already exists');
    });

    it('should create required subdirectories', async () => {
      jest.spyOn(fs, 'pathExists').mockResolvedValue(false as never);
      jest.spyOn(fs, 'ensureDir').mockResolvedValue(undefined as never);
      jest.spyOn(fs, 'writeFile').mockResolvedValue(undefined as never);

      await handbookManager.init('test-handbook');

      // Should create multiple directories
      expect(fs.ensureDir).toHaveBeenCalledWith(expect.stringContaining('test-handbook'));
      expect(fs.ensureDir).toHaveBeenCalledWith(expect.stringContaining('apis'));
      expect(fs.ensureDir).toHaveBeenCalledWith(expect.stringContaining('docs'));
    });
  });

  describe('list', () => {
    it('should return list of handbooks', async () => {
      // Mock readdir - first call returns Dirent objects, subsequent calls return string arrays
      const mockDirents = [
        { name: 'handbook1', isDirectory: () => true },
        { name: 'handbook2', isDirectory: () => true },
      ];
      const readdirSpy = jest.spyOn(fs, 'readdir');
      readdirSpy
        .mockResolvedValueOnce(mockDirents as never) // First call for listing handbooks
        .mockResolvedValue([] as never); // Subsequent calls for validation (apis/, docs/ dirs)
      
      // Mock pathExists - return true for validation checks, false for config files
      const pathExistsSpy = jest.spyOn(fs, 'pathExists');
      pathExistsSpy.mockImplementation((path: string) => {
        // Return false for config file paths (so loadHandbookConfig returns null)
        if (typeof path === 'string' && path.includes('/config/handbook.yaml')) {
          return Promise.resolve(false);
        }
        // Return true for all other paths (handbook dirs, Agent.md, etc.)
        return Promise.resolve(true);
      });

      const handbooks = await handbookManager.list();

      expect(handbooks.length).toBe(2);
      expect(handbooks[0].name).toBe('handbook1');
      expect(handbooks[1].name).toBe('handbook2');
    });

    it('should return empty array if handbooks directory does not exist', async () => {
      jest.spyOn(fs, 'readdir').mockRejectedValue(new Error('ENOENT') as never);

      await expect(handbookManager.list()).rejects.toThrow();
    });
  });

  describe('validate', () => {
    it('should validate handbook structure', async () => {
      const pathExistsSpy = jest.spyOn(fs, 'pathExists');
      pathExistsSpy
        .mockResolvedValueOnce(true as never) // handbook dir exists
        .mockResolvedValueOnce(true as never) // Agent.md exists
        .mockResolvedValueOnce(true as never) // apis/ directory exists (required check)
        .mockResolvedValueOnce(true as never) // docs/ directory exists (recommended check)
        .mockResolvedValueOnce(true as never) // state/ directory exists (recommended check)
        .mockResolvedValueOnce(true as never) // apis/ directory exists (for OpenAPI check)
        .mockResolvedValueOnce(true as never); // docs/ directory exists (for docs check)

      // Mock readdir for apis and docs directories
      jest.spyOn(fs, 'readdir').mockResolvedValue([] as never);

      const result = await handbookManager.validate('test-handbook');

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return errors if handbook directory does not exist', async () => {
      jest.spyOn(fs, 'pathExists').mockResolvedValue(false as never);

      const result = await handbookManager.validate('non-existent');

      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });

    it('should return errors if Agent.md is missing', async () => {
      const pathExistsSpy = jest.spyOn(fs, 'pathExists');
      pathExistsSpy
        .mockResolvedValueOnce(true as never)  // handbook dir exists
        .mockResolvedValueOnce(false as never); // Agent.md missing

      const result = await handbookManager.validate('test-handbook');

      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.includes('Agent.md'))).toBe(true);
    });
  });
});
