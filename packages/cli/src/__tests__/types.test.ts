import { describe, it, expect } from '@jest/globals';
import type {
  GlobalConfig,
  ServiceConfig,
  HandbookConfig,
  ModelProvider,
  ServiceStatus,
  AgentStatus,
  ChatMessage,
  ChatSession,
  LogEntry,
  ServiceReference,
} from '../types.js';

describe('Type Definitions', () => {
  describe('ModelProvider', () => {
    it('should accept valid provider types', () => {
      const validProviders: ModelProvider[] = ['openai', 'gemini', 'anthropic'];
      expect(validProviders).toHaveLength(3);
    });
  });

  describe('GlobalConfig', () => {
    it('should create valid config object', () => {
      const config: GlobalConfig = {
        provider: 'openai',
        apiKeys: {
          openai: 'sk-test-123',
        },
        modelName: 'gpt-4',
        handbookDir: '/path/to/handbooks',
        defaultPort: 8080,
        logDir: '/var/log',
      };

      expect(config.provider).toBe('openai');
      expect(config.apiKeys.openai).toBe('sk-test-123');
    });

    it('should allow optional fields to be undefined', () => {
      const minimalConfig: GlobalConfig = {
        provider: 'openai',
        apiKeys: {
          openai: 'sk-test-123',
        },
        defaultPort: 8080,
        handbookDir: '/handbooks',
        logDir: '/logs',
      };

      expect(minimalConfig.baseUrl).toBeUndefined();
      expect(minimalConfig.modelName).toBeUndefined();
    });
  });

  describe('ServiceConfig', () => {
    it('should create valid service config', () => {
      const config: ServiceConfig = {
        service: 'test-service',
        header: 'Authorization',
        pattern: 'Bearer {token}',
        token: 'test-token-123',
        baseUrl: 'https://api.test.com',
      };

      expect(config.service).toBe('test-service');
      expect(config.header).toBe('Authorization');
      expect(config.pattern).toBe('Bearer {token}');
    });

    it('should allow optional fields', () => {
      const config: ServiceConfig = {
        service: 'test-service',
        header: 'Authorization',
        pattern: 'Bearer {token}',
        token: 'test-token',
      };

      expect(config.baseUrl).toBeUndefined();
      expect(config.expiresAt).toBeUndefined();
    });
  });

  describe('ServiceStatus', () => {
    it('should create valid service status', () => {
      const status: ServiceStatus = {
        name: 'app',
        running: true,
        pid: 12345,
        port: 8080,
        uptime: '1h 30m 45s',
        healthy: true,
      };

      expect(status.running).toBe(true);
      expect(status.pid).toBe(12345);
      expect(status.healthy).toBe(true);
    });

    it('should allow optional fields', () => {
      const status: ServiceStatus = {
        name: 'app',
        running: false,
      };

      expect(status.pid).toBeUndefined();
      expect(status.healthy).toBeUndefined();
    });
  });

  describe('AgentStatus', () => {
    it('should create valid agent status', () => {
      const status: AgentStatus = {
        running: true,
        services: [
          { name: 'app', running: true, pid: 12345 },
          { name: 'mock', running: true, pid: 12346 },
        ],
        mcpUrl: 'http://localhost:8080/mcp',
        handbookDir: '/path/to/handbooks',
      };

      expect(status.running).toBe(true);
      expect(status.services).toHaveLength(2);
      expect(status.mcpUrl).toBeDefined();
    });
  });

  describe('ChatMessage', () => {
    it('should create valid chat message', () => {
      const message: ChatMessage = {
        role: 'user',
        content: 'Hello, how are you?',
      };

      expect(message.role).toBe('user');
      expect(message.content).toBe('Hello, how are you?');
    });

    it('should accept assistant role', () => {
      const message: ChatMessage = {
        role: 'assistant',
        content: 'I am doing well, thank you!',
      };

      expect(message.role).toBe('assistant');
    });

    it('should accept system role', () => {
      const message: ChatMessage = {
        role: 'system',
        content: 'You are a helpful assistant.',
      };

      expect(message.role).toBe('system');
    });
  });

  describe('ChatSession', () => {
    it('should create valid chat session', () => {
      const session: ChatSession = {
        id: 'session-123',
        provider: 'openai',
        messages: [
          { role: 'user', content: 'Hello' },
          { role: 'assistant', content: 'Hi there!' },
        ],
        createdAt: '2025-01-01T00:00:00Z',
      };

      expect(session.id).toBe('session-123');
      expect(session.messages).toHaveLength(2);
    });
  });

  describe('HandbookConfig', () => {
    it('should create valid handbook config', () => {
      const config: HandbookConfig = {
        name: 'my-handbook',
        version: '1.0.0',
        description: 'Test handbook',
        services: [
          {
            name: 'test-service',
            baseUrl: 'https://api.test.com',
            authType: 'bearer',
          },
        ],
      };

      expect(config.name).toBe('my-handbook');
      expect(config.description).toBe('Test handbook');
      expect(config.services).toHaveLength(1);
    });

    it('should allow optional description', () => {
      const config: HandbookConfig = {
        name: 'my-handbook',
        version: '1.0.0',
        services: [],
      };

      expect(config.description).toBeUndefined();
    });
  });

  describe('ServiceReference', () => {
    it('should create valid service reference', () => {
      const ref: ServiceReference = {
        name: 'test-service',
        baseUrl: 'https://api.test.com',
        authType: 'bearer',
        openApiSpec: 'https://api.test.com/openapi.json',
      };

      expect(ref.authType).toBe('bearer');
      expect(ref.openApiSpec).toBeDefined();
    });

    it('should allow optional openApiSpec', () => {
      const ref: ServiceReference = {
        name: 'test-service',
        baseUrl: 'https://api.test.com',
        authType: 'api-key',
      };

      expect(ref.openApiSpec).toBeUndefined();
    });
  });

  describe('LogEntry', () => {
    it('should create valid log entry', () => {
      const entry: LogEntry = {
        timestamp: '2025-01-01T00:00:00Z',
        level: 'info',
        service: 'app',
        message: 'Application started',
      };

      expect(entry.level).toBe('info');
      expect(entry.service).toBe('app');
    });

    it('should accept all log levels', () => {
      const levels: LogEntry['level'][] = ['info', 'warn', 'error', 'debug'];
      expect(levels).toHaveLength(4);
    });
  });
});
