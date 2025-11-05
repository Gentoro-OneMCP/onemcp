/**
 * Core types for OneMCP CLI
 */

export type ModelProvider = 'openai' | 'gemini' | 'anthropic';

export interface GlobalConfig {
  provider: ModelProvider;
  apiKeys: {
    openai?: string;
    gemini?: string;
    anthropic?: string;
  };
  defaultPort: number;
  handbookDir: string;
  currentHandbook?: string; // Currently active handbook name
  logDir: string;
  baseUrl?: string;
  modelName?: string;
  chatTimeout?: number; // Timeout in milliseconds for chat requests
}

export interface ServiceConfig {
  service: string;
  header: string;
  pattern: string;
  token: string;
  expiresAt?: string;
  baseUrl?: string;
}

export interface HandbookConfig {
  name: string;
  version: string;
  description?: string;
  services: ServiceReference[];
  provider?: ModelProvider;
  apiKeys?: {
    openai?: string;
    gemini?: string;
    anthropic?: string;
  };
  modelName?: string;
  baseUrl?: string;
  chatTimeout?: number;
}

export interface ServiceReference {
  name: string;
  baseUrl: string;
  authType: 'bearer' | 'api-key' | 'custom';
  openApiSpec?: string;
}

export interface HandbookInfo {
  name: string;
  path: string;
  valid: boolean;
  config?: HandbookConfig;
}

export interface ServiceStatus {
  name: string;
  running: boolean;
  pid?: number;
  port?: number;
  uptime?: string;
  healthy?: boolean;
}

export interface AgentStatus {
  running: boolean;
  services: ServiceStatus[];
  mcpUrl?: string;
  handbookDir?: string;
  currentHandbook?: string;
}

export interface LogEntry {
  timestamp: string;
  level: 'info' | 'warn' | 'error' | 'debug';
  service: string;
  message: string;
}

export interface ChatMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

export interface ChatSession {
  id: string;
  provider: ModelProvider;
  messages: ChatMessage[];
  createdAt: string;
}

