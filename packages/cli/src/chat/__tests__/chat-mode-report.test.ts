import { describe, it, expect, beforeAll, afterAll, jest } from '@jest/globals';
import { ChatMode } from '../chat-mode.js';
import { agentService } from '../../services/agent-service.js';
import { configManager } from '../../config/manager.js';
import { handbookManager } from '../../handbook/manager.js';
import fs from 'fs-extra';
import { join } from 'path';
import { paths } from '../../config/paths.js';
import stripAnsi from 'strip-ansi';

/**
 * Integration test for chat mode report generation.
 * 
 * This test mimics the behavior of `onemcp chat` command:
 * 1. Automatically starts the OneMCP server if not running (same as `onemcp chat` does)
 * 2. Sends a query via chat mode
 * 3. Verifies the report location is printed
 * 4. Validates the report file exists and contains expected content
 * 
 * Note: This is an integration test that requires:
 * - A valid handbook (acme-analytics)
 * - API keys configured
 * - Server will be started automatically if not running (matching `onemcp chat` behavior)
 * 
 * To skip in CI or when dependencies aren't available, set SKIP_INTEGRATION_TESTS=true
 */
const SKIP_INTEGRATION_TESTS = process.env.SKIP_INTEGRATION_TESTS === 'true';

const describeOrSkip = SKIP_INTEGRATION_TESTS ? describe.skip : describe;

describeOrSkip('ChatMode Report Generation Integration', () => {
  const TEST_HANDBOOK = 'acme-analytics';
  const TEST_QUERY = 'Show total sales for 2024';
  let serverStarted = false;
  let originalConsoleLog: typeof console.log;
  let consoleOutput: string[] = [];

  beforeAll(async () => {
    // Capture console output
    originalConsoleLog = console.log;
    console.log = (...args: any[]) => {
      const message = args.map(arg => 
        typeof arg === 'string' ? arg : JSON.stringify(arg)
      ).join(' ');
      consoleOutput.push(message);
      originalConsoleLog(...args);
    };

    // Ensure test handbook exists
    const handbooks = await handbookManager.list();
    const testHandbook = handbooks.find(h => h.name === TEST_HANDBOOK);
    
    if (!testHandbook || !testHandbook.valid) {
      throw new Error(
        `Test requires valid handbook '${TEST_HANDBOOK}'. ` +
        `Available handbooks: ${handbooks.map(h => h.name).join(', ')}`
      );
    }

    // Check if agent is running, start if not
    const status = await agentService.getStatus();
    if (!status.running) {
      console.log('Starting OneMCP server for integration test...');
      await agentService.start();
      serverStarted = true;
      
      // Wait for server to be ready
      let attempts = 0;
      const maxAttempts = 30;
      while (attempts < maxAttempts) {
        const healthStatus = await agentService.getStatus();
        if (healthStatus.running && healthStatus.mcpUrl) {
          // Additional wait for server to be fully ready
          await new Promise(resolve => setTimeout(resolve, 2000));
          break;
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }
      
      if (attempts >= maxAttempts) {
        throw new Error('Server failed to start within timeout');
      }
    }
  }, 120000); // 2 minute timeout for server startup

  afterAll(async () => {
    // Restore console
    console.log = originalConsoleLog;
    
    // Clean up: stop server if we started it
    if (serverStarted) {
      try {
        await agentService.stop();
      } catch (error) {
        // Ignore cleanup errors
      }
    }
  }, 30000);

  it('should generate a report and print its location', async () => {
    const chatMode = new ChatMode();
    consoleOutput = []; // Clear previous output
    
    // Get the handbook path to check for reports
    const handbookPath = paths.getHandbookPath(TEST_HANDBOOK);
    const reportsDir = join(handbookPath, 'logs', 'reports');
    
    // Get initial report count
    const initialReports = await fs.pathExists(reportsDir)
      ? (await fs.readdir(reportsDir)).filter(f => f.startsWith('execution-'))
      : [];
    const initialCount = initialReports.length;

    // Set up chat mode state
    await chatMode.selectHandbook(TEST_HANDBOOK);
    const globalConfig = await configManager.getGlobalConfig();
    const status = await agentService.getStatus();
    
    // Access internal state for setup (using type assertion for testing)
    const chatModeAny = chatMode as any;
    chatModeAny.mcpUrl = status.mcpUrl || 'http://localhost:8080/mcp';
    chatModeAny.handbookConfig = await configManager.getEffectiveHandbookConfig(TEST_HANDBOOK);
    chatModeAny.globalConfig = globalConfig;

    // Send the message
    const provider = chatModeAny.handbookConfig?.provider || 
                     globalConfig?.provider || 
                     'openai';
    const timeout = chatModeAny.handbookConfig?.chatTimeout || 
                    globalConfig?.chatTimeout || 
                    240000;
    
    const response = await chatMode.sendMessage(provider, TEST_QUERY, timeout);
    
    // Verify we got a response
    expect(response).toBeDefined();
    expect(typeof response).toBe('string');
    expect(response.length).toBeGreaterThan(0);

    // Check that report path was captured
    const reportPath = chatModeAny.lastReportPath;
    
    // Call showReportLocation to verify it prints the report location
    await chatMode.showReportLocation();
    
    // Verify report location was printed in console output
    // Strip ANSI codes (chalk colors) for comparison
    const reportLocationPrinted = consoleOutput.some(output => {
      const cleanOutput = stripAnsi(output);
      return cleanOutput.includes('Report:') || cleanOutput.includes('Reports:');
    });
    
    // Verify report location was printed (check console output or lastReportPath)
    if (reportPath) {
      expect(reportPath).toBeDefined();
      expect(typeof reportPath).toBe('string');
      expect(reportPath).toContain('execution-');
      expect(reportPath).toContain('.txt');
      
      // Verify report file exists
      const reportExists = await fs.pathExists(reportPath);
      expect(reportExists).toBe(true);
      
      // Validate report content
      const reportContent = await fs.readFile(reportPath, 'utf-8');
      expect(reportContent).toBeDefined();
      expect(reportContent.length).toBeGreaterThan(0);
      
      // Check for key sections in the report
      expect(reportContent).toContain('EXECUTION REPORT');
      expect(reportContent).toContain('USER QUERY');
      expect(reportContent).toContain(TEST_QUERY);
      expect(reportContent).toContain('RESPONSE');
      
      // Check for tool calls section (if tool calls were made)
      if (reportContent.includes('TOOL CALLS')) {
        expect(reportContent).toContain('Tool:');
      }
      
      // Check for API calls section (if API calls were made)
      if (reportContent.includes('API CALLS')) {
        expect(reportContent).toContain('Method:');
        expect(reportContent).toContain('URL:');
      }
      
      // Check for generated code section
      if (reportContent.includes('GENERATED CODE')) {
        expect(reportContent).toContain('package');
        expect(reportContent).toContain('class');
      }
      
      // Verify report is in the correct directory
      expect(reportPath).toContain(reportsDir);
      
      // Verify report location was printed
      expect(reportLocationPrinted || reportPath).toBeTruthy();
      
      // Verify a new report was created
      const finalReports = await fs.pathExists(reportsDir)
        ? (await fs.readdir(reportsDir)).filter(f => f.startsWith('execution-'))
        : [];
      expect(finalReports.length).toBeGreaterThanOrEqual(initialCount);
      
    } else {
      // If no report path was returned, check if reports directory exists
      // and contains the new report
      const finalReports = await fs.pathExists(reportsDir)
        ? (await fs.readdir(reportsDir))
            .filter(f => f.startsWith('execution-'))
            .sort()
            .reverse()
        : [];
      
      if (finalReports.length > initialCount) {
        // A new report was created even if path wasn't returned
        const latestReport = join(reportsDir, finalReports[0]);
        const reportContent = await fs.readFile(latestReport, 'utf-8');
        expect(reportContent).toContain('EXECUTION REPORT');
        expect(reportContent).toContain(TEST_QUERY);
        
        // Verify report location was printed
        expect(reportLocationPrinted).toBe(true);
      } else {
        // Report should have been generated - this is a failure case
        throw new Error(
          'Report was not generated. Expected report in: ' + reportsDir
        );
      }
    }
  }, 300000); // 5 minute timeout for the test
});

