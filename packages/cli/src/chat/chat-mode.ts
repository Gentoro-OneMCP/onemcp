/**
 * Interactive chat mode for OneMCP
 */
import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { configManager } from '../config/manager.js';
import { agentService } from '../services/agent-service.js';
import { handbookManager } from '../handbook/manager.js';
import { paths } from '../config/paths.js';
import { ChatMessage, ModelProvider, GlobalConfig, HandbookConfig } from '../types.js';

export class ChatMode {
  private messages: ChatMessage[] = [];
  private mcpUrl: string = '';
  private agentStatus: any = null;
  private currentHandbook: string = '';
  private handbookConfig: HandbookConfig | null = null;
  private globalConfig: GlobalConfig | null = null;
  private lastReportPath: string | null = null;
  private lastUserMessage: string | null = null;
  private cacheEnabled: boolean = true;

  /**
   * Start interactive chat session
   */
  async start(handbookName?: string): Promise<void> {
    // Select handbook
    await this.selectHandbook(handbookName);
    this.globalConfig = await configManager.getGlobalConfig();
    
    // Load cache enabled setting from config (not hardcoded default)
    this.cacheEnabled = this.globalConfig?.cache?.enabled ?? false;
    
    // Check if agent is running
    this.agentStatus = await agentService.getStatus();
    if (!this.agentStatus.running) {
      console.log(chalk.red('❌ OneMCP is not running.'));
      console.log(chalk.yellow('Services should start automatically when you run: onemcp chat'));
      return;
    }

    this.mcpUrl = this.agentStatus.mcpUrl || 'http://localhost:8080/mcp';
    const providerName = this.handbookConfig?.provider || this.globalConfig?.provider || 'Not configured';
    // Get model name from effective config (handbook config already merged with global)
    const modelName = this.handbookConfig?.modelName || this.globalConfig?.modelName;

    console.log();
    console.log(chalk.bold.cyan('╔══════════════════════════════════════╗'));
    console.log(chalk.bold.cyan('║    Gentoro OneMCP - Chat Mode        ║'));
    console.log(chalk.bold.cyan('╚══════════════════════════════════════╝'));
    console.log();
    // Show absolute path of handbook, like in the report
    const handbookPath = paths.getHandbookPath(this.currentHandbook);
    console.log(chalk.dim(`Handbook: ${handbookPath}`));
    console.log(chalk.dim(`Provider: ${providerName}`));
    if (modelName) {
      console.log(chalk.dim(`Model: ${modelName}`));
    }
    console.log(chalk.dim(`MCP URL: ${this.mcpUrl}`));
    const cacheStatus = this.cacheEnabled ? chalk.green('enabled') : chalk.yellow('disabled');
    console.log(chalk.dim('Cache: ') + cacheStatus);
    console.log();
    if (this.isAcmeHandbook()) {
      this.showAcmeExamples();
    } else {
      console.log(chalk.dim('━'.repeat(60)));
      console.log();
    }

    // Main chat loop
    await this.chatLoop();
  }

  /**
   * Main chat interaction loop
   */
  private async chatLoop(): Promise<void> {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      console.log(chalk.dim('Enter prompt or ') + chalk.cyan('?') + chalk.dim(' for special commands'));
      const { message } = await inquirer.prompt([
        {
          type: 'input',
          name: 'message',
          message: chalk.cyan('> '),
          prefix: '',
          validate: (input: string) => {
            if (!input || input.trim() === '') {
              return 'Enter a prompt or ? for list of special commands';
            }
            return true;
          },
        },
      ]);
      const userMessage = message.trim();

      // Handle special commands
      if (userMessage.toLowerCase() === 'exit' || userMessage.toLowerCase() === 'quit') {
        console.log(chalk.yellow('\n👋 Goodbye!'));
        break;
      }

      if (userMessage === '?' || userMessage.toLowerCase() === 'help') {
        this.showHelp();
        continue;
      }

      if (userMessage.toLowerCase() === 'cache') {
        // Toggle cache setting
        this.cacheEnabled = !this.cacheEnabled;
        const status = this.cacheEnabled ? chalk.green('enabled') : chalk.yellow('disabled');
        console.log(chalk.cyan(`Cache mode: ${status}`));
        
        // Update global config
        try {
          await configManager.updateGlobalConfig({ cache: { enabled: this.cacheEnabled } });
          
          // Reload config to ensure we have the latest value
          this.globalConfig = await configManager.getGlobalConfig();
          this.cacheEnabled = this.globalConfig?.cache?.enabled ?? false;
          
          // Check if server is running and restart it
          const agentStatus = await agentService.getStatus();
          if (agentStatus.running) {
            console.log(chalk.dim('🔄 Restarting server to apply changes...'));
            try {
              await agentService.restart('app');
              console.log(chalk.green('✅  Server restarted successfully'));
            } catch (restartError: any) {
              console.log(chalk.yellow('⚠️  Failed to restart server:'), restartError.message);
              console.log(chalk.dim('Please restart the server manually for changes to take effect'));
            }
          } else {
            console.log(chalk.dim('Server is not running. Changes will take effect when you start the server.'));
          }
        } catch (error: any) {
          console.log(chalk.yellow('⚠️  Failed to update config:'), error.message);
        }
        
        console.log();
        continue;
      }

      if (userMessage.toLowerCase() === 'index') {
        console.log(chalk.cyan('🔄 Re-indexing handbook schema...'));
        console.log();
        
        const spinner = ora({
          text: 'Starting index operation...',
          spinner: 'dots',
          color: 'cyan'
        }).start();
        
        try {
          // Create MCP transport and client (scoped to this command execution)
          const transport = new StreamableHTTPClientTransport(new URL(this.mcpUrl), {
            requestInit: {
              signal: AbortSignal.timeout(this.handbookConfig?.chatTimeout || this.globalConfig?.chatTimeout || 240000),
            },
          });
          const client = new Client(
            {
              name: 'onemcp-cli',
              version: '0.1.0',
            },
            {
              capabilities: {},
            }
          );

          await client.connect(transport);

          try {
            // Call the onemcp.run tool with __index_schema__ command
            const result: any = await client.callTool(
              {
                name: 'onemcp.run',
                arguments: {
                  prompt: '__index_schema__',
                },
              },
              undefined,
              { timeout: this.handbookConfig?.chatTimeout || this.globalConfig?.chatTimeout || 240000 }
            );

            spinner.stop();

            // Parse response
            if (result.content && result.content.length > 0) {
              const content = result.content[0];
              if (content && typeof content === 'object' && 'type' in content && 'text' in content && content.type === 'text') {
                try {
                  const parsed = JSON.parse(content.text);
                  
                  // Display status messages if available
                  if (parsed && typeof parsed === 'object' && 'messages' in parsed && Array.isArray(parsed.messages)) {
                    for (const msg of parsed.messages) {
                      if (typeof msg === 'string') {
                        if (msg.startsWith('✓') || msg.startsWith('✅')) {
                          console.log(chalk.green(msg));
                        } else if (msg.startsWith('⚠')) {
                          console.log(chalk.yellow(msg));
                        } else if (msg.startsWith('ℹ')) {
                          console.log(chalk.blue(msg));
                        } else {
                          console.log(chalk.cyan(msg));
                        }
                      }
                    }
                  }
                  
                  // Display final result
                  if (parsed && typeof parsed === 'object' && 'content' in parsed) {
                    const responseContent = parsed.content;
                    if (typeof responseContent === 'string' && responseContent.trim().length > 0) {
                      if (parsed.status === 'success') {
                        console.log(chalk.green('✅ ' + responseContent));
                      } else {
                        console.log(chalk.red('❌ ' + responseContent));
                      }
                    }
                  }
                  
                  // Display stats if available
                  if (parsed && typeof parsed === 'object' && 'stats' in parsed && parsed.stats) {
                    const stats = parsed.stats;
                    console.log(chalk.dim(`   Actions: ${stats.actions || 0}, Entities: ${stats.entities || 0}, Fields: ${stats.fields || 0}`));
                    if (stats.durationMs) {
                      console.log(chalk.dim(`   Duration: ${(stats.durationMs / 1000).toFixed(1)}s`));
                    }
                  }
                  
                  // Display report path if available
                  if (parsed && typeof parsed === 'object' && 'reportPath' in parsed && parsed.reportPath) {
                    console.log(chalk.dim('   Report: ' + parsed.reportPath));
                  }
                  
                  console.log();
                  continue;
                } catch (parseError: any) {
                  // If parsing fails, show raw response
                  spinner.stop();
                  console.log(chalk.red('❌ Failed to parse server response for index command:'), parseError.message);
                  console.log(chalk.dim('Raw response: ' + content.text));
                  console.log();
                  continue;
                }
              }
            }
            
            spinner.stop();
            console.log(chalk.red('❌ Unexpected response format for index command.'));
            console.log();
            continue;
          } catch (toolCallError: any) {
            spinner.stop();
            console.log(chalk.red('❌ Failed to execute index command via tool call:'), toolCallError.message);
            if (toolCallError.stack) {
              console.log(chalk.dim(toolCallError.stack));
            }
            console.log();
            continue;
          } finally {
            await client.close();
          }
        } catch (transportError: any) {
          spinner.stop();
          console.log(chalk.red('❌ Failed to connect to OneMCP server for index command:'), transportError.message);
          if (transportError.stack) {
            console.log(chalk.dim(transportError.stack));
          }
          console.log();
          continue;
        }
      }

      if (userMessage.toLowerCase() === 'replan' || userMessage.toLowerCase() === 'regenerate') {
        if (!this.lastUserMessage) {
          console.log(chalk.yellow('⚠️  No previous message to replan.'));
          console.log();
        continue;
      }
        console.log(chalk.cyan(`🔄 Replanning for: "${this.lastUserMessage}"`));
        console.log();
        // Delete cache file and resubmit
        await this.replanWithCacheDeletion(
          this.handbookConfig?.provider || this.globalConfig?.provider || 'openai',
          this.lastUserMessage,
          this.handbookConfig?.chatTimeout || this.globalConfig?.chatTimeout || 240000
        );
        continue;
      }

      // Store user message for potential regeneration
      this.lastUserMessage = userMessage;

      // Add user message to history
      this.messages.push({
        role: 'user',
        content: userMessage,
      });

      // Send to OneMCP and get response
      const spinner = ora({
        text: 'Thinking...',
        spinner: 'dots',
        color: 'cyan'
      }).start();

      const startTime = Date.now();
      try {
        const timeout = this.handbookConfig?.chatTimeout || this.globalConfig?.chatTimeout || 240000; // Use handbook timeout or default to 4 minutes
        const provider = this.handbookConfig?.provider || this.globalConfig?.provider || 'openai'; // Use handbook provider or fallback to global/default
        const response = await this.sendMessage(provider, userMessage, timeout);
        const latency = Date.now() - startTime;
        spinner.stop();
        
        // Add assistant response to history
        this.messages.push({
          role: 'assistant',
          content: response,
        });

        // Display execution summary (cache, latency, tokens, report) with checkmark
        await this.displayExecutionSummary(latency, true, false);

        // Display response (no newline before) with truncation
        console.log(chalk.green('✨'));
        this.displayTruncatedResponse(response);
        console.log();
      } catch (error: any) {
        spinner.stop();
        const errorLatency = Date.now() - startTime;
        
        // Display execution summary with error status
        await this.displayExecutionSummary(errorLatency, true, true);
        
        // Display error message (clean, without duplicate report link)
        let errorMessage = error.message || 'Unknown error';
        // Remove report link if it's in the error message (we show it in summary)
        errorMessage = errorMessage.replace(/\n\nReport:.*$/, '');
        errorMessage = errorMessage.replace(/See report for details:.*$/, '');
        
        console.log(chalk.red('✨'));
        console.log(chalk.red(errorMessage));
        console.log();
        
        // Re-throw so caller knows there was an error
        throw error;
      }
    }
  }

  /**
   * Send message to OneMCP via MCP protocol
   */
  private async sendMessage(provider: ModelProvider, userMessage: string, timeout: number): Promise<string> {
    return this.sendMessageInternal(provider, userMessage, timeout);
  }

  /**
   * Replan by deleting cache file and resubmitting
   * 
   * Strategy: Extract the cache key from the last execution report, then delete
   * the cache file directly from the filesystem.
   */
  private async replanWithCacheDeletion(provider: ModelProvider, userMessage: string, timeout: number): Promise<void> {
    const spinner = ora({
      text: 'Deleting cached plan...',
      spinner: 'dots',
      color: 'cyan'
    }).start();

    try {
      // Extract cache key from the last report
      const cacheKey = await this.extractCacheKeyFromLastReport();
      
      if (!cacheKey) {
        spinner.fail('Could not find cache key in last report. No previous execution found.');
        console.log();
        return;
      }

      // Delete the cache file
      const fs = (await import('fs-extra')).default;
      const { join } = await import('path');
      
      const cacheDir = paths.cacheDir;
      const cacheFile = join(cacheDir, `${cacheKey}.json`);
      
      if (await fs.pathExists(cacheFile)) {
        await fs.remove(cacheFile);
        spinner.succeed(`Deleted cached plan: ${cacheKey}`);
      } else {
        spinner.succeed(`Cache file not found: ${cacheKey}.json (may already be deleted)`);
      }
      
      console.log();
      
      // Now resubmit - it will be a cache miss
      const startTime = Date.now();
      try {
        const response = await this.sendMessageInternal(provider, userMessage, timeout);
        const latency = Date.now() - startTime;
        
        // Add assistant response to history
        this.messages.push({
          role: 'assistant',
          content: response,
        });

        // Display execution summary (cache, latency, tokens, report) with checkmark
        await this.displayExecutionSummary(latency, true, false);

        // Display response (no newline before) with truncation
        console.log(chalk.green('✨'));
        this.displayTruncatedResponse(response);
        console.log();
      } catch (error: any) {
        spinner.stop();
        const errorLatency = Date.now() - startTime;
        
        // Display execution summary with error status
        await this.displayExecutionSummary(errorLatency, true, true);
        
        // Display error message (clean, without duplicate report link)
        let errorMessage = error.message || 'Unknown error';
        // Remove report link if it's in the error message (we show it in summary)
        errorMessage = errorMessage.replace(/\n\nReport:.*$/, '');
        errorMessage = errorMessage.replace(/See report for details:.*$/, '');
        
        console.log(chalk.red('✨'));
        console.log(chalk.red(errorMessage));
        console.log();
        
        throw error;
      }
    } catch (error: any) {
      spinner.stop();
      console.log(chalk.red(`Error during replan: ${error.message}`));
      console.log();
    }
  }

  /**
   * Extract cache key from the last execution report
   * 
   * The report contains cache_key in JSON format like:
   * "cache_key" : "query-sale-filter_date_year_ops_equals-shape_sale_amount"
   * 
   * If lastReportPath is not available, finds the most recent report file.
   */
  private async extractCacheKeyFromLastReport(): Promise<string | null> {
    try {
      const fs = (await import('fs-extra')).default;
      const { join } = await import('path');
      
      let reportPath = this.lastReportPath;
      
      // If lastReportPath is not set, find the most recent report file
      if (!reportPath) {
        const reportsDir = join(paths.logDir, 'reports');
        
        if (!(await fs.pathExists(reportsDir))) {
          console.error(`[DEBUG] Reports directory does not exist: ${reportsDir}`);
          return null;
        }
        
        // Find the most recent report file
        const files = await fs.readdir(reportsDir);
        const reportFiles = files
          .filter(f => f.startsWith('execution-') && f.endsWith('.txt'))
          .map(f => ({
            name: f,
            path: join(reportsDir, f),
            mtime: 0
          }));
        
        if (reportFiles.length === 0) {
          console.error(`[DEBUG] No report files found in ${reportsDir}`);
          return null;
        }
        
        // Get modification times and sort by most recent
        for (const file of reportFiles) {
          const stats = await fs.stat(file.path);
          file.mtime = stats.mtimeMs;
        }
        
        reportFiles.sort((a, b) => b.mtime - a.mtime);
        reportPath = reportFiles[0].path;
        console.log(`[DEBUG] Using most recent report: ${reportPath}`);
      } else {
        console.log(`[DEBUG] Using lastReportPath: ${reportPath}`);
      }
      
      if (!(await fs.pathExists(reportPath))) {
        console.error(`[DEBUG] Report file does not exist: ${reportPath}`);
        return null;
      }

      const reportContent = await fs.readFile(reportPath, 'utf-8');
      console.log(`[DEBUG] Read report file, size: ${reportContent.length} bytes`);
      
      // Clean the report content to handle box-drawing characters and line breaks
      // The report uses box-drawing characters (│, ┌, └, etc.) and text can be split across lines
      // Replace box-drawing chars with spaces and normalize whitespace for easier pattern matching
      const cleanedContent = reportContent
        .replace(/[│┌└├┤┬┴┼─═║╔╗╚╝╠╣╦╩╬]/g, ' ') // Replace box-drawing chars with spaces
        .replace(/\s+/g, ' '); // Normalize whitespace (handles line breaks)
      
      // Try multiple patterns to find the cache key (PSK) - 32 hex characters
      // Pattern 1: Look for any 32-char hex string after "PSK:" (most flexible and simple)
      // This should catch "PSK: <hex>" anywhere in the cleaned content
      const pskColonPattern = /PSK:\s*([a-f0-9]{32})/i;
      const pskColonMatch = cleanedContent.match(pskColonPattern);
      if (pskColonMatch && pskColonMatch[1]) {
        console.log(`[DEBUG] Found cache key via Pattern 1 (PSK:): ${pskColonMatch[1].trim()}`);
        return pskColonMatch[1].trim();
      }
      
      // Pattern 1b: Also try in original content (in case cleaning removed something important)
      const pskColonPatternOriginal = /PSK:\s*([a-f0-9]{32})/i;
      const pskColonMatchOriginal = reportContent.match(pskColonPatternOriginal);
      if (pskColonMatchOriginal && pskColonMatchOriginal[1]) {
        return pskColonMatchOriginal[1].trim();
      }
      
      // Pattern 2: "Normalized prompt to PSK: <cache_key>" (now handles line breaks after cleaning)
      const normalizePattern = /Normalized\s+prompt\s+to\s+PSK:\s*([a-f0-9]{32})/i;
      const normalizeMatch = cleanedContent.match(normalizePattern);
      if (normalizeMatch && normalizeMatch[1]) {
        return normalizeMatch[1].trim();
      }
      
      // Pattern 3: "Cache HIT for cache key: <cache_key>" or "Cache HIT for PSK: <cache_key>"
      const cacheHitPattern = /Cache\s+HIT\s+for\s+(?:cache\s+key|PSK):\s*([a-f0-9]{32})/i;
      const cacheHitMatch = cleanedContent.match(cacheHitPattern);
      if (cacheHitMatch && cacheHitMatch[1]) {
        return cacheHitMatch[1].trim();
      }
      
      // Pattern 4: Look for "cacheKey" in JSON (normalized_prompt_schema) - use original content for JSON
      const cacheKeyPattern = /"cacheKey"\s*:\s*"([a-f0-9]{32})"/i;
      const cacheKeyMatch = reportContent.match(cacheKeyPattern);
      if (cacheKeyMatch && cacheKeyMatch[1]) {
        return cacheKeyMatch[1].trim();
      }
      
      // Pattern 5: Legacy: Look for "cache_key" in the report (old format)
      const legacyCacheKeyMatch = reportContent.match(/"cache_key"\s*:\s*"([^"]+)"/);
      if (legacyCacheKeyMatch && legacyCacheKeyMatch[1]) {
        return legacyCacheKeyMatch[1].trim();
      }
      
      // Pattern 6: Fallback - find any 32-char hex string in server logs section (last resort)
      // Look for patterns like "PSK: <hex>" or "cache key: <hex>" anywhere
      const fallbackPattern = /(?:PSK|cache\s+key):\s*([a-f0-9]{32})/i;
      const fallbackMatch = cleanedContent.match(fallbackPattern);
      if (fallbackMatch && fallbackMatch[1]) {
        return fallbackMatch[1].trim();
      }
      
      // If cache_key not found, extract schema from normalize output and compute cache key
      // The schema appears in the OUTPUT section of the normalize LLM call
      // It's wrapped in Optional[{...}] and nested in steps[0].ps
      try {
        // Find the OUTPUT section for normalize (try both formats)
        let outputStart = reportContent.indexOf('┌─ OUTPUT (LLM Call');
        if (outputStart === -1) {
          outputStart = reportContent.indexOf('┌─ OUTPUT ─');
        }
        if (outputStart === -1) {
          return null;
        }
        
        // Find the next section marker to get the full output
        const outputEnd = reportContent.indexOf('┌─', outputStart + 1);
        const outputSection = outputEnd !== -1 
          ? reportContent.substring(outputStart, outputEnd)
          : reportContent.substring(outputStart);
        
        // Extract JSON from the output section
        // It might be wrapped in Optional[{...}] or just {...}
        // The JSON has box-drawing characters (│) that need to be removed
        let jsonStr = '';
        const jsonStart = outputSection.indexOf('{');
        if (jsonStart !== -1) {
          // Find matching closing brace
          let braceCount = 0;
          let foundStart = false;
          for (let i = jsonStart; i < outputSection.length; i++) {
            const char = outputSection[i];
            if (char === '{') {
              braceCount++;
              foundStart = true;
            } else if (char === '}') {
              braceCount--;
            }
            if (foundStart) {
              jsonStr += char;
              if (braceCount === 0) break;
            }
          }
        }
        
        if (!jsonStr) {
          return null;
        }
        
        // Clean the JSON string - remove box-drawing characters and leading pipes
        // Replace lines starting with │ followed by spaces with just the content
        jsonStr = jsonStr
          .split('\n')
          .map(line => {
            // Remove leading │ and spaces
            const cleaned = line.replace(/^│\s*/, '').trim();
            return cleaned;
          })
          .filter(line => line.length > 0) // Remove empty lines
          .join('\n');
        
        // Remove "Optional[" prefix if present
        jsonStr = jsonStr.replace(/^Optional\[/, '').replace(/\]$/, '');
        
        // Parse the workflow JSON
        const workflow = JSON.parse(jsonStr);
        
        // Extract ps from steps[0].ps
        if (!workflow.steps || !Array.isArray(workflow.steps) || workflow.steps.length === 0) {
          return null;
        }
        
        const psObj = workflow.steps[0].ps;
        if (!psObj) {
          return null;
        }
        
        // Extract components for cache key
        const action = psObj.action || '';
        const entity = psObj.entity || '';
        const filterFields: string[] = [];
        const filterOps: string[] = [];
        const paramFields: string[] = [];
        const shapeFields: string[] = [];
        let hasLimit = false;
        let hasOffset = false;
        
        // Extract filter fields and operators
        if (Array.isArray(psObj.filter)) {
          for (const f of psObj.filter) {
            if (f.field) filterFields.push(f.field);
            if (f.operator) filterOps.push(f.operator);
          }
        }
        
        // Extract param fields
        if (psObj.params && typeof psObj.params === 'object') {
          paramFields.push(...Object.keys(psObj.params));
        }
        
        // Extract shape fields
        if (psObj.shape) {
          if (Array.isArray(psObj.shape.group_by)) {
            shapeFields.push(...psObj.shape.group_by);
          }
          if (Array.isArray(psObj.shape.aggregates)) {
            for (const agg of psObj.shape.aggregates) {
              if (agg.field) shapeFields.push(agg.field);
            }
          }
          if (Array.isArray(psObj.shape.order_by)) {
            for (const order of psObj.shape.order_by) {
              if (order.field) shapeFields.push(order.field);
            }
          }
          hasLimit = psObj.shape.limit != null;
          hasOffset = psObj.shape.offset != null;
        }
        
        // Build cache key (matching PromptSchemaKey.generateStringKey logic)
        const parts: string[] = [];
        if (action) parts.push(action);
        if (entity) parts.push(entity);
        
        if (filterFields.length > 0) {
          const sortedFilters = [...filterFields].sort();
          parts.push('filter_' + sortedFilters.join('_'));
          if (filterOps.length > 0) {
            const sortedOps = [...filterOps].sort();
            parts[parts.length - 1] += '_ops_' + sortedOps.join('_');
          }
        }
        
        if (paramFields.length > 0) {
          const sortedParams = [...paramFields].sort();
          parts.push('params_' + sortedParams.join('_'));
        }
        
        if (shapeFields.length > 0) {
          const sortedShape = [...shapeFields].sort();
          parts.push('shape_' + sortedShape.join('_'));
        }
        
        if (hasLimit || hasOffset) {
          const limitParts: string[] = [];
          if (hasLimit) limitParts.push('limit');
          if (hasOffset) limitParts.push('offset');
          parts.push(limitParts.join('_'));
        }
        
        const computedKey = parts.join('-');
        
        // Verify this key exists in cache
        const cacheDir = paths.cacheDir;
        const cacheFile = join(cacheDir, `${computedKey}.json`);
        if (await fs.pathExists(cacheFile)) {
          return computedKey;
        }
      } catch (e) {
        // JSON parsing failed, fall through
      }

      console.error(`[DEBUG] All patterns failed to find cache key`);
      return null;
    } catch (error) {
      console.error(`[DEBUG] Error in extractCacheKeyFromLastReport:`, error);
      return null;
    }
  }


  /**
   * Internal method to send message to OneMCP via MCP protocol
   */
  private async sendMessageInternal(provider: ModelProvider, userMessage: string, timeout: number): Promise<string> {
    try {
      // Create MCP transport and client with configured timeout
      const transport = new StreamableHTTPClientTransport(new URL(this.mcpUrl), {
        requestInit: {
          signal: AbortSignal.timeout(timeout),
        },
        reconnectionOptions: {
          maxReconnectionDelay: 30000,
          initialReconnectionDelay: 1000,
          reconnectionDelayGrowFactor: 1.5,
          maxRetries: 3,
        },
      });
      const client = new Client(
        {
          name: 'onemcp-cli',
          version: '0.1.0',
        },
        {
          capabilities: {},
        }
      );

      // Connect to the MCP server
      await client.connect(transport);

      try {
        // Call the onemcp.run tool with timeout
        const result: any = await client.callTool(
          {
            name: 'onemcp.run',
            arguments: {
              prompt: userMessage,
            },
          },
          undefined, // resultSchema
          { timeout } // request options with configured timeout
        );

        // Parse the response content
        if (result.content && result.content.length > 0) {
          const content = result.content[0];
          if (content && typeof content === 'object' && 'type' in content && 'text' in content && content.type === 'text') {
            // Try to parse the JSON response from the tool
            try {
              const parsed = JSON.parse(content.text);
              // Extract report path if available (always extract, even on errors)
              if (parsed && typeof parsed === 'object' && 'reportPath' in parsed) {
                this.lastReportPath = parsed.reportPath;
              }
              
              // Check if this is an error response
              if (result.isError) {
                // For errors, show the message if available, otherwise the content
                // Don't append report link here - it will be shown in the summary
                let errorMessage = parsed?.message || parsed?.content || content.text;
                // Remove any existing report link from the error message
                errorMessage = errorMessage.replace(/\n\nReport:.*$/, '');
                errorMessage = errorMessage.replace(/See report for details:.*$/, '');
                return errorMessage;
              }
              
              // Return the content field if it exists and is a non-empty string
              if (parsed && typeof parsed === 'object' && 'content' in parsed) {
                const responseContent = parsed.content;
                if (typeof responseContent === 'string' && responseContent.trim().length > 0) {
                  // Skip if content is the literal string "null"
                  if (responseContent.trim() === 'null') {
                    return 'Response received (no content)';
                  }
                  // Try to parse the content as JSON and return the whole JSON object
                  try {
                    const contentJson = JSON.parse(responseContent);
                    // Skip if parsed result is null
                    if (contentJson === null) {
                      return 'Response received (no content)';
                    }
                    if (typeof contentJson === 'object') {
                      // Return the whole JSON object, pretty-printed
                      return JSON.stringify(contentJson, null, 2);
                    }
                  } catch {
                    // Not JSON, return as-is
                  }
                  return responseContent;
                }
              }
              // If content is missing or empty, return a fallback message
              return 'Response received (no content)';
            } catch {
              // If JSON parsing fails, return the raw text
              return content.text;
            }
          }
        }

        return 'No response from agent';
      } catch (error: any) {
        // Try to extract report path from error if it's a structured error
        try {
          if (error.message) {
            const errorMatch = error.message.match(/Report:\s+([^\s\n]+)/);
            if (errorMatch && errorMatch[1]) {
              this.lastReportPath = errorMatch[1];
            }
          }
        } catch {
          // Ignore extraction errors
        }
        
        // Re-throw with more context
        throw new Error(`Failed to communicate with OneMCP: ${error.message}`);
      } finally {
        // Always close the connection
        await client.close();
      }
    } catch (error: any) {
      // Re-throw outer errors
      throw error;
    }
  }

  /**
   * Show help message
   */
  private showHelp(): void {
    console.log();
    console.log(chalk.bold('Special Commands:'));
    console.log();
    console.log(chalk.cyan('  ?') + '     - Show this list of special commands');
    console.log(chalk.cyan('  index') + ' - Re-index the handbook schema');
    console.log(chalk.cyan('  cache') + ' - Toggle cache mode (currently: ' + (this.cacheEnabled ? chalk.green('enabled') : chalk.yellow('disabled')) + ')');
    console.log(chalk.cyan('  replan') + ' - Regenerate plan for the last prompt (deletes cached plan)');
    console.log(chalk.cyan('  exit') + '   - Exit chat mode');
    console.log();
    console.log(chalk.bold('Example Prompts:'));
    console.log();
    console.log(chalk.dim('  > Show the total sales in California of Automotive in the first quarter.'));
    console.log(chalk.dim('  > List top customers by revenue.'));
    console.log(chalk.dim('  > Compare revenue trends by region.'));
    console.log();
  }


  /**
   * Select a handbook for chatting
   */
  private async selectHandbook(handbookName?: string): Promise<void> {
    const handbooks = await handbookManager.list();

    if (handbooks.length === 0) {
      console.log(chalk.red('❌ No handbooks found.'));
      console.log(chalk.yellow('Create one first: onemcp handbook init <name>'));
      process.exit(1);
    }

    let selectedHandbook: string;

    if (handbookName) {
      // Handbook specified as argument
      const handbook = handbooks.find(h => h.name === handbookName);
      if (!handbook) {
        console.log(chalk.red(`❌ Handbook '${handbookName}' not found.`));
        process.exit(1);
      }
      if (!handbook.valid) {
        console.log(chalk.red(`❌ Handbook '${handbookName}' is not valid.`));
        process.exit(1);
      }
      selectedHandbook = handbookName;
    } else {
      // Get current handbook from config
      const currentHandbook = await handbookManager.getCurrentHandbook();

      if (currentHandbook && handbooks.find(h => h.name === currentHandbook && h.valid)) {
        // Use current handbook if it exists and is valid
        selectedHandbook = currentHandbook;
      } else if (handbooks.length === 1) {
        // Only one handbook, use it
        selectedHandbook = handbooks[0].name;
      } else {
        // Multiple handbooks, let user choose
        const { handbook } = await inquirer.prompt([
          {
            type: 'list',
            name: 'handbook',
            message: 'Select a handbook to chat with:',
            choices: handbooks
              .filter(h => h.valid)
              .map(h => ({
                name: `${h.name}${h.config?.description ? ` - ${h.config.description}` : ''}`,
                value: h.name,
              })),
          },
        ]);
        selectedHandbook = handbook;
      }
    }

    // Load handbook configuration
    this.currentHandbook = selectedHandbook;
    this.handbookConfig = await configManager.getEffectiveHandbookConfig(selectedHandbook);

    // Set as current handbook in global config
    await handbookManager.setCurrentHandbook(selectedHandbook);
  }

  /**
   * Switch to a different handbook during chat
   */
  private async switchHandbook(): Promise<void> {
    const handbooks = await handbookManager.list();
    const validHandbooks = handbooks.filter(h => h.valid);

    if (validHandbooks.length <= 1) {
      console.log(chalk.yellow('No other valid handbooks to switch to.'));
      return;
    }

    const { handbook } = await inquirer.prompt([
      {
        type: 'list',
        name: 'handbook',
        message: 'Switch to handbook:',
        choices: validHandbooks
          .filter(h => h.name !== this.currentHandbook)
          .map(h => ({
            name: `${h.name}${h.config?.description ? ` - ${h.config.description}` : ''}`,
            value: h.name,
          })),
      },
    ]);

    // Switch to new handbook
    this.currentHandbook = handbook;
    this.handbookConfig = await configManager.getEffectiveHandbookConfig(handbook);
    await handbookManager.setCurrentHandbook(handbook);

    // Clear chat history when switching
    this.messages = [];

    console.log(chalk.green(`✅ Switched to handbook: ${handbook}`));
    console.log(chalk.dim(`Provider: ${this.handbookConfig?.provider || 'Not configured'}`));
    console.log(chalk.dim('Chat history cleared.'));
    console.log();
  }

  /**
   * Show example queries tailored for the bundled Acme Analytics handbook
   */
  private showAcmeExamples(): void {
    console.log(chalk.bold.yellow('💡 Acme Analytics Example Queries'));
    console.log();
    console.log(chalk.cyan('  > Show total sales for 2024.'));
    console.log(chalk.cyan('  > Show me total revenue by category in 2024.'));
    console.log(chalk.cyan('  > Show the total sales in California of Automotive in the first quarter.'));
    console.log(chalk.cyan('  > What are the top 3 selling products this month?'));
    console.log(chalk.cyan('  > Show me sales data for New York vs Texas.'));
    console.log();
    console.log(chalk.dim('━'.repeat(60)));
    console.log();
  }

  /**
   * Determine if the current handbook matches the bundled Acme Analytics example
   */
  private isAcmeHandbook(): boolean {
    const name = this.currentHandbook?.toLowerCase() ?? '';
    if (name.includes('acme')) {
      return true;
    }

    const configName = this.handbookConfig?.name?.toLowerCase() ?? '';
    return configName.includes('acme');
  }

  /**
   * Show the location of the generated report
   */
  private showReportLocation(): void {
    if (this.lastReportPath) {
      console.log(chalk.dim('Report: ') + chalk.cyan(this.lastReportPath));
      // Don't clear lastReportPath - it's used for replan command
    }
  }

  /**
   * Display a beautiful, compact execution summary with cache status, latency, tokens, and report link.
   */
  private async displayExecutionSummary(clientLatency: number, showCheckmark: boolean = false, hasError: boolean = false): Promise<void> {
    const fs = (await import('fs-extra')).default;
    const reportPath = this.lastReportPath;
    
    if (!reportPath) {
      // No report available, just show client latency
      const prefix = showCheckmark ? chalk.green('✔ ') : '  ';
      console.log(prefix + chalk.dim(`⏱  ${clientLatency}ms`));
      return;
    }

    try {
      // Parse report to extract metadata
      const reportContent = await fs.readFile(reportPath, 'utf-8');
      
      // Extract cache status from EXECUTION SUMMARY header
      const cacheMatch = reportContent.match(/EXECUTION SUMMARY\s*(-\s*Cache (hit|miss|hit\/miss))?/);
      let cacheStatus: string | null = null;
      if (cacheMatch && cacheMatch[1]) {
        const cacheType = cacheMatch[2];
        if (cacheType === 'hit') {
          cacheStatus = '✓';
        } else if (cacheType === 'miss') {
          cacheStatus = '✗';
        } else {
          cacheStatus = '~';
        }
      }
      
      // Extract total duration from summary
      const durationMatch = reportContent.match(/Total Duration:\s*(\d+)\s*ms/);
      const duration = durationMatch ? parseInt(durationMatch[1], 10) : null;
      
      // Extract total tokens from summary
      const tokensMatch = reportContent.match(/Total Tokens:\s*(\d+)\+(\d+)=(\d+)/);
      const totalTokens = tokensMatch ? parseInt(tokensMatch[3], 10) : null;
      
      // Build compact summary line
      const parts: string[] = [];
      
      // Error status (first if error occurred)
      if (hasError) {
        parts.push(chalk.red('✗ Execution failed - see report for details'));
      }
      
      // Cache status (if available and cache is enabled)
      if (cacheStatus !== null && this.cacheEnabled) {
        const cacheColor = cacheStatus === '✓' ? chalk.green : cacheStatus === '✗' ? chalk.yellow : chalk.blue;
        parts.push(cacheColor(`Cache ${cacheStatus}`));
      } else if (!this.cacheEnabled) {
        parts.push(chalk.dim('Cache disabled'));
      }
      
      // Latency (use report duration if available, otherwise client latency)
      const latencyMs = duration ?? clientLatency;
      parts.push(chalk.dim(`⏱  ${latencyMs}ms`));
      
      // Tokens (if available)
      if (totalTokens !== null) {
        parts.push(chalk.dim(`📊 ${totalTokens.toLocaleString()} tokens`));
      }
      
      // Report link
      parts.push(chalk.cyan('📄 Report'));
      
      // Display as a compact line with optional checkmark
      const prefix = showCheckmark ? chalk.green('✔ ') : '  ';
      console.log(prefix + parts.join('  '));
      console.log(chalk.dim(`  ${reportPath}`));
      
      // Don't clear lastReportPath here - it's used for replan command
      
    } catch (error) {
      // If parsing fails, show basic info
      const prefix = showCheckmark ? chalk.green('✔ ') : '  ';
      console.log(prefix + chalk.dim(`⏱  ${clientLatency}ms`));
      if (reportPath) {
        console.log(chalk.dim(`  ${reportPath}`));
      }
    }
  }

  /**
   * Display response with truncation if it's too long
   */
  private displayTruncatedResponse(response: string, maxLines: number = 10): void {
    // Guard against null/undefined responses
    if (!response || response === 'null' || response === 'undefined') {
      return;
    }
    
    // Filter out lines that are just "null" (trimmed)
    const lines = response.split('\n').filter(line => line.trim() !== 'null');
    
    // If all lines were filtered out, don't display anything
    if (lines.length === 0) {
      return;
    }
    
    const totalLines = lines.length;
    const filteredResponse = lines.join('\n');
    
    if (totalLines <= maxLines) {
      // Response is short enough, display it all
      console.log(filteredResponse);
      return;
    }
    
    // Display first few lines
    const displayLines = lines.slice(0, maxLines);
    console.log(displayLines.join('\n'));
    
    // Show truncation message
    const remainingLines = totalLines - maxLines;
    console.log(chalk.dim(`\n... (${remainingLines} more line${remainingLines !== 1 ? 's' : ''}, ${totalLines} total)`));
    console.log(chalk.dim('See report for full details.'));
  }
}

export const chatMode = new ChatMode();

