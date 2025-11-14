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
import { ChatMessage, ModelProvider, HandbookConfig } from '../types.js';

export class ChatMode {
  private messages: ChatMessage[] = [];
  private mcpUrl: string = '';
  private agentStatus: any = null;
  private currentHandbook: string = '';
  private handbookConfig: HandbookConfig | null = null;

  /**
   * Start interactive chat session
   */
  async start(handbookName?: string): Promise<void> {
    // Select handbook
    await this.selectHandbook(handbookName);

    // Check if agent is running
    this.agentStatus = await agentService.getStatus();
    if (!this.agentStatus.running) {
      console.log(chalk.red('❌ OneMCP is not running.'));
      console.log(chalk.yellow('Services should start automatically when you run: onemcp chat'));
      return;
    }

    this.mcpUrl = this.agentStatus.mcpUrl || 'http://localhost:8080/mcp';

    console.log();
    console.log(chalk.bold.cyan('╔══════════════════════════════════════╗'));
    console.log(chalk.bold.cyan('║    Gentoro OneMCP - Chat Mode        ║'));
    console.log(chalk.bold.cyan('╚══════════════════════════════════════╝'));
    console.log();
    console.log(chalk.dim(`Handbook: ${this.currentHandbook}`));
    console.log(chalk.dim(`Provider: ${this.handbookConfig?.provider || 'Not configured'}`));
    console.log(chalk.dim(`MCP URL: ${this.mcpUrl}`));
    console.log(chalk.dim(`Type 'exit' to quit, 'clear' to clear history, 'switch' to change handbook`));
    console.log();
    console.log(chalk.dim('Ask questions about your configured handbook. Type "help" for tips.'));
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
      const { message } = await inquirer.prompt([
        {
          type: 'input',
          name: 'message',
          message: chalk.cyan('You:'),
          validate: (input: string) => {
            if (!input || input.trim() === '') {
              return 'Please enter a message';
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

      if (userMessage.toLowerCase() === 'clear') {
        this.messages = [];
        console.log(chalk.yellow('🧹 Chat history cleared.'));
        console.log();
        continue;
      }

      if (userMessage.toLowerCase() === 'help') {
        this.showHelp();
        continue;
      }

      if (userMessage.toLowerCase() === 'switch') {
        await this.switchHandbook();
        continue;
      }

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

      try {
        const timeout = this.handbookConfig?.chatTimeout || 240000; // Use handbook timeout or default to 4 minutes
        const provider = this.handbookConfig?.provider || 'openai'; // Use handbook provider or default
        const response = await this.sendMessage(provider, userMessage, timeout);
        spinner.succeed('Response received');
        
        // Add assistant response to history
        this.messages.push({
          role: 'assistant',
          content: response,
        });

        // Display response
        console.log(chalk.green('Agent:'));
        console.log(response);
        console.log();
      } catch (error: any) {
        spinner.fail(`Error: ${error.message}`);
        console.log();
      }
    }
  }

  /**
   * Send message to OneMCP via MCP protocol
   */
  private async sendMessage(provider: ModelProvider, userMessage: string, timeout: number): Promise<string> {
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
              options: {},
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
              return parsed.content || content.text;
            } catch {
              return content.text;
            }
          }
        }

        return 'No response from agent';
      } finally {
        // Always close the connection
        await client.close();
      }
    } catch (error: any) {
      throw new Error(`Failed to communicate with OneMCP: ${error.message}`);
    }
  }

  /**
   * Show help message
   */
  private showHelp(): void {
    console.log();
    console.log(chalk.bold('Chat Mode Commands:'));
    console.log();
    console.log(chalk.cyan('  help   ') + ' - Show this help message');
    console.log(chalk.cyan('  clear  ') + ' - Clear chat history');
    console.log(chalk.cyan('  switch ') + ' - Switch to a different handbook');
    console.log(chalk.cyan('  exit   ') + ' - Exit chat mode');
    console.log();
    console.log(chalk.bold('Example Prompts:'));
    console.log();
    console.log(chalk.dim('  > Show me electronics sales in California last quarter.'));
    console.log(chalk.dim('  > List top customers by revenue.'));
    console.log(chalk.dim('  > Compare revenue trends by region.'));
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
    console.log(chalk.cyan('  > Show me electronics sales in California last quarter.'));
    console.log(chalk.cyan('  > What are the top-selling products this month?'));
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
}

export const chatMode = new ChatMode();

