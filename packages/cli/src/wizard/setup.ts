/**
 * Interactive setup wizard for first-time configuration
 */
import inquirer from 'inquirer';
import chalk from 'chalk';
import boxen from 'boxen';
import { configManager } from '../config/manager.js';
import { paths } from '../config/paths.js';
import { GlobalConfig, ModelProvider } from '../types.js';
import fs from 'fs-extra';

export class SetupWizard {
  /**
   * Run the complete setup wizard
   */
  async run(): Promise<void> {
    console.log(
      boxen(chalk.bold.cyan('Gentoro OneMCP Setup'), {
        padding: 1,
        margin: 1,
        borderStyle: 'round',
        borderColor: 'cyan',
      })
    );

    console.log(chalk.dim('━'.repeat(60)));
    console.log();

    // Step 1: Choose model provider
    const provider = await this.selectProvider();
    const apiKey = await this.getApiKey(provider);

    // Step 2: Choose starting mode
    const mode = await this.selectMode();

    let currentHandbook: string | undefined;
    let serviceConfig = null;

    if (mode === 'own') {
      // Step 3: Handbook configuration
      const handbookPath = await this.configureHandbook();

      // Extract handbook name from path
      const handbookName = handbookPath.split('/').pop() || 'unknown';
      currentHandbook = handbookName;

      // Step 4: Service authentication
      serviceConfig = await this.configureService();
    } else if (mode === 'example') {
      // Copy ACME Analytics example handbook
      await this.setupExampleHandbook();
      currentHandbook = 'acme-analytics';
    }

    // Validate handbook before saving configuration
    console.log();
    console.log(chalk.bold('Validating handbook configuration...'));
    const handbookPathToValidate = currentHandbook ? paths.getHandbookPath(currentHandbook) : paths.handbooksDir;
    const isValidHandbook = await this.validateHandbookStructure(handbookPathToValidate);
    if (!isValidHandbook) {
      console.log(chalk.red('❌  Handbook validation failed. Configuration not saved.'));
      console.log(chalk.dim('Please ensure the handbook directory exists and contains the required Agent.md file.'));
      return; // Exit without saving configuration
    }

    // Save global configuration
    const config: GlobalConfig = {
      provider,
      apiKeys: {
        [provider]: apiKey
      },
      defaultPort: 8080,
      handbookDir: paths.handbooksDir, // Always point to parent handbooks directory
      currentHandbook, // Set the current handbook name
      logDir: paths.logDir,
      chatTimeout: 240000, // 4 minutes default timeout for chat requests
    };

    await configManager.saveGlobalConfig(config);
    console.log(chalk.green('✅  Configuration saved successfully!'));

    // Save service configuration if provided
    if (serviceConfig) {
      await configManager.saveServiceConfig(serviceConfig.service, serviceConfig);
      console.log(chalk.green(`✅  Service configuration saved to ${serviceConfig.service}.yaml`));
    }

    // Show next steps
    this.showNextSteps(mode);
  }

  /**
   * Select model provider
   */
  private async selectProvider(): Promise<ModelProvider> {
    console.log(chalk.bold('Step 1 — Choose a model provider:'));
    console.log();

    const { provider } = await inquirer.prompt<{ provider: ModelProvider }>([
      {
        type: 'list',
        name: 'provider',
        message: 'Select provider',
        choices: [
          { name: 'OpenAI', value: 'openai' },
          { name: 'Google Gemini', value: 'gemini' },
          { name: 'Anthropic Claude', value: 'anthropic' },
        ],
      },
    ]);

    return provider;
  }

  /**
   * Get API key for selected provider
   */
  private async getApiKey(provider: ModelProvider): Promise<string> {
    const providerNames = {
      openai: 'OpenAI',
      gemini: 'Google Gemini',
      anthropic: 'Anthropic',
    };

    const { apiKey } = await inquirer.prompt([
      {
        type: 'password',
        name: 'apiKey',
        message: `Enter your ${providerNames[provider]} API key:`,
        validate: (input: string) => {
          if (!input || input.trim() === '') {
            return 'API key is required';
          }
          return true;
        },
      },
    ]);

    return apiKey;
  }

  /**
   * Select starting mode
   */
  private async selectMode(): Promise<'example' | 'own'> {
    console.log();
    console.log(chalk.bold('Step 2 — Choose how to start:'));
    console.log();

    const { mode } = await inquirer.prompt<{ mode: 'example' | 'own' }>([
      {
        type: 'list',
        name: 'mode',
        message: 'Select mode',
        choices: [
          {
            name: 'Start with sample Acme Analytics handbook (recommended)',
            value: 'example',
          },
          {
            name: 'Connect your own API service',
            value: 'own',
          },
        ],
      },
    ]);

    return mode;
  }

  /**
   * Configure handbook directory
   */
  private async configureHandbook(): Promise<string> {
    console.log();
    console.log(chalk.bold('Step 3 — Specify a handbook directory'));
    console.log();

    const { handbookPath } = await inquirer.prompt([
      {
        type: 'input',
        name: 'handbookPath',
        message: 'Handbook directory',
        default: './handbook',
      },
    ]);

    const fullPath = handbookPath.startsWith('/')
      ? handbookPath
      : paths.getHandbookPath(handbookPath.replace('./', ''));

    // Check if directory exists
    const exists = await fs.pathExists(fullPath);

    if (!exists) {
      const { create } = await inquirer.prompt([
        {
          type: 'confirm',
          name: 'create',
          message: `Directory not found — create it?`,
          default: true,
        },
      ]);

      if (create) {
        await this.createHandbook(fullPath);
        console.log(chalk.green(`✅  Created handbook at ${fullPath}`));
      } else {
        console.log(chalk.yellow('⚠️  Using non-existent directory. Create it before starting.'));
      }
    }

    return fullPath;
  }

  /**
   * Create handbook structure
   */
  private async createHandbook(dir: string): Promise<void> {
    await fs.ensureDir(dir);
    await fs.ensureDir(`${dir}/apis`);
    await fs.ensureDir(`${dir}/docs`);
    await fs.ensureDir(`${dir}/regression`);
    await fs.ensureDir(`${dir}/state`);

    // Create basic Agent.md
    const agentMd = `# Agent Instructions

This is your OneMCP handbook. Configure your agent's behavior here.

## Purpose

Describe what your agent does and its primary responsibilities.

## Available APIs

List the APIs your agent can interact with.

## Behavior Guidelines

Define how your agent should respond to requests.
`;

    await fs.writeFile(`${dir}/Agent.md`, agentMd, 'utf-8');

    // Create example documentation
    const exampleDoc = `# Example Documentation

Add your API documentation and guides here.
`;

    await fs.writeFile(`${dir}/docs/getting-started.md`, exampleDoc, 'utf-8');

    // Create service.yaml template
    const serviceYaml = `# Service configuration will be created when you configure authentication
`;

    await fs.writeFile(`${dir}/service.yaml`, serviceYaml, 'utf-8');
  }

  /**
   * Setup ACME Analytics handbook for example mode
   */
  private async setupExampleHandbook(): Promise<void> {
    console.log();
    console.log(chalk.bold('Setting up ACME Analytics handbook...'));
    console.log();

    // Ensure handbooks directory exists
    await paths.ensureDirectories();

    // Copy the ACME Analytics example handbook
    const handbookManager = (await import('../handbook/manager.js')).handbookManager;
    const acmeHandbookDir = paths.getHandbookPath('acme-analytics');

    try {
      await handbookManager.copyExample(acmeHandbookDir);

      // Validate that the handbook was created successfully
      const isValid = await this.validateHandbookStructure(acmeHandbookDir);
      if (!isValid) {
        throw new Error('Handbook structure validation failed - required files are missing');
      }

      console.log(chalk.green(`✅  ACME Analytics handbook copied to ${acmeHandbookDir}`));
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log(chalk.red(`❌  Failed to setup ACME Analytics handbook: ${errorMessage}`));
      console.log(chalk.dim('The agent cannot start without a valid handbook. Please try again or set up manually.'));
      throw error; // Re-throw to prevent configuration from being saved
    }
  }

  /**
   * Validate handbook directory structure
   */
  private async validateHandbookStructure(handbookDir: string): Promise<boolean> {
    try {
      // Check if directory exists
      if (!(await fs.pathExists(handbookDir))) {
        console.log(chalk.red(`❌  Handbook directory does not exist: ${handbookDir}`));
        return false;
      }

      // Check for required Agent.md file
      const agentMdPath = `${handbookDir}/Agent.md`;
      if (!(await fs.pathExists(agentMdPath))) {
        console.log(chalk.red(`❌  Required Agent.md file not found in handbook directory`));
        return false;
      }

      // Check for basic directory structure
      const requiredDirs = ['apis', 'docs', 'state'];
      for (const dir of requiredDirs) {
        const dirPath = `${handbookDir}/${dir}`;
        if (!(await fs.pathExists(dirPath))) {
          console.log(chalk.yellow(`⚠️  Recommended directory '${dir}' not found in handbook`));
          // Don't fail validation for missing recommended dirs, just warn
        }
      }

      console.log(chalk.green(`✅  Handbook structure validated successfully`));
      return true;
    } catch (error) {
      console.log(chalk.red(`❌  Error validating handbook structure: ${error instanceof Error ? error.message : String(error)}`));
      return false;
    }
  }

  /**
   * Configure service authentication
   */
  private async configureService(): Promise<{
    service: string;
    baseUrl: string;
    header: string;
    pattern: string;
    token: string;
  }> {
    console.log();
    console.log(chalk.bold('Step 4 — Configure service authentication'));
    console.log();

    const answers = await inquirer.prompt([
      {
        type: 'input',
        name: 'serviceName',
        message: 'Service name:',
        default: 'my-service',
        validate: (input: string) => {
          if (!input || input.trim() === '') {
            return 'Service name is required';
          }
          if (!/^[a-z0-9-]+$/.test(input)) {
            return 'Service name must contain only lowercase letters, numbers, and hyphens';
          }
          return true;
        },
      },
      {
        type: 'input',
        name: 'baseUrl',
        message: 'Service base URL:',
        default: 'https://api.example.com',
        validate: (input: string) => {
          if (!input || input.trim() === '') {
            return 'Base URL is required';
          }
          try {
            new URL(input);
            return true;
          } catch {
            return 'Please enter a valid URL';
          }
        },
      },
      {
        type: 'input',
        name: 'header',
        message: 'Header name:',
        default: 'Authorization',
      },
      {
        type: 'input',
        name: 'pattern',
        message: 'Pattern:',
        default: 'Bearer {token}',
        validate: (input: string) => {
          if (!input.includes('{token}')) {
            return 'Pattern must include {token} placeholder';
          }
          return true;
        },
      },
      {
        type: 'password',
        name: 'token',
        message: 'Access token:',
        validate: (input: string) => {
          if (!input || input.trim() === '') {
            return 'Access token is required';
          }
          return true;
        },
      },
    ]);

    return {
      service: answers.serviceName,
      baseUrl: answers.baseUrl,
      header: answers.header,
      pattern: answers.pattern,
      token: answers.token,
    };
  }

  /**
   * Show next steps after setup
   */
  private showNextSteps(mode: 'example' | 'own'): void {
    console.log();
    console.log(chalk.dim('━'.repeat(60)));
    console.log();
    console.log(chalk.bold.green('✅  Setup Complete!'));
    console.log();

    if (mode === 'example') {
      console.log(chalk.bold('Acme Analytics sample handbook'));
      console.log('You are ready to explore the bundled Acme Analytics documentation and queries.');
      console.log();
    } else {
      console.log(chalk.bold('Custom handbook'));
      console.log('Add your API specifications, docs, and tests to your handbook directory to guide the agent.');
      console.log();
    }

    console.log(chalk.bold('Next Steps:'));
    console.log();
    console.log(chalk.cyan('1. Start chatting:'));
    console.log('   $ onemcp chat');
    console.log();
    console.log(chalk.cyan('2. Check status:'));
    console.log('   $ onemcp status');
    console.log();
  }

  /**
   * Check if setup is needed
   */
  static async isSetupNeeded(): Promise<boolean> {
    return !(await configManager.hasConfiguration());
  }
}

export const setupWizard = new SetupWizard();

