/**
 * Handbook management utilities
 */
import fs from 'fs-extra';
import chalk from 'chalk';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { paths } from '../config/paths.js';
import { configManager } from '../config/manager.js';
import { HandbookInfo } from '../types.js';

export class HandbookManager {
  /**
   * Initialize a new handbook directory
   */
  async init(name: string, targetDir?: string): Promise<string> {
    const dir = targetDir || paths.getHandbookPath(name);

    if (await fs.pathExists(dir)) {
      throw new Error(`Handbook directory already exists: ${dir}`);
    }

    console.log(chalk.blue(`Creating handbook at ${dir}...`));

    // Create directory structure
    await fs.ensureDir(dir);
    await fs.ensureDir(`${dir}/apis`);
    await fs.ensureDir(`${dir}/docs`);
    await fs.ensureDir(`${dir}/regression`);
    await fs.ensureDir(`${dir}/state`);

    // Create Agent.md
    const agentMd = this.getDefaultAgentInstructions(name);
    await fs.writeFile(`${dir}/Agent.md`, agentMd, 'utf-8');

    // Create README
    const readme = this.getDefaultReadme(name);
    await fs.writeFile(`${dir}/README.md`, readme, 'utf-8');

    // Create example documentation
    const exampleDoc = `# Getting Started

This handbook contains the configuration and documentation for your OneMCP.

## Structure

- **Agent.md** - Agent behavior and instructions
- **apis/** - OpenAPI specifications for your services
- **docs/** - Additional documentation and guides
- **config/** - Configuration files
- **regression/** - Test definitions (optional)
- **state/** - Runtime state (auto-generated)

## Next Steps

1. Add your OpenAPI specifications to the \`apis/\` directory
2. Add documentation to the \`docs/\` directory
3. Update \`Agent.md\` with your agent's instructions
4. Configure service authentication with \`onemcp service auth <service-name>\`
`;
    await fs.writeFile(`${dir}/docs/getting-started.md`, exampleDoc, 'utf-8');

    // Create .gitignore
    const gitignore = `state/
*.log
.DS_Store
`;
    await fs.writeFile(`${dir}/.gitignore`, gitignore, 'utf-8');

    console.log(chalk.green(`✅  Handbook initialized at ${dir}`));

    return dir;
  }

  /**
   * Validate handbook structure
   */
  async validate(dir: string): Promise<{ valid: boolean; errors: string[]; warnings: string[] }> {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check if directory exists
    if (!(await fs.pathExists(dir))) {
      errors.push(`Handbook directory does not exist: ${dir}`);
      return { valid: false, errors, warnings };
    }

    // Check required files
    const agentMdPath = `${dir}/Agent.md`;
    if (!(await fs.pathExists(agentMdPath))) {
      errors.push('Missing required file: Agent.md');
    }

    // Check required directories
    const requiredDirs = ['apis'];
    for (const reqDir of requiredDirs) {
      if (!(await fs.pathExists(`${dir}/${reqDir}`))) {
        errors.push(`Missing required directory: ${reqDir}/`);
      }
    }

    const recommendedDirs = ['docs', 'state'];
    for (const recDir of recommendedDirs) {
      if (!(await fs.pathExists(`${dir}/${recDir}`))) {
        warnings.push(`Missing recommended directory: ${recDir}/`);
      }
    }

    // Check for OpenAPI specs
    const apisDir = `${dir}/apis`;
    if (await fs.pathExists(apisDir)) {
      const files = await fs.readdir(apisDir);
      const specFiles = files.filter(
        (f) => f.endsWith('.yaml') || f.endsWith('.yml') || f.endsWith('.json')
      );
      if (specFiles.length === 0) {
        warnings.push('No OpenAPI specifications found in apis/ directory');
      }
    }

    // Check for documentation
    const docsDir = `${dir}/docs`;
    if (await fs.pathExists(docsDir)) {
      const files = await fs.readdir(docsDir);
      const docFiles = files.filter((f) => f.endsWith('.md'));
      if (docFiles.length === 0) {
        warnings.push('No documentation files found in docs/ directory');
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * List all handbooks
   */
  async list(): Promise<HandbookInfo[]> {
    await paths.ensureDirectories();

    const entries = await fs.readdir(paths.handbooksDir, { withFileTypes: true });
    const handbooks = entries.filter((e) => e.isDirectory());

    const results: HandbookInfo[] = [];
    for (const handbook of handbooks) {
      const handbookPath = paths.getHandbookPath(handbook.name);
      const validation = await this.validate(handbookPath);
      const config = await configManager.loadHandbookConfig(handbook.name);

      results.push({
        name: handbook.name,
        path: handbookPath,
        valid: validation.valid,
        config: config || undefined,
      });
    }

    return results;
  }

  /**
   * Get default Agent.md content
   */
  private getDefaultAgentInstructions(name: string): string {
    return `# ${name} Agent Instructions

## Purpose

Define your agent's purpose and primary responsibilities here.

## Scope

Describe what your agent can and cannot do:
- What APIs it can access
- What operations are allowed
- What data it can work with

## Tools

Your agent has access to the following tools:
- **RetrieveContext** - Retrieve documentation from the knowledge base
- **RunTypescriptSnippet** - Execute TypeScript code to interact with APIs

## Behavioral Guidelines

Define how your agent should behave:
- Be precise and factual
- Validate inputs against API specifications
- Ask for clarification when requests are ambiguous
- Refuse requests outside of scope

## Response Format

Define the expected response format for your agent's outputs.

## Safety Rules

- Never expose sensitive data
- Always validate API schemas
- Handle errors gracefully
- Log all operations for audit
`;
  }

  /**
   * Get default README content
   */
  private getDefaultReadme(name: string): string {
    return `# ${name}

OneMCP handbook for ${name}.

## Setup

1. Add your OpenAPI specifications to \`apis/\`
2. Add documentation to \`docs/\`
3. Configure Agent.md with your instructions

## Usage

Start chatting with this handbook by setting it as your default or using the handbook directory:

\`\`\`bash
# Set as default handbook
onemcp
# Then run chat
onemcp chat

# Or specify handbook directly (future feature)
\`\`\`

For now, set this as your default handbook:

\`\`\`bash
onemcp config set handbookDir ~/handbooks/${name}
\`\`\`

## Testing

Run validation:

\`\`\`bash
onemcp handbook validate ./${name}
\`\`\`

## Documentation

See the \`docs/\` directory for additional documentation.
`;
  }

  /**
   * Copy example handbook
   */
  async copyExample(targetDir: string): Promise<void> {
    // Copy the ACME Analytics example
    // In production, it's in dist/example-handbook (copied by build script)
    // In development, it might be in src/example-handbook or we need to copy from source
    const __dirname = dirname(fileURLToPath(import.meta.url));

    const candidates = [
      resolve(__dirname, '../../../dist/example-handbook'),
      resolve(__dirname, '../../../src/acme-analytics-server/onemcp-handbook'),
      resolve(__dirname, '../../../src/onemcp/src/main/resources/acme-handbook'),
      resolve(__dirname, '../example-handbook'),
    ];

    const resolvedSource = await this.findFirstExisting(candidates);

    if (!resolvedSource) {
      throw new Error(
        `Example handbook not found in any known location. Checked: ${candidates.join(', ')}.`
      );
    }

    await fs.copy(resolvedSource, targetDir);

    // Rename instructions.md to Agent.md if needed
    const instructionsPath = `${targetDir}/instructions.md`;
    const agentPath = `${targetDir}/Agent.md`;

    if (await fs.pathExists(instructionsPath)) {
      await fs.move(instructionsPath, agentPath, { overwrite: true });
    }

    // Rename openapi/ to apis/ to match expected directory structure
    const openapiPath = `${targetDir}/openapi`;
    const apisPath = `${targetDir}/apis`;

    if (await fs.pathExists(openapiPath)) {
      await fs.move(openapiPath, apisPath, { overwrite: true });
    }

    // Ensure state directory exists for runtime artifacts
    await fs.ensureDir(`${targetDir}/state`);

    console.log(chalk.green(`✅  Example handbook copied to ${targetDir}`));
  }

  private async findFirstExisting(pathsToCheck: string[]): Promise<string | null> {
    for (const candidate of pathsToCheck) {
      if (await fs.pathExists(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Get the currently active handbook
   */
  async getCurrentHandbook(): Promise<string | null> {
    const config = await configManager.getGlobalConfig();
    return config?.currentHandbook || null;
  }

  /**
   * Set the currently active handbook
   */
  async setCurrentHandbook(handbookName: string): Promise<void> {
    // Validate handbook exists
    const handbookPath = paths.getHandbookPath(handbookName);
    if (!(await fs.pathExists(handbookPath))) {
      throw new Error(`Handbook '${handbookName}' does not exist`);
    }

    // Validate handbook is valid
    const validation = await this.validate(handbookPath);
    if (!validation.valid) {
      throw new Error(`Handbook '${handbookName}' is not valid: ${validation.errors.join(', ')}`);
    }

    await configManager.updateGlobalConfig({ currentHandbook: handbookName });
    console.log(chalk.green(`✅  Current handbook set to '${handbookName}'`));
  }

  /**
   * Get handbook info by name
   */
  async getHandbookInfo(handbookName: string): Promise<HandbookInfo | null> {
    const handbooks = await this.list();
    return handbooks.find(h => h.name === handbookName) || null;
  }

}

export const handbookManager = new HandbookManager();

