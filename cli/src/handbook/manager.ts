/**
 * Handbook management utilities
 */
import fs from 'fs-extra';
import chalk from 'chalk';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { paths } from '../config/paths.js';

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
    await fs.ensureDir(`${dir}/config`);

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
    const requiredDirs = ['apis', 'docs', 'state'];
    for (const reqDir of requiredDirs) {
      if (!(await fs.pathExists(`${dir}/${reqDir}`))) {
        warnings.push(`Missing recommended directory: ${reqDir}/`);
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
  async list(): Promise<Array<{ name: string; path: string; valid: boolean }>> {
    await paths.ensureDirectories();

    const entries = await fs.readdir(paths.handbooksDir, { withFileTypes: true });
    const handbooks = entries.filter((e) => e.isDirectory());

    const results = [];
    for (const handbook of handbooks) {
      const handbookPath = paths.getHandbookPath(handbook.name);
      const validation = await this.validate(handbookPath);
      results.push({
        name: handbook.name,
        path: handbookPath,
        valid: validation.valid,
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

    let resolvedSource = resolve(__dirname, '../example-handbook');

    // If running in development (src directory), try alternative locations
    if (__dirname.includes('/src/')) {
      // Try the built dist directory first
      const distSource = resolve(__dirname, '../../../dist/example-handbook');
      if (await fs.pathExists(distSource)) {
        resolvedSource = distSource;
      } else {
        // Fallback: try to copy from the source location
        const sourceSource = resolve(__dirname, '../../../src/acme-analytics-server/onemcp-handbook');
        if (await fs.pathExists(sourceSource)) {
          resolvedSource = sourceSource;
        }
      }
    }


    if (!(await fs.pathExists(resolvedSource))) {
      throw new Error(`Example handbook not found at ${resolvedSource}. Checked dist and source locations.`);
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

    console.log(chalk.green(`✅  Example handbook copied to ${targetDir}`));
  }
}

export const handbookManager = new HandbookManager();

