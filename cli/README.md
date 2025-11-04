# Gentoro One MCP CLI

The Gentoro One MCP CLI provides a fast, consistent way to connect your APIs to AI models that support the Model Context Protocol (MCP).

It removes the need to build custom adapters or middleware. You can start with a built-in mock environment or connect your own service with minimal setup.

## Quick Start

Get started in minutes with:

First, install the CLI:

```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | bash
```

Then start chatting (auto-setup wizard launches on first run):

```bash
onemcp chat
```

You'll see an interactive chat interface like this:

```
╔══════════════════════════════════════╗
║     Gentoro OneMCP - Chat Mode       ║
╚══════════════════════════════════════╝

Provider: openai
MCP URL: http://localhost:8080/mcp
Type 'exit' to quit, 'clear' to clear history
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 Mock Server Active - Try These Example Queries:

  > Show me electronics sales in California last quarter.
  > List top customers by revenue.
  > Compare revenue trends by region.
  > What are the top-selling products this month?
  > Show me sales data for New York vs Texas.

Type "help" anytime for more commands.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

? You:
```

That's it! The CLI will automatically:
- Run the setup wizard (choose your AI provider) - for the first time.
- Start all required services
- Launch interactive chat with example queries

## Requirements

- **Node.js** >= 20
- **Java** 21 (for running the Java application)
- **Maven** (for building the Java application)

Optional:
- **OpenTelemetry Collector** (for telemetry)

### Uninstallation

**To uninstall CLI:** `curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/uninstall.sh | bash`

For detailed uninstallation options, see the [Advanced Installation](#advanced-installation) section below.

### For Local Development

```bash
git clone https://github.com/Gentoro-OneMCP/onemcp.git
cd onemcp/cli
npm install && npm run build && npm link
```

## Usage

### Getting Started

Start chatting with the built-in Acme Analytics mock service:

```bash
onemcp chat
```

**Choose your starting point**: During setup, you can either select the included ACME Analytics Server sample mode to explore a complete example with documentation, API specs, and sample queries, or set up your own API service from scratch.

The first time you run this, it will:
1. Launch an interactive setup wizard to choose your AI provider
2. Start all required services automatically
3. Open the interactive chat interface with example queries

### Try Example Queries

With the mock Acme Analytics service, you can try queries like:
- "Show me electronics sales in California last quarter"
- "List top customers by revenue"
- "Compare revenue trends by region"
- "What are the top-selling products this month?"

Type `help` in chat mode for additional commands.

### Connecting Your Own API

To connect your own API service:

```bash
# Create a handbook for your service
onemcp handbook init my-service

# Configure authentication
onemcp service auth my-service

# Start chatting
onemcp chat
```

## Core Commands

### Chat & Services
```bash
onemcp chat          # Start interactive chat (auto-starts services)
onemcp stop          # Stop all services
onemcp status        # Show service status
onemcp update        # Update to latest version
```

### Handbook Management
```bash
onemcp handbook init <name>     # Create new handbook
onemcp handbook validate        # Validate handbook structure
onemcp handbook list           # List all handbooks
```

### Service Management
```bash
onemcp service auth <name>     # Configure service authentication
onemcp service renew <name>    # Renew service token
onemcp service list           # List configured services
```

### Provider Management
```bash
onemcp provider set           # Set AI model provider
onemcp provider switch        # Switch between providers
```

### Diagnostics
```bash
onemcp doctor        # Check system requirements
onemcp logs [service] # View logs (-f to follow, -n for lines)
onemcp --version     # Show version
onemcp --help        # Show help
```

## Architecture

The CLI manages multiple services that run as child processes:

- **One MCP Server** (port 8080) - Main Spring Boot application
- **TypeScript Runtime** (port 7070) - Executes TypeScript snippets
- **Mock Server** (port 8082) - Acme Analytics (optional)
- **OpenTelemetry Collector** (port 4317) - Telemetry collection (optional)

All services include automatic health checks and log management.

## Configuration

### Handbook Structure

Each One MCP service runs from a handbook directory:

```
handbook/
├── Agent.md                    # Instructions and behavior
├── apis/                       # OpenAPI specifications
├── docs/                       # Supplementary markdown docs
├── regression/                 # Optional test definitions
└── state/                      # Auto-generated runtime data
    └── knowledge-base-state.json  # Indexed knowledge base
```

### Service Authentication

Services define authentication via a `service.yaml` file:

```yaml
service: my-api
header: Authorization
pattern: Bearer {token}
token: eyJh...
expiresAt: 2025-10-25T14:02:12Z
```

### Global Configuration

Location: `~/.onemcp/config.yaml`

```yaml
provider: openai
apiKey: sk-...
handbookDir: ~/handbooks
logDir: ~/.onemcp/logs
```

## Troubleshooting

### Common Issues

**CLI won't start:**
```bash
onemcp doctor  # Check system requirements
```

**Services not healthy:**
```bash
onemcp status  # Check service status
onemcp logs app  # View logs
onemcp stop && onemcp chat  # Restart services
```

**Token expired:**
```bash
onemcp service renew <service-name>
```

### Logs Location

Logs are stored in `~/.onemcp/logs/` with automatic archiving.

## Advanced Installation

### Installation Methods

The installer supports multiple installation methods:

#### npm Global (Default)
```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | bash
```

#### System-wide Installation
```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | \
  ONEMCP_INSTALL_METHOD=system-wide bash
```

#### Local Installation
```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | \
  ONEMCP_INSTALL_METHOD=local-bin bash
```

**Note:** Add `~/.local/bin` to PATH if not already included.

#### Custom Branch
```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | \
  ONEMCP_REPO_BRANCH=your-branch-name bash
```

### Uninstallation

#### Automatic Uninstall
```bash
curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/uninstall.sh | bash
```

#### Manual Uninstall
```bash
onemcp stop
# Remove based on installation method:
# npm: npm uninstall -g @gentoro/onemcp-cli
# system-wide: sudo rm /usr/local/bin/onemcp
# local-bin: rm ~/.local/bin/onemcp
rm -rf ~/.onemcp-src ~/.onemcp ~/handbooks
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for details.

## License

See [LICENSE](../LICENSE) for details.

## Support

- **Documentation**: https://github.com/Gentoro-OneMCP/onemcp
- **Issues**: https://github.com/Gentoro-OneMCP/onemcp/issues
- **Discussions**: https://github.com/Gentoro-OneMCP/onemcp/discussions
