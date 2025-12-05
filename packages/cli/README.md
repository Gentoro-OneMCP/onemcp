# OneMCP CLI

> Connect your APIs to AI models via the Model Context Protocol

The OneMCP CLI is a powerful command-line tool that enables you to interact with AI models through your own APIs and data sources using the Model Context Protocol (MCP).

## ✨ Features

- 🤖 **Multi-Provider Support** - Works with OpenAI, Google Gemini, and Anthropic Claude
- 🐳 **Docker-based** - Zero local dependencies except Docker
- 📚 **Custom Handbooks** - Define your own API specifications and knowledge bases
- 💬 **Interactive Chat** - Natural language interface to your APIs
- 🎯 **Built-in Example** - ACME Analytics dataset for immediate testing
- 🔄 **Hot-switching** - Change providers and handbooks without restarting

## 🚀 Installation

### macOS / Linux (Recommended: Homebrew)

```bash
brew tap gentoro-onemcp/onemcp
brew install onemcp
```

### Windows (Recommended: Scoop)

```powershell
scoop bucket add gentoro-onemcp https://github.com/Gentoro-OneMCP/onemcp-scoop
scoop install onemcp
```

### From Source

```bash
git clone https://github.com/Gentoro-OneMCP/onemcp.git
cd onemcp/packages/cli
go build -o onemcp main.go
sudo mv onemcp /usr/local/bin/  # macOS/Linux
# Or move to PATH on Windows
```

## 🗑️ Uninstallation

### Homebrew (macOS/Linux)

```bash
brew uninstall onemcp
```

### Scoop (Windows)

```powershell
scoop uninstall onemcp
```

### Script/Manual Install

```bash
# Download and run uninstall script
curl -fsSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/go-cli/uninstall.sh | bash

# Or manually:
sudo rm /usr/local/bin/onemcp
rm -rf ~/.onemcp
rm -rf ~/onemcp-handbooks  # Optional: removes your handbooks
docker rmi admingentoro/gentoro:latest  # Optional: removes Docker image
```

## 🖥️ Requirements

- **Docker** - Required to run the OneMCP server
- macOS, Linux, or Windows

## 🎯 Quick Start

```bash
# Start interactive chat
onemcp chat

# Follow the setup wizard to configure your API key
# Try the built-in ACME Analytics example:
You: Show total sales for 2024
```

## 📖 Commands

### Core Commands

| Command | Description |
|---------|-------------|
| `onemcp chat` | Start interactive chat mode |
| `onemcp status` | Show server and configuration status |
| `onemcp stop` | Stop the OneMCP server |
| `onemcp logs [-n lines] [-f]` | View server logs |
| `onemcp doctor` | Check system requirements |
| `onemcp update` | Update to latest version |
| `onemcp reset` | Reset all configuration |

### Provider Management

| Command | Description |
|---------|-------------|
| `onemcp provider list` | Show configured AI providers |
| `onemcp provider set` | Configure a provider and API key |
| `onemcp provider switch` | Switch between configured providers |

### Service Management

| Command | Description |
|---------|-------------|
| `onemcp service auth` | Configure authentication for external services |
| `onemcp service list` | List configured services |
| `onemcp service renew` | Renew service authentication token |

### Handbook Management

| Command | Description |
|---------|-------------|
| `onemcp handbook init <name>` | Create a new handbook |
| `onemcp handbook list` | List all handbooks |
| `onemcp handbook use [name]` | Set active handbook (interactive if no name) |
| `onemcp handbook current` | Show current handbook info |
| `onemcp handbook validate [name]` | Validate handbook structure (current if no name) |

### Shell Completion

| Command | Description |
|---------|-------------|
| `onemcp completion bash` | Generate bash completion script |
| `onemcp completion zsh` | Generate zsh completion script |
| `onemcp completion fish` | Generate fish completion script |
| `onemcp completion powershell` | Generate PowerShell completion script |

## 📚 Handbooks

Handbooks define your API specifications and knowledge base. Each handbook contains:

```
my-handbook/
├── instructions.md       # Agent instructions and behavior
├── openapi/             # OpenAPI/Swagger specifications
│   └── api.yaml
├── docs/                # Additional documentation (optional)
└── data/                # Data files like CSVs (optional)
```

### Creating a Handbook

```bash
# Initialize a new handbook
onemcp handbook init my-api

# Edit the files
cd ~/onemcp-handbooks/my-api
# Add your OpenAPI specs to openapi/
# Customize instructions.md

# Validate
onemcp handbook validate my-api

# Use it
onemcp handbook use my-api
```

## ⚙️ Configuration

Configuration is stored in `~/.onemcp/config.yaml`:

```yaml
provider: gemini              # Current AI provider
apikeys:
  gemini: YOUR_API_KEY
  openai: YOUR_API_KEY
currenthandbook: acme-analytics
defaultport: 8080
handbookdir: /Users/you/onemcp-handbooks
chattimeout: 240
```

## 🎮 Chat Mode Commands

While in chat mode, you can use these special commands:

- `help` - Show available commands
- `clear` - Clear chat history
- `switch` - Switch to a different handbook
- `exit` / `quit` - Exit chat mode

## 🔧 Development

### Building

```bash
# Build for your platform
go build -o onemcp main.go

# Build for all platforms
./build-release.sh
```

### Project Structure

```
packages/go-cli/
├── cmd/                 # Command implementations
├── pkg/
│   ├── chat/           # Chat mode logic
│   ├── config/         # Configuration management
│   ├── docker/         # Docker integration
│   ├── handbook/       # Handbook management
│   ├── mcp/            # MCP client
│   └── wizard/         # Setup wizard
├── homebrew/           # Homebrew formula
├── install.sh          # Installation script
└── main.go            # Entry point
```

## 📝 Examples

### Sales Analysis

```
You: Show total sales for 2024
Agent: Total revenue for 2024: $5,640,255.26

You: Which product category had the highest sales?
Agent: Electronics category led with $2.1M in sales
```

### API Integration

```
You: How many active users do we have?
Agent: There are 1,247 active users this month

You: Show me the top 5 customers by revenue
Agent: [Returns structured data from your API]
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

[Your License Here]

## 🔗 Links

- [Documentation](https://onemcp.gentoro.com/docs)
- [Report Issues](https://github.com/Gentoro-OneMCP/onemcp/issues)
- [Model Context Protocol](https://modelcontextprotocol.io)

---

Made with ❤️ by the Gentoro team
