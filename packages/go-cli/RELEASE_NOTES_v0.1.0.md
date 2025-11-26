# OneMCP CLI v0.1.0

First official release of the Go CLI for OneMCP - Connect your APIs to AI models via the Model Context Protocol.

## 🚀 Installation

### Homebrew (macOS/Linux)
```bash
brew tap gentoro-onemcp/onemcp
brew install onemcp
```

### Install Script (macOS/Linux)
```bash
curl -fsSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/go-cli/install.sh | bash
```

### Manual Download
Download the appropriate binary for your platform from the assets below, extract it, and move to your PATH.

## 📋 Features

### Core Commands
- **`onemcp chat`** - Interactive chat mode with AI
- **`onemcp status`** - Show server and configuration status
- **`onemcp stop`** - Stop the OneMCP server
- **`onemcp logs`** - View server logs (`-n` for lines, `-f` to follow)

### Provider Management
- **`onemcp provider list`** - Show configured AI providers
- **`onemcp provider switch`** - Switch between providers
- **`onemcp provider set`** - Configure new provider/API key

### Handbook Management
- **`onemcp handbook init <name>`** - Create new handbook
- **`onemcp handbook list`** - List all handbooks
- **`onemcp handbook use <name>`** - Set active handbook
- **`onemcp handbook current`** - Show current handbook
- **`onemcp handbook validate`** - Validate handbook structure

### Utilities
- **`onemcp doctor`** - Check system requirements
- **`onemcp update`** - Update to latest version
- **`onemcp reset`** - Reset configuration

## ✨ Highlights

- ✅ **Docker-based** - Zero local dependencies except Docker
- ✅ **Built-in ACME Analytics** - Example dataset for testing
- ✅ **Multi-provider** - OpenAI, Google Gemini, Anthropic Claude
- ✅ **Auto-setup wizard** - Guided configuration on first run
- ✅ **Timeout handling** - Automatic session reset on timeouts
- ✅ **Clean UX** - Spinner with elapsed time, structured responses

## 📦 What's Included

- macOS binaries (Intel + Apple Silicon)
- Linux binary (amd64)
- Homebrew formula
- Install script

## 🔧 Requirements

- **Docker** - Required to run the OneMCP server
- macOS or Linux

## 🎯 Quick Start

```bash
# Install via Homebrew
brew tap gentoro-onemcp/onemcp
brew install onemcp

# Start chatting
onemcp chat

# Follow the setup wizard to configure your API key and provider
```

## 📝 Notes

- First run will pull the Docker image (~300MB)
- ACME Analytics handbook is built into the Docker image
- Configuration stored in `~/.onemcp/config.yaml`
- Custom handbooks stored in `~/onemcp-handbooks/` (visible, not hidden)

## 🐛 Known Issues

None at this time.

## 📖 Documentation

Full documentation: https://github.com/Gentoro-OneMCP/onemcp

---

**Full Changelog**: https://github.com/Gentoro-OneMCP/onemcp/commits/v0.1.0
