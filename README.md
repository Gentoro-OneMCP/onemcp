# Gentoro OneMCP

OneMCP is an open-source runtime that makes it easy for AI agents to use your API accurately and efficiently.

You provide your API materials — such as the specification, documentation, and authentication details (collectively called the handbook) — and OneMCP immediately exposes your system through a single, natural-language interface.

It removes the need to handcraft MCP tools or connectors while achieving high performance and low token cost through a smart execution-plan system designed for caching and reuse.

## 🚀 Quick Start


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

## 📚 Documentation

Full documentation is available at [https://onemcp.gentoro.com/docs](https://onemcp.gentoro.com/docs)

### ℹ️ CLI Details

For detailed CLI documentation, all commands, and advanced features, see the [CLI README](cli/README.md).

### 🐳 Alternative: Using Docker

**Prerequisites:**
- **Docker**
- **API Keys** for at least one of the supported providers:
  - OpenAI API key
  - Google Gemini API key
  - Anthropic API key

```bash
# Pull latest image
docker pull admingentoro/gentoro:latest
```

```bash
# Run with OPENAI API
docker run --name onemcp -p 8080:8080 -e OPENAI_API_KEY=your-key admingentoro/gentoro:latest
```

#### Using Docker with Custom Foundation Folder

```bash
# Run within your custom foundation folder
docker run --name onemcp -p 8080:8080 \
  -v $(pwd):/var/foundation \
  -e OPENAI_API_KEY=your-key \
  admingentoro/gentoro:latest
```

**Foundation Validation**: The agent automatically validates your foundation folder structure. You can also validate manually:

```bash
# Validate foundation before running
docker run --rm \
  -v $(pwd):/var/foundation \
  -e APP_ARGS="--process=validate" \
  admingentoro/gentoro:latest
```

If validation fails, check the container logs for specific error messages and guidance.

### 🐳 Container Management

With named containers, you can easily manage OneMCP:

```bash
# Start/stop the container
docker start onemcp
docker stop onemcp

# View logs
docker logs onemcp

# Access container shell
docker exec -it onemcp bash

# Remove container when done
docker rm onemcp
```

### 🔍 Using Your Agent

Once installed (via CLI or Docker), interact with your OneMCP using the MCP Inspector:

#### MCP Inspector

Use the MCP Inspector to interact with your agent:

```bash
# Launch the MCP Inspector
npx @modelcontextprotocol/inspector@latest --port 3001 --open
```

When the inspector opens:
1. MCP URL should be set to `http://localhost:8080/mcp` 
2. Tweak configuration based on your preference and requirements
3. Click "Connect"

**Note:** Wait 30-60 seconds for the server to fully initialize before connecting the MCP Inspector.

### 🛠️ Troubleshooting

#### Slow Startup (30-60 seconds)
The default configuration uses AI hint generation which can be slow. For faster startup:

```bash
# Disable AI hint generation
docker run --name onemcp -p 8080:8080 \
  -e OPENAI_API_KEY=your-key \
  -e KNOWLEDGE_BASE_HINT_USE_AI=false \
  admingentoro/gentoro:latest
```

#### MCP Inspector Connection Issues
If the MCP Inspector shows "socket hang up" errors:
1. Wait 30-60 seconds for the server to fully initialize
2. Ensure the MCP URL is set to `http://localhost:8080/mcp`
3. Try refreshing the connection in the MCP Inspector

#### TypeScript Runtime Issues
If you see TypeScript runtime errors, the optimized version includes fixes:
- Proper startup order (TypeScript runtime starts first)
- Health check endpoints
- Retry logic for failed connections

