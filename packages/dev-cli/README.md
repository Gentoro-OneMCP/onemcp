# OneMCP Dev Wrapper

Developer CLI wrapper for testing local Java server changes without merging branches.

## Overview

The `onemcp-dev` wrapper enables you to:
- ✅ Test local code changes instantly
- ✅ Debug Java server directly
- ✅ Test branches before merging
- ✅ Build custom Docker images

## Installation

No installation needed! The wrapper is located at:
```bash
packages/dev-cli/onemcp-dev
```

**Prerequisite:** The Go CLI must be built first:
```bash
cd packages/go-cli
go build -o onemcp
```

## Usage

### Fast Mode (Default) - Recommended for Development

```bash
./packages/dev-cli/onemcp-dev chat
```

**What it does:**
1. Builds Java server with `mvn package -DskipTests` (~10-15 seconds)
2. Starts Java process locally on port 8080
3. Uses Go CLI with `--local` flag to connect
4. Auto-cleanup on exit

**When to use:**
- Daily development workflow
- Testing small changes
- Debugging (can attach debugger to Java process)

**Speed:** ~10-15 seconds after first build

---

### Docker Mode - For Production-Like Testing

```bash
./packages/dev-cli/onemcp-dev chat --docker
```

**Requirements:** Podman (not standard Docker)
```bash
# Install Podman first
brew install podman
podman machine init
podman machine start
```

**What it does:**
1. Cleans old dev images
2. Builds base image (if needed, ~2-3 min first time)
3. Builds product image with your JAR (~20s)
4. Transfers image from Podman to Docker/OrbStack
5. Removes redundant `:latest` tags
6. Uses Go CLI with `--image=admingentoro/gentoro:dev`

**When to use:**
- Testing before creating a PR
- Verifying Docker-specific behavior
- Testing deployment configuration

**Speed:** 
- First time: ~2-3 minutes (builds base)
- Subsequent: ~20 seconds (reuses base)

---

### Clean Build Mode

**Fast mode:**
```bash
./packages/dev-cli/onemcp-dev chat --clean
```
Forces `mvn clean package` (useful when dependencies change).

**Docker mode:**
```bash
./packages/dev-cli/onemcp-dev chat --docker --clean
```
Rebuilds both base and product images from scratch (~3 minutes).

**When to use `--clean`:**
- After changing `pom.xml` dependencies
- After updating `Dockerfile.base`
- When you have weird caching/build issues

---

### All Commands Supported

The wrapper supports **all** Go CLI commands:

```bash
# Server commands (starts local server)
./packages/dev-cli/onemcp-dev chat          # Fast mode
./packages/dev-cli/onemcp-dev chat --docker # Docker mode

# Logs commands
./packages/dev-cli/onemcp-dev logs          # Show last 50 lines
./packages/dev-cli/onemcp-dev logs --follow # Follow live logs

# Config commands (passthrough, no server)
./packages/dev-cli/onemcp-dev status
./packages/dev-cli/onemcp-dev handbook list
./packages/dev-cli/onemcp-dev provider list
./packages/dev-cli/onemcp-dev logs
```

Non-chat commands are passed through directly to the Go CLI.

---

## Logging

### Fast Mode Logs

Logs are automatically saved to `~/.onemcp/logs/` with timestamped filenames:

```bash
~/.onemcp/logs/
├── dev-2025-12-03-08-15-30.log   # Timestamped log file
├── dev-2025-12-03-09-20-15.log
└── dev-latest.log                 # Symlink to most recent
```

**View logs:**
```bash
# Show last 50 lines
./packages/dev-cli/onemcp-dev logs

# Follow live logs (like tail -f)
./packages/dev-cli/onemcp-dev logs --follow

# Or use standard tools
tail -f ~/.onemcp/logs/dev-latest.log
```

### Docker Mode Logs

Docker mode logs are managed by Docker:

```bash
# View Docker logs
./go-cli/onemcp logs

# Follow Docker logs
./go-cli/onemcp logs --follow
```

### Execution Reports

Execution reports show detailed LLM inference steps, API calls, and the full execution flow for each query. They're saved alongside logs:

```bash
~/.onemcp/logs/reports/
└── execution-2025-12-03T16-43-05.137829Z.txt
```

**View the most recent report:**
```bash
ls -t ~/.onemcp/logs/reports/ | head -n 1 | xargs -I {} cat ~/.onemcp/logs/reports/{}
```

Reports include:
- Query analysis and entity extraction
- Execution plan generation
- API calls with timings
- Response processing
- Token usage and costs

---

## Examples

### Test a Feature Branch

```bash
git checkout feature/new-api
./packages/dev-cli/onemcp-dev chat
# Ask questions, test the new feature
```

### Debug Java Locally

```bash
# Terminal 1: Start with fast mode
./packages/dev-cli/onemcp-dev chat

# Terminal 2: Attach debugger to the Java process
# (Find PID in output: "Server PID: 12345")
jdb -attach localhost:5005
```

### Test Docker Build

```bash
# Build custom Docker image and test
./packages/dev-cli/onemcp-dev chat --docker
```

---

## Configuration

The wrapper automatically reads your OneMCP config from `~/.onemcp/config.yaml`:

- **Provider & API Keys:** Passed to Java server
- **Handbook:** Uses current handbook setting
- **Port:** Uses configured port (default: 8080)

---

## Troubleshooting

### Port Already in Use

**Error:**
```
Error: Port 8080 is already in use
```

**Solution:**
```bash
# Kill existing process
lsof -ti:8080 | xargs kill -9

# Or change port in config
vim ~/.onemcp/config.yaml
```

### Server Failed to Start

**Check logs:**
```bash
# Fast mode
./packages/dev-cli/onemcp-dev logs

# Or directly
tail -f ~/.onemcp/logs/dev-latest.log

# Docker mode
./go-cli/onemcp logs
```

### Go CLI Not Found

**Error:**
```
Error: Go CLI not found at /path/to/go-cli/onemcp
```

**Solution:**
```bash
cd packages/go-cli
go build -o onemcp
```

### Docker Mode Requires Podman

**Error:**
```
⚠️  Docker mode requires buildah or podman (not standard Docker)
```

**Solution:**
```bash
# Install Podman
brew install podman

# Initialize and start VM
podman machine init
podman machine start

# Then try again
./packages/dev-cli/onemcp-dev chat --docker
```

### Image Transfer Issues

If the Docker mode builds but Go CLI can't find the image:

```bash
# Manually transfer
podman save admingentoro/gentoro:dev -o /tmp/image.tar
docker load -i /tmp/image.tar
docker tag localhost/admingentoro/gentoro:dev admingentoro/gentoro:dev
```

---

## How It Works

### Architecture

```
onemcp-dev (bash script)
    ↓
    ├─ Fast Mode
    │   ├─ Build: mvn package -DskipTests
    │   ├─ Start: java -jar target/*.jar
    │   └─ Connect: onemcp chat --local
    │
    ├─ Docker Mode (requires Podman)
    │   ├─ Clean old images
    │   ├─ Build base (Podman): admingentoro/gentoro:base-latest
    │   ├─ Build product (Podman): admingentoro/gentoro:dev
    │   ├─ Transfer: podman save → docker load
    │   ├─ Cleanup: remove :latest tags
    │   └─ Run: onemcp chat --image=admingentoro/gentoro:dev
    │
    └─ Other Commands (status, handbook, provider, logs)
        └─ Passthrough: onemcp <command>
```

### Environment Variables

The wrapper sets these environment variables for the Java server:

| Variable | Source | Example |
|----------|--------|---------|
| `SERVER_PORT` | Config | `8080` |
| `HANDBOOK_DIR` | Config + current handbook | `/path/to/handbook` |
| `INFERENCE_DEFAULT_PROVIDER` | Config | `gemini` |
| `OPENAI_API_KEY` | Config | `sk-...` |
| `GEMINI_API_KEY` | Config | `AI...` |
| `ANTHROPIC_API_KEY` | Config | `sk-ant-...` |

---

## Comparison: Dev Wrapper vs Regular CLI

| Feature | `onemcp chat` | `onemcp-dev chat` |
|---------|---------------|-------------------|
| **Uses** | Docker image | Local Java build |
| **Speed** | Fast (image already built) | ~10-15s build |
| **Testing** | Production code | Your local changes |
| **Debugging** | Harder (in container) | Easy (attach debugger) |
| **Use case** | Normal usage | Development/testing |

---

## Tips

1. **First run takes longer** (~30s for fast mode, ~3 min for Docker mode) because it downloads/builds everything
2. **Incremental builds are fast** (~10s) - Maven reuses compiled classes
3. **Use `--clean`** when you change `pom.xml`, dependencies, or Dockerfiles
4. **Docker mode requires Podman** - `brew install podman` if you need it
5. **Automatic cleanup** - Old dev images are removed before each build
6. **Logs** are written to `~/.onemcp/logs/` with timestamps (view with `./dev-cli/onemcp-dev logs`)
7. **All commands work** - Not just `chat`, also `status`, `handbook`, `provider`, etc.
8. **Base image is cached** - Docker mode is fast after first build (~20s vs ~3min)
9. **Database cleanup** - Fast mode cleans OrientDB data on start (stored in `packages/server/data/orient/`)

---

## Patrick's Testing Workflow

As requested, here's how to test branches without merging:

```bash
# Switch to the branch
git checkout jun/feature-branch

# Test it immediately
./packages/dev-cli/onemcp-dev chat

# Ask questions, verify the feature works
# ...

# Done! No merge needed for testing
```

---

## Contributing

If you improve the wrapper:
1. Test both fast and Docker modes
2. Update this README
3. Submit a PR

---

## Related Documentation

- [Go CLI README](../go-cli/README.md)
- [Server README](../server/README.md)
- [TypeScript CLI](../cli/README.md)
