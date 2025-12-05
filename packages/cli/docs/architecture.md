# OneMCP CLI Architecture

## Project Structure

```
onemcp-cli/
│
├── cmd/onemcp/              # Application entry point
│   └── main.go              # Calls cli.Execute()
│
├── internal/                # Private application code
│   │
│   ├── cli/                 # Command layer (Cobra)
│   │   ├── root.go          # Root command + viper config
│   │   ├── init.go          # onemcp init [target]
│   │   ├── server.go        # onemcp server [start|stop|status]
│   │   ├── handbook.go      # onemcp handbook [pull|push|acme]
│   │   └── chat.go          # onemcp chat
│   │
│   ├── interfaces/          # Core interface definitions
│   │   ├── project.go       # ProjectManager interface
│   │   ├── server.go        # ServerManager interface
│   │   ├── handbook.go      # HandbookManager interface
│   │   ├── chat.go          # ChatManager interface
│   │   └── state.go         # StateManager interface
│   │
│   └── errors/              # Error handling
│       └── errors.go        # CLIError + error codes 1-5
│
├── pkg/                     # Public packages (future)
│
├── go.mod                   # Go module definition
├── Makefile                 # Build automation
├── README.md                # Project overview
└── SETUP.md                 # Development setup guide
```

## Component Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                     User / Terminal                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   CLI Layer (Cobra)                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │   init   │  │  server  │  │ handbook │  │   chat   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Application Layer (Interfaces)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Project    │  │   Server     │  │  Handbook    │      │
│  │   Manager    │  │   Manager    │  │   Manager    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│  ┌──────────────┐  ┌──────────────┐                        │
│  │    Chat      │  │    State     │                        │
│  │   Manager    │  │   Manager    │                        │
│  └──────────────┘  └──────────────┘                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Infrastructure Layer (Future)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Docker     │  │   HTTP       │  │   SQLite     │      │
│  │   Client     │  │   Client     │  │   Database   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Interface Responsibilities

### ProjectManager
- Project initialization (local and remote modes)
- Directory structure validation
- Project context detection (find .onemcp/onemcp.yaml)
- Configuration loading and parsing

### ServerManager
- Docker container lifecycle (start, stop, status)
- Health check monitoring
- Auto-start functionality for local servers

### HandbookManager
- Handbook synchronization (pull/push)
- Acme template installation
- Handbook structure validation

### ChatManager
- Interactive terminal session (bubbletea)
- MCP protocol communication
- Chat logging

### StateManager
- SQLite database management
- Synchronization metadata tracking
- Server state persistence

## Error Handling Strategy

| Code | Type | Usage |
|------|------|-------|
| 1 | Generic | Unexpected failures, config parsing errors |
| 2 | Invalid Directory | Directory validation failures |
| 3 | Server Failure | Docker/health check failures |
| 4 | Sync Failure | Handbook pull/push failures |
| 5 | Not In Project | Commands run outside project context |

## Data Flow Example: `onemcp init`

```
User runs: onemcp init --server=https://remote.example.com

1. CLI Layer (init.go)
   └─> Parse arguments and flags
   └─> Create InitOptions struct

2. ProjectManager.Initialize()
   └─> ValidateDirectory() → Check if empty
   └─> ServerManager.HealthCheck() → Verify remote server
   └─> HandbookManager.Pull() → Fetch remote handbook
   └─> Create project structure (handbook/, logs/, reports/, .onemcp/)
   └─> Write .onemcp/onemcp.yaml with remote config
   └─> StateManager.Initialize() → Create state.db

3. Return success to CLI
   └─> Display success message to user
```

## Testing Strategy

### Unit Tests
- Test each interface implementation independently
- Mock dependencies where appropriate
- Focus on edge cases and error conditions

### Property-Based Tests (gopter)
- Minimum 100 iterations per property
- Test universal properties across all inputs
- Each property tagged with design document reference

### Integration Tests
- Test Docker operations end-to-end
- Test HTTP communication with mock servers
- Test filesystem operations

## Dependencies

- **cobra**: CLI framework
- **viper**: Configuration management (YAML)
- **gopter**: Property-based testing
- **docker/docker**: Docker client SDK
- **go-sqlite3**: SQLite database driver
- **bubbletea**: Terminal UI framework
