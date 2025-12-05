# 🪣 Scoop Bucket for OneMCP

This repository contains the Scoop manifest for installing the OneMCP CLI on Windows.

## Installation

First, add the OneMCP bucket:

```powershell
scoop bucket add onemcp https://github.com/gentoro-onemcp/onemcp-scoop
```

Then install the CLI:

```powershell
scoop install onemcp
```

## Usage

```powershell
# Start interactive chat
onemcp chat

# View status
onemcp status

# View logs
onemcp logs

# See all commands
onemcp --help
```

## Requirements

* Docker (must be installed separately on Windows)

    * Recommended: Docker Desktop
    * Scoop will **not** install Docker automatically

## Repository Structure

This bucket is maintained separately from the main
[OneMCP repository](https://github.com/Gentoro-OneMCP/onemcp).

All Scoop manifests are stored in:

```
bucket/
```

## Updating

Scoop will detect new versions when you run:

```powershell
scoop update
scoop update onemcp
```

If the manifest supports `"checkver"` and `"autoupdate"`, it can be automatically updated whenever new releases are published to the main repository.

## License

Apache-2.0