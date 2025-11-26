# Homebrew Tap for OneMCP

This repository contains the Homebrew formula for installing OneMCP CLI.

## Installation

```bash
brew tap gentoro-onemcp/onemcp
brew install onemcp
```

## Usage

```bash
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

- Docker (installed automatically if using `brew install --with-docker`)

## Repository Structure

This tap is maintained separately from the main [OneMCP repository](https://github.com/Gentoro-OneMCP/onemcp).

## Updating

The formula is automatically updated when new releases are published to the main repository.

To update to the latest version:

```bash
brew update
brew upgrade onemcp
```

## License

Apache-2.0
