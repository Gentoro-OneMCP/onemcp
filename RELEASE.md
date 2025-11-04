# Release Process

This document describes the release process for the OneMCP project. The release system creates GitHub repository tags and publishes corresponding Docker images to Docker Hub.

## Overview

The release process handles:
- **Base Image**: `admingentoro/gentoro:base-{version}` - Contains JRE 21, Node.js, OpenAPI Generator CLI, and OpenTelemetry Collector
- **Product Image**: `admingentoro/gentoro:{version}` - Contains the OneMCP application built on the base image

## Quick Start

### Prerequisites

- Docker installed and running
- Docker Hub account with push access to `admingentoro/gentoro` repository
- Git configured with user name and email
- Maven installed (for building the application JAR)

### Basic Release

```bash
# Patch release (0.1.0-SNAPSHOT → 0.1.1-SNAPSHOT)
./scripts/release.sh patch

# Minor release (0.1.0-SNAPSHOT → 0.2.0-SNAPSHOT)
./scripts/release.sh minor

# Major release (0.1.0-SNAPSHOT → 1.0.0-SNAPSHOT)
./scripts/release.sh major
```

### Advanced Options

```bash
# Dry run (no changes made)
./scripts/release.sh patch --dry-run

# Skip building images
./scripts/release.sh patch --skip-build

# Skip publishing to Docker Hub
./scripts/release.sh patch --skip-publish
```

## Docker Images

### Base Image (`admingentoro/gentoro:base-{version}`)

Contains the runtime environment:
- Debian Trixie base
- OpenJDK 21 JRE
- Node.js and npm
- OpenAPI Generator CLI
- OpenTelemetry Collector
- Supervisor for process management

### Product Image (`admingentoro/gentoro:{version}`)

Contains the OneMCP application:
- Built on the base image
- Includes the compiled JAR file
- Application-specific configuration
- Runtime scripts

### Image Tags

Each release creates multiple tags:
- `admingentoro/gentoro:base-v1.2.3` and `admingentoro/gentoro:base-latest`
- `admingentoro/gentoro:v1.2.3` and `admingentoro/gentoro:latest`

## GitHub Actions

The project includes automated release workflows:

- **Manual Release**: Triggered via GitHub Actions UI
- **Tag Release**: Automatically triggered when pushing tags to the repository

## Troubleshooting

### Common Issues

1. **Docker Build Fails**: Ensure Docker is running and you have sufficient disk space
2. **Publish Fails**: Verify Docker Hub credentials and repository access
3. **Git Tag Conflicts**: Ensure the tag doesn't already exist
4. **Maven Build Fails**: Check that all dependencies are available

### Manual Steps

If automated release fails, you can run steps manually:

```bash
# 1. Update version in POM
mvn versions:set -DnewVersion=1.2.3-SNAPSHOT

# 2. Build base image
./scripts/docker/build-base.sh v1.2.3

# 3. Build product image
./scripts/docker/build-product.sh v1.2.3

# 4. Publish images
./scripts/docker/publish.sh v1.2.3

# 5. Create and push tag
git tag -a v1.2.3 -m "Release v1.2.3"
git push origin main --tags
```

### Verification

After release, verify images are available:

```bash
# Pull and test the latest image
docker pull admingentoro/gentoro:latest
docker run --rm admingentoro/gentoro:latest
```