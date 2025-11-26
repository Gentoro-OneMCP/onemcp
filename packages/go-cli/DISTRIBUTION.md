# Distribution Setup Guide

This guide explains how to set up distribution for the OneMCP CLI.

## 1. Create Homebrew Tap Repository

### Create the Repository

1. Go to https://github.com/organizations/Gentoro-OneMCP/repositories/new
2. **Repository name:** `homebrew-onemcp` (exact name required by Homebrew)
3. **Description:** "Homebrew tap for OneMCP CLI"
4. **Visibility:** Public
5. Create repository

### Add Formula Files

Copy these files to the new `homebrew-onemcp` repository:

```bash
# Clone the new repo
git clone git@github.com:Gentoro-OneMCP/homebrew-onemcp.git
cd homebrew-onemcp

# Create structure
mkdir -p Formula

# Copy files from this directory
cp /path/to/go-cli/homebrew/Formula/onemcp.rb Formula/
cp /path/to/go-cli/homebrew/README.md .

# Commit and push
git add .
git commit -m "Add OneMCP formula"
git push origin main
```

### Usage

Once published, users install with:

```bash
brew tap gentoro-onemcp/onemcp
brew install onemcp
```

## 2. Install Script

The install script is at `packages/go-cli/install.sh`.

### Usage

Users can install with a one-liner:

```bash
curl -fsSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/go-cli/install.sh | bash
```

### Features

- Detects OS (macOS/Linux) and architecture (amd64/arm64)
- Downloads appropriate binary from GitHub Releases
- Checks for Docker
- Installs to `/usr/local/bin`
- Verifies installation

## 3. GitHub Releases (Required for Both)

Both Homebrew and the install script need **GitHub Releases** with binaries.

### Setup GoReleaser (Recommended)

Create `.goreleaser.yml` in `packages/go-cli/`:

```yaml
project_name: onemcp

builds:
  - main: ./main.go
    binary: onemcp
    goos:
      - darwin
      - linux
    goarch:
      - amd64
      - arm64
    env:
      - CGO_ENABLED=0

archives:
  - format: tar.gz
    name_template: "{{ .ProjectName }}-{{ .Os }}-{{ .Arch }}"

release:
  github:
    owner: Gentoro-OneMCP
    name: onemcp

changelog:
  sort: asc
```

### Create Release

**Manual:**
```bash
# Build for all platforms
GOOS=darwin GOARCH=arm64 go build -o onemcp-darwin-arm64
GOOS=darwin GOARCH=amd64 go build -o onemcp-darwin-amd64
GOOS=linux GOARCH=amd64 go build -o onemcp-linux-amd64

# Create tarballs
tar -czf onemcp-darwin-arm64.tar.gz onemcp-darwin-arm64
tar -czf onemcp-darwin-amd64.tar.gz onemcp-darwin-amd64
tar -czf onemcp-linux-amd64.tar.gz onemcp-linux-amd64

# Create GitHub release and upload tarballs
```

**With GoReleaser:**
```bash
# Tag version
git tag v0.1.0
git push origin v0.1.0

# Release (requires GITHUB_TOKEN)
goreleaser release --clean
```

### Update Formula After Release

After creating a release, update the Homebrew formula:

1. Calculate SHA256 checksums:
```bash
shasum -a 256 onemcp-darwin-arm64.tar.gz
shasum -a 256 onemcp-darwin-amd64.tar.gz
shasum -a 256 onemcp-linux-amd64.tar.gz
```

2. Update `homebrew-onemcp/Formula/onemcp.rb`:
   - Replace `v0.1.0` with actual version
   - Replace `PLACEHOLDER_*_SHA256` with actual checksums

3. Commit and push to `homebrew-onemcp` repo

## Next Steps

1. **Create GitHub Release** with binaries
2. **Create `homebrew-onemcp` repository**
3. **Update formula** with real version and checksums
4. **Test installation:**
   ```bash
   brew tap gentoro-onemcp/onemcp
   brew install onemcp
   ```

## Testing Install Script Locally

```bash
# Test the install script locally
./install.sh
```

## Updating After New Releases

1. Create new GitHub Release with binaries
2. Update formula in `homebrew-onemcp` repo with new version and checksums
3. Users update with: `brew upgrade onemcp`
4. Install script automatically uses latest release
