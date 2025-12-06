# Releasing the OneMCP CLI

This guide documents the release process for the OneMCP CLI.

## Prerequisites

Before releasing, ensure you have:

- ✅ Push access to `Gentoro-OneMCP/onemcp` repository
- ✅ Write access to `Gentoro-OneMCP/homebrew-onemcp` repository
- ✅ Write access to `Gentoro-OneMCP/onemcp-scoop` repository
- ✅ Go 1.21+ installed
- ✅ All changes merged to `main` branch
- ✅ All tests passing

---

## Release Process

### 1. Prepare for Release

**Decide on version number:**
- Follow semantic versioning: `cli-v0.0.X`
- Current latest: `cli-v0.0.4`
- Next release: `cli-v0.0.5`

**Update version in code (if needed):**
- Version is injected at build time via `ldflags`
- Check `internal/cli/version/version.go` exists

**Test locally:**
```bash
cd packages/cli
make clean
make build
bin/onemcp --help
bin/onemcp version
```

---

### 2. Build Binaries

Run the build script from the `packages/cli` directory:

```bash
make build-release VERSION=cli-0.0.5
```

**What this does:**
- Builds for all platforms (macOS Intel/ARM, Linux, Windows)
- Creates `.tar.gz` archives (macOS/Linux)
- Creates `.zip` archives (Windows)
- Generates SHA256 checksums
- Outputs to `build/` directory

**Output:**
```
build/
├── onemcp-darwin-arm64.tar.gz
├── onemcp-darwin-amd64.tar.gz
├── onemcp-linux-amd64.tar.gz
── onemcp-linux-386.tar.gz
├── onemcp-windows-amd64.zip
├── onemcp-windows-386.zip
└── checksums.txt
```

**Verify build:**
```bash
# Check files exist
ls -lh build/

# Verify checksums
cat build/checksums.txt
```

---

### 3. Create Git Tag

Tag the release and push to GitHub:

```bash
# From repository root
git tag -a cli-v0.0.5 -m "Release cli-v0.0.5

Features:
- Feature 1
- Feature 2

Fixes:
- Bug fix 1
- Bug fix 2
"

git push origin cli-v0.0.5
```

---

### 4. Create GitHub Release

**Manual process:**

1. Go to https://github.com/Gentoro-OneMCP/onemcp/releases/new?tag=cli-v0.0.5
2. **Title:** `CLI v0.0.5`
3. **Description:** See template below
4. **Upload binaries:**
   - Drag all files from `packages/go-cli/build/` folder
   - Include: 5 archives (.tar.gz and .zip files)
   - Include: `checksums.txt`
5. ☑ **Set as latest release**
6. **Publish release**

**Release description template:**
```markdown
## Installation

### Homebrew (macOS/Linux)
```bash
brew upgrade onemcp
# or
brew install onemcp
```

### Scoop (Windows)
```powershell
scoop update onemcp
# or
scoop install onemcp
```

### Manual Download
Download the appropriate binary below for your platform.

## What's New

### Features
- Feature description 1
- Feature description 2

### Improvements
- Improvement 1
- Improvement 2

### Bug Fixes
- Fix description 1
- Fix description 2

## Checksums

See `checksums.txt` in the release assets.

## Full Changelog
https://github.com/Gentoro-OneMCP/onemcp/compare/cli-v0.0.4...cli-v0.0.5
```

---

### 5. Update Homebrew Formula

**Repository:** https://github.com/Gentoro-OneMCP/homebrew-onemcp

```bash
# Clone or pull latest
git clone https://github.com/Gentoro-OneMCP/homebrew-onemcp.git
cd homebrew-onemcp

# Or if already cloned
cd homebrew-onemcp
git pull origin main
```

**Edit `Formula/onemcp.rb`:**

1. Update version number:
   ```ruby
   version "0.0.5"  # Remove cli-v prefix
   ```

2. Update URLs (3 platforms):
   ```ruby
   url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/cli-v0.0.5/onemcp-darwin-arm64.tar.gz"
   url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/cli-v0.0.5/onemcp-darwin-amd64.tar.gz"
   url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/cli-v0.0.5/onemcp-linux-amd64.tar.gz"
   ```

3. Update SHA256 checksums (from `build/checksums.txt`):
   ```ruby
   sha256 "darwin-arm64-checksum-here"
   sha256 "darwin-amd64-checksum-here"
   sha256 "linux-amd64-checksum-here"
   ```

**Commit and push:**
```bash
git add Formula/onemcp.rb
git commit -m "Update formula to cli-v0.0.5"
git push origin main
```

**Test Homebrew installation:**
```bash
# Uninstall current version
brew uninstall onemcp

# Reinstall to test
brew update
brew install onemcp

# Verify version
onemcp --help
```

---

### 6. Update Scoop Manifest

**Repository:** https://github.com/Gentoro-OneMCP/onemcp-scoop

```bash
# Clone or pull latest
git clone https://github.com/Gentoro-OneMCP/onemcp-scoop.git
cd onemcp-scoop

# Or if already cloned
cd onemcp-scoop
git pull origin main
```

**Edit `bucket/onemcp.json`:**

1. Update version:
   ```json
   "version": "0.0.5"
   ```

2. Update URLs (Windows only):
   ```json
   "url": "https://github.com/Gentoro-OneMCP/onemcp/releases/download/cli-v0.0.5/onemcp-windows-amd64.zip"
   "url": "https://github.com/Gentoro-OneMCP/onemcp/releases/download/cli-v0.0.5/onemcp-windows-arm64.zip"
   ```

3. Update checksums (from `build/checksums.txt`):
   ```json
   "hash": "windows-amd64-checksum-here"
   "hash": "windows-386-checksum-here"
   ```

**Commit and push:**
```bash
git add bucket/onemcp.json
git commit -m "Update manifest to cli-v0.0.5"
git push origin main
```

---

### 7. Verify Installation

Test installation on each platform:

**macOS (Homebrew):**
```bash
brew update
brew upgrade onemcp
onemcp --help  # Should show new version
```

**Windows (Scoop):**
```powershell
scoop update
scoop update onemcp
onemcp --help
```

**Manual download:**
- Download from GitHub release
- Extract and verify it works

---

### 8. Announce Release

**Optional but recommended:**

1. Update main repository README if needed
2. Tweet/announce on social media
3. Update documentation site if anything changed
4. Notify team in Slack/Discord

---

## Troubleshooting

### Build Fails

```bash
# Clean and rebuild
cd packages/cli
make clean
make build-release VERSION=cli-0.0.5
```

### Homebrew Checksum Mismatch

**Cause:** Wrong checksum or URL

**Fix:**
1. Double-check checksums from `build/checksums.txt`
2. Ensure URLs point to correct release tag
3. Update formula and push again

### Scoop Checksum Mismatch

Same as Homebrew - verify checksums and URLs.

### Wrong Files Uploaded to GitHub

**Fix:**
1. Delete the release (if not yet published widely)
2. Re-upload correct files from `build/` directory
3. Update Homebrew/Scoop with new checksums if needed

---

## Release Checklist

Use this checklist for each release:

- [ ] All changes merged to `main`
- [ ] Tests passing locally
- [ ] Version decided (cli-v0.0.X)
- [ ] Built binaries with `build-release.sh`
- [ ] Verified all 5 archives created
- [ ] Git tag created and pushed
- [ ] GitHub release created with all assets
- [ ] Homebrew formula updated (version, URLs, checksums)
- [ ] Homebrew formula tested
- [ ] Scoop manifest updated (version, URLs, checksums)
- [ ] Verified installation works on at least one platform
- [ ] Announcement prepared (optional)

---

## Version History

| Version | Date | Notes |
|---------|------|-------|
| cli-v0.0.3 | 2025-12-03 | Dev mode, logging improvements |
| cli-v0.0.2 | 2025-11-XX | Connection fix, versioning |
| cli-v0.0.1 | 2025-11-XX | Initial release |

---

## Future Automation

**Potential improvements:**

1. **GitHub Actions workflow** to build on tag push
2. **Automated Homebrew PR** via `brew bump-formula-pr`
3. **Automated Scoop update** via pull request
4. **Release notes generation** from commits

For now, manual process works well and gives full control.

---

## Questions?

If you run into issues during the release process, check:

1. Build script output for errors
2. GitHub release page for asset upload issues
3. Homebrew/Scoop logs for installation errors
4. Ask team for help if needed
