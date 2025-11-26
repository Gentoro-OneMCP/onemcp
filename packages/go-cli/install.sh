#!/bin/bash
# OneMCP CLI Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/go-cli/install.sh | bash

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REPO="Gentoro-OneMCP/onemcp"
INSTALL_DIR="/usr/local/bin"
BINARY_NAME="onemcp"

echo -e "${GREEN}OneMCP CLI Installer${NC}"
echo ""

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
  darwin)
    OS="darwin"
    ;;
  linux)
    OS="linux"
    ;;
  *)
    echo -e "${RED}Unsupported operating system: $OS${NC}"
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64)
    ARCH="amd64"
    ;;
  arm64|aarch64)
    ARCH="arm64"
    ;;
  *)
    echo -e "${RED}Unsupported architecture: $ARCH${NC}"
    exit 1
    ;;
esac

PLATFORM="${OS}-${ARCH}"
echo "Detected platform: ${PLATFORM}"

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}Warning: Docker not found${NC}"
    echo "OneMCP requires Docker to run. Please install Docker:"
    echo "  macOS: https://docs.docker.com/desktop/install/mac-install/"
    echo "  Linux: https://docs.docker.com/engine/install/"
    echo ""
    read -p "Continue installation anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Get latest release
echo "Fetching latest release..."
LATEST_VERSION=$(curl -s https://api.github.com/repos/${REPO}/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$LATEST_VERSION" ]; then
    echo -e "${RED}Failed to fetch latest version${NC}"
    exit 1
fi

echo "Latest version: ${LATEST_VERSION}"

# Download URL
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${LATEST_VERSION}/onemcp-${PLATFORM}.tar.gz"
TEMP_DIR=$(mktemp -d)
TEMP_FILE="${TEMP_DIR}/onemcp.tar.gz"

echo "Downloading from ${DOWNLOAD_URL}..."
if ! curl -fsSL -o "${TEMP_FILE}" "${DOWNLOAD_URL}"; then
    echo -e "${RED}Failed to download${NC}"
    rm -rf "${TEMP_DIR}"
    exit 1
fi

# Extract
echo "Extracting..."
tar -xzf "${TEMP_FILE}" -C "${TEMP_DIR}"

# Check if we need sudo for installation
if [ -w "$INSTALL_DIR" ]; then
    SUDO=""
else
    SUDO="sudo"
    echo "Installation requires sudo privileges..."
fi

# Install
echo "Installing to ${INSTALL_DIR}..."
$SUDO mv "${TEMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
$SUDO chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

# Cleanup
rm -rf "${TEMP_DIR}"

# Verify installation
if command -v onemcp &> /dev/null; then
    echo ""
    echo -e "${GREEN}✅ OneMCP CLI installed successfully!${NC}"
    echo ""
    echo "Get started:"
    echo "  onemcp chat"
    echo ""
    echo "View all commands:"
    echo "  onemcp --help"
else
    echo -e "${RED}Installation failed${NC}"
    exit 1
fi
