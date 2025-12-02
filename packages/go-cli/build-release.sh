#!/bin/bash
# Build script for OneMCP CLI releases

set -e

VERSION=${1:-cli-v0.0.1}
BUILD_DIR="build"

echo "Building OneMCP CLI ${VERSION}"
echo ""

# Extract semantic version (strip cli-v prefix)
SEMVER=${VERSION#cli-v}

# Build flags to inject version
LDFLAGS="-X github.com/gentoro/onemcp/go-cli/pkg/version.Version=${SEMVER}"

# Clean previous builds
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}

# Build for Mac Apple Silicon (M1/M2/M3)
echo "Building for macOS ARM64..."
GOOS=darwin GOARCH=arm64 go build -ldflags "${LDFLAGS}" -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-darwin-arm64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

# Build for Mac Intel
echo "Building for macOS AMD64..."
GOOS=darwin GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-darwin-amd64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

# Build for Linux
echo "Building for Linux AMD64..."
GOOS=linux GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-linux-amd64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

# Build for Windows
echo "Building for Windows AMD64..."
GOOS=windows GOARCH=amd64 go build -ldflags "${LDFLAGS}" -o ${BUILD_DIR}/onemcp.exe main.go
cd ${BUILD_DIR} && zip -q onemcp-windows-amd64.zip onemcp.exe && cd ..
rm ${BUILD_DIR}/onemcp.exe

echo "Building for Windows ARM64..."
GOOS=windows GOARCH=arm64 go build -ldflags "${LDFLAGS}" -o ${BUILD_DIR}/onemcp.exe main.go
cd ${BUILD_DIR} && zip -q onemcp-windows-arm64.zip onemcp.exe && cd ..
rm ${BUILD_DIR}/onemcp.exe

echo ""
echo "✅ Build complete!"
echo ""
echo "Archives created:"
ls -lh ${BUILD_DIR}/*.tar.gz ${BUILD_DIR}/*.zip 2>/dev/null || ls -lh ${BUILD_DIR}/*
echo ""

# Calculate checksums
echo "SHA256 Checksums:"
shasum -a 256 ${BUILD_DIR}/*.tar.gz ${BUILD_DIR}/*.zip | tee ${BUILD_DIR}/checksums.txt
echo ""
echo "Checksums saved to ${BUILD_DIR}/checksums.txt"
echo ""

echo "Next steps:"
echo "1. Go to: https://github.com/Gentoro-OneMCP/onemcp/releases/new?tag=${VERSION}"
echo "2. Upload files from ${BUILD_DIR}/"
echo "3. Update Homebrew formula with checksums from ${BUILD_DIR}/checksums.txt"
echo "4. Update Scoop manifest with Windows checksums"
