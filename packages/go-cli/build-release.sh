#!/bin/bash
# Build script for OneMCP CLI releases

set -e

VERSION=${1:-v0.1.0}
BUILD_DIR="build"

echo "Building OneMCP CLI ${VERSION}"
echo ""

# Clean previous builds
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}

# Build for Mac Apple Silicon (M1/M2/M3)
echo "Building for macOS ARM64..."
GOOS=darwin GOARCH=arm64 go build -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-darwin-arm64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

# Build for Mac Intel
echo "Building for macOS AMD64..."
GOOS=darwin GOARCH=amd64 go build -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-darwin-amd64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

# Build for Linux
echo "Building for Linux AMD64..."
GOOS=linux GOARCH=amd64 go build -o ${BUILD_DIR}/onemcp main.go
tar -czf ${BUILD_DIR}/onemcp-linux-amd64.tar.gz -C ${BUILD_DIR} onemcp
rm ${BUILD_DIR}/onemcp

echo ""
echo "✅ Build complete!"
echo ""
echo "Archives created:"
ls -lh ${BUILD_DIR}/*.tar.gz
echo ""

# Calculate checksums
echo "SHA256 Checksums:"
shasum -a 256 ${BUILD_DIR}/*.tar.gz | tee ${BUILD_DIR}/checksums.txt
echo ""
echo "Checksums saved to ${BUILD_DIR}/checksums.txt"
echo ""

echo "Next steps:"
echo "1. Go to: https://github.com/Gentoro-OneMCP/onemcp/releases/new"
echo "2. Tag: ${VERSION}"
echo "3. Upload files from ${BUILD_DIR}/"
echo "4. Update Homebrew formula with checksums from ${BUILD_DIR}/checksums.txt"
