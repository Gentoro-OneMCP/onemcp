#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-dev}"
JAR_NAME="${2:-}"
PUSH_FLAG="${3:-}"
PLATFORM_FLAG="${4:-}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
POM="$ROOT_DIR/src/onemcp/pom.xml"

# Default platforms for multi-arch builds
PLATFORMS="${DOCKER_PLATFORMS:-linux/amd64,linux/arm64}"

# Handle platform flag
if [[ "$PLATFORM_FLAG" == "--platform" && -n "${5:-}" ]]; then
  PLATFORMS="${5}"
fi

# Get version from POM if JAR_NAME not provided
if [[ -z "$JAR_NAME" ]]; then
  POM_VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" "$POM" 2>/dev/null || true)
  if [[ -z "$POM_VERSION" ]]; then
    echo "Cannot read version from $POM"; exit 1
  fi
  JAR_NAME="onemcp-$POM_VERSION.jar"

  # Build app JAR if not already built
  echo "Building application JAR..."
  ( cd "$ROOT_DIR/src/onemcp" && ./mvnw -q -DskipTests package spring-boot:repackage || mvn -q -DskipTests package spring-boot:repackage )
  
  # Build TypeScript runtime
  echo "Building TypeScript runtime..."
  ( cd "$ROOT_DIR/src/typescript-runtime" && npm install --legacy-peer-deps && npm run build )
  
  # Build mock server (ACME Analytics Server)
  echo "Building ACME Analytics Server..."
  ( cd "$ROOT_DIR/src/acme-analytics-server/server" && ./build.sh )
fi

# Validate JAR exists
JAR_PATH="$ROOT_DIR/src/onemcp/target/$JAR_NAME"
if [[ ! -f "$JAR_PATH" ]]; then
  echo "JAR file not found: $JAR_PATH"
  exit 1
fi

# Validate JAR has proper Spring Boot manifest
if ! unzip -p "$JAR_PATH" META-INF/MANIFEST.MF | grep -q "Main-Class: org.springframework.boot.loader.launch.JarLauncher"; then
  echo "ERROR: JAR file is missing Spring Boot Main-Class manifest attribute"
  echo "This usually means spring-boot:repackage was not run properly"
  echo "JAR manifest:"
  unzip -p "$JAR_PATH" META-INF/MANIFEST.MF
  exit 1
fi

echo "✅ JAR validation passed - Spring Boot fat JAR with proper manifest"

echo "Building product image with version: $VERSION"
echo "JAR: $JAR_NAME"
echo "Platforms: $PLATFORMS"

# Determine build command based on whether we're pushing
if [[ "$PUSH_FLAG" == "--push" ]]; then
  echo "Will push images to registry"
  BUILD_CMD="docker buildx build"
  BUILD_ARGS=(
    -f "$ROOT_DIR/Dockerfile"
    --platform "$PLATFORMS"
    --build-arg "APP_JAR=src/onemcp/target/$JAR_NAME"
    --build-arg "BASE_IMAGE=admingentoro/gentoro:base-$VERSION"
    -t "admingentoro/gentoro:$VERSION"
    -t "admingentoro/gentoro:latest"
    --push
  )
else
  echo "Building for local use (docker buildx with --load)"
  # For local builds, use docker buildx with --load to access local images
  # Don't override PLATFORMS if it was set via --platform parameter
  if [[ "$PLATFORM_FLAG" != "--platform" ]]; then
    PLATFORMS="linux/amd64"
  fi
  
  # Check if base image exists locally - try both versioned and latest
  BASE_IMAGE_NAME="admingentoro/gentoro:base-$VERSION"
  if ! docker image inspect "$BASE_IMAGE_NAME" >/dev/null 2>&1; then
    echo "⚠️  Base image $BASE_IMAGE_NAME not found, trying base-latest..."
    if docker image inspect "admingentoro/gentoro:base-latest" >/dev/null 2>&1; then
      BASE_IMAGE_NAME="admingentoro/gentoro:base-latest"
      echo "✅ Using admingentoro/gentoro:base-latest"
    else
      echo "❌ No base image found locally"
      echo "Available base images:"
      docker images | grep "admingentoro/gentoro.*base" || echo "No base images found"
      exit 1
    fi
  fi
  
  BUILD_CMD="docker build"
  BUILD_ARGS=(
    -f "$ROOT_DIR/Dockerfile"
    --build-arg "APP_JAR=src/onemcp/target/$JAR_NAME"
    --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME"
    -t "admingentoro/gentoro:$VERSION"
    -t "admingentoro/gentoro:latest"
    --platform "$PLATFORMS"
  )
fi

# Build Docker image
if ! $BUILD_CMD "${BUILD_ARGS[@]}" "$ROOT_DIR"; then
  echo "Failed to build product image"
  exit 1
fi

echo "Successfully built admingentoro/gentoro:$VERSION for platforms: $PLATFORMS"
