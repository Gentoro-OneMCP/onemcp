#!/usr/bin/env bash

# examples:
#  ./build-product.sh latest --push
#  ./build-product.sh --push latest
#  ./build-product.sh latest myapp.jar --push
#  ./build-product.sh --platform linux/amd64 latest --push

set -euo pipefail

# Initialize variables with defaults
VERSION="dev"
JAR_NAME=""
PUSH_FLAG=""
PLATFORM_FLAG=""
PLATFORM_VALUE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --push)
      PUSH_FLAG="--push"
      shift
      ;;
    --platform)
      PLATFORM_FLAG="--platform"
      PLATFORM_VALUE="${2:-}"
      shift 2
      ;;
    -*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      # First non-option argument is VERSION, second is JAR_NAME
      if [[ -z "$VERSION" || "$VERSION" == "dev" ]]; then
        VERSION="$1"
      elif [[ -z "$JAR_NAME" ]]; then
        JAR_NAME="$1"
      fi
      shift
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
POM="$ROOT_DIR/packages/server/pom.xml"

DEFAULT_PLATFORMS=(
    linux/amd64
    linux/arm64
)

# Default platforms for multi-arch builds
PLATFORMS="${DOCKER_PLATFORMS:-$(IFS=,; echo "${DEFAULT_PLATFORMS[*]}")}"

# Handle platform flag
if [[ -n "$PLATFORM_VALUE" ]]; then
  PLATFORMS="$PLATFORM_VALUE"
fi

# Get version from POM if JAR_NAME not provided
if [[ -z "$JAR_NAME" ]]; then
  POM_VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" "$POM" 2>/dev/null || true)
  if [[ -z "$POM_VERSION" ]]; then
    echo "Cannot read version from $POM"; exit 1
  fi
  JAR_NAME="onemcp-$POM_VERSION-jar-with-dependencies.jar"

  # Build app JAR if not already built
  echo "Building application JAR..."
  ( cd "$ROOT_DIR/packages/server" && ./mvnw -q -DskipTests package )
fi

# Validate JAR exists
JAR_PATH="$ROOT_DIR/packages/server/target/$JAR_NAME"
if [[ ! -f "$JAR_PATH" ]]; then
  echo "JAR file not found: $JAR_PATH"
  exit 1
fi

# Validate JAR has proper Spring Boot manifest
if ! unzip -p "$JAR_PATH" META-INF/MANIFEST.MF | grep -q "Main-Class: com.gentoro.onemcp.OneMcpApp"; then
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

# Use consistent Dockerfile path for local and push 
DOCKERFILE_PATH="$ROOT_DIR/scripts/docker/Dockerfile"
if [[ ! -f "$DOCKERFILE_PATH" ]]; then
  echo "Dockerfile not found at: $DOCKERFILE_PATH"
  exit 1
fi

# Determine build command based on whether we're pushing
if [[ "$PUSH_FLAG" == "--push" ]]; then
  echo "Will push images to registry"
  BUILD_CMD="docker buildx build --no-cache"
  BUILD_ARGS=(
    -f "$DOCKERFILE_PATH"
    --platform "$PLATFORMS"
    --build-arg "APP_JAR=packages/server/target/$JAR_NAME"
    --build-arg "BASE_IMAGE=admingentoro/gentoro:base-$VERSION"
    -t "admingentoro/gentoro:$VERSION"
    -t "admingentoro/gentoro:latest"
    --push
    "$ROOT_DIR"
  )
else
  echo "Building for local use"
  # For local builds, use docker buildx with --load to access local images
  # Don't override PLATFORMS if it was set via --platform parameter
  if [[ -z "$PLATFORM_VALUE" ]]; then
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
    -f "$DOCKERFILE_PATH"
    --build-arg "APP_JAR=packages/server/target/$JAR_NAME"
    --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME"
    -t "admingentoro/gentoro:$VERSION"
    -t "admingentoro/gentoro:latest"
    --platform "$PLATFORMS"
    "$ROOT_DIR"
  )
fi

echo "Running: $BUILD_CMD ${BUILD_ARGS[*]}"

# Build Docker image
if ! $BUILD_CMD "${BUILD_ARGS[@]}"; then
  echo "Failed to build product image"
  exit 1
fi

echo "Successfully built admingentoro/gentoro:$VERSION for platforms: $PLATFORMS"
