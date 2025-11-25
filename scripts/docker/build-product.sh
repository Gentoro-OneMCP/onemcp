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

# Detect container runtime (Buildah vs Podman)
if command -v buildah &> /dev/null; then
    RUNTIME="buildah"
    echo "Using Buildah runtime"
elif command -v podman &> /dev/null; then
    RUNTIME="podman"
    echo "Using Podman runtime"
else
    echo "Error: Neither Buildah nor Podman is available"
    exit 1
fi

# Set storage driver for non-Linux environments
if [[ "$(uname)" != "Linux" ]] || [[ -n "${CI:-}" ]]; then
    export STORAGE_DRIVER="vfs"
    echo "Using VFS storage driver for compatibility"
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
echo "Runtime: $RUNTIME"

# Use consistent Dockerfile path for local and push
DOCKERFILE_PATH="$ROOT_DIR/scripts/docker/Dockerfile"
if [[ ! -f "$DOCKERFILE_PATH" ]]; then
  echo "Dockerfile not found at: $DOCKERFILE_PATH"
  exit 1
fi

IMAGE_NAME="admingentoro/gentoro:$VERSION"
IMAGE_LATEST="admingentoro/gentoro:latest"
BASE_IMAGE_NAME="admingentoro/gentoro:base-$VERSION"

# Check authentication before pushing
check_auth() {
    local registry="docker.io"
    echo "Checking authentication to $registry..."
    
    if [[ "$RUNTIME" == "buildah" ]]; then
        if ! buildah login --get-login "$registry" &> /dev/null; then
            echo "❌ Not logged in to $registry. Please run:"
            echo "   buildah login $registry"
            return 1
        fi
    else
        if ! podman login --get-login "$registry" &> /dev/null; then
            echo "❌ Not logged in to $registry. Please run:"
            echo "   podman login $registry"
            return 1
        fi
    fi
    echo "✅ Authenticated to $registry"
    return 0
}

########################################
# PUSH MODE → full multi-arch build    #
########################################
if [[ "$PUSH_FLAG" == "--push" ]]; then
  echo "Multi-arch push mode"
  
  # Check authentication before proceeding
  if ! check_auth; then
    exit 1
  fi

  # Convert comma-separated platforms to array
  IFS=',' read -ra PLATFORM_ARRAY <<< "$PLATFORMS"

  MANIFEST_NAME="gentoro-product-${VERSION}-manifest"
  
  # Remove existing manifest if it exists
  if [[ "$RUNTIME" == "buildah" ]]; then
    buildah manifest rm "$MANIFEST_NAME" 2>/dev/null || true
    buildah manifest create "$MANIFEST_NAME"
  else
    podman manifest rm "$MANIFEST_NAME" 2>/dev/null || true
    podman manifest create "$MANIFEST_NAME"
  fi

  for PLATFORM in "${PLATFORM_ARRAY[@]}"; do
    echo "Building $PLATFORM"

    # Include your Docker Hub username in the temporary tag
    TMP_TAG="admingentoro/gentoro-product-${VERSION}-$(echo "$PLATFORM" | tr '/' '-')"

    if [[ "$RUNTIME" == "buildah" ]]; then
      buildah bud \
        --no-cache \
        --platform "$PLATFORM" \
        -f "$DOCKERFILE_PATH" \
        --build-arg "APP_JAR=packages/server/target/$JAR_NAME" \
        --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME" \
        -t "$TMP_TAG" \
        "$ROOT_DIR"

      # Push directly to Docker Hub with your namespace
      buildah push "$TMP_TAG" "docker://$TMP_TAG"
      buildah manifest add "$MANIFEST_NAME" "docker://$TMP_TAG"
      
      # Clean up temporary image
      buildah rmi "$TMP_TAG" 2>/dev/null || true
    else
      podman build \
        --no-cache \
        --platform "$PLATFORM" \
        -f "$DOCKERFILE_PATH" \
        --build-arg "APP_JAR=packages/server/target/$JAR_NAME" \
        --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME" \
        -t "$TMP_TAG" \
        "$ROOT_DIR"

      # Push directly to Docker Hub with your namespace
      podman push "$TMP_TAG" "docker://$TMP_TAG"
      podman manifest add "$MANIFEST_NAME" "docker://$TMP_TAG"
      
      # Clean up temporary image
      podman rmi "$TMP_TAG" 2>/dev/null || true
    fi
  done

  echo "Pushing multi-arch manifest to Docker Hub"
  if [[ "$RUNTIME" == "buildah" ]]; then
    buildah manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_NAME"
    buildah manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_LATEST"
    
    # Clean up manifest
    buildah manifest rm "$MANIFEST_NAME" 2>/dev/null || true
  else
    podman manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_NAME"
    podman manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_LATEST"
    
    # Clean up manifest
    podman manifest rm "$MANIFEST_NAME" 2>/dev/null || true
  fi

  echo "✅ Successfully pushed multi-arch product image: $IMAGE_NAME"
  exit 0
fi

########################################
# LOCAL MODE → single-platform build   #
########################################
echo "Local build mode (single platform)"

# Auto-select host architecture unless user specified a platform
if [[ -z "$PLATFORM_VALUE" ]]; then
  HOST_ARCH=$(uname -m)
  case "$HOST_ARCH" in
    x86_64) HOST_ARCH=amd64 ;;
    arm64|aarch64) HOST_ARCH=arm64 ;;
    *) echo "Unsupported host architecture: $HOST_ARCH"; exit 1 ;;
  esac
  PLATFORMS="linux/$HOST_ARCH"
  echo "Default local build platform: $PLATFORMS"
fi

# Check if base image exists locally - try both versioned and latest
if [[ "$RUNTIME" == "buildah" ]]; then
  if ! buildah images | grep -q "admingentoro/gentoro.*base-$VERSION"; then
    echo "⚠️  Base image $BASE_IMAGE_NAME not found, trying base-latest..."
    if buildah images | grep -q "admingentoro/gentoro.*base-latest"; then
      BASE_IMAGE_NAME="admingentoro/gentoro:base-latest"
      echo "✅ Using admingentoro/gentoro:base-latest"
    else
      echo "❌ No base image found locally"
      echo "Available base images:"
      buildah images | grep "admingentoro/gentoro.*base" || echo "No base images found"
      exit 1
    fi
  else
    echo "✅ Base image $BASE_IMAGE_NAME found"
  fi
else
  if ! podman image exists "$BASE_IMAGE_NAME"; then
    echo "⚠️  Base image $BASE_IMAGE_NAME not found, trying base-latest..."
    if podman image exists "admingentoro/gentoro:base-latest"; then
      BASE_IMAGE_NAME="admingentoro/gentoro:base-latest"
      echo "✅ Using admingentoro/gentoro:base-latest"
    else
      echo "❌ No base image found locally"
      echo "Available base images:"
      podman images | grep "admingentoro/gentoro.*base" || echo "No base images found"
      exit 1
    fi
  fi
fi

if [[ "$RUNTIME" == "buildah" ]]; then
  buildah bud \
    --no-cache \
    --platform "$PLATFORMS" \
    -f "$DOCKERFILE_PATH" \
    --build-arg "APP_JAR=packages/server/target/$JAR_NAME" \
    --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME" \
    -t "$IMAGE_NAME" \
    -t "$IMAGE_LATEST" \
    "$ROOT_DIR"
else
  podman build \
    --no-cache \
    --platform "$PLATFORMS" \
    -f "$DOCKERFILE_PATH" \
    --build-arg "APP_JAR=packages/server/target/$JAR_NAME" \
    --build-arg "BASE_IMAGE=$BASE_IMAGE_NAME" \
    -t "$IMAGE_NAME" \
    -t "$IMAGE_LATEST" \
    "$ROOT_DIR"
fi

echo "✅ Successfully built local product image: $IMAGE_NAME ($PLATFORMS)"
