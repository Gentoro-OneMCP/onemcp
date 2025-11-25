#!/usr/bin/env bash

# build-base-universal.sh
# Works with Buildah, Podman, and GitHub Actions
# Usage:
#   ./build-base-universal.sh latest --push
#   ./build-base-universal.sh latest

set -euo pipefail

VERSION="${1:-latest}"
PUSH_FLAG="${2:-}"
PLATFORM_FLAG="${3:-}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOCKERFILE="$ROOT_DIR/scripts/docker/Dockerfile.base"

DEFAULT_PLATFORMS=(
    linux/amd64
    linux/arm64
)

# Default multi-arch platforms
PLATFORMS="${DOCKER_PLATFORMS:-$(IFS=,; echo "${DEFAULT_PLATFORMS[*]}")}"

# Override platform if provided
if [[ "$PLATFORM_FLAG" == "--platform" && -n "${4:-}" ]]; then
  PLATFORMS="${4}"
fi

# Validate Dockerfile
if [[ ! -f "$DOCKERFILE" ]]; then
  echo "Dockerfile not found: $DOCKERFILE"
  exit 1
fi

# Detect container runtime (Buildah vs Podman)
if command -v buildah &> /dev/null; then
    RUNTIME="buildah"
    echo "Using Buildah runtime"
elif command -v podman &> /dev/null; then
    RUNTIME="podman"
    echo "Using Podman runtime (Buildah compatible)"
else
    echo "Error: Neither Buildah nor Podman is available"
    exit 1
fi

echo "Building base image with version: $VERSION"
echo "Dockerfile: $DOCKERFILE"
echo "Platforms: $PLATFORMS"
echo "Runtime: $RUNTIME"

IMAGE_NAME="admingentoro/gentoro:base-$VERSION"
IMAGE_LATEST="admingentoro/gentoro:base-latest"

# Set storage driver for non-Linux environments (macOS, CI)
if [[ "$(uname)" != "Linux" ]] || [[ -n "${CI:-}" ]]; then
    export STORAGE_DRIVER="vfs"
    echo "Using VFS storage driver for compatibility"
fi

########################################
# PUSH MODE → full multi-arch build    #
########################################
if [[ "$PUSH_FLAG" == "--push" ]]; then
  echo "Multi-arch push mode"

  # Convert comma-separated platforms to array
  IFS=',' read -ra PLATFORM_ARRAY <<< "$PLATFORMS"

  MANIFEST_NAME="gentoro-base-${VERSION}-manifest"
  
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

    TMP_TAG="admingentoro/gentoro-base-${VERSION}-$(echo "$PLATFORM" | tr '/' '-')"

    if [[ "$RUNTIME" == "buildah" ]]; then
      buildah bud \
        --no-cache \
        --platform "$PLATFORM" \
        -f "$DOCKERFILE" \
        -t "$TMP_TAG" \
        "$ROOT_DIR"

      buildah push "$TMP_TAG" "docker://$TMP_TAG"
      buildah manifest add "$MANIFEST_NAME" "$TMP_TAG"
    else
      podman build \
        --no-cache \
        --platform "$PLATFORM" \
        -f "$DOCKERFILE" \
        -t "$TMP_TAG" \
        "$ROOT_DIR"

      podman push "$TMP_TAG" "docker://$TMP_TAG"
      podman manifest add "$MANIFEST_NAME" "$TMP_TAG"
    fi
  done

  echo "Pushing multi-arch manifest to Docker Hub"
  if [[ "$RUNTIME" == "buildah" ]]; then
    buildah manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_NAME"
    buildah manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_LATEST"
  else
    podman manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_NAME"
    podman manifest push --all "$MANIFEST_NAME" "docker://$IMAGE_LATEST"
  fi

  echo "✅ Successfully pushed multi-arch image: $IMAGE_NAME"
  exit 0
fi

########################################
# LOCAL MODE → single-platform build   #
########################################
echo "Local build mode (single platform)"

# Auto-select host architecture unless user specified a platform
if [[ "$PLATFORM_FLAG" != "--platform" ]]; then
  HOST_ARCH=$(uname -m)
  case "$HOST_ARCH" in
    x86_64) HOST_ARCH=amd64 ;;
    arm64|aarch64) HOST_ARCH=arm64 ;;
    *) echo "Unsupported host architecture: $HOST_ARCH"; exit 1 ;;
  esac
  PLATFORMS="linux/$HOST_ARCH"
  echo "Default local build platform: $PLATFORMS"
fi

if [[ "$RUNTIME" == "buildah" ]]; then
  buildah bud \
    --no-cache \
    --platform "$PLATFORMS" \
    -f "$DOCKERFILE" \
    -t "$IMAGE_NAME" \
    -t "$IMAGE_LATEST" \
    "$ROOT_DIR"
else
  podman build \
    --no-cache \
    --platform "$PLATFORMS" \
    -f "$DOCKERFILE" \
    -t "$IMAGE_NAME" \
    -t "$IMAGE_LATEST" \
    "$ROOT_DIR"
fi

echo "✅ Successfully built local image: $IMAGE_NAME ($PLATFORMS)"
