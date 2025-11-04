#!/usr/bin/env bash
set -euo pipefail

# Simple tag-based release script
# Usage: ./scripts/release.sh [patch|minor|major]

VERSION_TYPE="${1:-patch}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Get the latest tag
LATEST_TAG=$(git tag -l | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)

if [[ -z "$LATEST_TAG" ]]; then
  echo "❌ No existing version tags found"
  exit 1
fi

echo "Latest tag: $LATEST_TAG"

# Parse version
VERSION_PART=${LATEST_TAG#v}
IFS='.' read -r MAJ MIN PAT <<<"$VERSION_PART"

# Calculate new version
case "$VERSION_TYPE" in
  major)
    MAJ=$((MAJ+1)); MIN=0; PAT=0
    ;;
  minor)
    MIN=$((MIN+1)); PAT=0
    ;;
  patch)
    PAT=$((PAT+1))
    ;;
  *)
    echo "❌ Invalid version type: $VERSION_TYPE"
    echo "Usage: $0 [patch|minor|major]"
    exit 1
    ;;
esac

NEW_VERSION="v${MAJ}.${MIN}.${PAT}"
NEXT_SNAPSHOT="${MAJ}.${MIN}.$((PAT+1))-SNAPSHOT"

echo "New version: $NEW_VERSION"
echo "Next development version: $NEXT_SNAPSHOT"

# Confirm release
read -p "Create release $NEW_VERSION? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Release cancelled"
  exit 0
fi

# Create and push tag
echo "Creating tag $NEW_VERSION..."
git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION"
git push origin "$NEW_VERSION"

echo "✅ Tag $NEW_VERSION created and pushed"
echo "🚀 Release workflow will now:"
echo "   - Build and push Docker images"
echo "   - Create GitHub release"
echo "   - Deploy documentation"
echo "   - Update POM version to $NEW_VERSION"

echo ""
echo "To update to next development version:"
echo "  mvn -f src/onemcp/pom.xml versions:set -DnewVersion=\"$NEXT_SNAPSHOT\" -DgenerateBackupPoms=false"
echo "  git add src/onemcp/pom.xml"
echo "  git commit -m \"chore: bump version to $NEXT_SNAPSHOT\""
echo "  git push origin main"