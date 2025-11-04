# Simple tag-based release script for PowerShell
# Usage: .\scripts\release.ps1 [patch|minor|major]

param(
    [string]$VersionType = "patch"
)

$ErrorActionPreference = "Stop"

# Get the latest tag
$LatestTag = git tag -l | Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } | Sort-Object { [version]($_ -replace '^v', '') } | Select-Object -Last 1

if (-not $LatestTag) {
    Write-Host "❌ No existing version tags found" -ForegroundColor Red
    exit 1
}

Write-Host "Latest tag: $LatestTag" -ForegroundColor Green

# Parse version
$VersionPart = $LatestTag -replace '^v', ''
$VersionComponents = $VersionPart -split '\.'
$MAJ = [int]$VersionComponents[0]
$MIN = [int]$VersionComponents[1]
$PAT = [int]$VersionComponents[2]

# Calculate new version
switch ($VersionType.ToLower()) {
    "major" {
        $MAJ++
        $MIN = 0
        $PAT = 0
    }
    "minor" {
        $MIN++
        $PAT = 0
    }
    "patch" {
        $PAT++
    }
    default {
        Write-Host "❌ Invalid version type: $VersionType" -ForegroundColor Red
        Write-Host "Usage: .\scripts\release.ps1 [patch|minor|major]"
        exit 1
    }
}

$NewVersion = "v$MAJ.$MIN.$PAT"
$NextSnapshot = "$MAJ.$MIN.$($PAT + 1)-SNAPSHOT"

Write-Host "New version: $NewVersion" -ForegroundColor Cyan
Write-Host "Next development version: $NextSnapshot" -ForegroundColor Cyan

# Confirm release
$Confirmation = Read-Host "Create release $NewVersion? (y/N)"
if ($Confirmation -notmatch '^[Yy]$') {
    Write-Host "Release cancelled" -ForegroundColor Yellow
    exit 0
}

# Create and push tag
Write-Host "Creating tag $NewVersion..." -ForegroundColor Blue
git tag -a $NewVersion -m "Release $NewVersion"
git push origin $NewVersion

Write-Host "✅ Tag $NewVersion created and pushed" -ForegroundColor Green
Write-Host "🚀 Release workflow will now:" -ForegroundColor Blue
Write-Host "   - Build and push Docker images" -ForegroundColor White
Write-Host "   - Create GitHub release" -ForegroundColor White
Write-Host "   - Deploy documentation" -ForegroundColor White
Write-Host "   - Update POM version to $NewVersion" -ForegroundColor White

Write-Host ""
Write-Host "To update to next development version:" -ForegroundColor Yellow
Write-Host "  mvn -f src/onemcp/pom.xml versions:set -DnewVersion=`"$NextSnapshot`" -DgenerateBackupPoms=false" -ForegroundColor Gray
Write-Host "  git add src/onemcp/pom.xml" -ForegroundColor Gray
Write-Host "  git commit -m `"chore: bump version to $NextSnapshot`"" -ForegroundColor Gray
Write-Host "  git push origin main" -ForegroundColor Gray