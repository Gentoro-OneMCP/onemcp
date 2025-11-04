Param(
    [string]$Version="dev",
    [string]$JarName="",
    [switch]$Push,
    [string]$Platform=""
)

$ErrorActionPreference = "Stop"

$ROOT = (Resolve-Path "$PSScriptRoot\..\..").Path
$POM = "$ROOT\src\onemcp\pom.xml"

# Default platforms for multi-arch builds
$Platforms = if ($Platform) { $Platform } elseif ($env:DOCKER_PLATFORMS) { $env:DOCKER_PLATFORMS } else { "linux/amd64,linux/arm64" }

# Get version from POM if JarName not provided
if ([string]::IsNullOrEmpty($JarName)) {
    [xml]$xml = Get-Content $POM
    $pomVersion = $xml.project.version
    $JarName = "onemcp-$pomVersion.jar"
    
    # Build app JAR if not already built
    Write-Host "Building application JAR..."
    Push-Location "$ROOT\src\onemcp"
    if (Test-Path .\mvnw) { 
        ./mvnw -q -DskipTests package spring-boot:repackage
    } else { 
        mvn -q -DskipTests package spring-boot:repackage
    }
    Pop-Location
}

# Validate JAR exists
$jarPath = "$ROOT\src\onemcp\target\$JarName"
if (-not (Test-Path $jarPath)) {
    Write-Error "JAR file not found: $jarPath"
    exit 1
}

# Validate JAR has proper Spring Boot manifest
$manifest = & unzip -p $jarPath META-INF/MANIFEST.MF
if (-not ($manifest -match "Main-Class: org.springframework.boot.loader.launch.JarLauncher")) {
    Write-Error "JAR file is missing Spring Boot Main-Class manifest attribute"
    Write-Host "This usually means spring-boot:repackage was not run properly"
    Write-Host "JAR manifest:"
    Write-Host $manifest
    exit 1
}

Write-Host "✅ JAR validation passed - Spring Boot fat JAR with proper manifest"

Write-Host "Building product image with version: $Version"
Write-Host "JAR: $JarName"
Write-Host "Platforms: $Platforms"

# Check if base image exists locally - try both versioned and latest
$baseImageName = "admingentoro/gentoro:base-$Version"
try {
    docker image inspect $baseImageName | Out-Null
    Write-Host "✅ Using base image: $baseImageName"
} catch {
    Write-Host "⚠️  Base image $baseImageName not found, trying base-latest..."
    try {
        docker image inspect "admingentoro/gentoro:base-latest" | Out-Null
        $baseImageName = "admingentoro/gentoro:base-latest"
        Write-Host "✅ Using admingentoro/gentoro:base-latest"
    } catch {
        Write-Error "❌ No base image found locally"
        Write-Host "Available base images:"
        docker images | Select-String "admingentoro/gentoro.*base" | ForEach-Object { Write-Host $_.Line }
        exit 1
    }
}

# Build Docker image
try {
    if ($Push) {
        Write-Host "Will push images to registry"
        $buildArgs = @(
            "buildx", "build",
            "-f", "$ROOT\Dockerfile",
            "--platform", "$Platforms",
            "--build-arg", "APP_JAR=src/onemcp/target/$JarName",
            "--build-arg", "BASE_IMAGE=$baseImageName",
            "-t", "admingentoro/gentoro:$Version",
            "-t", "admingentoro/gentoro:latest",
            "--push"
        )
    } else {
        Write-Host "Building for local use (docker buildx with --load)"
        # For local builds, use docker buildx with --load to access local images
        # Don't override PLATFORMS if it was set via --platform parameter
        if (-not $Platform) {
            $Platforms = "linux/amd64"
        }
        $buildArgs = @(
            "build",
            "-f", "$ROOT\Dockerfile",
            "--build-arg", "APP_JAR=src/onemcp/target/$JarName",
            "--build-arg", "BASE_IMAGE=$baseImageName",
            "-t", "admingentoro/gentoro:$Version",
            "-t", "admingentoro/gentoro:latest",
            "--platform", "$Platforms"
        )
    }

    $buildArgs += "$ROOT"
    
    docker @buildArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Docker build failed"
    }
    Write-Host "Successfully built admingentoro/gentoro:$Version for platforms: $Platforms"
} catch {
    Write-Error "Failed to build product image: $_"
    exit 1
}
