# Launch MCP Server Inspector with auto-configuration for OneMCP (Windows PowerShell)
# This script automatically configures the inspector to connect to OneMCP

param(
    [int]$OneMCPPort = 8080,
    [string]$OneMCPHost = "localhost",
    [string]$MCPEndpoint = "/mcp",
    [int]$InspectorPort = 3001
)

# Build the MCP URL
$MCPUrl = "http://${OneMCPHost}:${OneMCPPort}${MCPEndpoint}"

Write-Host "OneMCP Inspector Launcher" -ForegroundColor Blue
Write-Host "==============================" -ForegroundColor Blue
Write-Host "OneMCP URL: $MCPUrl" -ForegroundColor Blue
Write-Host "Inspector Port: $InspectorPort" -ForegroundColor Blue
Write-Host ""

# Check if OneMCP is running
Write-Host "Checking if OneMCP is running..." -ForegroundColor Blue
try {
    $response = Invoke-WebRequest -Uri $MCPUrl -Method GET -TimeoutSec 5 -ErrorAction Stop
    Write-Host "✅ OneMCP is running and accessible" -ForegroundColor Green
} catch {
    Write-Host "⚠️  OneMCP is not accessible at $MCPUrl" -ForegroundColor Yellow
    Write-Host "Make sure OneMCP is running:" -ForegroundColor Blue
    Write-Host "  docker run --rm -p ${OneMCPPort}:${OneMCPPort} -e APP_ARGS=`"--process=mock-server --tcp-port=8082`" admingentoro/gentoro:latest" -ForegroundColor Blue
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/N)"
    if ($continue -ne "y" -and $continue -ne "Y") {
        Write-Host "Exiting..." -ForegroundColor Blue
        exit 1
    }
}

# Check if npx is available
try {
    $null = Get-Command npx -ErrorAction Stop
} catch {
    Write-Host "❌ npx is not available. Please install Node.js and npm first." -ForegroundColor Red
    Write-Host "Visit: https://nodejs.org/" -ForegroundColor Blue
    exit 1
}

# Launch the MCP Server Inspector
Write-Host "Launching MCP Server Inspector..." -ForegroundColor Blue
Write-Host "The inspector will open in your default browser at: http://localhost:$InspectorPort" -ForegroundColor Blue
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Blue
Write-Host "  - MCP Server URL: $MCPUrl" -ForegroundColor Blue
Write-Host "  - Inspector Port: $InspectorPort" -ForegroundColor Blue
Write-Host ""
Write-Host "Press Ctrl+C to stop the inspector" -ForegroundColor Blue
Write-Host ""

# Launch inspector with auto-configuration
npx @modelcontextprotocol/inspector@latest --mcp-url $MCPUrl --port $InspectorPort --open
