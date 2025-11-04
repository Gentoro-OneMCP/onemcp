#!/usr/bin/env bash
set -euo pipefail

# Simple MCP Inspector launcher for OneMCP
# This version works with the standard MCP Inspector

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Configuration
MCP_URL="http://localhost:8080/mcp"
INSPECTOR_PORT="${INSPECTOR_PORT:-3001}"

log_info "OneMCP Inspector Launcher (Simple)"
log_info "====================================="
log_info "OneMCP URL: ${MCP_URL}"
log_info "Inspector Port: ${INSPECTOR_PORT}"
log_info ""

# Check if OneMCP is running
log_info "Checking if OneMCP is running..."
if curl -s -f "${MCP_URL}" >/dev/null 2>&1; then
    log_success "OneMCP is running and accessible"
else
    log_warning "OneMCP is not accessible at ${MCP_URL}"
    log_info "Make sure OneMCP is running:"
    log_info "  docker run --rm -p 8080:8080 -e APP_ARGS=\"--process=mock-server --tcp-port=8082\" admingentoro/gentoro:latest"
    log_info ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Exiting..."
        exit 1
    fi
fi

# Check if npx is available
if ! command -v npx &> /dev/null; then
    log_error "npx is not available. Please install Node.js and npm first."
    log_info "Visit: https://nodejs.org/"
    exit 1
fi

# Launch the MCP Server Inspector
log_info "Launching MCP Server Inspector..."
log_info "The inspector will open in your default browser at: http://localhost:${INSPECTOR_PORT}"
log_info ""
log_info "When the inspector opens:"
log_info "  1. Click 'Connect to Server'"
log_info "  2. Enter URL: ${MCP_URL}"
log_info "  3. Click 'Connect'"
log_info ""
log_info "Press Ctrl+C to stop the inspector"
log_info ""

# Launch inspector
npx @modelcontextprotocol/inspector@latest --port ${INSPECTOR_PORT} --open
