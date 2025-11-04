#!/usr/bin/env bash
set -euo pipefail

# Installation script for Gentoro One MCP CLI
#
# Usage:
#   # Default (main branch from official repo):
#   curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | bash
#
#   # Specific branch or fork:
#   curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | bash
#
# Testing with a specific branch:
#   curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/install.sh | \
#     ONEMCP_REPO_BRANCH=your-branch-name \
#     bash
#
# Installation methods:
#   ONEMCP_INSTALL_METHOD - Installation method: npm-global, system-wide, local-bin, wrapper
#                           - npm-global: Install via npm globally (recommended, no PATH changes needed)
#                           - system-wide: Install to /usr/local/bin (requires sudo)
#                           - local-bin: Install to ~/.local/bin (current behavior)
#                           - wrapper: Create a wrapper script (specify path with ONEMCP_WRAPPER_PATH)
#
# Environment variables:
#   ONEMCP_REPO_URL         - Git repository URL (default: https://github.com/Gentoro-OneMCP/onemcp.git)
#   ONEMCP_REPO_BRANCH      - Git branch name (default: main)
#   ONEMCP_INSTALL_METHOD   - Installation method (default: npm-global)
#   ONEMCP_WRAPPER_PATH     - Path for wrapper script (only used with wrapper method)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
CYAN='\033[0;36m'
DIM='\033[2m' # Dim
NC='\033[0m' # No Color

INSTALL_DIR="$HOME/.onemcp-src"
REPO_URL="${ONEMCP_REPO_URL:-https://github.com/Gentoro-OneMCP/onemcp.git}"
REPO_BRANCH="${ONEMCP_REPO_BRANCH:-main}"
INSTALL_METHOD="${ONEMCP_INSTALL_METHOD:-npm-global}"
WRAPPER_PATH="${ONEMCP_WRAPPER_PATH:-}"

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

banner() {
    echo -e "${CYAN}"
    cat << "EOF"
   ____            _                  
  / ___| ___ _ __ | |_ ___  _ __ ___  
 | |  _ / __| '_ \| __/ _ \| '__/ _ \  
 | |_| \ __ \ | | | || (_) | | | (_) | 
  \____|____/_| |_|\__\___/|_|  \___/  
   ___             __  __  ____ ____
  / _ \ _ __   ___|  \/  |/ ___|  _ \
 | | | | '_ \ / _ \ |\/| | |   | |_) |
 | |_| | | | |  __/ |  | | |___|  __/
  \___/|_| |_|\___|_|  |_|\____|_|
  
EOF
    echo -e "${NC}"
    echo -e "${CYAN}Complete Installation - CLI and Runtime${NC}"

    # Show installation method
    case "$INSTALL_METHOD" in
        "npm-global")
            echo -e "${GREEN}Installation Method: npm global (recommended - no PATH changes needed)${NC}"
            ;;
        "system-wide")
            echo -e "${GREEN}Installation Method: System-wide (/usr/local/bin)${NC}"
            ;;
        "local-bin")
            echo -e "${YELLOW}Installation Method: Local bin (~/.local/bin)${NC}"
            ;;
        "wrapper")
            echo -e "${BLUE}Installation Method: Wrapper script${NC}"
            ;;
        *)
            echo -e "${YELLOW}Installation Method: $INSTALL_METHOD${NC}"
            ;;
    esac
    echo ""
}

check_requirements() {
    log_info "Checking requirements..."
    
    local missing_deps=()
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        log_error "Node.js is not installed"
        missing_deps+=("Node.js 20+")
    else
        NODE_VERSION=$(node --version | sed 's/v//' | cut -d. -f1)
        if [ "$NODE_VERSION" -lt 20 ]; then
            log_error "Node.js version $NODE_VERSION is too old (need 20+)"
            missing_deps+=("Node.js 20+")
        else
            log_success "Node.js $(node --version) found"
        fi
    fi
    
    # Check npm
    if ! command -v npm &> /dev/null; then
        log_error "npm is not installed"
        missing_deps+=("npm")
    else
        log_success "npm $(npm --version) found"
    fi
    
    # Check Java
    if ! command -v java &> /dev/null; then
        log_error "Java is not installed"
        missing_deps+=("Java 21")
    else
        JAVA_VERSION=$(java --version 2>&1 | head -n1 | grep -oE '[0-9]+' | head -1)
        if [ "$JAVA_VERSION" -lt 21 ]; then
            log_warning "Java $JAVA_VERSION found (recommended: 21+)"
        else
            log_success "Java $(java --version 2>&1 | head -n1) found"
        fi
    fi
    
    # Check Maven
    if ! command -v mvn &> /dev/null; then
        log_error "Maven is not installed"
        missing_deps+=("Maven")
    else
        log_success "Maven $(mvn --version 2>&1 | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1) found"
    fi
    
    # Check Git
    if ! command -v git &> /dev/null; then
        log_error "Git is not installed"
        missing_deps+=("Git")
    else
        log_success "Git found"
    fi
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        echo ""
        log_error "Missing required dependencies:"
        for dep in "${missing_deps[@]}"; do
            echo "  - $dep"
        done
        echo ""
        log_info "Installation instructions:"
        echo "  - Node.js 20+: https://nodejs.org/"
        echo "  - Java 21: https://adoptium.net/"
        echo "  - Maven: https://maven.apache.org/install.html"
        echo "  - Git: https://git-scm.com/downloads"
        exit 1
    fi
}

clone_repository() {
    log_info "Cloning One MCP repository..."
    log_info "Repository: $REPO_URL"
    log_info "Branch: $REPO_BRANCH"
    echo ""
    
    if [ -d "$INSTALL_DIR" ]; then
        log_info "Installation directory exists. Updating..."
        cd "$INSTALL_DIR"
        git fetch origin
        git checkout "$REPO_BRANCH"
        # Reset to remote branch to ensure clean state
        git reset --hard "origin/$REPO_BRANCH" || {
            log_warning "Failed to update. Removing and re-cloning..."
            cd ..
            rm -rf "$INSTALL_DIR"
            git clone -b "$REPO_BRANCH" "$REPO_URL" "$INSTALL_DIR"
        }
    else
        git clone -b "$REPO_BRANCH" "$REPO_URL" "$INSTALL_DIR"
    fi
    
    cd "$INSTALL_DIR"
    log_success "Repository cloned to $INSTALL_DIR"
}

build_java_app() {
    log_info "Building Java application..."

    cd "$INSTALL_DIR/src/onemcp"
    mvn clean package spring-boot:repackage -DskipTests -q

    if [ $? -eq 0 ]; then
        log_success "Java application built successfully"
        echo ""
    else
        log_error "Failed to build Java application"
        exit 1
    fi
}

build_typescript_runtime() {
    log_info "Building TypeScript Runtime (this may take a while)..."
    
    cd "$INSTALL_DIR/src/typescript-runtime"
    npm install --silent
    npm run build
    
    if [ $? -eq 0 ]; then
        log_success "TypeScript Runtime built successfully"
        echo ""
    else
        log_error "Failed to build TypeScript Runtime"
        exit 1
    fi
}

build_mock_server() {
    log_info "Building Mock Server..."
    
    cd "$INSTALL_DIR/src/acme-analytics-server/server"
    mvn clean package -DskipTests -q
    
    if [ $? -eq 0 ]; then
        log_success "Mock Server built successfully"
    else
        log_warning "Mock Server build failed (optional component)"
    fi
}

install_cli() {
    log_info "Installing One MCP CLI..."

    cd "$INSTALL_DIR/cli"
    # Clean build artifacts for fresh compilation
    rm -rf dist node_modules package-lock.json
    npm install --silent --no-cache
    npm run build

    # Verify build succeeded
    if [ ! -f "$INSTALL_DIR/cli/dist/index.js" ] || [ ! -s "$INSTALL_DIR/cli/dist/index.js" ]; then
        log_error "Build failed - dist/index.js not found or empty"
        exit 1
    fi

    case "$INSTALL_METHOD" in
        "npm-global")
            install_via_npm_global
            ;;
        "system-wide")
            install_system_wide
            ;;
        "local-bin")
            install_local_bin
            ;;
        "wrapper")
            install_wrapper_script
            ;;
        *)
            log_error "Unknown installation method: $INSTALL_METHOD"
            log_info "Available methods: npm-global, system-wide, local-bin, wrapper"
            exit 1
            ;;
    esac
}

install_via_npm_global() {
    log_info "Installing via npm global (recommended)..."

    cd "$INSTALL_DIR/cli"

    # Create a tarball and install it globally
    npm pack
    PACKAGE_FILE=$(ls -t *.tgz | head -1)

    if [ -z "$PACKAGE_FILE" ]; then
        log_error "Failed to create package tarball"
        log_info "Falling back to local installation..."
        install_local_bin
        return
    fi

    npm install -g "$PACKAGE_FILE"

    # Clean up the tarball
    rm -f "$PACKAGE_FILE"

    # Verify installation
    if command -v onemcp &> /dev/null; then
        ONEMCP_PATH=$(which onemcp)
        log_success "CLI installed globally via npm: $ONEMCP_PATH"
        log_success "No PATH modifications needed!"
    else
        log_error "npm global installation failed"
        log_info "Falling back to local installation..."
        install_local_bin
    fi
}

install_system_wide() {
    log_info "Installing system-wide to /usr/local/bin (requires sudo)..."

    if [ "$EUID" -eq 0 ]; then
        log_error "Please run as regular user, sudo will be requested when needed"
        exit 1
    fi

    sudo mkdir -p "/usr/local/bin"
    sudo cp "$INSTALL_DIR/cli/dist/index.js" "/usr/local/bin/onemcp"
    sudo chmod +x "/usr/local/bin/onemcp"

    log_success "CLI installed to /usr/local/bin/onemcp"
    log_success "Available system-wide, no PATH changes needed"
}

install_local_bin() {
    log_info "Installing to ~/.local/bin..."

    # Install symlink
    mkdir -p "$HOME/.local/bin"
    rm -f "$HOME/.local/bin/onemcp"
    ln -sf "$INSTALL_DIR/cli/dist/index.js" "$HOME/.local/bin/onemcp"
    chmod +x "$HOME/.local/bin/onemcp"

    log_success "CLI installed to $HOME/.local/bin/onemcp"

    # Check PATH configuration
    if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
        echo ""
        log_warning "$HOME/.local/bin is not in your PATH"
        echo "Add it with: export PATH=\"\$HOME/.local/bin:\$PATH\""
        echo ""
        echo "Or use the full path: $HOME/.local/bin/onemcp"
    fi
}

install_wrapper_script() {
    log_info "Creating wrapper script..."

    if [ -z "$WRAPPER_PATH" ]; then
        log_error "ONEMCP_WRAPPER_PATH must be specified for wrapper installation method"
        log_info "Example: ONEMCP_WRAPPER_PATH=/usr/local/bin/onemcp ./install.sh"
        exit 1
    fi

    # Create wrapper script
    cat > "$WRAPPER_PATH" << 'EOF'
#!/bin/bash
# One MCP CLI Wrapper
# This script calls the actual One MCP binary

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ONEMCP_DIR="$HOME/.onemcp-src/cli"

if [ ! -f "$ONEMCP_DIR/dist/index.js" ]; then
    echo "Error: One MCP not found at $ONEMCP_DIR"
    echo "Please reinstall using the installation script"
    exit 1
fi

exec node "$ONEMCP_DIR/dist/index.js" "$@"
EOF

    chmod +x "$WRAPPER_PATH"

    log_success "Wrapper script created at $WRAPPER_PATH"
    log_success "You can move this script anywhere or create additional copies"
}

check_otel_collector() {
    log_info "Checking for OpenTelemetry Collector (optional)..."
    
    if command -v otelcol &> /dev/null; then
        log_success "OpenTelemetry Collector found"
    else
        log_warning "OpenTelemetry Collector not found (optional)"
        log_info "Install from: https://opentelemetry.io/docs/collector/installation/"
    fi
}

post_install() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log_success "Installation complete!"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    log_info "Installation location: $INSTALL_DIR"
    log_info "Installation method: $INSTALL_METHOD"
    echo ""

    # Check if onemcp is in PATH and provide appropriate guidance
    if command -v onemcp &> /dev/null; then
        ONEMCP_PATH=$(which onemcp)
        log_success "CLI command available: $ONEMCP_PATH"

        case "$INSTALL_METHOD" in
            "npm-global")
                log_success "npm global installation successful - no PATH changes needed!"
                echo ""
                ;;
            "system-wide")
                log_success "System-wide installation successful - available to all users!"
                echo ""
                ;;
            "local-bin")
                if [[ ":$PATH:" == *":$HOME/.local/bin:"* ]]; then
                    log_success "~/.local/bin is in your PATH"
                else
                    log_info "~/.local/bin was added to PATH during installation"
                fi
                echo ""
                ;;
            "wrapper")
                log_success "Wrapper script created and functional"
                echo ""
                ;;
        esac
    else
        case "$INSTALL_METHOD" in
            "local-bin")
                log_warning "onemcp command not found in current PATH"
                echo ""
                echo "To use the CLI, add ~/.local/bin to your PATH:"
                echo ""
                if [ -f "$HOME/.zshrc" ]; then
                    echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.zshrc"
                    echo "  source ~/.zshrc"
                elif [ -f "$HOME/.bashrc" ]; then
                    echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
                    echo "  source ~/.bashrc"
                fi
                echo ""
                echo "Or use the full path: $HOME/.local/bin/onemcp"
                ;;
            *)
                log_error "Installation method '$INSTALL_METHOD' did not make onemcp available in PATH"
                log_info "Please check the installation and try again"
                ;;
        esac
    fi

    log_info "Next steps:"
    echo ""
    echo -e "${CYAN}Start chatting:${NC}"
    echo "   $ onemcp chat"
    echo ""
    echo -e "${DIM}This will automatically run the setup wizard if needed.${NC}"
    echo ""
    echo -e "${CYAN}4. View available commands:${NC}"
    echo "   $ onemcp --help"
    echo ""
    log_info "Documentation: https://github.com/Gentoro-OneMCP/onemcp"
    log_info "File an issue: https://github.com/Gentoro-OneMCP/onemcp/issues"
    echo ""
}

cleanup_on_error() {
    log_error "Installation failed. Cleaning up..."
    if [ -d "$INSTALL_DIR" ]; then
        rm -rf "$INSTALL_DIR"
    fi
    exit 1
}

main() {
    # Set up error handler
    trap cleanup_on_error ERR
    
    banner
    echo ""
    
    check_requirements
    echo ""
    
    clone_repository
    echo ""
    
    log_info "Building components (this may take a few minutes)..."
    echo ""
    
    build_java_app
    build_typescript_runtime
    build_mock_server
    echo ""
    
    install_cli
    echo ""
    
    check_otel_collector
    echo ""
    
    post_install
}

# Run main installation
main

