#!/usr/bin/env bash
set -euo pipefail

# Uninstallation script for Gentoro One MCP CLI
# Usage: bash ~/.onemcp-src/cli/uninstall.sh
# Or: curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/cli/uninstall.sh | bash
#
# Environment variables:
#   ONEMCP_UNINSTALL_METHOD - Specify uninstall method (auto-detect if not set)
#   ONEMCP_WRAPPER_PATH     - Path to wrapper script (only for wrapper method)
#   ONEMCP_YES              - Auto-confirm prompts
#   ONEMCP_REMOVE_CONFIG    - Auto-remove config directory
#   ONEMCP_REMOVE_HANDBOOKS - Auto-remove handbooks directory

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

INSTALL_DIR="$HOME/.onemcp-src"
CONFIG_DIR="$HOME/.onemcp"
LOCAL_BIN="$HOME/.local/bin/onemcp"

# Non-interactive controls (env overrides)
# - ONEMCP_YES=1                     → auto-confirm prompts
# - ONEMCP_REMOVE_CONFIG=1           → remove ~/.onemcp without prompting
# - ONEMCP_REMOVE_HANDBOOKS=1        → remove ~/handbooks without prompting
# - ONEMCP_UNINSTALL_METHOD          → specify uninstall method (auto-detect if not set)
AUTO_YES="${ONEMCP_YES:-}"
REMOVE_CONFIG_FLAG="${ONEMCP_REMOVE_CONFIG:-}"
REMOVE_HANDBOOKS_FLAG="${ONEMCP_REMOVE_HANDBOOKS:-}"
UNINSTALL_METHOD="${ONEMCP_UNINSTALL_METHOD:-}"

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
}

stop_services() {
    log_info "Stopping any running One MCP services..."
    
    # Try to stop via CLI if available
    if command -v onemcp &> /dev/null; then
        onemcp stop 2>/dev/null || true
        log_success "Services stopped via CLI"
    fi
    
    # Kill any remaining Java processes
    pkill -f "onemcp.*\.jar" 2>/dev/null || true
    pkill -f "acme-analytics-server.*\.jar" 2>/dev/null || true

    # Kill TypeScript runtime
    pkill -f "typescript-runtime/dist/server.js" 2>/dev/null || true

    # Kill OTEL collector (if started by our CLI)
    pkill -f "otelcol.*onemcp" 2>/dev/null || true

    # Remove stale PID files
    rm -f "$CONFIG_DIR/state/"*.pid 2>/dev/null || true

    log_success "All services stopped"
}

detect_installation_method() {
    # Auto-detect installation method if not specified
    if [ -n "$UNINSTALL_METHOD" ]; then
        return
    fi

    log_info "Detecting installation method..."

    # Check for npm global installation
    if npm list -g @gentoro/onemcp-cli &> /dev/null 2>&1; then
        UNINSTALL_METHOD="npm-global"
        log_info "Detected: npm global installation"
        return
    fi

    # Check for system-wide installation
    if [ -f "/usr/local/bin/onemcp" ]; then
        UNINSTALL_METHOD="system-wide"
        log_info "Detected: system-wide installation (/usr/local/bin)"
        return
    fi

    # Check for local bin installation
    if [ -f "$LOCAL_BIN" ] || [ -L "$LOCAL_BIN" ]; then
        UNINSTALL_METHOD="local-bin"
        log_info "Detected: local bin installation (~/.local/bin)"
        return
    fi

    # Check for npm link (development)
    if npm list -g | grep -q "@gentoro/onemcp-cli"; then
        UNINSTALL_METHOD="npm-link"
        log_info "Detected: npm link (development)"
        return
    fi

    # Check other common locations
    for bin_dir in ~/.nvm/versions/node/*/bin /opt/homebrew/bin; do
        if [ -f "$bin_dir/onemcp" ] || [ -L "$bin_dir/onemcp" ]; then
            UNINSTALL_METHOD="other"
            log_info "Detected: installation at $bin_dir"
            return
        fi
    done

    log_warning "Could not auto-detect installation method"
    log_info "You may need to specify ONEMCP_UNINSTALL_METHOD"
}

uninstall_npm_global() {
    log_info "Uninstalling from npm global..."

    if npm list -g @gentoro/onemcp-cli &> /dev/null 2>&1; then
        npm uninstall -g @gentoro/onemcp-cli
        log_success "Removed from npm global"
    else
        log_info "Not found in npm global packages"
    fi
}

uninstall_npm_link() {
    log_info "Unlinking npm development link..."

    if npm list -g | grep -q "@gentoro/onemcp-cli"; then
        npm unlink -g @gentoro/onemcp-cli 2>/dev/null || true
        log_success "Unlinked from npm global"
    else
        log_info "No npm link found"
    fi
}

uninstall_system_wide() {
    log_info "Uninstalling system-wide installation..."

    if [ -f "/usr/local/bin/onemcp" ]; then
        if [ "$EUID" -eq 0 ]; then
            rm -f "/usr/local/bin/onemcp"
            log_success "Removed from /usr/local/bin"
        else
            log_info "System-wide installation requires sudo for removal"
            sudo rm -f "/usr/local/bin/onemcp"
            log_success "Removed from /usr/local/bin (with sudo)"
        fi
    else
        log_info "Not found in /usr/local/bin"
    fi
}

uninstall_local_bin() {
    log_info "Uninstalling local bin installation..."

    if [ -f "$LOCAL_BIN" ] || [ -L "$LOCAL_BIN" ]; then
        rm -f "$LOCAL_BIN"
        log_success "Removed from $LOCAL_BIN"
    else
        log_info "Not found in ~/.local/bin"
    fi
}

uninstall_other_locations() {
    log_info "Checking other common locations..."

    local found=false

    # Check various bin directories
    for bin_dir in ~/.nvm/versions/node/*/bin /opt/homebrew/bin /usr/local/bin; do
        if [ -f "$bin_dir/onemcp" ] || [ -L "$bin_dir/onemcp" ]; then
            rm -f "$bin_dir/onemcp" 2>/dev/null || {
                log_warning "Could not remove $bin_dir/onemcp (may require sudo)"
                sudo rm -f "$bin_dir/onemcp" 2>/dev/null || {
                    log_warning "Failed to remove $bin_dir/onemcp"
                }
            }
            found=true
        fi
    done

    if [ "$found" = true ]; then
        log_success "Removed from detected locations"
    else
        log_info "No installations found in common locations"
    fi
}

uninstall_wrapper_script() {
    log_info "Wrapper script uninstallation..."

    # For wrapper scripts, we can't auto-detect the location
    # The user needs to specify ONEMCP_WRAPPER_PATH or remove manually
    if [ -n "${ONEMCP_WRAPPER_PATH:-}" ]; then
        if [ -f "$ONEMCP_WRAPPER_PATH" ]; then
            rm -f "$ONEMCP_WRAPPER_PATH"
            log_success "Removed wrapper script from $ONEMCP_WRAPPER_PATH"
        else
            log_info "Wrapper script not found at $ONEMCP_WRAPPER_PATH"
        fi
    else
        log_warning "Cannot auto-detect wrapper script location"
        log_info "Specify ONEMCP_WRAPPER_PATH or remove manually"
        log_info "Example: ONEMCP_WRAPPER_PATH=/path/to/onemcp ./uninstall.sh"
    fi
}

unlink_cli() {
    log_info "Removing CLI command..."

    # Detect installation method if not specified
    detect_installation_method

    # Uninstall based on detected or specified method
    case "$UNINSTALL_METHOD" in
        "npm-global")
            uninstall_npm_global
            ;;
        "system-wide")
            uninstall_system_wide
            ;;
        "local-bin")
            uninstall_local_bin
            ;;
        "npm-link")
            uninstall_npm_link
            ;;
        "wrapper")
            uninstall_wrapper_script
            ;;
        "other"|*)
            # Fallback: check all locations
            uninstall_npm_global
            uninstall_npm_link
            uninstall_system_wide
            uninstall_local_bin
            uninstall_other_locations
            ;;
    esac

    # Final verification
    if command -v onemcp &> /dev/null; then
        case "$UNINSTALL_METHOD" in
            "npm-global")
                log_info "Note: You may need to restart your shell for PATH changes to take effect"
                ;;
            *)
                log_warning "onemcp command still found in PATH"
                log_info "You may need to restart your shell or check your PATH"
                ;;
        esac
    else
        log_success "CLI command successfully removed"
    fi
}

remove_source() {
    log_info "Removing source files..."
    
    if [ -d "$INSTALL_DIR" ]; then
        local size=$(du -sh "$INSTALL_DIR" 2>/dev/null | cut -f1)
        rm -rf "$INSTALL_DIR"
        log_success "Removed source directory ($size)"
    else
        log_info "Source directory not found (already removed)"
    fi
}

remove_config() {
    log_info "Configuration and data:"
    echo ""
    
    if [ -d "$CONFIG_DIR" ]; then
        local size=$(du -sh "$CONFIG_DIR" 2>/dev/null | cut -f1)
        echo -e "${YELLOW}  $CONFIG_DIR${NC} ($size)"
        echo "  Contains:"
        echo "    - Configuration files (config.yaml)"
        echo "    - Service configurations"
        echo "    - Logs"
        echo "    - Runtime state"
    else
        echo -e "${YELLOW}  $CONFIG_DIR${NC} (not found)"
        echo "  Would contain configuration, logs, and state"
    fi
    
    echo ""
    
    DO_REMOVE=""
    if [ -n "$AUTO_YES" ] || [ -n "$REMOVE_CONFIG_FLAG" ]; then
        DO_REMOVE="y"
    elif [ -t 1 ]; then
        # Interactive mode - stdout is a TTY, so user is present
        # If stdin is piped, read from /dev/tty instead
        if [ -t 0 ]; then
            read -p "Remove configuration directory? [y/N]: " -n 1 -r; echo
        else
            read -p "Remove configuration directory? [y/N]: " -n 1 -r < /dev/tty; echo
        fi
        DO_REMOVE="$REPLY"
    else
        DO_REMOVE="n"
    fi

    if [[ $DO_REMOVE =~ ^[Yy]$ ]]; then
        if [ -d "$CONFIG_DIR" ]; then
            rm -rf "$CONFIG_DIR"
            log_success "Configuration removed"
        else
            log_info "Configuration directory does not exist"
        fi
    else
        if [ -d "$CONFIG_DIR" ]; then
            log_info "Configuration preserved at $CONFIG_DIR"
        else
            log_info "Configuration directory does not exist"
        fi
    fi
}

remove_handbooks() {
    local handbooks_dir="$HOME/handbooks"
    
    echo ""
    log_info "Handbooks:"
    echo ""
    
    if [ -d "$handbooks_dir" ]; then
        local size=$(du -sh "$handbooks_dir" 2>/dev/null | cut -f1)
        echo -e "${YELLOW}  $handbooks_dir${NC} $size"
        echo "  Your AI handbooks and configurations"
    else
        echo -e "${YELLOW}  $handbooks_dir${NC} (not found)"
        echo "  Default location for handbooks"
    fi
    
    echo ""
    
    DO_REMOVE=""
    if [ -n "$AUTO_YES" ] || [ -n "$REMOVE_HANDBOOKS_FLAG" ]; then
        DO_REMOVE="y"
    elif [ -t 1 ]; then
        # Interactive mode - stdout is a TTY, so user is present
        # If stdin is piped, read from /dev/tty instead
        if [ -t 0 ]; then
            read -p "Remove handbooks directory? [y/N]: " -n 1 -r; echo
        else
            read -p "Remove handbooks directory? [y/N]: " -n 1 -r < /dev/tty; echo
        fi
        DO_REMOVE="$REPLY"
    else
        DO_REMOVE="n"
    fi

    if [[ $DO_REMOVE =~ ^[Yy]$ ]]; then
        if [ -d "$handbooks_dir" ]; then
            rm -rf "$handbooks_dir"
            log_success "Handbooks removed"
        else
            log_info "Handbooks directory does not exist"
        fi
    else
        if [ -d "$handbooks_dir" ]; then
            log_info "Handbooks preserved at $handbooks_dir"
        else
            log_info "Handbooks directory does not exist"
        fi
    fi
}

cleanup_temp() {
    log_info "Cleaning up temporary files..."
    
    # Remove OTEL config if created by our CLI
    [ -f "/tmp/onemcp-otel-config.yaml" ] && rm -f "/tmp/onemcp-otel-config.yaml"
    
    log_success "Temporary files cleaned"
}

show_summary() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log_success "Uninstallation complete!"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    log_info "What was removed:"
    echo "  ✓ One MCP CLI command"
    echo "  ✓ Source code and built binaries"
    echo "  ✓ Running services"
    echo "  ✓ Temporary files"
    echo ""
    
    local has_preserved=false

    if [ -d "$CONFIG_DIR" ]; then
        if [ "$has_preserved" = false ]; then
            log_info "Preserved (remove manually if needed):"
            has_preserved=true
        fi
        echo "  • Configuration: $CONFIG_DIR"
    fi

    if [ -d "$HOME/handbooks" ]; then
        if [ "$has_preserved" = false ]; then
            log_info "Preserved (remove manually if needed):"
            has_preserved=true
        fi
        echo "  • Handbooks: $HOME/handbooks"
    fi

    if [ "$has_preserved" = false ]; then
        log_info "All optional data has been removed"
    fi
    
    echo ""
    log_info "Thank you for trying Gentoro One MCP!"
    echo ""
}

confirm_uninstall() {
    echo ""
    log_warning "This will uninstall Gentoro One MCP CLI and remove:"
    echo "  • CLI command (onemcp)"
    echo "  • Source code (~500MB)"
    echo "  • Built binaries"
    echo "  • Running services"
    echo ""
    echo "You will be prompted before removing:"
    echo "  • Configuration files"
    echo "  • Handbooks"
    echo ""

    if [ -n "$AUTO_YES" ]; then
        REPLY="y"
    elif [ -t 1 ]; then
        # Interactive mode - stdout is a TTY, so user is present
        # If stdin is piped, read from /dev/tty instead
        if [ -t 0 ]; then
            read -p "Continue with uninstallation? [y/N]: " -n 1 -r
            echo
        else
            read -p "Continue with uninstallation? [y/N]: " -n 1 -r < /dev/tty
            echo
        fi
    else
        # Non-interactive mode - no TTY available
        log_info "Running in non-interactive mode, proceeding with uninstallation..."
        REPLY="y"
    fi

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Uninstallation cancelled"
        exit 0
    fi
}

main() {
    banner
    
    confirm_uninstall
    
    echo ""
    log_info "Starting uninstallation..."
    echo ""
    
    stop_services
    echo ""
    
    unlink_cli
    echo ""
    
    remove_source
    echo ""
    
    cleanup_temp
    echo ""
    
    remove_config
    
    remove_handbooks
    
    show_summary
}

# Run main uninstallation
main

