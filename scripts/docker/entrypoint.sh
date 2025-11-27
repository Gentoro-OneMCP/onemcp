#!/usr/bin/env bash
set -euo pipefail

# Enhanced entrypoint with command parsing capabilities
# Usage examples:
#   docker run image                    # Start all services (default)
#   docker run image help               # Show help
#   docker run image status             # Show service status
#   docker run image logs [service]     # Show logs
#   docker run image shell              # Start interactive shell
#   docker run image app-only           # Start only the main app
#   docker run image otel-only          # Start only OpenTelemetry collector
#   docker run image arangodb-only      # Start only ArangoDB database
#   docker run image echo "hello"       # Run custom command

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

show_help() {
    cat << EOF
OneMCP Docker Container Commands:

Default behavior (no arguments):
  docker run image                    # Start all services via supervisord

Service management:
  docker run image status             # Show supervisor service status
  docker run image logs [service]     # Show logs for specific service or all
  docker run image restart [service]  # Restart specific service

Single service modes:
  docker run image app-only           # Start only the main OneMCP app
  docker run image otel-only          # Start only OpenTelemetry collector
  docker run image arangodb-only      # Start only ArangoDB database

Utility commands:
  docker run image shell              # Start interactive bash shell
  docker run image help               # Show this help message
  docker run image version            # Show version information

Custom commands:
  docker run image <any-command>      # Execute custom command directly

Environment variables:
  JAVA_OPTS                           # Java JVM options
  APP_ARGS                            # Application arguments
  OTEL_CONFIG                         # OpenTelemetry config file path
  DISABLE_SERVICES                    # Comma-separated list of services to disable
  ARANGODB_ENABLED                    # Enable/disable ArangoDB (default: true)
  ARANGODB_HOST                       # ArangoDB host (default: localhost)
  ARANGODB_PORT                       # ArangoDB port (default: 8529)
  ARANGODB_USER                       # ArangoDB user (default: root)
  ARANGODB_PASSWORD                   # ArangoDB password (default: test123)

Process modes (via APP_ARGS):
  --process=validate                  # Validates foundation data and exits
  --process=regression                # Runs regression test suite and exits
  --process=standard (default)        # Starts MCP server + orchestrator

Examples:
  docker run -e JAVA_OPTS="-Xmx1g" image app-only
  docker run -e APP_ARGS="--process=validate" image
  docker run -e APP_ARGS="--process=regression" image
  docker run -e ARANGODB_ENABLED=false image  # Run without ArangoDB
  docker run -e ARANGODB_PASSWORD="mypass" image
  docker run image logs app
  docker run image logs arangodb
  docker run image arangodb-only
  docker run image shell
EOF
}

show_version() {
    log_info "OneMCP Docker Container"
    log_info "Base Image: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)"
    log_info "Java Version: $(java -version 2>&1 | head -n1)"
    log_info "Node Version: $(node --version)"
    log_info "OpenTelemetry Collector: $(otelcol --version 2>&1 | head -n1)"
    if command -v arangod >/dev/null 2>&1; then
        log_info "ArangoDB Version: $(arangod --version 2>&1 | head -n1)"
    fi
    if [[ -f "/opt/app/onemcp.jar" ]]; then
        log_info "Application JAR: $(ls -la /opt/app/onemcp.jar)"
    fi
}

show_status() {
    if command -v supervisorctl >/dev/null 2>&1; then
        log_info "Service Status:"
        supervisorctl status 2>/dev/null || log_warning "Supervisor not running"
    else
        log_warning "Supervisor not available"
    fi
}

show_logs() {
    local service="${1:-all}"

    if [[ "$service" == "all" ]]; then
        log_info "Showing logs for all services:"
        for log_file in /var/log/supervisor/*.log; do
            if [[ -f "$log_file" ]]; then
                echo "=== $(basename "$log_file") ==="
                tail -n 20 "$log_file" 2>/dev/null || true
                echo
            fi
        done
        # Also show ArangoDB logs if they exist
        if [[ -f "/var/log/arangodb3/arangod.log" ]]; then
            echo "=== arangodb.log ==="
            tail -n 20 "/var/log/arangodb3/arangod.log" 2>/dev/null || true
            echo
        fi
    else
        case "$service" in
            "arangodb")
                local log_file="/var/log/arangodb3/arangod.log"
                if [[ -f "$log_file" ]]; then
                    log_info "Showing logs for service: $service"
                    tail -n 50 "$log_file"
                else
                    log_error "No logs found for service: $service"
                    return 1
                fi
                ;;
            *)
                local log_file="/var/log/supervisor/${service}.out.log"
                if [[ -f "$log_file" ]]; then
                    log_info "Showing logs for service: $service"
                    tail -n 50 "$log_file"
                else
                    log_error "No logs found for service: $service"
                    return 1
                fi
                ;;
        esac
    fi
}

start_single_service() {
    local service="$1"
    log_info "Starting single service: $service"

    case "$service" in
        "app-only")
            exec /opt/bin/run-app.sh
            ;;
        "otel-only")
            exec /opt/bin/run-otel.sh
            ;;
        "arangodb-only")
            exec /opt/bin/run-arangodb.sh
            ;;
        *)
            log_error "Unknown single service: $service"
            return 1
            ;;
    esac
}

start_shell() {
    log_info "Starting interactive shell..."
    exec /bin/bash
}

start_supervisord() {
    log_info "Starting all services via supervisord..."

    # Check for disabled services
    if [[ -n "${DISABLE_SERVICES:-}" ]]; then
        log_info "Disabled services: $DISABLE_SERVICES"
        # This would require modifying supervisord.conf dynamically
        # For now, just log the information
    fi

    # Check if ArangoDB is enabled
    if [[ "${ARANGODB_ENABLED:-true}" == "false" ]]; then
        log_info "ArangoDB is disabled via environment variable"
    fi

    # Start supervisord in background
    /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &
    local supervisord_pid=$!

    # Wait for services to start
    log_info "Waiting for services to start..."
    sleep 8

    # Show service status
    log_info "Service Status:"
    supervisorctl status 2>/dev/null || log_warning "Supervisor not responding yet"

    # Show application logs
    log_info "Application logs:"
    echo "=========================================="
    if [[ -f "/var/log/supervisor/app.out.log" ]]; then
        cat /var/log/supervisor/app.out.log
    else
        log_warning "Application log file not found"
    fi
    echo "=========================================="

    # Keep container running and show live logs
    log_info "Container is running. Showing live logs..."

    # Show live logs from all services
    tail -f /var/log/supervisor/app.out.log
}

# Main command parsing logic
main() {
    # If no arguments provided, start supervisord (default behavior)
    if [[ $# -eq 0 ]]; then
        start_supervisord
        return
    fi

    # Parse the first argument as the command
    local command="$1"
    shift # Remove first argument, rest become $@

    case "$command" in
        "help"|"-h"|"--help")
            show_help
            ;;
        "version"|"-v"|"--version")
            show_version
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs "$@"
            ;;
        "shell"|"bash"|"sh")
            start_shell
            ;;
        "app-only"|"otel-only"|"arangodb-only")
            start_single_service "$command"
            ;;
        "restart")
            if [[ -n "${1:-}" ]]; then
                log_info "Restarting service: $1"
                supervisorctl restart "$1" 2>/dev/null || log_error "Failed to restart service: $1"
            else
                log_error "Please specify a service to restart"
                return 1
            fi
            ;;
        *)
            # For any other command, execute it directly
            log_info "Executing custom command: $command $*"
            exec "$command" "$@"
            ;;
    esac
}

# Run main function with all arguments
main "$@"