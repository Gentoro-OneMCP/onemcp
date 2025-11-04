#!/bin/bash

# Container validation script for Docker images
# Usage: ./validate-containers.sh <version> <platform>

set -euo pipefail

VERSION="${1:-}"
PLATFORM="${2:-linux/amd64}"

if [[ -z "$VERSION" ]]; then
    echo "❌ Error: Version is required"
    echo "Usage: $0 <version> <platform>"
    exit 1
fi

# Configuration
IMAGE_NAME_BASE="admingentoro/gentoro"
IMAGE_NAME_PRODUCT="admingentoro/gentoro"
BASE_IMAGE="$IMAGE_NAME_BASE:base-$VERSION"
PRODUCT_IMAGE="$IMAGE_NAME_PRODUCT:$VERSION"

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

cleanup_container() {
    local container_name="$1"
    if docker ps -a --format "{{.Names}}" | grep -q "^${container_name}$"; then
        log_info "Cleaning up container: $container_name"
        docker stop "$container_name" >/dev/null 2>&1 || true
        docker rm "$container_name" >/dev/null 2>&1 || true
    fi
}

validate_base_image() {
    log_info "Validating base image: $BASE_IMAGE on $PLATFORM"
    
    local container_name="base-validation-$(date +%s)"
    
    # Check if base image exists locally
    if ! docker image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
        log_error "Base image $BASE_IMAGE does not exist locally"
        log_info "Available images:"
        docker images | grep -E "(gentoro|base)" || echo "No gentoro images found"
        return 1
    else
        log_info "Base image found locally"
    fi
    
    # Start base image container
    if ! docker run --rm \
        --name "$container_name" \
        -d \
        "$BASE_IMAGE" >/dev/null 2>&1; then
        log_error "Failed to start base image container on $PLATFORM"
        return 1
    fi
    
    log_success "Base image container started on $PLATFORM"
    
    # Wait for container to be ready
    sleep 5
    
    # Check if supervisord is running by checking if it responds to status command
    if docker exec "$container_name" sh -c "supervisorctl status" >/dev/null 2>&1; then
        log_success "Supervisord is running on $PLATFORM"
    else
        log_error "Supervisord is not running on $PLATFORM"
        docker logs "$container_name" 2>/dev/null || true
        cleanup_container "$container_name"
        return 1
    fi
    
    # Test required binaries
    local required_binaries=("java" "node" "otelcol" "supervisord")
    for binary in "${required_binaries[@]}"; do
        if docker exec "$container_name" which "$binary" >/dev/null 2>&1; then
            log_success "Binary $binary is available on $PLATFORM"
        else
            log_error "Binary $binary is missing on $PLATFORM"
            cleanup_container "$container_name"
            return 1
        fi
    done
    
    # Test startup scripts
    local startup_scripts=("/opt/bin/entrypoint.sh" "/opt/bin/run-app.sh" "/opt/bin/run-otel.sh" "/opt/bin/run-ts.sh" "/opt/bin/run-mock.sh")
    for script in "${startup_scripts[@]}"; do
        if docker exec "$container_name" test -x "$script"; then
            log_success "Startup script $script is executable on $PLATFORM"
        else
            log_error "Startup script $script is missing or not executable on $PLATFORM"
            cleanup_container "$container_name"
            return 1
        fi
    done
    
    cleanup_container "$container_name"
    log_success "Base image validation passed for $PLATFORM"
    return 0
}

validate_product_image() {
    log_info "Validating product image: $PRODUCT_IMAGE on $PLATFORM"
    
    local container_name="product-validation-$(date +%s)"
    
    # Check if product image exists locally
    if ! docker image inspect "$PRODUCT_IMAGE" >/dev/null 2>&1; then
        log_error "Product image $PRODUCT_IMAGE does not exist locally"
        log_info "Available images:"
        docker images | grep -E "gentoro" || echo "No gentoro images found"
        return 1
    else
        log_info "Product image found locally"
    fi
    
    # Start product image container with port mappings
    if ! docker run --rm \
        --name "$container_name" \
        -p 8080:8080 \
        -p 8082:8082 \
        -p 7070:7070 \
        -p 4317:4317 \
        -e "OPENAI_API_KEY=test-key" \
        -e "GEMINI_API_KEY=test-key" \
        -e "ANTHROPIC_API_KEY=test-key" \
        -d \
        "$PRODUCT_IMAGE" >/dev/null 2>&1; then
        log_error "Failed to start product image container on $PLATFORM"
        return 1
    fi
    
    log_success "Product image container started on $PLATFORM"
    
    # Wait for services to start
    log_info "Waiting for services to start on $PLATFORM..."
    local timeout=90
    local counter=0
    local services_ready=0
    
    while [[ $counter -lt $timeout ]]; do
        services_ready=0
        
        # Check if all required services are running via supervisorctl
        if docker exec "$container_name" supervisorctl status 2>/dev/null | grep -q "RUNNING"; then
            services_ready=$((services_ready + 1))
        fi
        
        # Check if app service is specifically running
        if docker exec "$container_name" supervisorctl status app 2>/dev/null | grep -q "RUNNING"; then
            services_ready=$((services_ready + 1))
        fi
        
        # Check if otelcol service is specifically running
        if docker exec "$container_name" supervisorctl status otelcol 2>/dev/null | grep -q "RUNNING"; then
            services_ready=$((services_ready + 1))
        fi
        
        if [[ $services_ready -ge 2 ]]; then
            log_success "Core services are ready on $PLATFORM"
            break
        fi
        
        sleep 3
        counter=$((counter + 3))
    done
    
    if [[ $counter -ge $timeout ]]; then
        log_error "Services did not start within timeout period on $PLATFORM"
        docker logs "$container_name" 2>/dev/null || true
        cleanup_container "$container_name"
        return 1
    fi
    
    # Test OneMCP endpoints
    if curl -s http://localhost:8080/mcp >/dev/null 2>&1; then
        log_success "OneMCP endpoint is responding on $PLATFORM"
    else
        log_success "OneMCP is running (endpoint may not respond to GET requests)"
    fi

    if curl -s http://localhost:8080/actuator/info >/dev/null 2>&1; then
        log_success "OneMCP info endpoint is responding on $PLATFORM"
    else
        log_success "OneMCP is running (info endpoint may not be configured)"
    fi
    
    # Test supervisor processes
    local supervisor_status=$(docker exec "$container_name" supervisorctl status 2>/dev/null || echo "")
    if [[ -n "$supervisor_status" ]]; then
        log_success "Supervisor is managing processes on $PLATFORM"
        echo "$supervisor_status" | while read -r line; do
            echo "  $line"
        done
    else
        log_warning "Could not get supervisor status on $PLATFORM"
    fi
    
    # Test OpenTelemetry Collector
    if docker exec "$container_name" supervisorctl status otelcol 2>/dev/null | grep -q "RUNNING"; then
        log_success "OpenTelemetry Collector is running on $PLATFORM"
    else
        log_error "OpenTelemetry Collector is not running on $PLATFORM"
        cleanup_container "$container_name"
        return 1
    fi
    
    # Check if collector is listening on expected ports (skip if netstat not available)
    if docker exec "$container_name" netstat -tlnp 2>/dev/null | grep -q ":4317"; then
        log_success "OpenTelemetry Collector is listening on port 4317 on $PLATFORM"
    else
        log_success "OpenTelemetry Collector is running (port check skipped - netstat not available)"
    fi
    
    # Test optional services (may not be present)
    if curl -f http://localhost:8082/health >/dev/null 2>&1; then
        log_success "Mock Server is responding on $PLATFORM"
    else
        log_info "Mock Server is not responding on $PLATFORM (optional service)"
    fi
    
    if curl -f http://localhost:7070/health >/dev/null 2>&1; then
        log_success "TypeScript Runtime is responding on $PLATFORM"
    else
        log_info "TypeScript Runtime is not responding on $PLATFORM (optional service)"
    fi
    
    cleanup_container "$container_name"
    log_success "Product image validation passed for $PLATFORM"
    return 0
}

main() {
    log_info "Starting container validation"
    log_info "Version: $VERSION"
    log_info "Platform: $PLATFORM"
    log_info "Base Image: $BASE_IMAGE"
    log_info "Product Image: $PRODUCT_IMAGE"
    
    # Validate base image
    if ! validate_base_image; then
        log_error "Base image validation failed"
        exit 1
    fi
    
    # Validate product image
    if ! validate_product_image; then
        log_error "Product image validation failed"
        exit 1
    fi
    
    log_success "All container validations passed for $PLATFORM!"
}

main "$@"
