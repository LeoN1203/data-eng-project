#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

build_jar() {
    log "Building JAR file..."
    cd data-pipeline/spark
    if sbt clean assembly; then
        success "JAR build completed successfully"
        cd "$PROJECT_ROOT"
    else
        error "JAR build failed"
        exit 1
    fi
}

build_container() {
    local job_type=$1
    local dockerfile="Dockerfile.$job_type"
    local image_name="data-pipeline-$job_type"
    
    log "Building $job_type container..."
    
    if [ ! -f "docker/$dockerfile" ]; then
        error "Dockerfile not found: docker/$dockerfile"
        return 1
    fi
    
    if docker build -f "docker/$dockerfile" -t "$image_name:latest" data-pipeline/spark/; then
        success "$job_type container built successfully: $image_name:latest"
    else
        error "Failed to build $job_type container"
        return 1
    fi
}

# Function to build unified container
build_unified_container() {
    log "Building unified container..."
    
    # Build JAR first
    build_jar
    
    # Copy docker-entrypoint.sh to spark directory for Docker build context
    log "Copying docker-entrypoint.sh to spark directory..."
    cp scripts/docker-entrypoint.sh data-pipeline/spark/
    
    # Build container
    log "Building unified container..."
    if docker build -f docker/Dockerfile.unified -t data-pipeline-unified:latest data-pipeline/spark/; then
        log "unified container built successfully: data-pipeline-unified:latest"
    else
        log_error "Failed to build unified container"
        return 1
    fi
}

show_usage() {
    echo "Usage: $0 [bronze|silver|gold|grafana|unified|all]"
    echo ""
    echo "Options:"
    echo "  bronze    - Build only Bronze job container"
    echo "  silver    - Build only Silver job container"
    echo "  gold      - Build only Gold job container"
    echo "  grafana   - Build only Grafana export job container"
    echo "  unified   - Build only Unified job container"
    echo "  all       - Build all containers (default)"
    echo ""
    echo "Examples:"
    echo "  $0 bronze           # Build only Bronze container"
    echo "  $0 grafana          # Build only Grafana export container"
    echo "  $0 all              # Build all containers"
    echo "  $0                  # Build all containers (default)"
}

main() {
    local build_target=${1:-all}
    
    case $build_target in
        bronze|silver|gold|grafana|unified)
            log "Building $build_target container..."
            build_jar
            build_container "$build_target"
            ;;
        all)
            log "Building all containers..."
            build_jar
            build_container "bronze"
            build_container "silver"
            build_container "gold"
            build_container "grafana"
            build_container "unified"
            success "All containers built successfully!"
            ;;
        help|--help|-h)
            show_usage
            exit 0
            ;;
        *)
            error "Unknown build target: $build_target"
            show_usage
            exit 1
            ;;
    esac
    
    log "Build process completed!"
    
    echo ""
    log "Built images:"
    docker images | grep "data-pipeline" | head -10
}

main "$@" 