#!/bin/bash

# =============================================================================
# BUILD JAR SCRIPT - Build Data Pipeline JAR File
# =============================================================================
# This script builds the data pipeline JAR file using sbt assembly
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS:${NC} $1"
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

# Check if sbt is available
if ! command -v sbt &> /dev/null; then
    error "sbt is not installed. Please install sbt first."
    exit 1
fi

# Check if the data-pipeline directory exists
if [ ! -d "data-pipeline/spark" ]; then
    error "data-pipeline/spark directory not found."
    exit 1
fi

log "Building data pipeline JAR file..."

# Navigate to the spark directory and build
cd data-pipeline/spark

# Clean and build the JAR
if sbt clean assembly; then
    success "JAR build completed successfully"
    
    # Check if the JAR file was created
    JAR_PATH="target/scala-2.12/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar"
    if [ -f "$JAR_PATH" ]; then
        JAR_SIZE=$(du -h "$JAR_PATH" | cut -f1)
        success "JAR file created: $JAR_PATH (Size: $JAR_SIZE)"
    else
        error "JAR file not found at expected location: $JAR_PATH"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
else
    error "JAR build failed"
    cd "$PROJECT_ROOT"
    exit 1
fi

success "JAR build process completed successfully! 🎉" 