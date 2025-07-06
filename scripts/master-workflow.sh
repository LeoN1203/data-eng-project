#!/bin/bash

# =============================================================================
# MASTER WORKFLOW SCRIPT - Complete Data Pipeline Orchestration
# =============================================================================
# This script orchestrates the entire data pipeline workflow:
# 1. Environment setup and validation
# 2. Build all Docker images
# 3. Start infrastructure services (including Grafana & PostgreSQL)
# 4. Start IoT data producer
# 5. Start Kafka ingestion
# 6. Execute Bronze → Silver → Gold → Grafana pipeline
# 7. Keep services running for monitoring
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
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
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

step() {
    echo -e "${PURPLE}[$(date '+%Y-%m-%d %H:%M:%S')] STEP:${NC} $1"
}


PROCESS_DATE=${PROCESS_DATE:-$(date '+%Y-%m-%d')}
PRODUCER_DURATION=${PRODUCER_DURATION:-300}  # 5 minutes default
INGESTION_DURATION=${INGESTION_DURATION:-360}  # 6 minutes default
CLEANUP_ON_EXIT=${CLEANUP_ON_EXIT:-false}  # Changed default to false to keep containers running
SKIP_BUILD=${SKIP_BUILD:-false}
SKIP_INFRASTRUCTURE=${SKIP_INFRASTRUCTURE:-false}
KEEP_SERVICES_RUNNING=${KEEP_SERVICES_RUNNING:-true}

print_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                        🚀 DATA PIPELINE MASTER WORKFLOW 🚀                   ║"
    echo "╠═══════════════════════════════════════════════════════════════════════════════╣"
    echo "║                                                                               ║"
    echo "║  This script will orchestrate the complete data pipeline workflow:           ║"
    echo "║  • Build Docker images                                                        ║"
    echo "║  • Start infrastructure services (Kafka, Spark, PostgreSQL, Grafana)        ║"
    echo "║  • Generate IoT data                                                          ║"
    echo "║  • Ingest data to S3                                                          ║"
    echo "║  • Process through Bronze → Silver → Gold layers                             ║"
    echo "║  • Export to Grafana                                                          ║"
    echo "║  • Keep services running for monitoring                                       ║"
    echo "║                                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --date DATE              Process date (YYYY-MM-DD, default: today)"
    echo "  --producer-duration SEC  IoT producer duration in seconds (default: 300)"
    echo "  --ingestion-duration SEC Kafka ingestion duration in seconds (default: 360)"
    echo "  --skip-build            Skip Docker image building"
    echo "  --skip-infrastructure   Skip infrastructure startup"
    echo "  --cleanup-on-exit       Cleanup containers on exit (default: keep running)"
    echo "  --help                  Show this help message"
    echo ""
    echo "Environment Variables (set in .env file):"
    echo "  AWS_ACCESS_KEY_ID       AWS access key"
    echo "  AWS_SECRET_ACCESS_KEY   AWS secret key"
    echo "  AWS_DEFAULT_REGION      AWS region (default: eu-north-1)"
    echo "  S3_BUCKET               S3 bucket name"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Full workflow for today, keep services running"
    echo "  $0 --date 2024-01-15                # Full workflow for specific date"
    echo "  $0 --skip-build                     # Skip building, use existing images"
    echo "  $0 --cleanup-on-exit                # Cleanup containers when done"
    echo "  $0 --producer-duration 600          # Run producer for 10 minutes"
}

check_prerequisites() {
    step "Checking prerequisites..."
    
    if ! docker ps >/dev/null 2>&1; then
        error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    if ! docker compose version >/dev/null 2>&1; then
        error "Docker Compose is not installed or not in PATH."
        exit 1
    fi
    
    if [ ! -f ".env" ]; then
        error ".env file not found. Please create it with AWS credentials."
        echo "Example .env file:"
        echo "AWS_ACCESS_KEY_ID=your_access_key"
        echo "AWS_SECRET_ACCESS_KEY=your_secret_key"
        echo "AWS_DEFAULT_REGION=eu-north-1"
        echo "S3_BUCKET=your-bucket-name"
        exit 1
    fi
    
    source ".env"
    
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        error "AWS credentials not set. Please check your .env file."
        exit 1
    fi
    
    if [ "$SKIP_BUILD" = false ] && ! sbt --version >/dev/null 2>&1; then
        error "sbt is not installed. Please install sbt or use --skip-build."
        exit 1
    fi
    
    success "Prerequisites check passed"
}

build_images() {
    if [ "$SKIP_BUILD" = true ]; then
        warning "Skipping Docker image building as requested"
        return 0
    fi
    
    step "Building Docker images..."
    
    log "Building all pipeline containers..."
    if ./scripts/build-containers.sh all; then
        success "All Docker images built successfully"
    else
        error "Failed to build Docker images"
        exit 1
    fi
}

check_and_free_ports() {
    step "Checking required ports..."
    
    if lsof -i :5432 >/dev/null 2>&1; then
        warning "Port 5432 is in use. Attempting to free it..."
        sudo fuser -k 5432/tcp >/dev/null 2>&1 || true
        sleep 2
        if lsof -i :5432 >/dev/null 2>&1; then
            error "Could not free port 5432. Please stop the service using this port manually."
            exit 1
        else
            success "Port 5432 freed successfully"
        fi
    fi
    
    if lsof -i :3001 >/dev/null 2>&1; then
        warning "Port 3001 is in use. Attempting to free it..."
        sudo fuser -k 3001/tcp >/dev/null 2>&1 || true
        sleep 2
    fi
    
    success "Required ports are available"
}

start_infrastructure() {
    if [ "$SKIP_INFRASTRUCTURE" = true ]; then
        warning "Skipping infrastructure startup as requested"
        return 0
    fi
    
    step "Starting infrastructure services..."
    
    check_and_free_ports
    
    log "Starting core pipeline services (Kafka, Zookeeper, Spark)..."
    docker compose -f docker/docker-compose.yml up -d
    
    log "Waiting for core services to be ready..."
    sleep 30
    
    log "Starting PostgreSQL and Grafana services..."
    docker compose -f grafana-config/docker-compose.yml up -d
    
    log "Waiting for PostgreSQL to be ready..."
    sleep 15
    
    local retries=0
    local max_retries=10
    while [ $retries -lt $max_retries ]; do
        if docker ps | grep kafka | grep -q "Up"; then
            success "Kafka is running"
            break
        else
            log "Waiting for Kafka to be ready... (attempt $((retries + 1))/$max_retries)"
            sleep 10
            retries=$((retries + 1))
        fi
    done
    
    if [ $retries -eq $max_retries ]; then
        error "Kafka failed to start properly"
        exit 1
    fi
    
    retries=0
    while [ $retries -lt $max_retries ]; do
        if docker exec postgres pg_isready -U grafana >/dev/null 2>&1; then
            success "PostgreSQL is ready"
            break
        else
            log "Waiting for PostgreSQL to be ready... (attempt $((retries + 1))/$max_retries)"
            sleep 5
            retries=$((retries + 1))
        fi
    done
    
    if [ $retries -eq $max_retries ]; then
        error "PostgreSQL failed to become ready"
        exit 1
    fi
    
    log "Verifying network connectivity..."
    local network_containers=$(docker network inspect docker_data-pipeline-network --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || echo "")
    if echo "$network_containers" | grep -q "postgres" && echo "$network_containers" | grep -q "kafka"; then
        success "All services are connected to the data-pipeline network"
    else
        error "Network connectivity issue detected"
        exit 1
    fi
    
    success "All infrastructure services started successfully"
}

# =============================================================================
# DATA GENERATION PHASE
# =============================================================================

start_iot_producer() {
    step "Starting IoT data producer..."
    
    log "Starting IoT producer for $PRODUCER_DURATION seconds..."
    cd sensor-simulator
    docker compose up -d
    cd ..
    
    log "IoT producer started, generating data for $PRODUCER_DURATION seconds..."
    sleep $PRODUCER_DURATION
    
    log "Stopping IoT producer..."
    cd sensor-simulator
    docker compose stop
    cd ..
    
    success "IoT data generation completed"
}

start_kafka_ingestion() {
    step "Starting Kafka to S3 ingestion..."
    
    log "Starting Kafka ingestion service for $INGESTION_DURATION seconds..."
    export SPARK_APPLICATION_MAIN_CLASS="ingestion.KafkaToS3Ingestion"
    export PROCESS_DATE="$PROCESS_DATE"
    
    docker compose -f docker/docker-compose.pipeline.yml --profile unified up -d unified-job
    
    log "Kafka ingestion running, streaming data to S3 for $INGESTION_DURATION seconds..."
    sleep $INGESTION_DURATION
    
    log "Stopping Kafka ingestion..."
    docker compose -f docker/docker-compose.pipeline.yml stop unified-job
    docker compose -f docker/docker-compose.pipeline.yml rm -f unified-job
    
    success "Kafka to S3 ingestion completed"
}

# =============================================================================
# DATA PROCESSING PHASE
# =============================================================================

run_bronze_job() {
    step "Running Bronze job..."
    
    log "Processing raw data to Bronze layer for date: $PROCESS_DATE"
    export SPARK_APPLICATION_MAIN_CLASS="processing.BronzeJob"
    export PROCESS_DATE="$PROCESS_DATE"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile unified up unified-job; then
        success "Bronze job completed successfully"
    else
        error "Bronze job failed"
        exit 1
    fi
}

run_silver_job() {
    step "Running Silver job..."
    
    log "Processing Bronze to Silver layer for date: $PROCESS_DATE"
    export SPARK_APPLICATION_MAIN_CLASS="processing.SilverJob"
    export PROCESS_DATE="$PROCESS_DATE"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile unified up unified-job; then
        success "Silver job completed successfully"
    else
        error "Silver job failed"
        exit 1
    fi
}

run_gold_job() {
    step "Running Gold job..."
    
    log "Processing Silver to Gold layer for date: $PROCESS_DATE"
    export SPARK_APPLICATION_MAIN_CLASS="processing.GoldJob"
    export PROCESS_DATE="$PROCESS_DATE"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile unified up unified-job; then
        success "Gold job completed successfully"
    else
        error "Gold job failed"
        exit 1
    fi
}

run_grafana_export() {
    step "Running Grafana export job..."
    
    log "Exporting Gold data to PostgreSQL for Grafana for date: $PROCESS_DATE"
    export SPARK_APPLICATION_MAIN_CLASS="processing.GrafanaExportJob"
    export PROCESS_DATE="$PROCESS_DATE"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile unified up unified-job; then
        success "Grafana export completed successfully"
    else
        error "Grafana export failed"
        exit 1
    fi
}

# =============================================================================
# MONITORING & CLEANUP
# =============================================================================

show_pipeline_status() {
    step "Pipeline Status Summary"
    
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                           PIPELINE STATUS SUMMARY                             ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    log "Process Date: $PROCESS_DATE"
    log "Producer Duration: $PRODUCER_DURATION seconds"
    log "Ingestion Duration: $INGESTION_DURATION seconds"
    
    echo ""
    log "Running Docker Services:"
    echo "Core Services:"
    docker compose -f docker/docker-compose.yml ps
    echo ""
    echo "Grafana Services:"
    docker compose -f grafana-config/docker-compose.yml ps
    
    echo ""
    log " Access Points:"
    echo "  Grafana Dashboard:   http://localhost:3001 (admin/admin)"
    echo "  Spark Master UI:     http://localhost:8081"
    echo "  PostgreSQL:          localhost:5432 (grafana/grafana)"
    echo "  Kafka (if needed):    localhost:9092"
    echo "  S3 Data Lake:        Check your S3 bucket: ${S3_BUCKET:-your-bucket}"
    
    echo ""
    if [ "$KEEP_SERVICES_RUNNING" = true ]; then
        success "Pipeline completed successfully! Services are kept running for monitoring"
        echo ""
        log "To stop all services later, run:"
        echo "  ./scripts/stop-all-services.sh"
        echo ""
        log "To stop with cleanup, run:"
        echo "  ./scripts/stop-all-services.sh --clean"
    else
        success "Pipeline execution completed successfully! 🎉"
    fi
}

cleanup_services() {
    if [ "$CLEANUP_ON_EXIT" = true ]; then
        step "Cleaning up services..."
        
        log "Stopping all services..."
        docker compose -f docker/docker-compose.pipeline.yml down 2>/dev/null || true
        docker compose -f grafana-config/docker-compose.yml down 2>/dev/null || true
        docker compose -f docker/docker-compose.yml down 2>/dev/null || true
        
        log "Removing unused containers and networks..."
        docker system prune -f --volumes 2>/dev/null || true
        
        success "Cleanup completed"
    else
        log "Services are kept running. Use ./scripts/stop-all-services.sh to stop them later."
    fi
}

# =============================================================================
# MAIN WORKFLOW
# =============================================================================

main() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --date)
                PROCESS_DATE="$2"
                shift 2
                ;;
            --producer-duration)
                PRODUCER_DURATION="$2"
                shift 2
                ;;
            --ingestion-duration)
                INGESTION_DURATION="$2"
                shift 2
                ;;
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-infrastructure)
                SKIP_INFRASTRUCTURE=true
                shift
                ;;
            --cleanup-on-exit)
                CLEANUP_ON_EXIT=true
                KEEP_SERVICES_RUNNING=false
                shift
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    if [ "$CLEANUP_ON_EXIT" = true ]; then
        trap cleanup_services EXIT
    fi
    
    print_banner
    
    check_prerequisites
    build_images
    start_infrastructure
    start_iot_producer
    start_kafka_ingestion
    run_bronze_job
    run_silver_job
    run_gold_job
    run_grafana_export
    show_pipeline_status
    
    if [ "$CLEANUP_ON_EXIT" = false ]; then
        cleanup_services
    fi
    
    log "Master workflow completed successfully! 🚀"
    
    if [ "$KEEP_SERVICES_RUNNING" = true ]; then
        log "Press Ctrl+C to exit (services will continue running)"
        log "Or run './scripts/stop-all-services.sh' in another terminal to stop services"
        while true; do
            sleep 60
        done
    fi
}

main "$@" 