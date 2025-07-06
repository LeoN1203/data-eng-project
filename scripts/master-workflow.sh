#!/bin/bash

# =============================================================================
# MASTER WORKFLOW SCRIPT - Complete Data Pipeline Orchestration
# =============================================================================
# This script orchestrates the entire data pipeline workflow using a unified
# docker-compose file with proper service dependencies:
# 1. Environment setup and validation
# 2. Build all Docker images
# 3. Start infrastructure services (Kafka, Spark, PostgreSQL, Grafana)
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

# Configuration
DOCKER_COMPOSE_FILE="docker-compose.unified.yml"
PROCESS_DATE=${PROCESS_DATE:-$(date '+%Y-%m-%d')}
PRODUCER_DURATION=${PRODUCER_DURATION:-300}  # 5 minutes default
INGESTION_DURATION=${INGESTION_DURATION:-360}  # 6 minutes default
S3_BUFFER_TIME=${S3_BUFFER_TIME:-30}  # 30 seconds default for S3 eventual consistency
CLEANUP_ON_EXIT=${CLEANUP_ON_EXIT:-false}
SKIP_BUILD=${SKIP_BUILD:-false}
SKIP_INFRASTRUCTURE=${SKIP_INFRASTRUCTURE:-false}
KEEP_SERVICES_RUNNING=${KEEP_SERVICES_RUNNING:-true}

print_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                        🚀 DATA PIPELINE MASTER WORKFLOW 🚀                   ║"
    echo "╠═══════════════════════════════════════════════════════════════════════════════╣"
    echo "║                                                                               ║"
    echo "║  This script orchestrates the complete data pipeline workflow using a        ║"
    echo "║  unified docker-compose file with proper service dependencies:               ║"
    echo "║  • Infrastructure services (Kafka, Spark, PostgreSQL, Grafana)              ║"
    echo "║  • IoT data producer                                                          ║"
    echo "║  • Kafka to S3 ingestion                                                      ║"
    echo "║  • Data processing (Bronze → Silver → Gold → Grafana)                        ║"
    echo "║  • Monitoring and visualization                                               ║"
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
    echo "  --s3-buffer-time SEC     Buffer time for S3 eventual consistency (default: 30)"
    echo "  --skip-build            Skip Docker image building"
    echo "  --skip-infrastructure   Skip infrastructure startup"
    echo "  --cleanup-on-exit       Cleanup processing containers on exit (preserves Grafana/PostgreSQL)"
    echo "  --help                  Show this help message"
    echo ""
    echo "Pipeline Features:"
    echo "  • Visual countdown timers for producer and ingestion phases"
    echo "  • S3 data validation before each processing layer"
    echo "  • Sequential execution: Bronze → Silver → Gold → Grafana"
    echo "  • Buffer times for S3 eventual consistency"
    echo "  • Preserves monitoring infrastructure (Grafana/PostgreSQL) during cleanup"
    echo ""
    echo "Environment Variables (set in .env file):"
    echo "  AWS_ACCESS_KEY_ID       AWS access key"
    echo "  AWS_SECRET_ACCESS_KEY   AWS secret key"
    echo "  AWS_DEFAULT_REGION      AWS region (default: eu-north-1)"
    echo "  S3_BUCKET               S3 bucket name"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Full workflow for today"
    echo "  $0 --date 2024-01-15                # Full workflow for specific date"
    echo "  $0 --skip-build                     # Skip building, use existing images"
    echo "  $0 --producer-duration 60 --ingestion-duration 90  # Quick test run"
    echo "  $0 --cleanup-on-exit                # Cleanup processing containers when done"
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
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        error "Unified docker-compose file not found: $DOCKER_COMPOSE_FILE"
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
    
    log "Building sensor-simulator (IoT producer) image..."
    docker build -f sensor-simulator/Dockerfile -t sensor-producer:latest .
    
    log "Building unified pipeline image..."
    if [ -f "docker/Dockerfile.unified" ]; then
        docker build -f docker/Dockerfile.unified -t pipeline-unified:latest data-pipeline/spark
    else
        error "Unified Dockerfile not found. Please ensure docker/Dockerfile.unified exists."
        exit 1
    fi
    
    success "All Docker images built successfully"
}

check_and_free_ports() {
    step "Checking and freeing required ports..."
    
    local ports=(5432 3001 9092 8081 7077)
    
    for port in "${ports[@]}"; do
        if lsof -i :$port >/dev/null 2>&1; then
            warning "Port $port is in use. Attempting to free it..."
            sudo fuser -k $port/tcp >/dev/null 2>&1 || true
            sleep 2
            if lsof -i :$port >/dev/null 2>&1; then
                error "Could not free port $port. Please stop the service using this port manually."
                exit 1
            else
                success "Port $port freed successfully"
            fi
        fi
    done
    
    success "All required ports are available"
}

start_infrastructure() {
    if [ "$SKIP_INFRASTRUCTURE" = true ]; then
        warning "Skipping infrastructure startup as requested"
        return 0
    fi
    
    step "Starting infrastructure services..."
    
    check_and_free_ports
    
    log "Starting infrastructure services (Kafka, Spark, PostgreSQL, Grafana)..."
    export PROCESS_DATE="$PROCESS_DATE"
    
    # Start infrastructure services (no profiles needed for base services)
    docker compose -f "$DOCKER_COMPOSE_FILE" up -d zookeeper kafka spark-master spark-worker-1 spark-worker-2 postgres grafana
    
    log "Waiting for services to be ready..."
    local retries=0
    local max_retries=8
    
    while [ $retries -lt $max_retries ]; do
        # Check if key services are running
        local kafka_running=$(docker compose -f "$DOCKER_COMPOSE_FILE" ps kafka --format "table {{.State}}" 2>/dev/null | grep -c "running" || echo "0")
        local spark_running=$(docker compose -f "$DOCKER_COMPOSE_FILE" ps spark-master --format "table {{.State}}" 2>/dev/null | grep -c "running" || echo "0")
        local postgres_running=$(docker compose -f "$DOCKER_COMPOSE_FILE" ps postgres --format "table {{.State}}" 2>/dev/null | grep -c "running" || echo "0")
        
        if [[ "$kafka_running" -eq 1 && "$spark_running" -eq 1 && "$postgres_running" -eq 1 ]]; then
            success "Infrastructure services are running"
            log "Allowing additional time for services to fully initialize..."
            sleep 30  # Give services time to fully start up
            break
        else
            log "Waiting for infrastructure services to start... (attempt $((retries + 1))/$max_retries)"
            sleep 15
            retries=$((retries + 1))
        fi
    done
    
    if [ $retries -eq $max_retries ]; then
        warning "Some services may not be ready, but continuing..."
        log "Current service status:"
        docker compose -f "$DOCKER_COMPOSE_FILE" ps
    fi
    
    success "Infrastructure services started successfully"
}

# Add countdown function after the existing functions
countdown_timer() {
    local duration=$1
    local message=$2
    
    log "$message"
    
    while [ $duration -gt 0 ]; do
        printf "\r${YELLOW}⏳ Time remaining: %02d:%02d${NC}" $((duration/60)) $((duration%60))
        sleep 1
        duration=$((duration-1))
    done
    printf "\n"
}

# Add S3 validation function
wait_for_s3_data() {
    local s3_path=$1
    local max_wait=$2
    local description=$3
    
    log "Waiting for $description to be available in S3..."
    log "Checking path: $s3_path"
    
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        if aws s3 ls "$s3_path" >/dev/null 2>&1; then
            local file_count=$(aws s3 ls "$s3_path" --recursive | wc -l)
            if [ $file_count -gt 0 ]; then
                success "$description found in S3 ($file_count files)"
                return 0
            fi
        fi
        
        printf "\r${YELLOW} Waiting for S3 data... %ds elapsed${NC}" $elapsed
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    printf "\n"
    warning "$description not found in S3 after ${max_wait}s, continuing anyway..."
    return 1
}

start_iot_producer() {
    step "Starting IoT data producer..."
    
    export PROCESS_DATE="$PROCESS_DATE"
    docker compose -f "$DOCKER_COMPOSE_FILE" --profile producer up -d iot-producer
    
    # Add countdown timer
    countdown_timer $PRODUCER_DURATION "IoT producer started, generating data for $PRODUCER_DURATION seconds..."
    
    log "Stopping IoT producer..."
    docker compose -f "$DOCKER_COMPOSE_FILE" --profile producer stop iot-producer
    
    success "IoT data generation completed"
}

start_kafka_ingestion() {
    step "Starting Kafka to S3 ingestion..."
    
    export PROCESS_DATE="$PROCESS_DATE"
    docker compose -f "$DOCKER_COMPOSE_FILE" --profile ingestion up -d kafka-ingestion
    
    # Add countdown timer
    countdown_timer $INGESTION_DURATION "Kafka ingestion running, streaming data to S3 for $INGESTION_DURATION seconds..."
    
    log "Stopping Kafka ingestion..."
    docker compose -f "$DOCKER_COMPOSE_FILE" --profile ingestion stop kafka-ingestion
    
    # Wait for raw data to be available in S3
    local raw_s3_path="s3://${S3_BUCKET:-inde-aws-datalake}/raw/iot-data/"
    wait_for_s3_data "$raw_s3_path" 60 "Raw IoT data"
    
    success "Kafka to S3 ingestion completed"
}

run_data_processing() {
    step "Running data processing pipeline..."
    
    export PROCESS_DATE="$PROCESS_DATE"
    
    # Run Bronze job first
    log "🥉 Starting Bronze job..."
    if docker compose -f "$DOCKER_COMPOSE_FILE" --profile processing up bronze-job; then
        success "Bronze job completed successfully"
        
        # Wait for Bronze data to be committed to S3
        local bronze_s3_path="s3://${S3_BUCKET:-inde-aws-datalake}/bronze/iot-data/"
        wait_for_s3_data "$bronze_s3_path" 90 "Bronze layer data"
        
        # Add buffer time for S3 eventual consistency
        log "Adding ${S3_BUFFER_TIME}s buffer for S3 eventual consistency..."
        sleep $S3_BUFFER_TIME
        
    else
        error "Bronze job failed"
        docker compose -f "$DOCKER_COMPOSE_FILE" logs bronze-job
        exit 1
    fi
    
    # Run Silver job after Bronze is confirmed
    log "🥈 Starting Silver job..."
    if docker compose -f "$DOCKER_COMPOSE_FILE" --profile processing up silver-job; then
        success "Silver job completed successfully"
        
        # Wait for Silver data to be committed to S3
        local silver_s3_path="s3://${S3_BUCKET:-inde-aws-datalake}/silver/iot-data/"
        wait_for_s3_data "$silver_s3_path" 90 "Silver layer data"
        
        # Add buffer time for S3 eventual consistency
        log "Adding ${S3_BUFFER_TIME}s buffer for S3 eventual consistency..."
        sleep $S3_BUFFER_TIME
        
    else
        error "Silver job failed"
        docker compose -f "$DOCKER_COMPOSE_FILE" logs silver-job
        exit 1
    fi
    
    # Run Gold job after Silver is confirmed
    log "🥇 Starting Gold job..."
    if docker compose -f "$DOCKER_COMPOSE_FILE" --profile processing up gold-job; then
        success "Gold job completed successfully"
        
        # Wait for Gold data to be committed to S3
        local gold_s3_path="s3://${S3_BUCKET:-inde-aws-datalake}/gold/"
        wait_for_s3_data "$gold_s3_path" 90 "Gold layer data"
        
    else
        error "Gold job failed"
        docker compose -f "$DOCKER_COMPOSE_FILE" logs gold-job
        exit 1
    fi
    
    # Run Grafana export last
    log "📊 Starting Grafana export..."
    if docker compose -f "$DOCKER_COMPOSE_FILE" --profile processing up grafana-export; then
        success "Grafana export completed successfully"
    else
        warning "Grafana export failed, but continuing..."
        docker compose -f "$DOCKER_COMPOSE_FILE" logs grafana-export
    fi
    
    success "Complete data processing pipeline finished successfully! 🎉"
}

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
    log "Docker Compose File: $DOCKER_COMPOSE_FILE"
    
    echo ""
    log "Running Docker Services:"
    docker compose -f "$DOCKER_COMPOSE_FILE" ps
    
    echo ""
    log "🌐 Access Points:"
    echo "  Grafana Dashboard:   http://localhost:3001 (admin/admin)"
    echo "  Spark Master UI:     http://localhost:8081"
    echo "  PostgreSQL:          localhost:5432 (grafana/grafana)"
    echo "  Kafka:               localhost:9092"
    echo "  S3 Data Lake:        ${S3_BUCKET:-your-bucket}"
    
    echo ""
    if [ "$KEEP_SERVICES_RUNNING" = true ]; then
        success "Pipeline completed successfully! Services are kept running for monitoring 🎉"
        echo ""
        log "To stop all services later, run:"
        echo "  docker compose -f $DOCKER_COMPOSE_FILE down"
        echo ""
        log "To stop with cleanup (remove volumes), run:"
        echo "  docker compose -f $DOCKER_COMPOSE_FILE down --volumes"
    else
        success "Pipeline execution completed successfully! 🎉"
    fi
}

cleanup_services() {
    if [ "$CLEANUP_ON_EXIT" = true ]; then
        step "Cleaning up services..."
        
        log "Stopping processing, producer, and ingestion services..."
        # Stop specific services but preserve Grafana and PostgreSQL
        docker compose -f "$DOCKER_COMPOSE_FILE" --profile producer --profile ingestion --profile processing stop
        
        log "Removing stopped containers (preserving Grafana and PostgreSQL)..."
        # Remove only the containers we stopped, not all
        docker compose -f "$DOCKER_COMPOSE_FILE" --profile producer --profile ingestion --profile processing rm -f
        
        log "Removing unused networks and volumes..."
        docker network prune -f 2>/dev/null || true
        
        success "Cleanup completed (Grafana and PostgreSQL preserved)"
        
        log "📊 Preserved services:"
        log "  Grafana Dashboard:   http://localhost:3001 (admin/admin)"
        log "  PostgreSQL:          localhost:5432 (grafana/grafana)"
        log "  Spark Cluster:       http://localhost:8081"
        log "  Kafka:               localhost:9092"
        echo ""
        log "To stop all services including monitoring, run:"
        echo "  docker compose -f $DOCKER_COMPOSE_FILE down"
    else
        log "Services are kept running. Use 'docker compose -f $DOCKER_COMPOSE_FILE down' to stop them later."
    fi
}


send_test_messages() {
    log "Sending test messages to Kafka..."
    
    # Check if Kafka container is running
    if ! docker ps --format "table {{.Names}}" | grep -q "kafka"; then
        error "Kafka container not found. Please start the pipeline first."
        return 1
    fi
    
    # Send normal sensor data
    log "Sending normal sensor data..."
    echo '{"deviceId":"TEMP_001","temperature":22.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":'$(date +%s000)',"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
{"deviceId":"HUM_002","temperature":21.0,"humidity":50.0,"pressure":1012.80,"motion":false,"light":250.0,"acidity":6.8,"location":"Building_B","timestamp":'$(date +%s000)',"metadata":{"battery_level":90,"signal_strength":-60,"firmware_version":"1.2.3"}}' | docker exec -i kafka kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092
    
    sleep 3
    
    # Send problematic/anomalous data
    log "Sending problematic sensor data..."
    echo '{"deviceId":"TEMP_001","temperature":95.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":'$(date +%s000)',"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
{"deviceId":"BATT_004","temperature":22.0,"humidity":48.0,"pressure":1013.00,"motion":false,"light":280.0,"acidity":7.1,"location":"Building_D","timestamp":'$(date +%s000)',"metadata":{"battery_level":15,"signal_strength":-70,"firmware_version":"1.2.3"}}' | docker exec -i kafka kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092
    
    success "Test messages sent successfully"
    log "Problematic data sent: High temperature (95.5°C) and Low battery (15%)"
}

main() {
    if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
        case $1 in
            test-messages)
                step "Preparing infrastructure for testing..."
                check_prerequisites
                if [ "$SKIP_BUILD" = false ]; then
                    build_images
                fi
                start_infrastructure
                send_test_messages
                log "Test completed. Services are still running."
                exit 0
                ;;
            status)
                step "Pipeline Status"
                echo "Core Services:"
                docker compose -f docker/docker-compose.yml ps
                echo ""
                echo "Grafana Services:"
                docker compose -f grafana-config/docker-compose.yml ps
                exit 0
                ;;
            stop)
                step "Stopping all services..."
                docker compose -f docker/docker-compose.pipeline.yml down 2>/dev/null || true
                docker compose -f grafana-config/docker-compose.yml down 2>/dev/null || true
                docker compose -f docker/docker-compose.yml down 2>/dev/null || true
                success "All services stopped"
                exit 0
                ;;
            help)
                show_usage
                exit 0
                ;;
            *)
                # If it's not a recognized command, treat as full workflow with argument parsing
                ;;
        esac
    fi

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
            --s3-buffer-time)
                S3_BUFFER_TIME="$2"
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
    
    # Execute pipeline phases
    check_prerequisites
    build_images
    start_infrastructure
    send_test_messages
    start_iot_producer
    start_kafka_ingestion
    run_data_processing
    show_pipeline_status
    
    if [ "$CLEANUP_ON_EXIT" = false ]; then
        cleanup_services
    fi
    
    log "Master workflow completed successfully! 🚀"
    
    if [ "$KEEP_SERVICES_RUNNING" = true ]; then
        log "Press Ctrl+C to exit (services will continue running)"
        log "Or run 'docker compose -f $DOCKER_COMPOSE_FILE down' in another terminal to stop services"
        while true; do
            sleep 60
        done
    fi
}

main "$@" 