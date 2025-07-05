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

check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! docker ps >/dev/null 2>&1; then
        error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    if [ ! -f ".env" ]; then
        error ".env file not found. Please create it with AWS credentials."
        exit 1
    fi
    
    source ".env"
    
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        error "AWS credentials not set. Please check your .env file."
        exit 1
    fi
    
    success "Prerequisites check passed"
}

run_job() {
    local job_type=$1
    local process_date=$2
    
    log "Running $job_type job for date: $process_date"
    
    export PROCESS_DATE="$process_date"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile "$job_type" up --build --abort-on-container-exit; then
        success "$job_type job completed successfully"
    else
        error "$job_type job failed"
        return 1
    fi
    
    docker compose -f docker/docker-compose.pipeline.yml --profile "$job_type" down
}

run_unified_job() {
    local main_class=$1
    local process_date=$2
    
    log "Running unified job: $main_class for date: $process_date"
    
    export PROCESS_DATE="$process_date"
    export SPARK_APPLICATION_MAIN_CLASS="$main_class"
    
    if docker compose -f docker/docker-compose.pipeline.yml --profile unified up --build --abort-on-container-exit; then
        success "Unified job ($main_class) completed successfully"
    else
        error "Unified job ($main_class) failed"
        return 1
    fi
    
    docker compose -f docker/docker-compose.pipeline.yml --profile unified down
}

run_complete_pipeline() {
    local process_date=$1
    
    log "Running complete pipeline for date: $process_date"
    
    log "Step 1/4: Running Bronze job..."
    if ! run_unified_job "processing.BronzeJob" "$process_date"; then
        error "Bronze job failed. Pipeline aborted."
        return 1
    fi
    
    log "Step 2/4: Running Silver job..."
    if ! run_unified_job "processing.SilverJob" "$process_date"; then
        error "Silver job failed. Pipeline aborted."
        return 1
    fi
    
    log "Step 3/4: Running Gold job..."
    if ! run_unified_job "processing.GoldJob" "$process_date"; then
        error "Gold job failed. Pipeline aborted."
        return 1
    fi
    
    log "Step 4/4: Running Grafana export job..."
    if ! run_unified_job "processing.GrafanaExportJob" "$process_date"; then
        error "Grafana export job failed. Pipeline aborted."
        return 1
    fi
    
    success "Complete pipeline executed successfully!"
    success "Data is now available in PostgreSQL for Grafana dashboards!"
}

show_usage() {
    echo "Usage: $0 [job_type] [date]"
    echo ""
    echo "Job Types:"
    echo "  bronze    - Run only Bronze job"
    echo "  silver    - Run only Silver job"
    echo "  gold      - Run only Gold job"
    echo "  grafana   - Run only Grafana export job"
    echo "  all       - Run complete pipeline (default)"
    echo ""
    echo "Date Format: YYYY-MM-DD (default: today)"
    echo ""
    echo "Examples:"
    echo "  $0 bronze 2024-01-15    # Run Bronze job for specific date"
    echo "  $0 grafana              # Run Grafana export for today"
    echo "  $0 all                  # Run complete pipeline for today"
    echo "  $0                      # Run complete pipeline for today"
    echo ""
    echo "Environment Variables (set in .env file):"
    echo "  AWS_ACCESS_KEY_ID"
    echo "  AWS_SECRET_ACCESS_KEY"
    echo "  AWS_DEFAULT_REGION"
    echo "  POSTGRES_HOST (default: localhost)"
    echo "  POSTGRES_PORT (default: 5432)"
    echo "  POSTGRES_DB (default: grafana_db)"
    echo "  POSTGRES_USER (default: grafana)"
    echo "  POSTGRES_PASSWORD (default: grafana)"
}

main() {
    local job_type=${1:-all}
    local process_date=${2:-$(date '+%Y-%m-%d')}
    
    case $job_type in
        bronze)
            check_prerequisites
            run_unified_job "processing.BronzeJob" "$process_date"
            ;;
        silver)
            check_prerequisites
            run_unified_job "processing.SilverJob" "$process_date"
            ;;
        gold)
            check_prerequisites
            run_unified_job "processing.GoldJob" "$process_date"
            ;;
        grafana)
            check_prerequisites
            run_unified_job "processing.GrafanaExportJob" "$process_date"
            ;;
        all)
            check_prerequisites
            run_complete_pipeline "$process_date"
            ;;
        help|--help|-h)
            show_usage
            exit 0
            ;;
        *)
            error "Unknown job type: $job_type"
            show_usage
            exit 1
            ;;
    esac
}

main "$@" 