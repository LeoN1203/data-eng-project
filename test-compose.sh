#!/bin/bash

# IoT Alerting Pipeline - Docker Compose Test Script
# This script uses Docker Compose to orchestrate the entire IoT alerting pipeline

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
LOG_FILE="$SCRIPT_DIR/compose_test.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

# Start services with Docker Compose
start_services() {
    log "Starting IoT alerting pipeline with Docker Compose..."
    
    cd "$SCRIPT_DIR"
    
    # Start all services
    docker-compose up -d
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    # Check service status
    docker-compose ps
    
    success "Services started successfully"
}

# Stop services
stop_services() {
    log "Stopping IoT alerting pipeline services..."
    
    cd "$SCRIPT_DIR"
    docker-compose down -v
    
    success "Services stopped successfully"
}

# Send test messages
send_test_messages() {
    log "Sending test messages via Kafka..."
    
    # Send normal data
    log "Sending normal sensor data..."
    echo '{"deviceId":"TEMP_001","temperature":22.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":1720101000000,"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
{"deviceId":"HUM_002","temperature":21.0,"humidity":50.0,"pressure":1012.80,"motion":false,"light":250.0,"acidity":6.8,"location":"Building_B","timestamp":1720101060000,"metadata":{"battery_level":90,"signal_strength":-60,"firmware_version":"1.2.3"}}' | docker exec -i kafka-iot kafka-console-producer --topic iot-sensor-data --bootstrap-server kafka:29092
    
    sleep 5
    
    # Send anomalous data
    log "Sending anomalous sensor data..."
    echo '{"deviceId":"TEMP_001","temperature":95.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":1720101180000,"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
{"deviceId":"BATT_004","temperature":22.0,"humidity":48.0,"pressure":1013.00,"motion":false,"light":280.0,"acidity":7.1,"location":"Building_D","timestamp":1720101240000,"metadata":{"battery_level":15,"signal_strength":-70,"firmware_version":"1.2.3"}}' | docker exec -i kafka-iot kafka-console-producer --topic iot-sensor-data --bootstrap-server kafka:29092
    
    success "Test messages sent"
}

# Check logs
check_logs() {
    log "Checking consumer logs..."
    
    sleep 15
    
    docker-compose logs iot-alerting-pipeline
    
    if docker-compose logs iot-alerting-pipeline 2>&1 | grep -q "ANOMALY DETECTED"; then
        success "Anomalies detected successfully"
    else
        warning "No anomalies found in logs"
    fi
}

# Main execution
case "${1:-start}" in
    start)
        log "Starting full test with Docker Compose"
        > "$LOG_FILE"
        start_services
        send_test_messages
        check_logs
        log "Test completed. Use 'docker-compose logs -f iot-alerting-pipeline' to monitor in real-time"
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 5
        start_services
        ;;
    logs)
        docker-compose logs -f iot-alerting-pipeline
        ;;
    status)
        docker-compose ps
        ;;
    test)
        send_test_messages
        check_logs
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|logs|status|test}"
        echo "  start   - Start all services and run test"
        echo "  stop    - Stop all services"
        echo "  restart - Restart all services"
        echo "  logs    - Follow consumer logs"
        echo "  status  - Show service status"
        echo "  test    - Send test messages and check logs"
        exit 1
        ;;
esac
