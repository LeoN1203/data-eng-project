#!/bin/bash

# IoT Alerting Pipeline - Containerized Test Script
# This script tests the end-to-end IoT alerting pipeline using Docker containers
# It starts Kafka, deploys the alerting pipeline as a Docker container, and tests email alerting

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/containerized_test_consumer.log"
DOCKER_NETWORK="iot-test-network"
KAFKA_CONTAINER="kafka-test"
ZOOKEEPER_CONTAINER="zookeeper-test"
CONSUMER_CONTAINER="iot-alerting-consumer"
KAFKA_TOPIC="iot-sensor-data"

# SMTP Configuration (can be overridden with environment variables)
SMTP_HOST="${SMTP_HOST:-sandbox.smtp.mailtrap.io}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_USER="${SMTP_USER:-}"
SMTP_PASSWORD="${SMTP_PASSWORD:-}"
EMAIL_FROM="${EMAIL_FROM:-iot-alerts@company.com}"
EMAIL_TO="${EMAIL_TO:-admin@company.com}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
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

# Cleanup function
cleanup() {
    log "Cleaning up containers and network..."
    
    # Stop and remove containers
    docker stop $CONSUMER_CONTAINER $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER 2>/dev/null || true
    docker rm $CONSUMER_CONTAINER $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER 2>/dev/null || true
    
    # Remove network
    docker network rm $DOCKER_NETWORK 2>/dev/null || true
    
    log "Cleanup completed"
}

# Setup Docker network
setup_network() {
    log "Setting up Docker network: $DOCKER_NETWORK"
    
    # Remove existing network if it exists
    docker network rm $DOCKER_NETWORK 2>/dev/null || true
    
    # Create new network
    docker network create $DOCKER_NETWORK
    
    success "Docker network created: $DOCKER_NETWORK"
}

# Start Zookeeper container
start_zookeeper() {
    log "Starting Zookeeper container..."
    
    docker run -d \
        --name $ZOOKEEPER_CONTAINER \
        --network $DOCKER_NETWORK \
        -p 2181:2181 \
        -e ZOOKEEPER_CLIENT_PORT=2181 \
        -e ZOOKEEPER_TICK_TIME=2000 \
        confluentinc/cp-zookeeper:latest
    
    # Wait for Zookeeper to be ready
    log "Waiting for Zookeeper to be ready..."
    sleep 10
    
    success "Zookeeper started successfully"
}

# Start Kafka container
start_kafka() {
    log "Starting Kafka container..."
    
    docker run -d \
        --name $KAFKA_CONTAINER \
        --network $DOCKER_NETWORK \
        -p 9092:9092 \
        -e KAFKA_BROKER_ID=1 \
        -e KAFKA_ZOOKEEPER_CONNECT=$ZOOKEEPER_CONTAINER:2181 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://$KAFKA_CONTAINER:29092 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        confluentinc/cp-kafka:latest
    
    # Wait for Kafka to be ready
    log "Waiting for Kafka to be ready..."
    sleep 15
    
    # Create topic
    log "Creating Kafka topic: $KAFKA_TOPIC"
    docker exec $KAFKA_CONTAINER kafka-topics --create \
        --topic $KAFKA_TOPIC \
        --bootstrap-server localhost:29092 \
        --partitions 1 \
        --replication-factor 1 || true
    
    success "Kafka started successfully with topic: $KAFKA_TOPIC"
}

# Build the consumer Docker image
build_consumer_image() {
    log "Building IoT Alerting Pipeline Docker image..."
    
    cd "$SCRIPT_DIR/data-pipeline/spark"
    
    # Build the Docker image
    docker build -t iot-alerting-pipeline:latest .
    
    success "Docker image built successfully: iot-alerting-pipeline:latest"
}

# Start the consumer container
start_consumer() {
    log "Starting IoT Alerting Pipeline consumer container..."
    
    # Check if SMTP credentials are provided
    if [[ -z "$SMTP_USER" || -z "$SMTP_PASSWORD" ]]; then
        warning "SMTP credentials not provided. Email alerts will not work."
        warning "Set SMTP_USER and SMTP_PASSWORD environment variables to enable email alerts."
    fi
    
    docker run -d \
        --name $CONSUMER_CONTAINER \
        --network $DOCKER_NETWORK \
        -e KAFKA_BOOTSTRAP_SERVERS=$KAFKA_CONTAINER:29092 \
        -e KAFKA_TOPIC=$KAFKA_TOPIC \
        -e KAFKA_CONSUMER_GROUP_ID=iot-alerting-consumer-docker \
        -e KAFKA_STARTING_OFFSETS=earliest \
        -e SPARK_APP_NAME=IoT-Kafka-Alerting-Pipeline-Docker \
        -e SPARK_CHECKPOINT_LOCATION=/tmp/kafka-alerting-checkpoints/ \
        -e SPARK_PROCESSING_TIME_SECONDS=5 \
        -e SPARK_LOG_LEVEL=WARN \
        -e ALERT_RECIPIENT_EMAIL=$EMAIL_TO \
        -e SMTP_HOST=$SMTP_HOST \
        -e SMTP_PORT=$SMTP_PORT \
        -e SMTP_USER=$SMTP_USER \
        -e SMTP_PASSWORD=$SMTP_PASSWORD \
        -e EMAIL_FROM_ADDRESS=$EMAIL_FROM \
        -e EMAIL_SUBJECT_PREFIX="[IoT Alert - Docker]" \
        iot-alerting-pipeline:latest
    
    # Wait for consumer to start
    log "Waiting for consumer to start..."
    sleep 20
    
    # Check if container is running
    if docker ps | grep -q $CONSUMER_CONTAINER; then
        success "IoT Alerting Pipeline consumer started successfully"
    else
        error "Failed to start consumer container"
        docker logs $CONSUMER_CONTAINER
        return 1
    fi
}

# Send test messages to Kafka
send_test_messages() {
    log "Sending test messages to Kafka..."
    
    # Normal sensor data (should not trigger alerts)
    log "Sending normal sensor data..."
    docker exec $KAFKA_CONTAINER kafka-console-producer --topic $KAFKA_TOPIC --bootstrap-server localhost:29092 << 'EOF'
{"deviceId":"TEMP_001","temperature":22.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":1720101000000,"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
{"deviceId":"HUM_002","temperature":21.0,"humidity":50.0,"pressure":1012.80,"motion":false,"light":250.0,"acidity":6.8,"location":"Building_B","timestamp":1720101060000,"metadata":{"battery_level":90,"signal_strength":-60,"firmware_version":"1.2.3"}}
{"deviceId":"PRESS_003","temperature":23.0,"humidity":42.0,"pressure":1014.00,"motion":false,"light":320.0,"acidity":7.2,"location":"Building_C","timestamp":1720101120000,"metadata":{"battery_level":88,"signal_strength":-62,"firmware_version":"1.2.3"}}
EOF
    
    sleep 5
    
    # Anomalous sensor data (should trigger alerts)
    log "Sending anomalous sensor data (high temperature)..."
    docker exec $KAFKA_CONTAINER kafka-console-producer --topic $KAFKA_TOPIC --bootstrap-server localhost:29092 << 'EOF'
{"deviceId":"TEMP_001","temperature":95.5,"humidity":45.0,"pressure":1013.25,"motion":false,"light":300.0,"acidity":7.0,"location":"Building_A","timestamp":1720101180000,"metadata":{"battery_level":85,"signal_strength":-65,"firmware_version":"1.2.3"}}
EOF
    
    sleep 5
    
    log "Sending anomalous sensor data (low battery)..."
    docker exec $KAFKA_CONTAINER kafka-console-producer --topic $KAFKA_TOPIC --bootstrap-server localhost:29092 << 'EOF'
{"deviceId":"BATT_004","temperature":22.0,"humidity":48.0,"pressure":1013.00,"motion":false,"light":280.0,"acidity":7.1,"location":"Building_D","timestamp":1720101240000,"metadata":{"battery_level":15,"signal_strength":-70,"firmware_version":"1.2.3"}}
EOF
    
    sleep 5
    
    log "Sending anomalous sensor data (extreme pressure)..."
    docker exec $KAFKA_CONTAINER kafka-console-producer --topic $KAFKA_TOPIC --bootstrap-server localhost:29092 << 'EOF'
{"deviceId":"PRESS_003","temperature":21.5,"humidity":47.0,"pressure":850.00,"motion":false,"light":290.0,"acidity":6.9,"location":"Building_C","timestamp":1720101300000,"metadata":{"battery_level":82,"signal_strength":-68,"firmware_version":"1.2.3"}}
EOF
    
    success "Test messages sent successfully"
}

# Check consumer logs for processing
check_consumer_logs() {
    log "Checking consumer logs for message processing..."
    
    # Wait a bit for processing
    sleep 15
    
    log "Consumer logs:"
    docker logs $CONSUMER_CONTAINER --tail 50
    
    # Check if anomalies were detected
    if docker logs $CONSUMER_CONTAINER 2>&1 | grep -q "ANOMALY DETECTED"; then
        success "Anomaly detection is working - alerts were triggered"
    else
        warning "No anomalies detected in logs. Check if the anomaly detection logic is working correctly."
    fi
    
    # Check if emails were sent (if SMTP is configured)
    if [[ -n "$SMTP_USER" && -n "$SMTP_PASSWORD" ]]; then
        if docker logs $CONSUMER_CONTAINER 2>&1 | grep -q "Alert email sent successfully"; then
            success "Email alerting is working - alerts were sent via SMTP"
        else
            warning "No email alerts found in logs. Check SMTP configuration."
        fi
    fi
}

# Display container status
show_container_status() {
    log "Container status:"
    echo
    docker ps --filter "network=$DOCKER_NETWORK" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo
}

# Main execution
main() {
    log "Starting containerized IoT Alerting Pipeline test..."
    
    # Clear previous log
    > "$LOG_FILE"
    
    # Trap for cleanup on exit
    trap cleanup EXIT INT TERM
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not available in PATH"
        exit 1
    fi
    
    # Check if the IoT alerting pipeline image exists or needs to be built
    if ! docker images | grep -q "iot-alerting-pipeline.*latest"; then
        log "IoT alerting pipeline image not found. Building..."
        build_consumer_image
    else
        log "Using existing IoT alerting pipeline image"
    fi
    
    # Setup and start services
    setup_network
    start_zookeeper
    start_kafka
    start_consumer
    
    # Display status
    show_container_status
    
    # Send test data and monitor
    send_test_messages
    check_consumer_logs
    
    log "Test completed. Check the logs above for results."
    log "To view real-time consumer logs, run: docker logs -f $CONSUMER_CONTAINER"
    log "To cleanup manually, run: docker stop $CONSUMER_CONTAINER $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER && docker rm $CONSUMER_CONTAINER $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER && docker network rm $DOCKER_NETWORK"
    
    success "Containerized test execution completed successfully!"
}

# Script usage
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  --cleanup      Only perform cleanup"
    echo "  --build        Only build the Docker image"
    echo ""
    echo "Environment variables for SMTP configuration:"
    echo "  SMTP_HOST      SMTP server hostname (default: sandbox.smtp.mailtrap.io)"
    echo "  SMTP_PORT      SMTP server port (default: 587)"
    echo "  SMTP_USER      SMTP username"
    echo "  SMTP_PASSWORD  SMTP password"
    echo "  EMAIL_FROM     From email address (default: iot-alerts@company.com)"
    echo "  EMAIL_TO       To email address (default: admin@company.com)"
    echo ""
    echo "Example:"
    echo "  SMTP_USER=your_user SMTP_PASSWORD=your_pass $0"
}

# Handle command line arguments
case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    --cleanup)
        cleanup
        exit 0
        ;;
    --build)
        build_consumer_image
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown option: $1"
        usage
        exit 1
        ;;
esac
