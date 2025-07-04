#!/bin/bash

# Robust IoT Alerting Pipeline Test Script
# This script automates all the manual steps we successfully tested

set -e  # Exit on any error

echo "=== Robust IoT Alerting Pipeline Test ==="
echo "This script will:"
echo "1. Clean up any existing processes and checkpoints"
echo "2. Start Kafka containers"
echo "3. Create the topic"
echo "4. Start the consumer pipeline"
echo "5. Send test messages (2 wrong schema, 1 correct with anomaly)"
echo "6. Monitor results"
echo ""

# Step 1: Clean up everything first
echo "Step 1: Cleaning up existing processes and checkpoints..."

# Kill any existing SBT/consumer processes
echo "  - Stopping any existing consumer processes..."
pkill -f "KafkaAlertingPipeline" 2>/dev/null || echo "    No existing consumer processes found"
pkill -f sbt 2>/dev/null || echo "    No existing SBT processes found"
sleep 3

# Clean checkpoints
echo "  - Cleaning Spark checkpoints..."
rm -rf /tmp/kafka-alerting-checkpoints/* 2>/dev/null || echo "    Checkpoints already clean"

# Reset Kafka consumer group offsets
echo "  - Resetting Kafka consumer group offsets..."
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group iot-alerting-consumer 2>/dev/null || echo "    Consumer group already clean"

echo "  ✓ Cleanup complete"
echo ""

# Step 2: Start Kafka containers
echo "Step 2: Starting Kafka containers..."
docker-compose -f docker-compose-kafka.yml up -d

# Step 3: Wait for Kafka to be ready
echo "Step 3: Waiting for Kafka to be ready..."
sleep 15

# Check if Kafka is ready with timeout
TIMEOUT=60
ELAPSED=0
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "❌ Kafka failed to start within $TIMEOUT seconds"
        exit 1
    fi
    echo "   Waiting for Kafka... ($ELAPSED/$TIMEOUT seconds)"
    sleep 3
    ELAPSED=$((ELAPSED + 3))
done
echo "  ✓ Kafka is ready!"
echo ""

# Step 4: Create topic
echo "Step 4: Creating Kafka topic..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic iot-sensor-data --partitions 3 --replication-factor 1
echo "  ✓ Topic created"
echo ""

# Step 5: Start consumer pipeline
echo "Step 5: Starting consumer pipeline..."
LOG_FILE="$(pwd)/robust_test_consumer.log"
cd spark
nohup sbt "runMain processing.KafkaAlertingPipeline" > "$LOG_FILE" 2>&1 &
CONSUMER_PID=$!
echo "  Consumer started with PID: $CONSUMER_PID"
echo "  Log file: $LOG_FILE"

# Step 6: Wait for consumer to initialize
echo "Step 6: Waiting for consumer to initialize..."
TIMEOUT=60
ELAPSED=0
while ! grep -q "Pipeline Starting" "$LOG_FILE" 2>/dev/null; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "❌ Consumer failed to start within $TIMEOUT seconds"
        echo "Recent log output:"
        tail -20 "$LOG_FILE" 2>/dev/null || echo "Log file not found"
        kill $CONSUMER_PID 2>/dev/null
        exit 1
    fi
    echo "   Waiting for consumer initialization... ($ELAPSED/$TIMEOUT seconds)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

# Wait a bit more for full initialization
sleep 10
echo "  ✓ Consumer initialized"
echo ""

# Step 7: Send test messages
echo "Step 7: Sending test messages..."

# 7a. Send message with wrong schema (device_id instead of deviceId)
echo "  7a. Sending message with wrong schema (device_id)..."
echo '{"device_id": "wrong-schema-test", "temperature": 90.0, "humidity": 60.0}' | \
    docker exec -i kafka kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092
sleep 3

# 7b. Send message with missing required field (location)
echo "  7b. Sending message with missing location field..."
echo '{"deviceId": "missing-location", "temperature": 85.0}' | \
    docker exec -i kafka kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092
sleep 3

# 7c. Send valid message that should trigger an alert
echo "  7c. Sending valid anomaly message (temperature 95°C)..."
TIMESTAMP=$(date +%s)
echo "{\"deviceId\": \"test-device-anomaly\", \"temperature\": 95.0, \"humidity\": 75.0, \"location\": \"test-lab\", \"timestamp\": $TIMESTAMP}" | \
    docker exec -i kafka kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092

echo "  ✓ All test messages sent"
echo ""

# Step 8: Wait and monitor results
echo "Step 8: Monitoring results (waiting 30 seconds for processing)..."
sleep 30

echo ""
echo "=== RESULTS ==="

# Check batch processing
echo "📊 Batch Processing Summary:"
grep "Processing batch" "$LOG_FILE" | while read -r line; do
    echo "  $line"
done

echo ""

# Check schema validation (should see empty batches for wrong schema)
EMPTY_BATCHES=$(grep -c "0 records" "$LOG_FILE" 2>/dev/null || echo "0")
VALID_BATCHES=$(grep "Processing batch.*with [1-9]" "$LOG_FILE" | wc -l 2>/dev/null || echo "0")

echo "🔍 Schema Validation Results:"
echo "  - Empty batches (wrong schema filtered): $EMPTY_BATCHES"
echo "  - Valid batches (correct schema processed): $VALID_BATCHES"

if [ "$EMPTY_BATCHES" -ge 2 ] && [ "$VALID_BATCHES" -ge 1 ]; then
    echo "  ✅ Schema validation working correctly"
else
    echo "  ⚠️  Unexpected schema validation results"
fi

echo ""

# Check anomaly detection and alerts
echo "🚨 Anomaly Detection and Alert Results:"
if grep -q "ANOMALIES DETECTED" "$LOG_FILE"; then
    echo "  ✅ Anomaly detection: WORKING"
    grep "ANOMALIES DETECTED" "$LOG_FILE" | sed 's/^/    /'
else
    echo "  ❌ Anomaly detection: NO ANOMALIES DETECTED"
fi

if grep -q "Alert sent" "$LOG_FILE"; then
    echo "  ✅ Alert sending: WORKING"
    grep -E "(Sending alert|Alert sent)" "$LOG_FILE" | sed 's/^/    /'
else
    echo "  ❌ Alert sending: NO ALERTS SENT"
fi

echo ""

# Check for any errors
echo "🔧 Error Check:"
if grep -qi "error\|exception\|failed" "$LOG_FILE"; then
    echo "  ⚠️  Potential issues found:"
    grep -i "error\|exception\|failed" "$LOG_FILE" | head -5 | sed 's/^/    /'
else
    echo "  ✅ No errors detected"
fi

echo ""
echo "=== SUMMARY ==="

# Overall status
PIPELINE_SUCCESS=true

if [ "$EMPTY_BATCHES" -lt 2 ] || [ "$VALID_BATCHES" -lt 1 ]; then
    echo "❌ Schema validation not working as expected"
    PIPELINE_SUCCESS=false
fi

if ! grep -q "ANOMALIES DETECTED" "$LOG_FILE"; then
    echo "❌ Anomaly detection not working"
    PIPELINE_SUCCESS=false
fi

if ! grep -q "Alert sent" "$LOG_FILE"; then
    echo "❌ Alert system not working"
    PIPELINE_SUCCESS=false
fi

if [ "$PIPELINE_SUCCESS" = true ]; then
    echo "🎉 SUCCESS: IoT Alerting Pipeline is working correctly!"
    echo "   - Schema validation: ✅"
    echo "   - Anomaly detection: ✅"
    echo "   - Alert system: ✅"
    echo ""
    echo "📧 Check your email inbox for the alert!"
else
    echo "❌ ISSUES DETECTED: Pipeline needs attention"
fi

echo ""
echo "=== PROCESS INFORMATION ==="
echo "Consumer PID: $CONSUMER_PID (still running)"
echo "Log file: $LOG_FILE"
echo "Kafka containers: running"
echo ""
echo "To view live logs: tail -f $LOG_FILE"
echo "To stop everything:"
echo "  Consumer: kill $CONSUMER_PID"
echo "  Containers: docker-compose -f docker-compose-kafka.yml down"
echo "  Checkpoints: rm -rf /tmp/kafka-alerting-checkpoints/*"
echo ""
echo "=== Test Complete ==="
