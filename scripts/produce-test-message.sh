#!/bin/bash

# Produce a test message to the topic from inside the Kafka container
echo '{"deviceId":"test-001","temperature":"25.0","humidity":"70.0","pressure":"1015.0","motion":"false","acidity":"7.0","location":"lab","timestamp":1720000000000,"metadata":{"battery_level":90,"signal_strength":-60,"firmware_version":"1.0.0"}}' | \
docker exec -i spark-ingestion-kafka-1 kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:9092