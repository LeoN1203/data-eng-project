#!/bin/bash

# Create the Kafka topic inside the Kafka container
docker exec spark-ingestion-kafka-1 kafka-topics --create \
  --topic iot-sensor-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1