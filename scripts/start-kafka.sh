#!/bin/bash

# Start Kafka container (waits for Zookeeper)
docker run -d --name kafka \
  -p 9092:9092 \
  --link zookeeper \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  # define two listeners
  -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092 \
  # advertise them appropriately
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9093,EXTERNAL://localhost:9092 \
  # map protocols
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
  # inter‑broker talk over the internal listener
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.2.1
