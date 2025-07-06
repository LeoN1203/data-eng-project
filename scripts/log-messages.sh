#!/bin/bash

docker exec -i spark-ingestion-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning \
  --max-messages 1