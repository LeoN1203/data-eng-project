#!/bin/bash

# Start Zookeeper container
docker run -d --name zookeeper \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  confluentinc/cp-zookeeper:latest