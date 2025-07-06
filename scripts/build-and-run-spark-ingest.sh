docker build -f data-pipeline/spark/Dockerfile -t spark-ingest:latest .

# Start Spark master (if not running)
if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  docker run -d \
    --name spark-master \
    --network kafka-broker_iot-net \
    -e SPARK_MODE=master \
    -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
    -e SPARK_RPC_ENCRYPTION_ENABLED=no \
    -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    -e SPARK_SSL_ENABLED=no \
    -p 7077:7077 \
    -p 8080:8080 \
    bitnami/spark:3.3.2
fi

# Start Spark worker (if not running)
if ! docker ps --format '{{.Names}}' | grep -q '^spark-worker$'; then
  docker run -d \
    --name spark-worker \
    --network kafka-broker_iot-net \
    -e SPARK_MODE=worker \
    -e SPARK_MASTER_URL=spark://spark-master:7077 \
    bitnami/spark:3.3.2
fi

docker run -d \
  --name spark-ingest \
  --network kafka-broker_iot-net \
  -e HOME=/tmp \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION=$AWS_REGION \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e S3_BUCKET=my-iot-bucket \
  -e SPARK_MASTER=spark://spark-master:7077 \
  spark-ingest:latest
