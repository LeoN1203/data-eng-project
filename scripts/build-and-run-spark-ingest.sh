docker build -f data-pipeline/spark/Dockerfile -t spark-ingest:latest .

docker run -d \
  --name spark-ingest \
  --network iot-net \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION=eu-west-1 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e S3_BUCKET=my-iot-bucket \
  spark-ingest:latest
