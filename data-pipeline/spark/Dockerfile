# spark-ingest-app/Dockerfile
FROM bitnami/spark:3.3.2

ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
ARG SCALA_VERSION=2.13

WORKDIR /app
COPY data-pipeline/spark/target/scala-${SCALA_VERSION}/${JAR_NAME} ./ingest.jar

# Default env vars:
ENV S3_BUCKET=my-iot-bucket
ENV HOME=/tmp

# Spark submit as entrypoint
ENTRYPOINT spark-submit \
  --class com.mycompany.IngestApp \
  --master "${SPARK_MASTER}" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.endpoint="s3.${AWS_REGION}.amazonaws.com" \
  /app/ingest.jar