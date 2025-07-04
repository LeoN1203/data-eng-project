# # spark-ingest-app/Dockerfile
# FROM bitnami/spark:3.3.2

# ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
# ARG SCALA_VERSION=2.13

# WORKDIR /app
# COPY target/scala-${SCALA_VERSION}/${JAR_NAME} ./ingest.jar

# # Default env vars:
# ENV S3_BUCKET=my-iot-bucket
# ENV HOME=/root
# ENV SPARK_HOME=/opt/bitnami/spark

# # Spark submit as entrypoint
# ENTRYPOINT spark-submit \
#   --class ingestion.KafkaS3DataLakePipeline \
#   --master "${SPARK_MASTER}" \
#   --packages "org.apache.kafka:kafka-clients:3.2.0","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
#   --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
#   --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
#   --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
#   --conf spark.hadoop.fs.s3a.endpoint="s3.${AWS_REGION}.amazonaws.com" \
#   --conf spark.jars.ivy=/tmp/.ivy2 \
#   /app/ingest.jar


# Alternative Dockerfile that uses spark-submit with packages
FROM bitnami/spark:3.3.2

USER root

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Create app directory
RUN mkdir -p /app /app/logs
WORKDIR /app

ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
ARG SCALA_VERSION=2.13

WORKDIR /app
COPY target/scala-${SCALA_VERSION}/${JAR_NAME} /app/spark-ingest.jar

# Create entrypoint script that downloads packages automatically
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting Spark application with automatic package download..."\n\
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"\n\
echo "S3 Bucket: $S3_BUCKET"\n\
echo "Spark Master: $SPARK_MASTER"\n\
\n\
# Extract master host and port for health check\n\
MASTER_HOST=$(echo $SPARK_MASTER | sed "s/spark:\\/\\/\\([^:]*\\):.*/\\1/")\n\
MASTER_PORT=$(echo $SPARK_MASTER | sed "s/spark:\\/\\/[^:]*:\\([0-9]*\\).*/\\1/")\n\
\n\
# Wait for Spark master to be ready\n\
echo "Waiting for Spark master at $MASTER_HOST:$MASTER_PORT..."\n\
while ! nc -z $MASTER_HOST $MASTER_PORT; do\n\
  echo "Waiting for Spark master to be ready..."\n\
  sleep 5\n\
done\n\
\n\
echo "Spark master is ready, starting application..."\n\
\n\
# Submit Spark job with packages\n\
/opt/bitnami/spark/bin/spark-submit \\\n\
  --master $SPARK_MASTER \\\n\
  --deploy-mode client \\\n\
  --driver-memory 1g \\\n\
  --executor-memory 2g \\\n\
  --executor-cores 2 \\\n\
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \\\n\
  --conf "spark.sql.streaming.checkpointLocation.deleteTmpCheckpointDir=true" \\\n\
  --conf "spark.sql.adaptive.enabled=true" \\\n\
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \\\n\
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \\\n\
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \\\n\
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \\\n\
  --conf "spark.hadoop.fs.s3a.block.size=134217728" \\\n\
  --conf "spark.hadoop.fs.s3a.multipart.size=67108864" \\\n\
  --conf "spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer" \\\n\
  --conf "spark.jars.ivy=/tmp/.ivy2" \\\n\
  --class ingestion.KafkaS3DataLakePipeline \\\n\
  /app/spark-ingest.jar\n\
' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Switch back to spark user
USER 1001

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

EXPOSE 4040

CMD ["/app/entrypoint.sh"]