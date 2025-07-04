# spark-ingest-app/Dockerfile
FROM bitnami/spark:3.3.2

ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
ARG SCALA_VERSION=2.12

WORKDIR /app
COPY target/scala-${SCALA_VERSION}/${JAR_NAME} ./ingest.jar

# Set user and create directories as before
USER root

RUN useradd -m -u 1001 spark

RUN mkdir -p /tmp/.ivy2 && \
    chown -R 1001:1001 /app /tmp/.ivy2 && \
    chmod -R 755 /app /tmp/.ivy2
USER 1001

# Simplified Spark submit entrypoint
ENTRYPOINT spark-submit \
  --class ingestion.KafkaS3DataLakePipeline \
  --master "${SPARK_MASTER}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.endpoint="s3.${AWS_REGION}.amazonaws.com" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.driver.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4=true" \
  --conf spark.executor.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4=true" \
  /app/ingest.jar