#!/bin/bash

# Set environment variables for Spark
export SPARK_HOME=${SPARK_HOME:-/opt/bitnami/spark}
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-0.0.0.0}
export SPARK_DRIVER_BIND_ADDRESS=${SPARK_DRIVER_BIND_ADDRESS:-0.0.0.0}
export SPARK_DRIVER_HOST=${SPARK_DRIVER_HOST:-localhost}
export SPARK_DRIVER_PORT=${SPARK_DRIVER_PORT:-4040}
export SPARK_BLOCKMANAGER_PORT=${SPARK_BLOCKMANAGER_PORT:-4041}

# Set HOME environment variable to fix Ivy repository issue
export HOME=/tmp

# Disable Ivy dependency resolution
export SPARK_SUBMIT_OPTS="--conf spark.jars.ivy=/tmp/.ivy2"

# Set default values if not provided
SPARK_APPLICATION_MAIN_CLASS=${SPARK_APPLICATION_MAIN_CLASS:-"processing.BronzeJob"}
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"2g"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"2g"}
SPARK_TOTAL_EXECUTOR_CORES=${SPARK_TOTAL_EXECUTOR_CORES:-4}
PROCESS_DATE=${PROCESS_DATE:-$(date '+%Y-%m-%d')}

# Build spark-submit command
SPARK_SUBMIT_CMD="$SPARK_HOME/bin/spark-submit \
  --class $SPARK_APPLICATION_MAIN_CLASS \
  --master $SPARK_MASTER \
  --deploy-mode client \
  --driver-memory $SPARK_DRIVER_MEMORY \
  --executor-memory $SPARK_EXECUTOR_MEMORY \
  --total-executor-cores $SPARK_TOTAL_EXECUTOR_CORES \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 \
  --conf spark.hadoop.fs.s3a.attempts.maximum=10 \
  --conf spark.hadoop.fs.s3a.retry.interval=1000ms \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=10000 \
  /opt/spark-apps/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar \
  $PROCESS_DATE"

echo "Starting Spark job with:"
echo "  Main Class: $SPARK_APPLICATION_MAIN_CLASS"
echo "  Master: $SPARK_MASTER"
echo "  Driver Memory: $SPARK_DRIVER_MEMORY"
echo "  Executor Memory: $SPARK_EXECUTOR_MEMORY"
echo "  Process Date: $PROCESS_DATE"

# Execute the command
exec $SPARK_SUBMIT_CMD 