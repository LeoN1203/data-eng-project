#!/bin/bash

# Set default values if not provided
SPARK_APPLICATION_MAIN_CLASS=${SPARK_APPLICATION_MAIN_CLASS:-"processing.BronzeJob"}
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"2g"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"2g"}
PROCESS_DATE=${PROCESS_DATE:-$(date '+%Y-%m-%d')}

echo "Starting Spark job with:"
echo "  Main Class: $SPARK_APPLICATION_MAIN_CLASS"
echo "  Master: $SPARK_MASTER"
echo "  Driver Memory: $SPARK_DRIVER_MEMORY"
echo "  Executor Memory: $SPARK_EXECUTOR_MEMORY"
echo "  Process Date: $PROCESS_DATE"

# Build spark-submit command
exec spark-submit \
  --class "$SPARK_APPLICATION_MAIN_CLASS" \
  --master "$SPARK_MASTER" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --executor-memory "$SPARK_EXECUTOR_MEMORY" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.376,io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.0" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  "/opt/spark-apps/data-pipeline.jar" \
  "$PROCESS_DATE" 