```
#!/bin/bash

# ----------------------------------------------------------------------------
# Entrypoint for Spark processing jobs: Bronze, Silver, Gold, Grafana Export
# ----------------------------------------------------------------------------
set -e

# Default SPARK_HOME if not set
export SPARK_HOME=${SPARK_HOME:-/opt/bitnami/spark}

# Spark job environment variables (can be overridden by Compose)
export SPARK_APPLICATION_MAIN_CLASS=${SPARK_APPLICATION_MAIN_CLASS:-processing.BronzeJob}
export SPARK_MASTER=${SPARK_MASTER:-local[*]}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-2g}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-2g}
export SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-2}
export PROCESS_DATE=${PROCESS_DATE:-$(date '+%Y-%m-%d')}

# AWS credentials for S3A
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:?"AWS_ACCESS_KEY_ID is required"}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:?"AWS_SECRET_ACCESS_KEY is required"}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-eu-west-3}

# Show startup info
echo "----------------------------------------"
echo " Starting Spark job"
echo "   Main Class:      $SPARK_APPLICATION_MAIN_CLASS"
echo "   Master:          $SPARK_MASTER"
echo "   Driver Memory:   $SPARK_DRIVER_MEMORY"
echo "   Executor Memory: $SPARK_EXECUTOR_MEMORY"
echo "   Executor Cores:  $SPARK_EXECUTOR_CORES"
echo "   Process Date:    $PROCESS_DATE"
echo "----------------------------------------"

# Launch via spark-submit
exec "$SPARK_HOME/bin/spark-submit" \
  --class "$SPARK_APPLICATION_MAIN_CLASS" \
  --master "$SPARK_MASTER" \
  --deploy-mode client \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --executor-memory "$SPARK_EXECUTOR_MEMORY" \
  --conf "spark.executor.cores=$SPARK_EXECUTOR_CORES" \
  --conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
  --conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=true" \
  --conf "spark.hadoop.fs.s3a.path.style.access=false" \
  --conf "spark.hadoop.fs.s3a.connection.timeout=60000" \
  --conf "spark.hadoop.fs.s3a.connection.establish.timeout=60000" \
  --conf "spark.hadoop.fs.s3a.attempts.maximum=10" \
  --conf "spark.hadoop.fs.s3a.retry.interval=1000ms" \
  /opt/spark-apps/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar \
  "$PROCESS_DATE"
```