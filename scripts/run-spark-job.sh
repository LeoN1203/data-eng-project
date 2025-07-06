#!/bin/bash

# Run your Spark job from the host (assuming Spark is installed on the host)
$SPARK_HOME/bin/spark-submit \
  --class ingestion.KafkaS3DataLakePipeline \
  --master local[*] \
  /path/to/your/spark-job.jar