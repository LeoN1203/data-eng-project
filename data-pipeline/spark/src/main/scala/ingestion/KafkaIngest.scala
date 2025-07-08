package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import java.util.concurrent.TimeUnit
import scala.util.{Try, Success, Failure}

object KafkaS3DataLakePipeline {

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("IoT-Kafka-S3-DataLake")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.hadoop.security.authentication", "simple")
      .config("spark.hadoop.hadoop.security.authorization", "false")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
      .config("spark.hadoop.fs.s3a.path.style.access", "false")
      .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
      .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
      .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
      .config("spark.hadoop.fs.s3a.retry.interval", "1000ms")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
      .config("spark.jars.ivy", "/tmp/.ivy2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Configuration parameters - use environment variables with fallbacks
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "iot-sensor-data")
    val s3Bucket = sys.env.getOrElse("S3_BUCKET", "inde-aws-datalake")
    val awsRegion = sys.env.getOrElse("AWS_DEFAULT_REGION", "eu-north-1")
    val s3BucketPath = s"s3a://$s3Bucket/raw/iot-data/"
    val checkpointLocation = s"s3a://$s3Bucket/checkpoints/"

    println(s"Using S3 bucket: $s3Bucket")
    println(s"S3 path: $s3BucketPath")
    println(s"Checkpoint location: $checkpointLocation")
    println(s"AWS Region: $awsRegion")
    println(s"Kafka Bootstrap Servers: $kafkaBootstrapServers")
    println(s"Kafka Topic: $kafkaTopic")

    Try {
      // Read from Kafka stream
      val kafkaStream = readFromKafka(spark, kafkaBootstrapServers, kafkaTopic)
      
      println("=== DEBUG: Kafka stream schema ===")
      kafkaStream.printSchema()
      
      // Parse and transform the data
      val processedStream = processIoTData(kafkaStream)
      
      println("=== DEBUG: Processed stream schema ===")
      processedStream.printSchema()
      
      // Write to S3 Data Lake
      writeToS3DataLake(processedStream, s3BucketPath, checkpointLocation)
      
      println("=== DEBUG: Streaming query started, waiting for termination ===")
      
      // Add a simple test to verify data is flowing
      val testQuery = processedStream.writeStream
        .format("console")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
        .queryName("test-data-flow")
        .start()
      
      println(s"=== DEBUG: Test query started with ID: ${testQuery.id} ===")

      // Keep the application running
      spark.streams.awaitAnyTermination()

    } match {
      case Failure(e) =>
        println(s"Error in pipeline: ${e.getMessage}")
        e.printStackTrace()
      case _ =>
    }
    spark.stop()
  }

  /** Read streaming data from Kafka
    */
  def readFromKafka(
      spark: SparkSession,
      bootstrapServers: String,
      topic: String
  ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option(
        "startingOffsets",
        "earliest"
      ) // Use "earliest" for historical data
      .option("failOnDataLoss", "false")
      .option("kafka.group.id", "iot-sensor-spark-consumer")
      .option("kafka.metadata.max.age.ms", "5000")
      .option("kafka.max.poll.interval.ms", "60000")
      .option("kafka.request.timeout.ms", "30000")
      .option("minPartitions", "3")
      .load()
  }

  /** Process and transform IoT sensor data Assumes JSON format from IoT sensors
    */
  def processIoTData(kafkaStream: DataFrame): DataFrame = {
    import kafkaStream.sparkSession.implicits._

    // Define schema for IoT sensor data - aligned with actual producer structure
    val iotSchema = StructType(
      Seq(
        StructField("deviceId", StringType, nullable = false),
        StructField("temperature", DoubleType, nullable = true),
        StructField("humidity", DoubleType, nullable = true),
        StructField("pressure", DoubleType, nullable = true),
        StructField("motion", BooleanType, nullable = true),
        StructField("light", DoubleType, nullable = true),
        StructField("acidity", DoubleType, nullable = true),
        StructField("location", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField(
          "metadata",
          StructType(
            Seq(
              StructField("battery_level", IntegerType, nullable = true),
              StructField("signal_strength", IntegerType, nullable = true),
              StructField("firmware_version", StringType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    kafkaStream
      .select(
        col("key").cast("string").as("kafka_key"),
        col("value").cast("string").as("raw_data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").as("kafka_timestamp")
      )
      // Parse JSON data
      .withColumn("parsed_data", from_json(col("raw_data"), iotSchema))
      // Extract fields from parsed JSON and keep original structure
      .select(
        col("kafka_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("parsed_data.deviceId"),
        from_unixtime(col("parsed_data.timestamp") / 1000)
          .cast("timestamp")
          .as("sensor_timestamp"),
        col("parsed_data.temperature"),
        col("parsed_data.humidity"),
        col("parsed_data.pressure"),
        col("parsed_data.motion"),
        col("parsed_data.light"),
        col("parsed_data.acidity"),
        col("parsed_data.location"),
        col("parsed_data.metadata.battery_level").as("battery_level"),
        col("parsed_data.metadata.signal_strength").as("signal_strength"),
        col("parsed_data.metadata.firmware_version").as("firmware_version"),
        // Keep original raw data for downstream processing
        col("raw_data").as("original_json")
      )
      // Add processing metadata
      .withColumn("processing_time", current_timestamp())
      .withColumn("year", year(col("sensor_timestamp")))
      .withColumn("month", month(col("sensor_timestamp")))
      .withColumn("day", dayofmonth(col("sensor_timestamp")))
      .withColumn("hour", hour(col("sensor_timestamp")))
      // Filter out invalid records
      .filter(col("deviceId").isNotNull && col("sensor_timestamp").isNotNull)
  }

  /** Write processed data to S3 Data Lake as JSON files with partitioning
    */
  def writeToS3DataLake(
      processedStream: DataFrame,
      s3Path: String,
      checkpointLocation: String
  ): Unit = {
    println(s"=== DEBUG: Writing to S3 path: $s3Path ===")
    println(s"=== DEBUG: Checkpoint location: $checkpointLocation ===")
    
    // Create a simplified structure for JSON output
    val jsonStream = processedStream.select(
      col("original_json").as("value"),
      col("year"),
      col("month"),
      col("day"),
      col("hour")
    )
    
    println("=== DEBUG: JSON stream schema ===")
    jsonStream.printSchema()

    // Add a count query to monitor data flow
    val countQuery = jsonStream.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .queryName("iot-data-count")
      .start()

    val query = jsonStream.writeStream
      .format("json")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointLocation)
      .option("path", s3Path)
      .partitionBy(
        "year",
        "month",
        "day",
        "hour"
      ) // Partition for efficient querying
      .trigger(
        Trigger.ProcessingTime(10, TimeUnit.SECONDS)
      ) // Process every 10 seconds instead of 30
      .queryName("iot-s3-json-sink")
      .start()
      
    println(s"=== DEBUG: Streaming query started with ID: ${query.id} ===")
    println(s"=== DEBUG: Count query started with ID: ${countQuery.id} ===")
  }

  /** Batch processing with additional transformations
    */
  def processBatchWithAggregations(
      spark: SparkSession,
      kafkaStream: DataFrame,
      s3Path: String,
      checkpointLocation: String
  ): Unit = {
    import spark.implicits._

    val processedStream = processIoTData(kafkaStream)

    // Create aggregated metrics every 5 minutes
    val aggregatedStream = processedStream
      .withWatermark("sensor_timestamp", "10 minutes")
      .groupBy(
        window(col("sensor_timestamp"), "5 minutes"),
        col("deviceId")
      )
      .agg(
        avg("temperature").as("avg_temperature"),
        min("temperature").as("min_temperature"),
        max("temperature").as("max_temperature"),
        avg("humidity").as("avg_humidity"),
        avg("pressure").as("avg_pressure"),
        count("*").as("record_count"),
        first("location").as("location")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("deviceId"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_humidity"),
        col("avg_pressure"),
        col("record_count"),
        col("location")
      )

    // Write aggregated data to a separate path
    aggregatedStream.writeStream
      .format("parquet")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", s"$checkpointLocation/aggregated")
      .option("path", s"$s3Path/aggregated/")
      .partitionBy("deviceId")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      .queryName("iot-aggregated-s3-sink")
      .start()
  }

  /** Error handling and data quality checks
    */
  def addDataQualityChecks(df: DataFrame): DataFrame = {
    df.withColumn(
      "data_quality_flag",
      when(
        col("temperature").isNull || col("temperature") < -50 || col(
          "temperature"
        ) > 100,
        "INVALID_TEMP"
      )
        .when(
          col("humidity").isNull || col("humidity") < 0 || col(
            "humidity"
          ) > 100,
          "INVALID_HUMIDITY"
        )
        .when(col("pressure").isNull || col("pressure") < 0, "INVALID_PRESSURE")
        .otherwise("VALID")
    )
  }
}
