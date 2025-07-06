package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * BRONZE TIER JOB
 * 
 * Purpose:
 * - Ingest raw data from S3 raw folder
 * - Store as-is with minimal processing
 * - Add basic metadata (ingestion timestamp, partition info)
 * - Handle schema evolution gracefully
 * - Ensure data lineage tracking
 */
object BronzeJob extends App {

  private val spark = SparkSession.builder()
    .appName("Bronze-Data-Ingestion")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", sys.env.getOrElse("AWS_DEFAULT_REGION", "eu-west-3"))
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  println("Testing S3 connectivity...")
  try {
    // Test S3 connectivity by attempting to read/write
    import spark.implicits._
    val testDF = Seq(("test", "connectivity")).toDF("key", "value")
    val testPath = "s3a://inde-aws-datalake/bronze/test/connectivity_test"
    testDF.write.mode("overwrite").parquet(testPath)
    println("✓ S3 connectivity test successful!")
  } catch {
    case e: Exception =>
      println(s"✗ S3 connectivity test failed: ${e.getMessage}")
      println("Environment variables:")
      println(s"AWS_ACCESS_KEY_ID: ${sys.env.get("AWS_ACCESS_KEY_ID").map(_.take(10) + "...").getOrElse("NOT SET")}")
      println(s"AWS_SECRET_ACCESS_KEY: ${if (sys.env.contains("AWS_SECRET_ACCESS_KEY")) "***SET***" else "NOT SET"}")
      println(s"AWS_DEFAULT_REGION: ${sys.env.getOrElse("AWS_DEFAULT_REGION", "NOT SET")}")
  }

  println("Starting Bronze Data Ingestion Job...")
  
  try {
    ingestToBronze(spark, "s3a://inde-aws-datalake/raw/iot-data/", "s3a://inde-aws-datalake/bronze/iot-data/")
  } catch {
    case e: Exception =>
      println(s"Error in Bronze ingestion: ${e.getMessage}")
      e.printStackTrace()
  }

  private def ingestToBronze(
    spark: SparkSession,
    rawPath: String,
    bronzePath: String
  ): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    println(s"Reading raw data from: $rawPath")
    
    // Read raw JSON data
    val rawData = spark.read
      .format("json")
      .option("multiline", "true")
      .load(rawPath)
    
    val recordCount = rawData.count()
    println(s"Found $recordCount records in raw data")
    
    println("Raw data schema:")
    rawData.printSchema()
    
    // Parse the JSON string in the value column to extract sensor data
    val jsonSchema = StructType(Array(
      StructField("deviceId", StringType, true),
      StructField("temperature", DoubleType, true),
      StructField("humidity", DoubleType, true),
      StructField("pressure", DoubleType, true),
      StructField("motion", BooleanType, true),
      StructField("light", DoubleType, true),
      StructField("acidity", DoubleType, true),
      StructField("location", StringType, true),
      StructField("timestamp", LongType, true),
      StructField("metadata", StructType(Array(
        StructField("battery_level", IntegerType, true),
        StructField("signal_strength", IntegerType, true),
        StructField("firmware_version", StringType, true)
      )), true)
    ))
    
    // Parse JSON from value column and extract all fields
    val parsedData = rawData
      .withColumn("parsed_json", from_json(col("value"), jsonSchema))
      .select(
        col("parsed_json.deviceId").as("deviceId"),
        col("parsed_json.temperature").as("temperature"),
        col("parsed_json.humidity").as("humidity"),
        col("parsed_json.pressure").as("pressure"),
        col("parsed_json.motion").as("motion"),
        col("parsed_json.light").as("light"),
        col("parsed_json.acidity").as("acidity"),
        col("parsed_json.location").as("location"),
        col("parsed_json.timestamp").as("timestamp"),
        col("parsed_json.metadata.battery_level").as("battery_level"),
        col("parsed_json.metadata.signal_strength").as("signal_strength"),
        col("parsed_json.metadata.firmware_version").as("firmware_version"),
        // Keep original partition columns
        col("year").as("raw_year"),
        col("month").as("raw_month"),
        col("day").as("raw_day"),
        col("hour").as("raw_hour")
      )
    
    println("Parsed data schema:")
    parsedData.printSchema()
    
    // Add Bronze layer metadata - align with actual IoTSensorData structure
    val bronzeData = parsedData
      .withColumn("bronze_ingestion_time", current_timestamp())
      .withColumn("bronze_date", current_date())
      .withColumn("year", year(from_unixtime(col("timestamp") / 1000)))
      .withColumn("month", month(from_unixtime(col("timestamp") / 1000)))
      .withColumn("day", dayofmonth(from_unixtime(col("timestamp") / 1000)))
      .withColumn("data_source", lit("raw-s3-json"))
      .withColumn("ingestion_job", lit("bronze-batch-job-v2"))
      .withColumn("record_id", monotonically_increasing_id())
      .withColumn("processing_status", lit("ingested"))
      .withColumn("deviceType", 
        when(col("location").startsWith("warehouse"), "temperature-sensor")
        .when(col("location").startsWith("field"), "environmental-sensor")
        .otherwise("iot-sensor"))
    
    println("Bronze data schema:")
    bronzeData.printSchema()
    
    println("Sample Bronze data:")
    bronzeData.show(5, truncate = false)
    
    println(s"Writing Bronze data to: $bronzePath")
    bronzeData.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .save(bronzePath)
    
    val finalRecordCount = bronzeData.count()
    println(s"Bronze ingestion completed - $finalRecordCount records written to: $bronzePath")
    
    showBronzeQualityMetrics(bronzeData)
  }
  
  private def showBronzeQualityMetrics(df: DataFrame): Unit = {
    println("\n=== Bronze Data Quality Metrics ===")
    
    val totalRecords = df.count()
    val uniqueDevices = df.select("deviceId").distinct().count()
    val deviceTypes = df.select("deviceType").distinct().count()
    val locations = df.select("location").distinct().count()
    
    println(s"Total Records: $totalRecords")
    println(s"Unique Devices: $uniqueDevices")
    println(s"Device Types: $deviceTypes")
    println(s"Locations: $locations")
    
    println("\nDevice Type Distribution:")
    df.groupBy("deviceType")
      .count()
      .orderBy(desc("count"))
      .show()
      
    println("Location Distribution:")
    df.groupBy("location")
      .count()
      .orderBy(desc("count"))
      .show()
  }

  /**
   * Alternative batch ingestion method for historical data
   */
  def ingestBatchToBronze(
    spark: SparkSession,
    inputPath: String,
    bronzePath: String,
    dataFormat: String = "json"
  ): Unit = {
    import spark.implicits._

    val batchData = spark.read
      .format(dataFormat)
      .option("multiline", "true")
      .load(inputPath)

    val bronzeData = batchData
      .withColumn("bronze_ingestion_time", current_timestamp())
      .withColumn("bronze_date", current_date())
      .withColumn("year", year(from_unixtime(col("timestamp") / 1000)))
      .withColumn("month", month(from_unixtime(col("timestamp") / 1000)))
      .withColumn("day", dayofmonth(from_unixtime(col("timestamp") / 1000)))
      .withColumn("data_source", lit(s"batch-$dataFormat"))
      .withColumn("ingestion_job", lit("bronze-batch-job-v1"))

    bronzeData.write
      .format("parquet")
      .mode("append")
      .partitionBy("year", "month", "day")
      .save(bronzePath)

    println(s"Batch ingestion completed - data written to: $bronzePath")
  }

  /**
   * Utility method to check Bronze data quality
   */
  def checkBronzeDataQuality(spark: SparkSession, bronzePath: String): Unit = {
    import spark.implicits._

    val bronzeData = spark.read.format("parquet").load(bronzePath)

    val qualityMetrics = bronzeData
      .agg(
        count("*").as("total_records"),
        countDistinct("deviceId").as("unique_devices"),
        countDistinct("deviceType").as("device_types"),
        countDistinct("location").as("locations"),
        min("bronze_ingestion_time").as("earliest_ingestion"),
        max("bronze_ingestion_time").as("latest_ingestion")
      )

    println("=== Bronze Data Quality Report ===")
    qualityMetrics.show()

    bronzeData
      .groupBy("deviceType")
      .count()
      .orderBy(desc("count"))
      .show()
  }
} 