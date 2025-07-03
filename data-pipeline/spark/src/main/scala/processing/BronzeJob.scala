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
    // S3 Configuration
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
    ingestToBronze()
  } catch {
    case e: Exception =>
      println(s"Error in Bronze ingestion: ${e.getMessage}")
      e.printStackTrace()
  }

  private def ingestToBronze(): Unit = {
    val rawPath = "s3a://inde-aws-datalake/raw/iot-data/"
    val bronzePath = "s3a://inde-aws-datalake/bronze/iot-data/"
    
    println(s"Reading raw data from: $rawPath")
    
    // Check if raw data exists
    try {
      val rawFiles = spark.read.format("json").load(rawPath)
      val fileCount = rawFiles.count()
      println(s"Found $fileCount records in raw data")
      
      if (fileCount == 0) {
        println("No data found in raw folder. Exiting...")
        return
      }
    } catch {
      case e: Exception =>
        println(s"Error reading raw data: ${e.getMessage}")
        return
    }
    
    // Read raw JSON data from S3
    val rawData = spark.read
      .format("json")
      .option("multiline", "true")
      .load(rawPath)
    
    println("Raw data schema:")
    rawData.printSchema()
    
    // Add Bronze layer metadata
    val bronzeData = rawData
      .withColumn("bronze_ingestion_time", current_timestamp())
      .withColumn("bronze_date", current_date())
      .withColumn("year", year(current_date()))
      .withColumn("month", month(current_date()))
      .withColumn("day", dayofmonth(current_date()))
      .withColumn("data_source", lit("raw-s3-json"))
      .withColumn("ingestion_job", lit("bronze-batch-job-v2"))
      .withColumn("record_id", monotonically_increasing_id())
      .withColumn("processing_status", lit("ingested"))
    
    println("Bronze data schema:")
    bronzeData.printSchema()
    
    println("Sample Bronze data:")
    bronzeData.show(5, truncate = false)
    
    // Write to Bronze layer with partitioning
    println(s"Writing Bronze data to: $bronzePath")
    bronzeData.write
      .format("parquet")
      .mode("overwrite") // Use overwrite for batch processing
      .partitionBy("year", "month", "day")
      .save(bronzePath)
    
    val recordCount = bronzeData.count()
    println(s"✓ Bronze ingestion completed - $recordCount records written to: $bronzePath")
    
    // Show some quality metrics
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
      .withColumn("year", year(current_date()))
      .withColumn("month", month(current_date()))
      .withColumn("day", dayofmonth(current_date()))
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

    // Show device type distribution
    bronzeData
      .groupBy("deviceType")
      .count()
      .orderBy(desc("count"))
      .show()
  }
} 