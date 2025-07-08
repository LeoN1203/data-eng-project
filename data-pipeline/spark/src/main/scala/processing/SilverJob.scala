package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.{Try, Success, Failure}

/**
 * SILVER TIER JOB
 * 
 * Purpose:
 * - Read from Bronze tier
 * - Apply data quality rules and validation
 * - Clean and standardize data formats
 * - Enrich with business logic
 * - Filter out invalid records
 * - Prepare clean data for analytics
 */
object SilverJob {

  case class DataQualityReport(
    totalRecords: Long,
    validRecords: Long,
    invalidRecords: Long,
    validationTimestamp: String,
    qualityScore: Double
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Silver-Data-Processing")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
      .config("spark.hadoop.fs.s3a.endpoint.region", sys.env.getOrElse("AWS_DEFAULT_REGION", "eu-west-3"))
      .config("spark.hadoop.fs.s3a.path.style.access", "false")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Configuration
    val s3BucketPath = "s3a://inde-aws-datalake"
    val bronzePath = s"$s3BucketPath/bronze/iot-data"
    val silverPath = s"$s3BucketPath/silver/iot-data"

    // Process specific date range (can be parameterized)
    val processDate = if (args.nonEmpty) args(0) else getCurrentDate()
    
    Try {
      println(s"Starting Silver Data Processing Job for date: $processDate")
      
      // Process Bronze to Silver
      val qualityReport = processBronzeToSilver(spark, bronzePath, silverPath, processDate)
      
      // Print quality report
      printQualityReport(qualityReport)
      
      println("Silver processing completed successfully!")
      
    } match {
      case Failure(e) =>
        println(s"Error in Silver processing: ${e.getMessage}")
        e.printStackTrace()
      case _ =>
    }
    spark.stop()
  }

  /**
   * Main processing function: Bronze -> Silver
   */
  def processBronzeToSilver(
    spark: SparkSession,
    bronzePath: String,
    silverPath: String,
    processDate: String
  ): DataQualityReport = {
    import spark.implicits._

    val bronzeData = readBronzeData(spark, bronzePath, processDate)
    
    if (bronzeData.isEmpty) {
      println(s"No Bronze data found for date: $processDate")
      return DataQualityReport(0, 0, 0, getCurrentTimestamp(), 0.0)
    }

    val bronzeDf = bronzeData.get
    val totalRecords = bronzeDf.count()
    
    println(s"Processing $totalRecords records from Bronze tier...")

    val silverData = bronzeDf
      .transform(standardizeSensorData)
      .transform(applyDataQualityRules)
      .transform(enrichWithBusinessLogic)
      .transform(addProcessingMetadata)
      .filter(col("is_valid_record") === true)

    val validRecords = silverData.count()
    val invalidRecords = totalRecords - validRecords
    val qualityScore = if (totalRecords > 0) validRecords.toDouble / totalRecords.toDouble else 0.0

    writeSilverData(silverData, silverPath, processDate)

    DataQualityReport(totalRecords, validRecords, invalidRecords, getCurrentTimestamp(), qualityScore)
  }

  /**
   * Read Bronze data for a specific date
   */
  def readBronzeData(spark: SparkSession, bronzePath: String, processDate: String): Option[DataFrame] = {
    Try {
      val dateParts = processDate.split("-")
      val year = dateParts(0)
      val month = dateParts(1).toInt.toString
      val day = dateParts(2).toInt.toString
      
      val dateFilteredPath = s"$bronzePath/year=$year/month=$month/day=$day"
      
      println(s"Reading Bronze data from: $dateFilteredPath")
      
      val df = spark.read
        .format("parquet")
        .load(dateFilteredPath)
        .filter(col("processing_status") === "ingested")
      
      if (df.count() > 0) Some(df) else None
    } match {
      case Success(result) => result
      case Failure(e) =>
        println(s"Error reading Bronze data: ${e.getMessage}")
        None
    }
  }

  /**
   * Standardize sensor data from Bronze format
   */
  def standardizeSensorData(df: DataFrame): DataFrame = {
    df.select(
      col("bronze_ingestion_time"),
      col("bronze_date"),
      year(col("bronze_date")).as("year"),
      month(col("bronze_date")).as("month"),
      dayofmonth(col("bronze_date")).as("day"),
      col("data_source"),
      col("ingestion_job"),
      col("record_id"),
      col("deviceId"),
      col("deviceType"),
      col("location"),
      col("temperature"),
      col("humidity"),
      col("pressure"),
      col("motion"),
      col("light"),
      col("acidity"),
      from_unixtime(col("timestamp") / 1000).cast("timestamp").as("sensor_timestamp"),
      struct(
        col("battery_level"),
        col("signal_strength"),
        col("firmware_version")
      ).as("sensor_metadata")
    )
  }

  /**
   * Apply comprehensive data quality rules
   */
  def applyDataQualityRules(df: DataFrame): DataFrame = {
    df
      .withColumn("deviceId_valid", col("deviceId").isNotNull && length(col("deviceId")) > 0)
      .withColumn("deviceType_valid", col("deviceType").isNotNull && length(col("deviceType")) > 0)
      .withColumn("timestamp_valid", col("sensor_timestamp").isNotNull)
      
      .withColumn("temperature_valid", 
        col("temperature").isNotNull && 
        col("temperature").between(-50, 100))
      
      .withColumn("humidity_valid", 
        col("humidity").isNotNull && 
        col("humidity").between(0, 100))
      
      .withColumn("pressure_valid", 
        col("pressure").isNotNull && 
        col("pressure").between(500, 1500))
      
      .withColumn("location_valid", col("location").isNotNull && length(col("location")) > 0)
      
      .withColumn("motion_valid", col("motion").isNotNull)
      
      .withColumn("light_valid", 
        col("light").isNotNull && 
        col("light").between(0, 100000))
      
      .withColumn("acidity_valid", 
        col("acidity").isNotNull && 
        col("acidity").between(0, 14))
      
      .withColumn("is_valid_record",
        col("deviceId_valid") && 
        col("deviceType_valid") && 
        col("timestamp_valid") && 
        col("temperature_valid") && 
        col("humidity_valid") && 
        col("pressure_valid") && 
        col("location_valid") && 
        col("motion_valid") && 
        col("light_valid") && 
        col("acidity_valid"))
      
      .withColumn("data_quality_score",
        (col("deviceId_valid").cast("int") +
         col("deviceType_valid").cast("int") +
         col("timestamp_valid").cast("int") +
         col("temperature_valid").cast("int") +
         col("humidity_valid").cast("int") +
         col("pressure_valid").cast("int") +
         col("location_valid").cast("int") +
         col("motion_valid").cast("int") +
         col("light_valid").cast("int") +
         col("acidity_valid").cast("int")) / 10.0)
  }

  /**
   * Enrich data with business logic and derived fields
   */
  def enrichWithBusinessLogic(df: DataFrame): DataFrame = {
    df
      .withColumn("temperature_celsius", col("temperature"))
      .withColumn("temperature_fahrenheit", col("temperature") * 9/5 + 32)
      .withColumn("temperature_kelvin", col("temperature") + 273.15)
      
      .withColumn("temperature_category",
        when(col("temperature") < 0, "freezing")
        .when(col("temperature").between(0, 15), "cold")
        .when(col("temperature").between(16, 25), "comfortable")
        .when(col("temperature").between(26, 35), "warm")
        .otherwise("hot"))
      
      .withColumn("humidity_category",
        when(col("humidity") < 30, "dry")
        .when(col("humidity").between(30, 60), "comfortable")
        .when(col("humidity").between(61, 80), "humid")
        .otherwise("very_humid"))
      
      .withColumn("pressure_category",
        when(col("pressure") < 1000, "low")
        .when(col("pressure").between(1000, 1025), "normal")
        .otherwise("high"))
      
      .withColumn("light_category",
        when(col("light") < 1, "dark")
        .when(col("light").between(1, 50), "dim")
        .when(col("light").between(51, 500), "indoor")
        .when(col("light").between(501, 10000), "bright")
        .otherwise("very_bright"))
      
      .withColumn("acidity_category",
        when(col("acidity") < 3, "very_acidic")
        .when(col("acidity").between(3, 6), "acidic")
        .when(col("acidity").between(6, 8), "neutral")
        .when(col("acidity").between(8, 11), "basic")
        .otherwise("very_basic"))
      
      .withColumn("motion_status",
        when(col("motion") === true, "active")
        .otherwise("inactive"))
      
      .withColumn("comfort_index",
        when(col("temperature_category") === "comfortable" && 
             col("humidity_category") === "comfortable" && 
             col("light_category").isin("indoor", "bright"), "optimal")
        .when(col("temperature_category").isin("comfortable", "warm") && 
             col("humidity_category").isin("comfortable", "humid"), "good")
        .otherwise("poor"))
      
      .withColumn("hour_of_day", hour(col("sensor_timestamp")))
      .withColumn("day_of_week", dayofweek(col("sensor_timestamp")))
      .withColumn("is_weekend", dayofweek(col("sensor_timestamp")).isin(1, 7))
      
      .withColumn("season",
        when(month(col("sensor_timestamp")).isin(12, 1, 2), "winter")
        .when(month(col("sensor_timestamp")).isin(3, 4, 5), "spring")
        .when(month(col("sensor_timestamp")).isin(6, 7, 8), "summer")
        .otherwise("autumn"))
      
      .withColumn("battery_level", col("sensor_metadata.battery_level"))
      .withColumn("signal_strength", col("sensor_metadata.signal_strength"))
      .withColumn("firmware_version", col("sensor_metadata.firmware_version"))
      
      .withColumn("low_battery", col("battery_level") < 20)
      .withColumn("poor_signal", col("signal_strength") < -80)
      .withColumn("device_health_status",
        when(col("low_battery") && col("poor_signal"), "critical")
        .when(col("low_battery") || col("poor_signal"), "warning")
        .otherwise("good"))
  }

  /**
   * Add Silver tier processing metadata
   */
  def addProcessingMetadata(df: DataFrame): DataFrame = {
    df
      .withColumn("silver_processing_time", current_timestamp())
      .withColumn("silver_processing_date", current_date())
      .withColumn("data_lineage", lit("bronze_to_silver_v2"))
      .withColumn("processing_job", lit("silver-batch-job"))
      .withColumn("silver_tier_version", lit("2.0"))
  }

  /**
   * Write Silver data using Parquet format
   */
  def writeSilverData(df: DataFrame, silverPath: String, processDate: String): Unit = {
    println(s"Writing Silver data to: $silverPath")
    
    df.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("year", "month", "day", "deviceType")
      .save(silverPath)
    
    println(s"Silver data written successfully")
    println(s"Partitioned by: year, month, day, deviceType")
    
    println("Sample Silver data:")
    df.select("deviceId", "deviceType", "temperature", "humidity", "pressure", "comfort_index", "device_health_status")
      .show(5)
  }

  /**
   * Generate detailed quality report
   */
  def printQualityReport(report: DataQualityReport): Unit = {
    println("=" * 50)
    println("SILVER TIER DATA QUALITY REPORT")
    println("=" * 50)
    println(f"Processing Time: ${report.validationTimestamp}")
    println(f"Total Records: ${report.totalRecords}")
    println(f"Valid Records: ${report.validRecords}")
    println(f"Invalid Records: ${report.invalidRecords}")
    println(f"Quality Score: ${report.qualityScore * 100}%.2f%%")
    
    if (report.totalRecords > 0) {
      val validPercentage = (report.validRecords.toDouble / report.totalRecords.toDouble) * 100
      val invalidPercentage = (report.invalidRecords.toDouble / report.totalRecords.toDouble) * 100
      
      println(f"Valid Percentage: $validPercentage%.2f%%")
      println(f"Invalid Percentage: $invalidPercentage%.2f%%")
      
      if (validPercentage >= 95) {
        println("EXCELLENT data quality!")
      } else if (validPercentage >= 90) {
        println("GOOD data quality")
      } else if (validPercentage >= 80) {
        println("ACCEPTABLE data quality")
      } else {
        println("POOR data quality - investigate data sources")
      }
    }
    println("=" * 50)
  }

  /**
   * Utility methods
   */
  def getCurrentDate(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    formatter.format(new Date())
  }

  def getCurrentTimestamp(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    formatter.format(new Date())
  }

  /**
   * Utility method to analyze Silver data quality by device type
   */
  def analyzeSilverDataQuality(spark: SparkSession, silverPath: String): Unit = {
    import spark.implicits._

    val silverData = spark.read.format("parquet").load(silverPath)

    println("=== SILVER TIER ANALYSIS ===")
    
    val totalRecords = silverData.count()
    val uniqueDevices = silverData.select("deviceId").distinct().count()
    
    println(s"Total Records: $totalRecords")
    println(s"Unique Devices: $uniqueDevices")
    
    println("\nDevice Health Status:")
    silverData.groupBy("device_health_status")
      .count()
      .orderBy(desc("count"))
      .show()
    
    println("\nTemperature Categories:")
    silverData.groupBy("temperature_category")
      .count()
      .orderBy(desc("count"))
      .show()
    
    println("\nComfort Index Distribution:")
    silverData.groupBy("comfort_index")
      .count()
      .orderBy(desc("count"))
      .show()
  }
} 