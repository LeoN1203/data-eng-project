package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

/**
 * GOLD TIER JOB - Simplified and Reliable Version
 * 
 * Purpose:
 * - Read clean Silver data
 * - Create simple, reliable analytics tables
 * - Focus on core business metrics
 * - Ensure data quality and consistency
 */
object GoldJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Gold-Analytics-Processing")
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
    val silverPath = s"$s3BucketPath/silver/iot-data"
    val goldPath = s"$s3BucketPath/gold"

    // Process specific date (can be parameterized)
    val processDate = if (args.nonEmpty) args(0) else getCurrentDate()
    
    try {
      println(s"Starting Gold Analytics Processing Job for date: $processDate")
      
      // Read Silver data
      val silverDf = readSilverData(spark, silverPath, processDate)
      
      if (silverDf.isDefined) {
        val df = silverDf.get
        val recordCount = df.count()
        println(s"Processing $recordCount Silver records for Gold analytics...")
        
        // Create simple analytics tables
        createHourlyMetrics(df, s"$goldPath/hourly_metrics")
        createDailyReport(df, s"$goldPath/daily_report")
        createDeviceSummary(df, s"$goldPath/device_summary")
        
        println("✅ Gold processing completed successfully!")
      } else {
        println(s"No Silver data found for date: $processDate")
      }
      
    } catch {
      case e: Exception =>
        println(s"Error in Gold processing: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
    }
  }

  /**
   * Read Silver data for the specified date
   */
  def readSilverData(spark: SparkSession, silverPath: String, processDate: String): Option[DataFrame] = {
    try {
      val dateParts = processDate.split("-")
      val year = dateParts(0)
      val month = dateParts(1).toInt.toString
      val day = dateParts(2).toInt.toString
      
      val dateFilteredPath = s"$silverPath/year=$year/month=$month/day=$day"
      
      println(s"Reading Silver data from: $dateFilteredPath")
      
      val df = spark.read
        .format("parquet")
        .load(dateFilteredPath)
        .filter(col("is_valid_record") === true) // Only process valid records
      
      val recordCount = df.count()
      if (recordCount > 0) {
        println(s"Found $recordCount valid records in Silver data")
        Some(df)
      } else {
        println("No valid records found in Silver data")
        None
      }
    } catch {
      case e: Exception =>
        println(s"Error reading Silver data: ${e.getMessage}")
        None
    }
  }

  /**
   * Create hourly metrics - Simple aggregation by hour
   */
  def createHourlyMetrics(silverDf: DataFrame, outputPath: String): Unit = {
    println("Creating hourly metrics...")
    
    val hourlyMetrics = silverDf
      .groupBy(
        col("hour_of_day"),
        col("location"),
        col("deviceType")
      )
      .agg(
        count("*").as("total_readings"),
        countDistinct("deviceId").as("unique_devices"),
        avg("temperature").as("avg_temperature"),
        min("temperature").as("min_temperature"),
        max("temperature").as("max_temperature"),
        avg("humidity").as("avg_humidity"),
        avg("pressure").as("avg_pressure"),
        avg("light").as("avg_light"),
        avg("acidity").as("avg_acidity"),
        sum(when(col("motion") === true, 1).otherwise(0)).as("motion_detections"),
        avg("data_quality_score").as("avg_quality_score"),
        current_timestamp().as("processing_time")
      )
      .withColumn("processing_date", current_date())
      .withColumn("motion_percentage", col("motion_detections") * 100.0 / col("total_readings"))

    hourlyMetrics
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(outputPath)

    println(s"✅ Hourly metrics created: $outputPath")
  }

  /**
   * Create daily report - Summary by location and device type
   */
  def createDailyReport(silverDf: DataFrame, outputPath: String): Unit = {
    println("Creating daily report...")
    
    val dailyReport = silverDf
      .groupBy(
        col("location"),
        col("deviceType")
      )
      .agg(
        count("*").as("daily_readings"),
        countDistinct("deviceId").as("active_devices"),
        countDistinct("hour_of_day").as("active_hours"),
        avg("temperature").as("daily_avg_temp"),
        min("temperature").as("daily_min_temp"),
        max("temperature").as("daily_max_temp"),
        avg("humidity").as("daily_avg_humidity"),
        avg("pressure").as("daily_avg_pressure"),
        avg("light").as("daily_avg_light"),
        avg("acidity").as("daily_avg_acidity"),
        sum(when(col("motion") === true, 1).otherwise(0)).as("daily_motion_detections"),
        avg("data_quality_score").as("daily_quality_score"),
        sum(when(col("comfort_index") === "optimal", 1).otherwise(0)).as("optimal_readings"),
        sum(when(col("comfort_index") === "good", 1).otherwise(0)).as("good_readings"),
        sum(when(col("comfort_index") === "poor", 1).otherwise(0)).as("poor_readings"),
        sum(when(col("light_category") === "bright", 1).otherwise(0)).as("bright_light_readings"),
        sum(when(col("acidity_category") === "neutral", 1).otherwise(0)).as("neutral_acidity_readings"),
        current_timestamp().as("processing_time")
      )
      .withColumn("processing_date", current_date())
      .withColumn("comfort_percentage", 
        (col("optimal_readings") + col("good_readings")) * 100.0 / col("daily_readings"))
      .withColumn("motion_percentage", 
        col("daily_motion_detections") * 100.0 / col("daily_readings"))
      .withColumn("bright_light_percentage", 
        col("bright_light_readings") * 100.0 / col("daily_readings"))
      .withColumn("neutral_acidity_percentage", 
        col("neutral_acidity_readings") * 100.0 / col("daily_readings"))

    dailyReport
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(outputPath)

    println(s"✅ Daily report created: $outputPath")
  }

  /**
   * Create device summary - Performance by individual device
   */
  def createDeviceSummary(silverDf: DataFrame, outputPath: String): Unit = {
    println("Creating device summary...")
    
    val deviceSummary = silverDf
      .groupBy(
        col("deviceId"),
        col("deviceType"),
        col("location")
      )
      .agg(
        count("*").as("total_readings"),
        countDistinct("hour_of_day").as("active_hours"),
        avg("temperature").as("avg_temperature"),
        avg("humidity").as("avg_humidity"),
        avg("pressure").as("avg_pressure"),
        avg("light").as("avg_light"),
        avg("acidity").as("avg_acidity"),
        sum(when(col("motion") === true, 1).otherwise(0)).as("motion_detections"),
        avg("data_quality_score").as("device_quality_score"),
        avg("battery_level").as("avg_battery_level"),
        avg("signal_strength").as("avg_signal_strength"),
        min("sensor_timestamp").as("first_reading"),
        max("sensor_timestamp").as("last_reading"),
        current_timestamp().as("processing_time")
      )
      .withColumn("processing_date", current_date())
      .withColumn("motion_percentage", col("motion_detections") * 100.0 / col("total_readings"))
      .withColumn("device_health", 
        when(col("device_quality_score") >= 0.9 && col("active_hours") >= 20 && col("avg_battery_level") >= 80, "excellent")
        .when(col("device_quality_score") >= 0.8 && col("active_hours") >= 16 && col("avg_battery_level") >= 60, "good")
        .when(col("device_quality_score") >= 0.7 && col("active_hours") >= 12 && col("avg_battery_level") >= 40, "acceptable")
        .otherwise("needs_attention"))

    deviceSummary
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save(outputPath)

    println(s"✅ Device summary created: $outputPath")
  }

  // Utility functions
  private def getCurrentDate(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(new Date())
  }
} 