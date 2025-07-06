package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

/**
 * GRAFANA EXPORT JOB
 * 
 * Purpose:
 * - Read analytics data from Gold tier
 * - Transform data for optimal Grafana visualization
 * - Write to PostgreSQL tables for dashboard consumption
 * - Create time-series optimized tables for charts
 */
object GrafanaExportJob {

  case class ExportSummary(
    exportDate: String,
    tablesExported: Int,
    recordsExported: Long,
    exportTimestamp: String
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Grafana-Export-Job")
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

    // Configuration - Read from S3 bucket
    val s3BucketPath = "s3a://inde-aws-datalake"
    val goldPath = s"$s3BucketPath/gold"
    
    // PostgreSQL connection (matches your docker-compose.yml)
    val pgProps = new Properties()
    pgProps.setProperty("user", "grafana")
    pgProps.setProperty("password", "grafana")
    pgProps.setProperty("driver", "org.postgresql.Driver")
    val pgUrl = "jdbc:postgresql://postgres:5432/grafana_db"

    // Process specific date range (can be parameterized)
    val exportDate = if (args.nonEmpty) args(0) else getCurrentDate()
    
    try {
      println(s"Starting Grafana Export Job for date: $exportDate")
      println(s"Reading Gold data from: $goldPath")
      
      // Export Gold data to PostgreSQL
      val summary = exportGoldToPostgreSQL(spark, goldPath, pgUrl, pgProps, exportDate)
      
      // Print export summary
      printExportSummary(summary)
      
      println("Grafana export completed successfully!")
      
    } catch {
      case e: Exception =>
        println(s"Error in Grafana export: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * Main export function: Gold → PostgreSQL
   */
  def exportGoldToPostgreSQL(
    spark: SparkSession,
    goldPath: String,
    pgUrl: String,
    pgProps: Properties,
    exportDate: String
  ): ExportSummary = {

    var tablesExported = 0
    var totalRecords = 0L

    println("Exporting Gold tier analytics to PostgreSQL for Grafana...")

    // 1. Export Hourly Metrics - Core time-series data for Grafana
    val hourlyRecords = exportHourlyMetrics(spark, goldPath, pgUrl, pgProps, exportDate)
    tablesExported += 1
    totalRecords += hourlyRecords

    // 2. Export Daily Report - Location and device type analytics
    val dailyRecords = exportDailyReport(spark, goldPath, pgUrl, pgProps, exportDate)
    tablesExported += 1
    totalRecords += dailyRecords

    // 3. Export Device Summary - Individual device performance
    val deviceRecords = exportDeviceSummary(spark, goldPath, pgUrl, pgProps, exportDate)
    tablesExported += 1
    totalRecords += deviceRecords

    ExportSummary(exportDate, tablesExported, totalRecords, getCurrentTimestamp())
  }

  /**
   * 1. Export Hourly Metrics - Core time-series data for Grafana
   */
  def exportHourlyMetrics(
    spark: SparkSession,
    goldPath: String,
    pgUrl: String,
    pgProps: Properties,
    exportDate: String
  ): Long = {

    try {
      // Read hourly metrics from Gold tier
      val hourlyMetrics = spark.read
        .format("parquet")
        .load(s"$goldPath/hourly_metrics")
        .filter(col("processing_date") === exportDate)

      // Transform for Grafana time-series format
      val timeSeriesData = hourlyMetrics
        .select(
          // Create proper timestamp for Grafana
          to_timestamp(
            concat(
              col("processing_date"), 
              lit(" "), 
              lpad(col("hour_of_day"), 2, "0"), 
              lit(":00:00")
            ), 
            "yyyy-MM-dd HH:mm:ss"
          ).as("time"),
          
          // Metrics
          col("avg_temperature").as("temperature"),
          col("avg_humidity").as("humidity"),
          col("avg_pressure").as("pressure"),
          col("avg_quality_score").as("data_quality"),
          col("total_readings").as("sensor_readings"),
          
          // Dimensions for filtering
          col("deviceType").as("device_type"),
          col("location"),
          col("unique_devices"),
          
          // Processing metadata
          current_timestamp().as("exported_at")
        )
        .orderBy("time", "location", "device_type")

      val recordCount = timeSeriesData.count()

      if (recordCount > 0) {
        // Write to PostgreSQL
        timeSeriesData.write
          .mode("overwrite")
          .jdbc(pgUrl, "iot_timeseries", pgProps)

        println(s"✅ Hourly Metrics exported: $recordCount records")
      } else {
        println("⚠️ No hourly metrics data found for the specified date")
      }
      
      recordCount

    } catch {
      case e: Exception =>
        println(s"❌ Error exporting hourly metrics: ${e.getMessage}")
        e.printStackTrace()
        0L
    }
  }

  /**
   * 2. Export Daily Report - Location and device type analytics
   */
  def exportDailyReport(
    spark: SparkSession,
    goldPath: String,
    pgUrl: String,
    pgProps: Properties,
    exportDate: String
  ): Long = {

    try {
      // Read daily report from Gold tier
      val dailyReport = spark.read
        .format("parquet")
        .load(s"$goldPath/daily_report")
        .filter(col("processing_date") === exportDate)

      // Transform for Grafana dashboard
      val reportData = dailyReport
        .select(
          col("location"),
          col("deviceType").as("device_type"),
          col("daily_readings").as("total_readings"),
          col("active_devices"),
          col("active_hours"),
          
          // Environmental metrics
          col("daily_avg_temp").as("avg_temperature"),
          col("daily_min_temp").as("min_temperature"),
          col("daily_max_temp").as("max_temperature"),
          col("daily_avg_humidity").as("avg_humidity"),
          col("daily_avg_pressure").as("avg_pressure"),
          
          // Quality and comfort
          col("daily_quality_score").as("quality_score"),
          col("comfort_percentage"),
          col("optimal_readings"),
          col("good_readings"),
          col("poor_readings"),
          
          // Date for filtering
          to_date(col("processing_date")).as("report_date"),
          current_timestamp().as("exported_at")
        )
        .orderBy(desc("total_readings"))

      val recordCount = reportData.count()

      if (recordCount > 0) {
        // Write to PostgreSQL
        reportData.write
          .mode("overwrite")
          .jdbc(pgUrl, "daily_report", pgProps)

        println(s"✅ Daily Report exported: $recordCount records")
      } else {
        println("⚠️ No daily report data found for the specified date")
      }
      
      recordCount

    } catch {
      case e: Exception =>
        println(s"❌ Error exporting daily report: ${e.getMessage}")
        e.printStackTrace()
        0L
    }
  }

  /**
   * 3. Export Device Summary - Individual device performance
   */
  def exportDeviceSummary(
    spark: SparkSession,
    goldPath: String,
    pgUrl: String,
    pgProps: Properties,
    exportDate: String
  ): Long = {

    try {
      // Read device summary from Gold tier
      val deviceSummary = spark.read
        .format("parquet")
        .load(s"$goldPath/device_summary")
        .filter(col("processing_date") === exportDate)

      // Transform for device monitoring dashboard
      val deviceData = deviceSummary
        .select(
          col("deviceId").as("device_id"),
          col("deviceType").as("device_type"),
          col("location"),
          col("total_readings"),
          col("active_hours"),
          col("device_quality_score").as("quality_score"),
          col("device_health").as("health_status"),
          
          // Environmental metrics
          col("avg_temperature"),
          col("avg_humidity"),
          col("avg_pressure"),
          
          // Timestamps
          col("first_reading"),
          col("last_reading"),
          
          // Date for filtering
          to_date(col("processing_date")).as("report_date"),
          current_timestamp().as("exported_at")
        )
        .orderBy(col("health_status"), desc("quality_score"))

      val recordCount = deviceData.count()

      if (recordCount > 0) {
        // Write to PostgreSQL
        deviceData.write
          .mode("overwrite")
          .jdbc(pgUrl, "device_status", pgProps)

        println(s"✅ Device Summary exported: $recordCount records")
      } else {
        println("⚠️ No device summary data found for the specified date")
      }
      
      recordCount

    } catch {
      case e: Exception =>
        println(s"❌ Error exporting device summary: ${e.getMessage}")
        e.printStackTrace()
        0L
    }
  }

  /**
   * Print export summary
   */
  def printExportSummary(summary: ExportSummary): Unit = {
    println("=" * 50)
    println("GRAFANA EXPORT SUMMARY")
    println("=" * 50)
    println(f"Export Date: ${summary.exportDate}")
    println(f"Export Time: ${summary.exportTimestamp}")
    println(f"Tables Exported: ${summary.tablesExported}")
    println(f"Total Records: ${summary.recordsExported}")
    
    println("\n📊 PostgreSQL Tables Created for Grafana:")
    println("  1. ✅ iot_timeseries - Time-series charts (from hourly_metrics)")
    println("  2. ✅ daily_report - Location analytics (from daily_report)")
    println("  3. ✅ device_status - Device monitoring (from device_summary)")
    
    println(f"\n🎯 Ready for Grafana! Connect to PostgreSQL:")
    println(f"   Host: postgres:5432")
    println(f"   Database: grafana_db")
    println(f"   User: grafana")
    println("=" * 50)
  }

  // Utility functions
  private def getCurrentDate(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(new Date())
  }

  private def getCurrentTimestamp(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(new Date())
  }
} 