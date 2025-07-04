package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import java.util.concurrent.TimeUnit
import processing.alerts.core._
import processing.alerts.email._
import processing.config.PipelineConfig

/**
 * Kafka-based real-time alerting pipeline using Spark Structured Streaming.
 * Consumes IoT sensor data from Kafka and sends email alerts for detected anomalies.
 * Integrates the AlertDetection module with streaming data processing.
 */
object KafkaAlertingPipeline {

  def main(args: Array[String]): Unit = {
    
    // Load configuration
    val pipelineConfig = PipelineConfig.loadConfiguration()
    
    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName(pipelineConfig.spark.appName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel(pipelineConfig.spark.logLevel)

    // Initialize email gateway based on configuration
    val emailGateway = if (pipelineConfig.email.smtpUser.nonEmpty && pipelineConfig.email.smtpPassword.nonEmpty) {
      println(s"Using email gateway with SMTP host: ${pipelineConfig.email.smtpHost}")
      new CourrierEmailGateway(
        smtpHost = pipelineConfig.email.smtpHost,
        smtpPort = pipelineConfig.email.smtpPort,
        smtpUser = pipelineConfig.email.smtpUser,
        smtpPassword = pipelineConfig.email.smtpPassword,
        fromEmail = pipelineConfig.email.fromAddress,
        useTLS = pipelineConfig.email.tlsEnabled
      )
    } else {
      println("No email configuration found, using console gateway")
      new ConsoleEmailGateway()
    }

    try {
      // Read from Kafka stream
      val kafkaStream = readFromKafka(
        spark, 
        pipelineConfig.kafka.bootstrapServers, 
        pipelineConfig.kafka.topic,
        pipelineConfig.kafka.consumerGroupId,
        pipelineConfig.kafka.startingOffsets
      )

      // Parse and process the data with alerting
      val processedStream = processIoTDataWithAlerting(
        spark, 
        kafkaStream, 
        pipelineConfig.alerting.recipientEmail, 
        emailGateway,
        PipelineConfig.toSensorAlertConfig(pipelineConfig.alerting.thresholds)
      )

      // Start the streaming query
      val query = processedStream.writeStream
        .format("console")
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", pipelineConfig.spark.checkpointLocation)
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime(pipelineConfig.spark.processingTimeSeconds, TimeUnit.SECONDS))
        .queryName("iot-alerting-pipeline")
        .start()

      println("=== IoT Alerting Pipeline Started ===")
      println(s"Consuming from Kafka topic: ${pipelineConfig.kafka.topic}")
      println(s"Sending alerts to: ${pipelineConfig.alerting.recipientEmail}")
      println("Press Ctrl+C to stop...")

      // Keep the application running
      query.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"Error in alerting pipeline: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * Read streaming data from Kafka
   */
  def readFromKafka(
      spark: SparkSession,
      bootstrapServers: String,
      topic: String,
      consumerGroupId: String,
      startingOffsets: String
  ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .option("kafka.consumer.group.id", consumerGroupId)
      .load()
  }

  /**
   * Process IoT sensor data and trigger alerts for anomalies.
   * Parses JSON messages, detects anomalies, and sends email notifications.
   */
  def processIoTDataWithAlerting(
      spark: SparkSession,
      kafkaStream: DataFrame,
      recipientEmail: String,
      emailGateway: EmailGateway,
      alertConfig: SensorAlertConfig
  ): DataFrame = {
    import spark.implicits._

    // Define schema for IoT sensor data
    val iotSchema = StructType(Seq(
      StructField("deviceId", StringType, nullable = false),
      StructField("temperature", DoubleType, nullable = true),
      StructField("humidity", DoubleType, nullable = true),
      StructField("pressure", DoubleType, nullable = true),
      StructField("motion", BooleanType, nullable = true),
      StructField("light", DoubleType, nullable = true),
      StructField("acidity", DoubleType, nullable = true),
      StructField("location", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("metadata", StructType(Seq(
        StructField("battery_level", IntegerType, nullable = true),
        StructField("signal_strength", IntegerType, nullable = true),
        StructField("firmware_version", StringType, nullable = true)
      )), nullable = true)
    ))

    // Parse Kafka messages and extract sensor data
    val parsedStream = kafkaStream
      .select(
        col("key").cast("string").as("kafka_key"),
        col("value").cast("string").as("raw_data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").as("kafka_timestamp")
      )
      // Parse JSON data with error handling
      .withColumn("parsed_data", from_json(col("raw_data"), iotSchema))
      .filter(col("parsed_data").isNotNull) // Filter out malformed JSON
      // Extract fields from parsed JSON
      .select(
        col("kafka_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("parsed_data.deviceId"),
        from_unixtime(col("parsed_data.timestamp")).cast("timestamp").as("sensor_timestamp"),
        col("parsed_data.temperature"),
        col("parsed_data.humidity"),
        col("parsed_data.pressure"),
        col("parsed_data.motion"),
        col("parsed_data.light"),
        col("parsed_data.acidity"),
        col("parsed_data.location"),
        col("parsed_data.metadata.battery_level").as("battery_level"),
        col("parsed_data.metadata.signal_strength").as("signal_strength"),
        col("parsed_data.metadata.firmware_version").as("firmware_version")
      )
      // Add processing metadata
      .withColumn("processing_time", current_timestamp())
      // Filter out invalid records
      .filter(col("deviceId").isNotNull && col("sensor_timestamp").isNotNull)

    // Apply alerting logic using foreachBatch for side effects
    parsedStream
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatchWithAlerting(batchDF, batchId, recipientEmail, emailGateway, alertConfig)
      }
      .outputMode(OutputMode.Update())
      .start()

    // Return the parsed stream for monitoring/logging
    parsedStream
  }

  /**
   * Process each batch and trigger alerts for anomalies.
   * This function handles the side effects (email sending) in a controlled manner.
   */
  def processBatchWithAlerting(
      batchDF: DataFrame,
      batchId: Long,
      recipientEmail: String,
      emailGateway: EmailGateway,
      alertConfig: SensorAlertConfig
  ): Unit = {
    import batchDF.sparkSession.implicits._
    
    println(s"Processing batch $batchId with ${batchDF.count()} records...")

    // Convert DataFrame rows to IoTSensorData case classes
    val sensorDataList = batchDF.collect().toList.flatMap { row =>
      try {
        val sensorData = IoTSensorData(
          deviceId = row.getAs[String]("deviceId"),
          temperature = Option(row.getAs[Double]("temperature")),
          humidity = Option(row.getAs[Double]("humidity")),
          pressure = Option(row.getAs[Double]("pressure")),
          motion = Option(row.getAs[Boolean]("motion")),
          light = Option(row.getAs[Double]("light")),
          acidity = Option(row.getAs[Double]("acidity")),
          location = row.getAs[String]("location"),
          timestamp = row.getAs[java.sql.Timestamp]("sensor_timestamp").getTime,
          metadata = SensorMetadata(
            batteryLevel = Option(row.getAs[Integer]("battery_level")).map(_.intValue()),
            signalStrength = Option(row.getAs[Integer]("signal_strength")).map(_.intValue()),
            firmwareVersion = Option(row.getAs[String]("firmware_version"))
          )
        )
        Some(sensorData)
      } catch {
        case e: Exception =>
          println(s"Error parsing sensor data from row: ${e.getMessage}")
          None
      }
    }

    // Process each sensor reading for anomalies
    var totalAnomalies = 0
    var emailsSent = 0

    sensorDataList.foreach { sensorData =>
      try {
        // Use AlertDetection pure functions to detect anomalies and format alerts
        val alertEmailOpt = AlertDetection.processToAlert(
          sensorData, 
          alertConfig,
          recipientEmail
        )

        // Send email if alert detected
        alertEmailOpt.foreach { email =>
          AlertDetection.sendAlert(email, emailGateway)
          emailsSent += 1
          println(s"Alert sent for device ${sensorData.deviceId}: ${email.subject}")
        }

        // Count anomalies for statistics
        val anomalies = AlertDetection.detectAnomalies(sensorData)
        totalAnomalies += anomalies.size

      } catch {
        case e: Exception =>
          println(s"Error processing alerts for device ${sensorData.deviceId}: ${e.getMessage}")
      }
    }

    if (totalAnomalies > 0) {
      println(s"Batch $batchId: Detected $totalAnomalies anomalies, sent $emailsSent alerts")
    }
  }

  /**
   * Alternative streaming approach using structured streaming transformations.
   * This approach uses UDFs for anomaly detection but avoids side effects in the stream.
   */
  def processIoTDataWithDetectionOnly(
      spark: SparkSession,
      kafkaStream: DataFrame
  ): DataFrame = {
    import spark.implicits._

    // Define UDF for anomaly detection
    val detectAnomaliesUDF = udf((
      deviceId: String,
      temperature: Double,
      humidity: Double,
      pressure: Double,
      motion: Boolean,
      light: Double,
      acidity: Double,
      location: String,
      timestamp: Long
    ) => {
      try {
        val sensorData = IoTSensorData(
          deviceId = deviceId,
          temperature = Option(temperature),
          humidity = Option(humidity),
          pressure = Option(pressure),
          motion = Option(motion),
          light = Option(light),
          acidity = Option(acidity),
          location = location,
          timestamp = timestamp,
          metadata = SensorMetadata(None, None, None)
        )
        
        val anomalies = AlertDetection.detectAnomalies(sensorData)
        anomalies.map(_.anomalyType).mkString(", ")
      } catch {
        case _: Exception => ""
      }
    })

    // Apply the UDF to detect anomalies
    val iotSchema = StructType(Seq(
      StructField("deviceId", StringType, nullable = false),
      StructField("temperature", DoubleType, nullable = true),
      StructField("humidity", DoubleType, nullable = true),
      StructField("pressure", DoubleType, nullable = true),
      StructField("motion", BooleanType, nullable = true),
      StructField("light", DoubleType, nullable = true),
      StructField("acidity", DoubleType, nullable = true),
      StructField("location", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    kafkaStream
      .select(col("value").cast("string").as("raw_data"))
      .withColumn("parsed_data", from_json(col("raw_data"), iotSchema))
      .filter(col("parsed_data").isNotNull)
      .select(
        col("parsed_data.deviceId"),
        col("parsed_data.temperature"),
        col("parsed_data.humidity"),
        col("parsed_data.pressure"),
        col("parsed_data.motion"),
        col("parsed_data.light"),
        col("parsed_data.acidity"),
        col("parsed_data.location"),
        col("parsed_data.timestamp")
      )
      .withColumn("anomalies", detectAnomaliesUDF(
        col("deviceId"),
        col("temperature"),
        col("humidity"),
        col("pressure"),
        col("motion"),
        col("light"),
        col("acidity"),
        col("location"),
        col("timestamp")
      ))
      .filter(col("anomalies") =!= "")
      .withColumn("alert_timestamp", current_timestamp())
  }
}
