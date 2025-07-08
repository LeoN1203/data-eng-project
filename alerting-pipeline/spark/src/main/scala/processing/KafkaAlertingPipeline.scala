package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import java.util.concurrent.TimeUnit
import scala.util.{Try, Success, Failure}
import processing.alerts.core._
import processing.alerts.email._
import processing.config.PipelineConfig

/**
 * Kafka-based real-time alerting pipeline using Spark Structured Streaming.
 * Consumes IoT sensor data from Kafka and sends email alerts for detected anomalies.
 * Integrates the SensorAlerting module with streaming data processing.
 */
object KafkaAlertingPipeline {

  def main(args: Array[String]): Unit = {
    
    // Load configuration
    val pipelineConfig = PipelineConfig.loadConfiguration()
    
    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName(pipelineConfig.spark.appName)
      .master("local[*]")
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

    Try {
      println("=== IoT Alerting Pipeline Starting ===")
      println(s"Kafka servers: ${pipelineConfig.kafka.bootstrapServers}")
      println(s"Kafka topic: ${pipelineConfig.kafka.topic}")
      println(s"Consumer group: ${pipelineConfig.kafka.consumerGroupId}")
      println(s"Starting offsets: ${pipelineConfig.kafka.startingOffsets}")
      println(s"Alert recipient: ${pipelineConfig.alerting.recipientEmail}")
      
      // Read from Kafka stream
      val kafkaStream = readFromKafka(
        spark, 
        pipelineConfig.kafka.bootstrapServers, 
        pipelineConfig.kafka.topic,
        pipelineConfig.kafka.consumerGroupId,
        pipelineConfig.kafka.startingOffsets
      )
      
      println("Kafka stream created successfully")

      // Log any invalid messages that don't match the expected schema
      Try {
        logInvalidMessages(kafkaStream)
      } match {
        case Failure(e) =>
          println(s"Warning: Could not validate message schemas: ${e.getMessage}")
        case _ =>
      }

      // Start the alerting pipeline
      val alertingQuery = startAlertingPipeline(
        spark, 
        kafkaStream, 
        pipelineConfig.alerting.recipientEmail, 
        emailGateway,
        PipelineConfig.toSensorAlertConfig(pipelineConfig.alerting.thresholds),
        pipelineConfig.spark.checkpointLocation + "/alerting",
        pipelineConfig.spark.processingTimeSeconds
      )

      println("=== IoT Alerting Pipeline Started ===")
      println("Press Ctrl+C to stop...")

      // Keep the application running
      alertingQuery.awaitTermination()

    } match {
      case Failure(e) =>
        println(s"Error in alerting pipeline: ${e.getMessage}")
        e.printStackTrace()
      case _ =>
    }
    spark.stop()
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
    println(s"Creating Kafka stream with:")
    println(s"  Bootstrap servers: $bootstrapServers")
    println(s"  Topic: $topic")
    println(s"  Consumer group: $consumerGroupId")
    println(s"  Starting offsets: $startingOffsets")
    
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
   * Start the alerting pipeline that processes IoT sensor data and triggers alerts.
   * Returns a StreamingQuery that can be awaited for termination.
   */
  def startAlertingPipeline(
      spark: SparkSession,
      kafkaStream: DataFrame,
      recipientEmail: String,
      emailGateway: EmailGateway,
      alertConfig: SensorAlertConfig,
      checkpointLocation: String,
      processingTimeSeconds: Int
  ): org.apache.spark.sql.streaming.StreamingQuery = {
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
      // Log malformed/filtered JSON for debugging
      .withColumn("is_valid_json", col("parsed_data").isNotNull)
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

    println("Starting Kafka stream processing with foreachBatch...")

    // Start the streaming query with foreachBatch for alerting
    parsedStream
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatchWithAlerting(batchDF, batchId, recipientEmail, emailGateway, alertConfig)
      }
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(processingTimeSeconds, TimeUnit.SECONDS))
      .queryName("iot-alerting-pipeline")
      .start()
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
    
    val recordCount = batchDF.count()
    println(s"=== Processing batch $batchId with $recordCount records ===")
    
    if (recordCount == 0) {
      println("Batch is empty, skipping processing")
      println("Note: If messages were sent but batch is empty, check:")
      println("  - Message schema matches expected format (deviceId, location fields)")
      println("  - JSON is valid and properly formatted") 
      return
    }

    // Show some sample data for debugging
    println("Sample records from batch:")
    batchDF.select("deviceId", "temperature", "humidity", "pressure", "location").show(5, truncate = false)

    // Convert DataFrame rows to IoTSensorData case classes
    val sensorDataList = batchDF.collect().toList.flatMap { row =>
      Try {
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
        println(s"Parsed sensor data for device ${sensorData.deviceId}: temp=${sensorData.temperature}, humid=${sensorData.humidity}")
        sensorData
      } match {
        case Success(data) => Some(data)
        case Failure(e) =>
          println(s"Error parsing sensor data from row: ${e.getMessage}")
          e.printStackTrace()
          None
      }
    }

    println(s"Successfully parsed ${sensorDataList.size} sensor records")

    // Process each sensor reading for anomalies
    var totalAnomalies = 0
    var emailsSent = 0

    sensorDataList.foreach { sensorData =>
      Try {
        println(s"Checking anomalies for device ${sensorData.deviceId}...")
        
        // Use SensorAlerting pure functions to detect anomalies and format alerts
        val anomalies = SensorAlerting.checkForAnomalies(sensorData, alertConfig)
        
        if (anomalies.nonEmpty) {
          println(s"ANOMALIES DETECTED for device ${sensorData.deviceId}: ${anomalies.map(_.anomalyType).mkString(", ")}")
          
          val alertEmailOpt = SensorAlerting.formatEmergencyAlert(anomalies, recipientEmail)
          
          // Send email if alert detected
          alertEmailOpt.foreach { email =>
            println(s"Sending alert email: ${email.subject}")
            emailGateway.send(email)
            emailsSent += 1
            println(s"✓ Alert sent for device ${sensorData.deviceId}")
          }
        } else {
          println(s"No anomalies detected for device ${sensorData.deviceId}")
        }

        // Count anomalies for statistics
        totalAnomalies += anomalies.size

      } match {
        case Failure(e) =>
          println(s"Error processing alerts for device ${sensorData.deviceId}: ${e.getMessage}")
          e.printStackTrace()
        case _ =>
      }
    }

    println(s"=== Batch $batchId Summary: $totalAnomalies anomalies detected, $emailsSent alerts sent ===")
    println()
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
      Try {
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
        
        val anomalies = SensorAlerting.checkForAnomalies(sensorData, SensorAlertConfig())
        anomalies.map(_.anomalyType).mkString(", ")
      }.getOrElse("")
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

  /**
   * Log messages that failed to parse due to schema mismatch.
   * This helps debug cases where messages are silently filtered out.
   */
  def logInvalidMessages(kafkaStream: DataFrame): Unit = {
    // Log a sample of invalid/malformed messages for debugging
    val invalidMessages = kafkaStream
      .select(
        col("value").cast("string").as("raw_data"),
        col("topic"),
        col("partition"),
        col("offset")
      )
      .withColumn("parsed_data", from_json(col("raw_data"), StructType(Seq(
        StructField("deviceId", StringType, nullable = false),
        StructField("device_id", StringType, nullable = true), // Common mistake
        StructField("temperature", DoubleType, nullable = true),
        StructField("humidity", DoubleType, nullable = true),
        StructField("location", StringType, nullable = true)
      ))))
      .filter(col("parsed_data").isNull || col("parsed_data.deviceId").isNull || col("parsed_data.location").isNull)
      .limit(10)

    Try {
      val invalidCount = invalidMessages.count()
      if (invalidCount > 0) {
        println(s"Warning: $invalidCount messages failed schema validation. Sample invalid messages:")
        invalidMessages.show(5, truncate = false)
        println("Common issues: missing 'deviceId', 'location' fields, or using 'device_id' instead of 'deviceId'")
      }
    } match {
      case Failure(e) =>
        println(s"Could not check for invalid messages: ${e.getMessage}")
      case _ =>
    }
  }
}
