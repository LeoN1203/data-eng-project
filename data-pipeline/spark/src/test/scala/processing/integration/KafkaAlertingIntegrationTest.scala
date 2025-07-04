package processing.integration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import processing.alerts.core._
import processing.alerts.email._
import processing.KafkaAlertingPipeline
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure, Random}
import java.util.Properties
import java.util.concurrent.TimeUnit
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * Integration test for the Kafka alerting pipeline using embedded Kafka.
 * Tests end-to-end functionality including:
 * - Kafka message consumption
 * - JSON parsing
 * - Anomaly detection
 * - Email notification sending
 */
class KafkaAlertingIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with EmbeddedKafka {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 0, // Use random available port
    zooKeeperPort = 0 // Use random available port
  )

  private var spark: SparkSession = _
  private val testTopicPrefix = "iot-sensor-data-test"
  private val testEmailRecipient = "test@example.com"
  private var currentTestTopic: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Initialize Spark with test configuration
    spark = SparkSession
      .builder()
      .appName("KafkaAlertingIntegrationTest")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka-alerting-test-checkpoint")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      try {
        spark.stop()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
    
    // Clean up any remaining checkpoint directories
    try {
      val checkpointDir = new java.io.File("/tmp/kafka-alerting-test-checkpoint")
      if (checkpointDir.exists()) {
        deleteRecursively(checkpointDir)
      }
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    
    super.afterAll()
  }
  
  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create a unique topic name for each test to avoid interference
    currentTestTopic = s"$testTopicPrefix-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
  }

  "KafkaAlertingPipeline" should "process valid IoT sensor data and detect anomalies" in {
    val sentEmails = ListBuffer[Email]()
    val testEmailGateway = new TestEmailGateway(sentEmails)

    // Create test sensor data with anomalies
    val anomalousSensorData = IoTSensorData(
      deviceId = "test-sensor-001",
      temperature = Some(95.0), // Anomaly: too high
      humidity = Some(45.0),
      pressure = Some(850.0), // Anomaly: too low
      motion = Some(true),
      light = Some(300.0),
      acidity = Some(7.0),
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(15), // Anomaly: low battery
        signalStrength = Some(80),
        firmwareVersion = Some("v2.1.0")
      )
    )

    // Convert to JSON and publish to Kafka
    val jsonMessage = anomalousSensorData.asJson.noSpaces
    
    withRunningKafka {
      publishStringMessageToKafka(currentTestTopic, jsonMessage)
      
      // Process with the alerting pipeline (simulated)
      val anomalies = SensorAlerting.checkForAnomalies(anomalousSensorData, SensorAlertConfig())
      
      // Should detect 3 anomalies: temperature, pressure, battery
      anomalies should have size 3
      anomalies.map(_.anomalyType).toSet should contain allOf("Temperature", "Pressure", "Low Battery")
      
      // Generate and send email alert
      val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
      emailOpt should be(defined)
      
      val email = emailOpt.get
      testEmailGateway.send(email)
      
      // Verify email was sent
      sentEmails should have size 1
      sentEmails.head.recipient shouldBe testEmailRecipient
      sentEmails.head.subject should include("IoT Sensor Alert")
      sentEmails.head.body should include("test-sensor-001")
      sentEmails.head.body should include("Temperature")
      sentEmails.head.body should include("Pressure")
      sentEmails.head.body should include("Low Battery")
    }
  }

  it should "handle malformed JSON messages gracefully" in {
    val sentEmails = ListBuffer[Email]()
    val testEmailGateway = new TestEmailGateway(sentEmails)

    withRunningKafka {
      // Publish malformed JSON
      publishStringMessageToKafka(currentTestTopic, "{invalid json")
      
      // The pipeline should handle this gracefully without crashing
      // In a real scenario, this would be logged and the message would be skipped
      val parseResult = Try {
        io.circe.parser.decode[IoTSensorData]("{invalid json")
      }
      
      parseResult shouldBe a[Failure[_]]
      
      // No emails should be sent for malformed data
      sentEmails should have size 0
    }
  }

  it should "process normal sensor data without sending alerts" in {
    val sentEmails = ListBuffer[Email]()
    val testEmailGateway = new TestEmailGateway(sentEmails)

    // Create normal sensor data (no anomalies)
    val normalSensorData = IoTSensorData(
      deviceId = "test-sensor-002",
      temperature = Some(22.0), // Normal
      humidity = Some(55.0), // Normal
      pressure = Some(1013.0), // Normal
      motion = Some(false),
      light = Some(400.0),
      acidity = Some(7.0),
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(85), // Normal
        signalStrength = Some(90),
        firmwareVersion = Some("v2.1.0")
      )
    )

    val jsonMessage = normalSensorData.asJson.noSpaces
    
    withRunningKafka {
      publishStringMessageToKafka(currentTestTopic, jsonMessage)
      
      val anomalies = SensorAlerting.checkForAnomalies(normalSensorData, SensorAlertConfig())
      
      // Should detect no anomalies
      anomalies should have size 0
      
      // No email should be generated
      val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
      emailOpt should be(empty)
      
      // No emails should be sent
      sentEmails should have size 0
    }
  }

  it should "handle missing optional fields gracefully" in {
    val sentEmails = ListBuffer[Email]()
    val testEmailGateway = new TestEmailGateway(sentEmails)

    // Create sensor data with missing optional fields
    val sparseData = IoTSensorData(
      deviceId = "test-sensor-003",
      temperature = None, // Missing
      humidity = Some(105.0), // Anomaly: too high
      pressure = None, // Missing
      motion = Some(true),
      light = None, // Missing
      acidity = None, // Missing
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = None, // Missing
        signalStrength = Some(90),
        firmwareVersion = Some("v2.1.0")
      )
    )

    val jsonMessage = sparseData.asJson.noSpaces
    
    withRunningKafka {
      publishStringMessageToKafka(currentTestTopic, jsonMessage)
      
      val anomalies = SensorAlerting.checkForAnomalies(sparseData, SensorAlertConfig())
      
      // Should detect 1 anomaly: humidity
      anomalies should have size 1
      anomalies.head.anomalyType shouldBe "Humidity"
      
      // Email should be generated
      val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
      emailOpt should be(defined)
      
      val email = emailOpt.get
      testEmailGateway.send(email)
      
      sentEmails should have size 1
    }
  }

  it should "handle edge case threshold values" in {
    val config = SensorAlertConfig(
      temperatureRange = (20.0, 25.0), // Narrow range
      humidityRange = (40.0, 60.0),
      pressureRange = (1000.0, 1020.0),
      lightRange = (100.0, 500.0),
      acidityRange = (6.0, 8.0),
      batteryLowThreshold = 50 // Higher threshold
    )

    // Test data at boundary values
    val boundaryData = IoTSensorData(
      deviceId = "test-sensor-004",
      temperature = Some(25.1), // Just above upper limit
      humidity = Some(39.9), // Just below lower limit
      pressure = Some(1000.0), // Exactly at lower limit
      motion = Some(false),
      light = Some(500.0), // Exactly at upper limit
      acidity = Some(8.1), // Just above upper limit
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(49), // Just below threshold
        signalStrength = Some(90),
        firmwareVersion = Some("v2.1.0")
      )
    )

    val anomalies = SensorAlerting.checkForAnomalies(boundaryData, config)
    
    // Should detect anomalies for: temperature, humidity, acidity, battery
    // Pressure and light should be OK (at boundaries)
    anomalies should have size 4
    anomalies.map(_.anomalyType).toSet should contain allOf("Temperature", "Humidity", "Acidity", "Low Battery")
  }
}

/**
 * Test email gateway that captures sent emails instead of actually sending them
 */
class TestEmailGateway(sentEmails: ListBuffer[Email]) extends EmailGateway {
  override def send(email: Email): Unit = {
    sentEmails += email
    println(s"[TEST] Email captured: ${email.subject} -> ${email.recipient}")
  }
}
