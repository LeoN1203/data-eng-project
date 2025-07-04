package processing.integration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import processing.alerts.core._
import processing.alerts.email._
import scala.collection.mutable.ListBuffer
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * Simplified integration test for the alerting pipeline.
 * Tests the core integration between components without requiring embedded Kafka
 * to avoid port binding issues and test environment complexity.
 */
class SimplifiedIntegrationTest extends AnyFlatSpec with Matchers {

  private val testEmailRecipient = "test@example.com"

  "AlertingPipeline Integration" should "process IoT sensor data and detect anomalies end-to-end" in {
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

    // Test JSON serialization/deserialization (simulating Kafka message processing)
    val jsonMessage = anomalousSensorData.asJson.noSpaces
    val parseResult = io.circe.parser.decode[IoTSensorData](jsonMessage)
    parseResult.isRight shouldBe true
    val parsedData = parseResult.right.get
    
    // Test anomaly detection
    val anomalies = SensorAlerting.checkForAnomalies(parsedData, SensorAlertConfig())
    
    // Should detect 3 anomalies: temperature, pressure, battery
    anomalies should have size 3
    anomalies.map(_.anomalyType).toSet should contain allOf("Temperature", "Pressure", "Low Battery")
    
    // Test email generation and sending
    val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
    emailOpt should be(defined)
    
    val email = emailOpt.get
    testEmailGateway.send(email)
    
    // Verify email was sent correctly
    sentEmails should have size 1
    sentEmails.head.recipient shouldBe testEmailRecipient
    sentEmails.head.subject should include("IoT Alert")
    sentEmails.head.body should include("test-sensor-001")
    sentEmails.head.body should include("Temperature")
    sentEmails.head.body should include("Pressure")
    sentEmails.head.body should include("Low Battery")
  }

  it should "handle malformed JSON messages gracefully" in {
    // Test JSON parsing error handling
    val parseResult = io.circe.parser.decode[IoTSensorData]("{invalid json")
    
    parseResult.isLeft shouldBe true // Should be a parsing failure
    
    // In a real pipeline, this would be logged and the message would be skipped
    // No exceptions should be thrown to the main processing loop
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

    // Test the full pipeline
    val jsonMessage = normalSensorData.asJson.noSpaces
    val parsedData = io.circe.parser.decode[IoTSensorData](jsonMessage).right.get
    
    val anomalies = SensorAlerting.checkForAnomalies(parsedData, SensorAlertConfig())
    
    // Should detect no anomalies
    anomalies should have size 0
    
    // No email should be generated
    val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
    emailOpt should be(empty)
    
    // No emails should be sent
    sentEmails should have size 0
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

    // Test the full pipeline with sparse data
    val jsonMessage = sparseData.asJson.noSpaces
    val parsedData = io.circe.parser.decode[IoTSensorData](jsonMessage).right.get
    
    val anomalies = SensorAlerting.checkForAnomalies(parsedData, SensorAlertConfig())
    
    // Should detect 1 anomaly: humidity
    anomalies should have size 1
    anomalies.head.anomalyType shouldBe "Humidity"
    
    // Email should be generated and sent
    val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, testEmailRecipient)
    emailOpt should be(defined)
    
    val email = emailOpt.get
    testEmailGateway.send(email)
    
    sentEmails should have size 1
  }

  it should "handle edge case threshold values correctly" in {
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
