package scala.processing.alerts

import scala.processing.alerts.core._
import scala.processing.alerts.email._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

class AlertingTest extends AnyFlatSpec with Matchers {

  "SensorAlerting" should "detect temperature anomalies" in {
    val config = SensorAlertConfig()
    val testData = IoTSensorData(
      deviceId = "test-001",
      temperature = 35.0, // Above normal threshold
      humidity = 50.0,
      pressure = 1015.0,
      motion = false,
      light = 300.0,
      acidity = 7.0,
      location = "test-location",
      timestamp = Instant.now.toEpochMilli,
      metadata = Map.empty
    )

    val anomalies = SensorAlerting.checkForAnomalies(testData, config)
    anomalies should not be empty
    anomalies.head.anomalyType should be("Temperature")
  }

  it should "not detect anomalies for normal readings" in {
    val config = SensorAlertConfig()
    val testData = IoTSensorData(
      deviceId = "test-001",
      temperature = 20.0, // Normal
      humidity = 50.0,    // Normal
      pressure = 1015.0,  // Normal
      motion = false,     // Normal
      light = 300.0,
      acidity = 7.0,      // Normal
      location = "test-location",
      timestamp = Instant.now.toEpochMilli,
      metadata = Map.empty
    )

    val anomalies = SensorAlerting.checkForAnomalies(testData, config)
    anomalies should be(empty)
  }

  "ConsoleEmailGateway" should "print email to console" in {
    val gateway = new ConsoleEmailGateway()
    val email = Email("test@example.com", "Test Subject", "Test Body")
    
    // This should not throw an exception
    noException should be thrownBy gateway.send(email)
  }
}
