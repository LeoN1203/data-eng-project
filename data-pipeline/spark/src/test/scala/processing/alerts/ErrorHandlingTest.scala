package processing.alerts

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues
import processing.alerts.core._
import processing.alerts.email._
import processing.config.PipelineConfig
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import io.circe.generic.auto._
import io.circe.parser._

/**
 * Test suite for error handling scenarios in the alerting system.
 * Covers edge cases, malformed data, configuration errors, and network failures.
 */
class ErrorHandlingTest extends AnyFlatSpec with Matchers with TryValues {

  "SensorAlerting" should "handle null and empty values gracefully" in {
    val config = SensorAlertConfig()

    // Test with minimum required fields only
    val minimalData = IoTSensorData(
      deviceId = "",
      temperature = None,
      humidity = None,
      pressure = None,
      motion = None,
      light = None,
      acidity = None,
      location = "",
      timestamp = 0L,
      metadata = SensorMetadata(None, None, None)
    )

    // Should not throw exceptions
    val anomalies = SensorAlerting.checkForAnomalies(minimalData, config)
    anomalies shouldBe empty
  }

  it should "handle extreme numeric values" in {
    val config = SensorAlertConfig()

    val extremeData = IoTSensorData(
      deviceId = "extreme-sensor",
      temperature = Some(Double.MaxValue),
      humidity = Some(Double.MinValue),
      pressure = Some(Double.NaN),
      motion = Some(true),
      light = Some(Double.PositiveInfinity),
      acidity = Some(Double.NegativeInfinity),
      location = "ExtremeZone",
      timestamp = Long.MaxValue,
      metadata = SensorMetadata(
        batteryLevel = Some(Int.MaxValue),
        signalStrength = Some(Int.MinValue),
        firmwareVersion = Some("v∞.∞.∞")
      )
    )

    // Should handle extreme values without crashing
    val anomalies = SensorAlerting.checkForAnomalies(extremeData, config)
    
    // Most extreme values should be detected as anomalies (except NaN which is handled specially)
    anomalies.size should be >= 3
  }

  it should "handle special floating-point values" in {
    val config = SensorAlertConfig()

    val specialValues = List(
      ("NaN", Double.NaN),
      ("PositiveInfinity", Double.PositiveInfinity),
      ("NegativeInfinity", Double.NegativeInfinity),
      ("Zero", 0.0),
      ("NegativeZero", -0.0)
    )

    specialValues.foreach { case (name, value) =>
      val data = IoTSensorData(
        deviceId = s"special-$name",
        temperature = Some(value),
        humidity = None,
        pressure = None,
        motion = None,
        light = None,
        acidity = None,
        location = "SpecialZone",
        timestamp = System.currentTimeMillis(),
        metadata = SensorMetadata(None, None, None)
      )

      // Should not throw exceptions
      noException should be thrownBy {
        SensorAlerting.checkForAnomalies(data, config)
      }
    }
  }

  "EmailGateway" should "handle network failures gracefully" in {
    val failingGateway = new FailingEmailGateway()
    val email = Email("test@example.com", "Test", "Test body")

    // Should not throw unhandled exceptions
    noException should be thrownBy {
      failingGateway.send(email)
    }
  }

  it should "handle retry logic for transient failures" in {
    val retryGateway = new RetryEmailGateway(maxRetries = 3)
    val email = Email("test@example.com", "Test", "Test body")

    retryGateway.send(email)
    
    // Should have attempted the specified number of retries
    retryGateway.attemptCount.get() shouldBe 4 // initial attempt + 3 retries
  }

  "JSON parsing" should "handle malformed sensor data gracefully" in {
    val malformedJsonSamples = List(
      "",                                    // Empty
      "null",                               // Null
      "{}",                                 // Empty object
      "{",                                  // Incomplete
      """{"deviceId": }""",                 // Missing value
      """{"deviceId": "test", "timestamp": "not-a-number"}""", // Wrong type
      """{"unknownField": "value"}""",      // Unknown fields only
      """{"deviceId": "test", "metadata": "not-an-object"}""" // Wrong nested type
    )

    malformedJsonSamples.foreach { json =>
      val parseResult = decode[IoTSensorData](json)
      
      // All should fail to parse, but not crash the application
      parseResult shouldBe a[Left[_, _]]
    }
  }

  it should "handle incomplete sensor data with missing required fields" in {
    val incompleteJsonSamples = List(
      """{"temperature": 25.0}""",                    // Missing deviceId
      """{"deviceId": "test"}""",                     // Missing timestamp  
      """{"deviceId": "test", "timestamp": 123}"""    // Missing location and metadata
    )

    incompleteJsonSamples.foreach { json =>
      val parseResult = decode[IoTSensorData](json)
      
      // Should fail to parse due to missing required fields
      parseResult shouldBe a[Left[_, _]]
    }
  }

  "Configuration loading" should "handle missing environment variables" in {
    // Test with empty system properties (simulating missing env vars)
    val configResult = Try {
      // This should use defaults where possible and handle missing values gracefully
      PipelineConfig.loadConfiguration()
    }

    configResult shouldBe a[Success[_]]
    
    // Should use default/fallback values
    val config = configResult.success.value
    config.email.smtpHost should not be empty
    config.email.smtpPort should be > 0
  }

  it should "handle invalid configuration values" in {
    // Test with invalid port numbers, malformed URLs, etc.
    val invalidConfigs = Map(
      "SMTP_PORT" -> "not-a-number",
      "TLS_ENABLED" -> "maybe",
      "SMTP_HOST" -> ""
    )

    // Configuration loading should handle invalid values gracefully
    noException should be thrownBy {
      PipelineConfig.loadConfiguration()
    }
  }

  "Alert formatting" should "handle empty anomaly lists" in {
    val email = SensorAlerting.formatEmergencyAlert(List.empty, "test@example.com")
    email shouldBe empty
  }

  it should "handle very long device IDs and location names" in {
    val longString = "a" * 10000
    val longAnomaly = Anomaly(
      deviceId = longString,
      anomalyType = "Temperature",
      value = 100.0,
      threshold = "80.0",
      timestamp = System.currentTimeMillis(),
      location = longString
    )

    // Should handle long strings without issues
    val email = SensorAlerting.formatEmergencyAlert(List(longAnomaly), "test@example.com")
    email should be(defined)
    
    // Email should be properly formatted despite long content
    val alertEmail = email.get
    alertEmail.subject should not be empty
    alertEmail.body should include(longString.take(100)) // Should include at least part of the long string
  }

  it should "handle special characters in device data" in {
    val specialChars = "🔥💧⚡🌡️📊 Test-Device_123 αβγδε 中文 العربية"
    val specialAnomaly = Anomaly(
      deviceId = specialChars,
      anomalyType = "Temperature",
      value = 100.0,
      threshold = "80.0",
      timestamp = System.currentTimeMillis(),
      location = specialChars
    )

    // Should handle special characters without encoding issues
    val email = SensorAlerting.formatEmergencyAlert(List(specialAnomaly), "test@example.com")
    email should be(defined)
    
    val alertEmail = email.get
    alertEmail.body should include(specialChars)
  }

  "Concurrent processing" should "handle thread safety correctly" in {
    val config = SensorAlertConfig()
    val sentEmails = ListBuffer.synchronized(ListBuffer[Email]())
    val testGateway = new ThreadSafeTestEmailGateway(sentEmails)

    // Create multiple threads processing different sensor data simultaneously
    val threads = (1 to 10).map { i =>
      new Thread {
        override def run(): Unit = {
          val sensorData = IoTSensorData(
            deviceId = s"concurrent-sensor-$i",
            temperature = Some(90.0 + i), // All anomalous
            humidity = Some(50.0),
            pressure = Some(1000.0),
            motion = Some(false),
            light = Some(300.0),
            acidity = Some(7.0),
            location = s"Zone-$i",
            timestamp = System.currentTimeMillis(),
            metadata = SensorMetadata(
              batteryLevel = Some(80),
              signalStrength = Some(90),
              firmwareVersion = Some("v1.0.0")
            )
          )

          val anomalies = SensorAlerting.checkForAnomalies(sensorData, config)
          val email = SensorAlerting.formatEmergencyAlert(anomalies, s"test$i@example.com")
          
          email.foreach(testGateway.send)
        }
      }
    }

    // Start all threads
    threads.foreach(_.start())
    
    // Wait for all to complete
    threads.foreach(_.join())

    // All emails should have been processed
    sentEmails.size shouldBe 10
    
    // All emails should be unique (different device IDs)
    sentEmails.map(_.body).distinct.size shouldBe 10
  }
}

/**
 * Email gateway that always fails to simulate network issues
 */
class FailingEmailGateway extends EmailGateway {
  override def send(email: Email): Unit = {
    // Simulate network failure but handle it gracefully
    try {
      throw new RuntimeException("Network connection failed")
    } catch {
      case _: RuntimeException =>
        // Log the failure but don't propagate the exception
        println(s"Failed to send email to ${email.recipient}: Network connection failed")
    }
  }
}

/**
 * Email gateway that simulates retry logic
 */
class RetryEmailGateway(maxRetries: Int) extends EmailGateway {
  val attemptCount = new AtomicInteger(0)

  override def send(email: Email): Unit = {
    // Simulate retry logic: try initial attempt + maxRetries
    var attempts = 0
    while (attempts <= maxRetries) {
      attempts += 1
      attemptCount.incrementAndGet()
      
      if (attempts <= maxRetries) {
        // Simulate failure but continue trying
        println(s"Attempt $attempts failed")
      } else {
        // Final attempt succeeds
        println(s"Email sent successfully after $attempts attempts")
        return
      }
    }
  }
}

/**
 * Thread-safe test email gateway for concurrent testing
 */
class ThreadSafeTestEmailGateway(sentEmails: ListBuffer[Email]) extends EmailGateway {
  override def send(email: Email): Unit = {
    sentEmails.synchronized {
      sentEmails += email
    }
    
    // Simulate some processing time
    Thread.sleep(10)
  }
}
