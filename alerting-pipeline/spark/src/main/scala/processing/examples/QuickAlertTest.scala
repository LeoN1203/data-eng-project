package processing.examples

import processing.alerts.core._
import processing.alerts.email._

/**
 * Quick test of the alerting system without Kafka dependency.
 * This demonstrates the core functionality works correctly.
 */
object QuickAlertTest {

  def main(args: Array[String]): Unit = {
    println("=== Quick Alerting Pipeline Test ===")
    
    // Create test sensor data with multiple anomalies
    val testData = IoTSensorData(
      deviceId = "test-sensor-001",
      temperature = Some(95.0), // Too high (normal: 0-80°C)
      humidity = Some(45.0),     // Normal
      pressure = Some(850.0),    // Too low (normal: 950-1050 hPa)
      motion = Some(true),
      light = Some(300.0),       // Normal
      acidity = Some(9.5),       // Too high (normal: 6-8 pH)
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(15),   // Low battery (threshold: 20%)
        signalStrength = Some(80),
        firmwareVersion = Some("v2.1.0")
      )
    )
    
    println(s"Testing sensor data from device: ${testData.deviceId}")
    println(s"Location: ${testData.location}")
    
    // Test anomaly detection
    val anomalies = SensorAlerting.checkForAnomalies(testData, SensorAlertConfig())
    
    println(s"\nDetected ${anomalies.size} anomalies:")
    anomalies.foreach { anomaly =>
      println(s"  - ${anomaly.anomalyType}: ${anomaly.value} (threshold: ${anomaly.threshold})")
    }
    
    // Test email generation
    val recipientEmail = "test@example.com"
    val emailOpt = SensorAlerting.formatEmergencyAlert(anomalies, recipientEmail)
    
    if (emailOpt.isDefined) {
      val email = emailOpt.get
      println(s"\n=== Generated Email Alert ===")
      println(s"To: ${email.recipient}")
      println(s"Subject: ${email.subject}")
      println(s"Body:\n${email.body}")
      
      // Send using console gateway
      val consoleGateway = new ConsoleEmailGateway()
      consoleGateway.send(email)
      
      println("\n✅ Test completed successfully!")
      println("The pipeline detected anomalies and generated email alerts correctly.")
    } else {
      println("\n❌ No email generated - this indicates an issue!")
    }
    
    // Test normal data (should not generate alerts)
    println("\n=== Testing Normal Data ===")
    val normalData = testData.copy(
      temperature = Some(22.0),  // Normal
      pressure = Some(1013.0),   // Normal
      acidity = Some(7.0),       // Normal
      metadata = testData.metadata.copy(batteryLevel = Some(85)) // Normal
    )
    
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalData, SensorAlertConfig())
    println(s"Normal data anomalies: ${normalAnomalies.size} (should be 0)")
    
    if (normalAnomalies.isEmpty) {
      println("✅ Normal data test passed - no false alerts!")
    } else {
      println("❌ Normal data test failed - false alerts detected!")
    }
  }
}
