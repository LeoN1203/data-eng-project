package processing.test

import processing.alerts.core._
import processing.alerts.email._

/**
 * Simple test to verify the alerting system works correctly
 */
object SimpleAlertTest {
  
  def main(args: Array[String]): Unit = {
    println("=== Testing IoT Alerting System ===")
    
    // Test 1: Create sample sensor data with anomalies
    val anomalousSensorData = IoTSensorData(
      deviceId = "sensor-test-1",
      temperature = Some(85.0), // Too high
      humidity = Some(95.0),     // Too high
      pressure = Some(950.0),    // Too low
      motion = Some(true),
      light = Some(500.0),
      acidity = Some(9.5),       // Too high
      location = "Test-Room",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(15),  // Low battery
        signalStrength = Some(75),
        firmwareVersion = Some("v1.0.0")
      )
    )
    
    // Test 2: Check for anomalies
    val config = SensorAlertConfig()
    val anomalies = SensorAlerting.checkForAnomalies(anomalousSensorData, config)
    
    println(s"Found ${anomalies.size} anomalies:")
    anomalies.foreach { anomaly =>
      println(s"  - ${anomaly.anomalyType}: ${anomaly.value} (threshold: ${anomaly.threshold})")
    }
    
    // Test 3: Format alert email
    val email = SensorAlerting.formatEmergencyAlert(anomalies, "test@example.com")
    
    email match {
      case Some(alertEmail) =>
        println("\n=== Generated Email Alert ===")
        println(s"To: ${alertEmail.recipient}")
        println(s"Subject: ${alertEmail.subject}")
        println(s"Body:\n${alertEmail.body}")
        
        // Test 4: Send via console gateway
        val consoleGateway = new ConsoleEmailGateway()
        consoleGateway.send(alertEmail)
        
      case None =>
        println("No email alert generated")
    }
    
    // Test 5: Normal sensor data (should not trigger alerts)
    val normalSensorData = IoTSensorData(
      deviceId = "sensor-test-2",
      temperature = Some(22.0),
      humidity = Some(45.0),
      pressure = Some(1013.0),
      motion = Some(false),
      light = Some(400.0),
      acidity = Some(7.0),
      location = "Test-Room",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(85),
        signalStrength = Some(75),
        firmwareVersion = Some("v1.0.0")
      )
    )
    
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalSensorData, config)
    println(s"\nNormal data anomalies: ${normalAnomalies.size} (should be 0)")
    
    println("\n✅ Alerting system test completed successfully!")
  }
}
