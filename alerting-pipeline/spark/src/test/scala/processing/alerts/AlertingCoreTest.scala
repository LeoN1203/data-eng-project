package processing.alerts

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues
import processing.alerts.core._
import processing.alerts.email._

/**
 * Comprehensive unit tests for the core alerting functionality.
 * Tests pure business logic, threshold detection, and email formatting.
 */
class AlertingCoreTest extends AnyFlatSpec with Matchers with OptionValues {

  "SensorAlerting.checkForAnomalies" should "detect temperature anomalies correctly" in {
    val config = SensorAlertConfig(temperatureRange = (10.0, 30.0))
    
    // Test below range
    val coldData = createTestSensorData(temperature = Some(5.0))
    val coldAnomalies = SensorAlerting.checkForAnomalies(coldData, config)
    coldAnomalies should have size 1
    coldAnomalies.head.anomalyType shouldBe "Temperature"
    coldAnomalies.head.value shouldBe "5.0°C"
    coldAnomalies.head.threshold shouldBe "10.0°C to 30.0°C"

    // Test above range
    val hotData = createTestSensorData(temperature = Some(35.0))
    val hotAnomalies = SensorAlerting.checkForAnomalies(hotData, config)
    hotAnomalies should have size 1
    hotAnomalies.head.anomalyType shouldBe "Temperature"
    
    // Test within range
    val normalData = createTestSensorData(temperature = Some(20.0))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalData, config)
    normalAnomalies.filter(_.anomalyType == "Temperature") shouldBe empty

    // Test at boundaries
    val boundaryLowData = createTestSensorData(temperature = Some(10.0))
    val boundaryLowAnomalies = SensorAlerting.checkForAnomalies(boundaryLowData, config)
    boundaryLowAnomalies.filter(_.anomalyType == "Temperature") shouldBe empty

    val boundaryHighData = createTestSensorData(temperature = Some(30.0))
    val boundaryHighAnomalies = SensorAlerting.checkForAnomalies(boundaryHighData, config)
    boundaryHighAnomalies.filter(_.anomalyType == "Temperature") shouldBe empty
  }

  it should "detect humidity anomalies correctly" in {
    val config = SensorAlertConfig(humidityRange = (20.0, 80.0))
    
    val lowHumidityData = createTestSensorData(humidity = Some(15.0))
    val lowAnomalies = SensorAlerting.checkForAnomalies(lowHumidityData, config)
    lowAnomalies.filter(_.anomalyType == "Humidity") should have size 1

    val highHumidityData = createTestSensorData(humidity = Some(90.0))
    val highAnomalies = SensorAlerting.checkForAnomalies(highHumidityData, config)
    highAnomalies.filter(_.anomalyType == "Humidity") should have size 1

    val normalHumidityData = createTestSensorData(humidity = Some(50.0))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalHumidityData, config)
    normalAnomalies.filter(_.anomalyType == "Humidity") shouldBe empty
  }

  it should "detect pressure anomalies correctly" in {
    val config = SensorAlertConfig(pressureRange = (1000.0, 1020.0))
    
    val lowPressureData = createTestSensorData(pressure = Some(990.0))
    val lowAnomalies = SensorAlerting.checkForAnomalies(lowPressureData, config)
    lowAnomalies.filter(_.anomalyType == "Pressure") should have size 1

    val highPressureData = createTestSensorData(pressure = Some(1030.0))
    val highAnomalies = SensorAlerting.checkForAnomalies(highPressureData, config)
    highAnomalies.filter(_.anomalyType == "Pressure") should have size 1

    val normalPressureData = createTestSensorData(pressure = Some(1010.0))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalPressureData, config)
    normalAnomalies.filter(_.anomalyType == "Pressure") shouldBe empty
  }

  it should "detect light anomalies correctly" in {
    val config = SensorAlertConfig(lightRange = (100.0, 800.0))
    
    val lowLightData = createTestSensorData(light = Some(50.0))
    val lowAnomalies = SensorAlerting.checkForAnomalies(lowLightData, config)
    lowAnomalies.filter(_.anomalyType == "Light") should have size 1

    val highLightData = createTestSensorData(light = Some(900.0))
    val highAnomalies = SensorAlerting.checkForAnomalies(highLightData, config)
    highAnomalies.filter(_.anomalyType == "Light") should have size 1

    val normalLightData = createTestSensorData(light = Some(400.0))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalLightData, config)
    normalAnomalies.filter(_.anomalyType == "Light") shouldBe empty
  }

  it should "detect acidity anomalies correctly" in {
    val config = SensorAlertConfig(acidityRange = (6.0, 8.0))
    
    val lowAcidityData = createTestSensorData(acidity = Some(5.0))
    val lowAnomalies = SensorAlerting.checkForAnomalies(lowAcidityData, config)
    lowAnomalies.filter(_.anomalyType == "Acidity") should have size 1

    val highAcidityData = createTestSensorData(acidity = Some(9.0))
    val highAnomalies = SensorAlerting.checkForAnomalies(highAcidityData, config)
    highAnomalies.filter(_.anomalyType == "Acidity") should have size 1

    val normalAcidityData = createTestSensorData(acidity = Some(7.0))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalAcidityData, config)
    normalAnomalies.filter(_.anomalyType == "Acidity") shouldBe empty
  }

  it should "detect battery anomalies correctly" in {
    val config = SensorAlertConfig(batteryLowThreshold = 25)
    
    val lowBatteryData = createTestSensorData(batteryLevel = Some(20))
    val lowAnomalies = SensorAlerting.checkForAnomalies(lowBatteryData, config)
    lowAnomalies.filter(_.anomalyType == "Low Battery") should have size 1

    val normalBatteryData = createTestSensorData(batteryLevel = Some(50))
    val normalAnomalies = SensorAlerting.checkForAnomalies(normalBatteryData, config)
    normalAnomalies.filter(_.anomalyType == "Low Battery") shouldBe empty

    val boundaryBatteryData = createTestSensorData(batteryLevel = Some(25))
    val boundaryAnomalies = SensorAlerting.checkForAnomalies(boundaryBatteryData, config)
    boundaryAnomalies.filter(_.anomalyType == "Low Battery") shouldBe empty
  }

  it should "handle None values without throwing exceptions" in {
    val config = SensorAlertConfig()
    
    val dataWithNones = IoTSensorData(
      deviceId = "test-sensor",
      temperature = None,
      humidity = None,
      pressure = None,
      motion = None,
      light = None,
      acidity = None,
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = None,
        signalStrength = None,
        firmwareVersion = None
      )
    )
    
    // Should not throw exceptions and should return empty list
    val anomalies = SensorAlerting.checkForAnomalies(dataWithNones, config)
    anomalies shouldBe empty
  }

  it should "detect multiple anomalies in single sensor reading" in {
    val config = SensorAlertConfig(
      temperatureRange = (10.0, 30.0),
      humidityRange = (20.0, 80.0),
      pressureRange = (1000.0, 1020.0),
      batteryLowThreshold = 25
    )
    
    val multiAnomalyData = IoTSensorData(
      deviceId = "multi-anomaly-sensor",
      temperature = Some(5.0),    // Too low
      humidity = Some(90.0),      // Too high  
      pressure = Some(990.0),     // Too low
      motion = Some(true),
      light = Some(400.0),        // Normal
      acidity = Some(7.0),        // Normal
      location = "TestLab",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(15),  // Too low
        signalStrength = Some(90),
        firmwareVersion = Some("v1.0.0")
      )
    )
    
    val anomalies = SensorAlerting.checkForAnomalies(multiAnomalyData, config)
    anomalies should have size 4
    
    val anomalyTypes = anomalies.map(_.anomalyType).toSet
    anomalyTypes should contain allOf("Temperature", "Humidity", "Pressure", "Low Battery")
  }

  "SensorAlerting.formatEmergencyAlert" should "create email for single anomaly" in {
    val anomaly = Anomaly(
      deviceId = "test-sensor-001",
      anomalyType = "Temperature",
      value = 95.0,
      threshold = "10.0-30.0",
      timestamp = 1640995200000L, // Known timestamp for consistent testing
      location = "TestLab"
    )
    
    val email = SensorAlerting.formatEmergencyAlert(List(anomaly), "admin@example.com")
    
    email should be(defined)
    val alertEmail = email.value
    
    alertEmail.recipient shouldBe "admin@example.com"
    alertEmail.subject should include("[IoT Alert]")
    alertEmail.subject should include("test-sensor-001")
    
    alertEmail.body should include("test-sensor-001")
    alertEmail.body should include("Temperature")
    alertEmail.body should include("95.0")
    alertEmail.body should include("10.0-30.0")
    alertEmail.body should include("TestLab")
  }

  it should "create email for multiple anomalies" in {
    val anomalies = List(
      Anomaly("sensor-123", "Temperature", 95.0, "10.0-30.0", System.currentTimeMillis(), "Lab1"),
      Anomaly("sensor-123", "Humidity", 105.0, "20.0-80.0", System.currentTimeMillis(), "Lab1"),
      Anomaly("sensor-123", "Battery", 15, "25", System.currentTimeMillis(), "Lab1")
    )
    
    val email = SensorAlerting.formatEmergencyAlert(anomalies, "admin@example.com")
    
    email should be(defined)
    val alertEmail = email.value
    
    alertEmail.body should include("Temperature")
    alertEmail.body should include("Humidity") 
    alertEmail.body should include("Battery")
    alertEmail.body should include("95.0")
    alertEmail.body should include("105.0")
    alertEmail.body should include("15")
  }

  it should "return None for empty anomaly list" in {
    val email = SensorAlerting.formatEmergencyAlert(List.empty, "admin@example.com")
    email shouldBe empty
  }

  it should "format timestamps correctly" in {
    val anomaly = Anomaly(
      deviceId = "test-sensor",
      anomalyType = "Temperature",
      value = 95.0,
      threshold = "10.0-30.0",
      timestamp = 1640995200000L, // 2022-01-01 00:00:00 UTC
      location = "TestLab"
    )
    
    val email = SensorAlerting.formatEmergencyAlert(List(anomaly), "admin@example.com")
    email should be(defined)
    
    // Should contain formatted timestamp (not raw milliseconds)
    email.value.body should not include "1640995200000"
    email.value.body should include("2022") // Should contain year from formatted timestamp
  }

  "EmailGateway implementations" should "send emails through console gateway" in {
    val consoleGateway = new ConsoleEmailGateway()
    val email = Email("test@example.com", "Test Subject", "Test Body")
    
    // Should not throw exceptions
    noException should be thrownBy {
      consoleGateway.send(email)
    }
  }

  "SensorAlertConfig" should "have reasonable default values" in {
    val config = SensorAlertConfig()
    
    config.temperatureRange._1 should be < config.temperatureRange._2
    config.humidityRange._1 should be < config.humidityRange._2
    config.pressureRange._1 should be < config.pressureRange._2
    config.lightRange._1 should be < config.lightRange._2
    config.acidityRange._1 should be < config.acidityRange._2
    config.batteryLowThreshold should be > 0
    config.batteryLowThreshold should be < 100
  }

  it should "allow custom threshold configuration" in {
    val customConfig = SensorAlertConfig(
      temperatureRange = (0.0, 50.0),
      humidityRange = (10.0, 90.0),
      pressureRange = (950.0, 1050.0),
      lightRange = (50.0, 1500.0),
      acidityRange = (3.0, 11.0),
      batteryLowThreshold = 30
    )
    
    customConfig.temperatureRange shouldBe (0.0, 50.0)
    customConfig.humidityRange shouldBe (10.0, 90.0)
    customConfig.pressureRange shouldBe (950.0, 1050.0)
    customConfig.lightRange shouldBe (50.0, 1500.0)
    customConfig.acidityRange shouldBe (3.0, 11.0)
    customConfig.batteryLowThreshold shouldBe 30
  }

  // Helper method to create test sensor data
  private def createTestSensorData(
    deviceId: String = "test-sensor",
    temperature: Option[Double] = Some(25.0),
    humidity: Option[Double] = Some(50.0),
    pressure: Option[Double] = Some(1013.0),
    motion: Option[Boolean] = Some(false),
    light: Option[Double] = Some(400.0),
    acidity: Option[Double] = Some(7.0),
    location: String = "TestLab",
    batteryLevel: Option[Int] = Some(80),
    signalStrength: Option[Int] = Some(90),
    firmwareVersion: Option[String] = Some("v1.0.0")
  ): IoTSensorData = {
    IoTSensorData(
      deviceId = deviceId,
      temperature = temperature,
      humidity = humidity,
      pressure = pressure,
      motion = motion,
      light = light,
      acidity = acidity,
      location = location,
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = batteryLevel,
        signalStrength = signalStrength,
        firmwareVersion = firmwareVersion
      )
    )
  }
}
