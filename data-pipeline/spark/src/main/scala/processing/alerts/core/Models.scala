package scala.processing.alerts.core

/**
 * Data models for the IoT sensor alerting system
 */

// Sensor metadata with optional fields
case class SensorMetadata(
  batteryLevel: Option[Int],
  signalStrength: Option[Int],
  firmwareVersion: Option[String]
)

// IoT sensor data case class matching the JSON schema
case class IoTSensorData(
  deviceId: String,
  temperature: Option[Double],
  humidity: Option[Double],
  pressure: Option[Double],
  motion: Option[Boolean],
  light: Option[Double],
  acidity: Option[Double],
  location: String,
  timestamp: Long,
  metadata: SensorMetadata
)

/** Configuration for sensor alert thresholds. */
case class SensorAlertConfig(
  temperatureRange: (Double, Double) = (-10.0, 80.0),
  humidityRange: (Double, Double) = (0.0, 100.0),
  pressureRange: (Double, Double) = (900.0, 1100.0),
  lightRange: (Double, Double) = (0.0, 1000.0),
  acidityRange: (Double, Double) = (4.0, 10.0),
  batteryLowThreshold: Int = 20
)

/** Represents a single detected anomaly from sensor data. */
case class Anomaly(
  deviceId: String,
  anomalyType: String,
  value: Any,
  threshold: String,
  timestamp: Long,
  location: String
)

/** Represents the content of an email to be sent. */
case class Email(
  recipient: String,
  subject: String,
  body: String
)
