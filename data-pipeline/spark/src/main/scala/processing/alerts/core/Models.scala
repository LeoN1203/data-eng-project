package scala.processing.alerts.core

/**
 * Modèles de données pour le système d'alertes de capteurs IoT
 */

// This case class should match the one defined in your Producer.
// It's placed here to make the alerting module self-contained.
case class IoTSensorData(
  deviceId: String,
  temperature: Double,
  humidity: Double,
  pressure: Double,
  motion: Boolean,
  light: Double,
  acidity: Double,
  location: String,
  timestamp: Long,
  metadata: Map[String, Any]
)

/** Configuration for sensor alert thresholds. */
case class SensorAlertConfig(
  maxTemperature: Double = 30.0,
  minTemperature: Double = 5.0,
  maxHumidity: Double = 80.0,
  minHumidity: Double = 20.0,
  maxPressure: Double = 1030.0,
  minPressure: Double = 990.0,
  maxAcidity: Double = 8.5,
  minAcidity: Double = 5.5
)

/** Represents a single detected anomaly from sensor data. */
case class Anomaly(
  deviceId: String,
  anomalyType: String,
  value: Any,
  timestamp: Long,
  location: String
)

/** Represents the content of an email to be sent. */
case class Email(
  recipient: String,
  subject: String,
  body: String
)
