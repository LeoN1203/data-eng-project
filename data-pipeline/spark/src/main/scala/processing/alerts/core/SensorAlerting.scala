package processing.alerts.core

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

/**
 * Contains pure functions for handling sensor data alerting.
 * This is the core business logic for anomaly detection and alert formatting.
 */
object SensorAlerting {

  /**
   * Checks sensor data for values outside defined thresholds.
   */
  def checkForAnomalies(data: IoTSensorData, config: SensorAlertConfig): List[Anomaly] = {
    val anomalies = scala.collection.mutable.ListBuffer[Anomaly]()

    // Check temperature
    data.temperature.foreach { temp =>
      val (min, max) = config.temperatureRange
      if (temp < min || temp > max) {
        anomalies += Anomaly(
          data.deviceId, 
          "Temperature", 
          f"$temp%.1f°C", 
          s"${min}°C to ${max}°C",
          data.timestamp, 
          data.location
        )
      }
    }

    // Check humidity
    data.humidity.foreach { humid =>
      val (min, max) = config.humidityRange
      if (humid < min || humid > max) {
        anomalies += Anomaly(
          data.deviceId, 
          "Humidity", 
          f"$humid%.1f%%", 
          s"${min}% to ${max}%",
          data.timestamp, 
          data.location
        )
      }
    }

    // Check pressure
    data.pressure.foreach { press =>
      val (min, max) = config.pressureRange
      if (press < min || press > max) {
        anomalies += Anomaly(
          data.deviceId, 
          "Pressure", 
          f"$press%.1f hPa", 
          s"${min} to ${max} hPa",
          data.timestamp, 
          data.location
        )
      }
    }

    // Check light
    data.light.foreach { lightVal =>
      val (min, max) = config.lightRange
      if (lightVal < min || lightVal > max) {
        anomalies += Anomaly(
          data.deviceId, 
          "Light", 
          f"$lightVal%.1f lux", 
          s"${min} to ${max} lux",
          data.timestamp, 
          data.location
        )
      }
    }

    // Check acidity
    data.acidity.foreach { acid =>
      val (min, max) = config.acidityRange
      if (acid < min || acid > max) {
        anomalies += Anomaly(
          data.deviceId, 
          "Acidity", 
          f"$acid%.1f pH", 
          s"${min} to ${max} pH",
          data.timestamp, 
          data.location
        )
      }
    }

    // Check battery level
    data.metadata.batteryLevel.foreach { battery =>
      if (battery < config.batteryLowThreshold) {
        anomalies += Anomaly(
          data.deviceId, 
          "Low Battery", 
          s"$battery%", 
          s"> ${config.batteryLowThreshold}%",
          data.timestamp, 
          data.location
        )
      }
    }

    anomalies.toList
  }

  /**
   * Formats a list of anomalies into an Email object.
   */
  def formatEmergencyAlert(anomalies: List[Anomaly], recipientEmail: String): Option[Email] = {
    if (anomalies.isEmpty) {
      None
    } else {
      val deviceId = anomalies.head.deviceId
      val location = anomalies.head.location
      val subject = s"[IoT Alert] Anomaly detected in $deviceId at $location"
      
      val formattedTimestamp = Instant.ofEpochMilli(anomalies.head.timestamp)
        .atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      val anomalyList = anomalies.map { a =>
        s"- ${a.anomalyType}: ${a.value} (threshold: ${a.threshold})"
      }.mkString("\n")

      val body =
        s"""Device: $deviceId
           |Location: $location
           |Timestamp: $formattedTimestamp
           |
           |Anomalies detected:
           |$anomalyList
           |
           |Please investigate this issue immediately.
           |
           |---
           |IoT Monitoring System""".stripMargin

      Some(Email(recipientEmail, subject, body))
    }
  }
}
