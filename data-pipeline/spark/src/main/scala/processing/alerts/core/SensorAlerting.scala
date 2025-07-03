package scala.processing.alerts.core

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
    val temperatureAnomalies =
      if (data.temperature > config.maxTemperature || data.temperature < config.minTemperature)
        List(Anomaly(data.deviceId, "Temperature", f"${data.temperature}%.2f C", data.timestamp, data.location))
      else Nil

    val humidityAnomalies =
      if (data.humidity > config.maxHumidity || data.humidity < config.minHumidity)
        List(Anomaly(data.deviceId, "Humidity", f"${data.humidity}%.2f %%", data.timestamp, data.location))
      else Nil

    val pressureAnomalies =
      if (data.pressure > config.maxPressure || data.pressure < config.minPressure)
        List(Anomaly(data.deviceId, "Pressure", f"${data.pressure}%.2f hPa", data.timestamp, data.location))
      else Nil

    val acidityAnomalies =
      if (data.acidity > config.maxAcidity || data.acidity < config.minAcidity)
        List(Anomaly(data.deviceId, "Acidity", f"${data.acidity}%.2f pH", data.timestamp, data.location))
      else Nil

    val motionAnomalies =
      if (data.motion)
        List(Anomaly(data.deviceId, "Motion Detected", data.motion, data.timestamp, data.location))
      else Nil

    temperatureAnomalies ++ humidityAnomalies ++ pressureAnomalies ++ acidityAnomalies ++ motionAnomalies
  }

  /**
   * Formats a list of anomalies into an Email object.
   */
  def formatEmergencyAlert(anomalies: List[Anomaly], recipientEmail: String): Option[Email] = {
    if (anomalies.isEmpty) {
      None
    } else {
      val subject = s"EMERGENCY: ${anomalies.size} Sensor Anomalies Detected"
      val formattedAnomalies = anomalies.map { a =>
        val formattedTimestamp = Instant.ofEpochMilli(a.timestamp)
          .atZone(ZoneId.systemDefault())
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        s"- Device '${a.deviceId}' at '${a.location}' reported ${a.anomalyType} with value ${a.value} at $formattedTimestamp."
      }.mkString("\n")

      val body =
        s"""
           |Dear System Administrator,
           |
           |An emergency alert has been triggered due to one or more sensor anomalies.
           |Please investigate the following events immediately:
           |
           |$formattedAnomalies
           |
           |Regards,
           |IoT Monitoring System
           |""".stripMargin

      Some(Email(recipientEmail, subject, body))
    }
  }
}
