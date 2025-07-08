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
   * Pure functional approach using immutable collections and flatMap.
   */
  def checkForAnomalies(data: IoTSensorData, config: SensorAlertConfig): List[Anomaly] = {
    List(
      checkTemperature(data, config),
      checkHumidity(data, config),
      checkPressure(data, config),
      checkLight(data, config),
      checkAcidity(data, config),
      checkBatteryLevel(data, config)
    ).flatten
  }

  /**
   * Alternative functional approach using for-comprehensions and collections.
   * This demonstrates a more concise functional style.
   */
  def checkForAnomaliesAlternative(data: IoTSensorData, config: SensorAlertConfig): List[Anomaly] = {
    val checks: List[() => Option[Anomaly]] = List(
      () => checkTemperature(data, config),
      () => checkHumidity(data, config),
      () => checkPressure(data, config),
      () => checkLight(data, config),
      () => checkAcidity(data, config),
      () => checkBatteryLevel(data, config)
    )
    
    checks.flatMap(_())
  }

  private def checkTemperature(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.temperature.flatMap { temp =>
      val (min, max) = config.temperatureRange
      if (temp < min || temp > max) {
        Some(Anomaly(
          data.deviceId, 
          "Temperature", 
          f"$temp%.1f°C", 
          s"${min}°C to ${max}°C",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  private def checkHumidity(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.humidity.flatMap { humid =>
      val (min, max) = config.humidityRange
      if (humid < min || humid > max) {
        Some(Anomaly(
          data.deviceId, 
          "Humidity", 
          f"$humid%.1f%%", 
          s"${min}% to ${max}%",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  private def checkPressure(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.pressure.flatMap { press =>
      val (min, max) = config.pressureRange
      if (press < min || press > max) {
        Some(Anomaly(
          data.deviceId, 
          "Pressure", 
          f"$press%.1f hPa", 
          s"${min} to ${max} hPa",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  private def checkLight(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.light.flatMap { lightVal =>
      val (min, max) = config.lightRange
      if (lightVal < min || lightVal > max) {
        Some(Anomaly(
          data.deviceId, 
          "Light", 
          f"$lightVal%.1f lux", 
          s"${min} to ${max} lux",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  private def checkAcidity(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.acidity.flatMap { acid =>
      val (min, max) = config.acidityRange
      if (acid < min || acid > max) {
        Some(Anomaly(
          data.deviceId, 
          "Acidity", 
          f"$acid%.1f pH", 
          s"${min} to ${max} pH",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  private def checkBatteryLevel(data: IoTSensorData, config: SensorAlertConfig): Option[Anomaly] = {
    data.metadata.batteryLevel.flatMap { battery =>
      if (battery < config.batteryLowThreshold) {
        Some(Anomaly(
          data.deviceId, 
          "Low Battery", 
          s"$battery%", 
          s"> ${config.batteryLowThreshold}%",
          data.timestamp, 
          data.location
        ))
      } else None
    }
  }

  /**
   * Generic threshold checker to reduce code duplication.
   * This is a higher-order function that can be reused for different sensor types.
   */
  private def checkThreshold[T](
    value: Option[T], 
    range: (T, T), 
    anomalyType: String,
    unit: String,
    data: IoTSensorData
  )(implicit ord: Ordering[T], num: Numeric[T]): Option[Anomaly] = {
    value.flatMap { v =>
      val (min, max) = range
      if (ord.lt(v, min) || ord.gt(v, max)) {
        Some(Anomaly(
          data.deviceId,
          anomalyType,
          s"${num.toDouble(v)}$unit",
          s"${num.toDouble(min)} to ${num.toDouble(max)}$unit",
          data.timestamp,
          data.location
        ))
      } else None
    }
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
