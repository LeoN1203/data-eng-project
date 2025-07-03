package scala.processing

import scala.processing.alerts.core._
import scala.processing.alerts.email._

/**
 * Main alert detection module for the data pipeline.
 * This integrates sensor data processing with the alerting system.
 * Follows functional programming principles with pure functions.
 */
object AlertDetection {
  
  /**
   * Pure function: Process sensor data and return detected anomalies.
   * No side effects, immutable, composable.
   */
  def detectAnomalies(
    sensorData: IoTSensorData, 
    alertConfig: SensorAlertConfig = SensorAlertConfig()
  ): List[Anomaly] = {
    SensorAlerting.checkForAnomalies(sensorData, alertConfig)
  }
  
  /**
   * Pure function: Format anomalies into an email (if needed).
   * Returns Option[Email] - no side effects.
   */
  def formatAlert(
    anomalies: List[Anomaly], 
    recipientEmail: String
  ): Option[Email] = {
    SensorAlerting.formatEmergencyAlert(anomalies, recipientEmail)
  }
  
  /**
   * Pure function: Process sensor data and return alert email if anomalies detected.
   * Composable and testable without side effects.
   */
  def processToAlert(
    sensorData: IoTSensorData, 
    alertConfig: SensorAlertConfig = SensorAlertConfig(),
    recipientEmail: String
  ): Option[Email] = {
    val anomalies = detectAnomalies(sensorData, alertConfig)
    if (anomalies.nonEmpty) formatAlert(anomalies, recipientEmail) else None
  }
  
  /**
   * Pure function: Batch process multiple sensor readings.
   * Returns tuple (totalAnomalies, emails to send) - no mutations, no side effects.
   */
  def processBatchPure(
    sensorDataBatch: List[IoTSensorData],
    alertConfig: SensorAlertConfig = SensorAlertConfig(),
    recipientEmail: String
  ): (Int, List[Email]) = {
    val results = sensorDataBatch.map { sensorData =>
      val anomalies = detectAnomalies(sensorData, alertConfig)
      val emailOpt = if (anomalies.nonEmpty) formatAlert(anomalies, recipientEmail) else None
      (anomalies.size, emailOpt)
    }
    
    val totalAnomalies = results.map(_._1).sum
    val emails = results.flatMap(_._2)
    
    (totalAnomalies, emails)
  }
  
  // === Side-effect functions (impure, but isolated) ===
  
  /**
   * Impure function: Performs side effect of sending email.
   * Clearly separated from pure logic above.
   */
  def sendAlert(email: Email, emailGateway: EmailGateway): Unit = {
    emailGateway.send(email)
  }
  
  /**
   * Convenience function: Process sensor data and trigger alerts if anomalies detected.
   * Combines pure processing with controlled side effects.
   */
  def processAndAlert(
    sensorData: IoTSensorData, 
    alertConfig: SensorAlertConfig = SensorAlertConfig(),
    recipientEmail: String,
    emailGateway: EmailGateway = new ConsoleEmailGateway()
  ): Unit = {
    processToAlert(sensorData, alertConfig, recipientEmail)
      .foreach(sendAlert(_, emailGateway))
  }
  
  /**
   * Convenience function: Batch process with side effects.
   * Uses pure processBatchPure then applies side effects.
   */
  def processBatch(
    sensorDataBatch: List[IoTSensorData],
    alertConfig: SensorAlertConfig = SensorAlertConfig(),
    recipientEmail: String,
    emailGateway: EmailGateway = new ConsoleEmailGateway()
  ): Int = {
    val (totalAnomalies, emails) = processBatchPure(sensorDataBatch, alertConfig, recipientEmail)
    emails.foreach(sendAlert(_, emailGateway))
    totalAnomalies
  }
}