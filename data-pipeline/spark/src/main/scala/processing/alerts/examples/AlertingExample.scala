package scala.processing.alerts.examples

import scala.processing.alerts.core._
import scala.processing.alerts.email._
import scala.util.{Success, Failure}
import java.time.Instant

object AlertingExample {
  def main(args: Array[String]): Unit = {
    // --- Configuration ---
    val alertConfig = SensorAlertConfig()
    val recipient = "gabriel.calvente@epita.fr"

    // --- Dependency Injection: Choose your gateway ---
    
    // Console Gateway (pour tests et développement)
    val consoleGateway: EmailGateway = new ConsoleEmailGateway()
    
    // Choix de la gateway selon les arguments
    val useRealEmail = args.headOption.contains("--real-email")
    
    val emailGateway = if (useRealEmail) {
      println("🚀 Tentative d'utilisation de l'email réel...")
      
      // Charge la configuration sécurisée depuis les variables d'environnement
      EmailConfig.createEmailGateway() match {
        case Success(gateway) =>
          println("✅ Configuration email chargée depuis les variables d'environnement")
          gateway
        case Failure(exception) =>
          println(s"❌ Erreur de configuration: ${exception.getMessage}")
          println()
          EmailConfig.printRequiredEnvVars()
          println()
          println("🔄 Basculement vers la console gateway...")
          consoleGateway
      }
    } else {
      println("🧪 Utilisation de la console gateway")
      consoleGateway
    }

    // --- Application Logic ---
    println("--- Test Case 1: Anomalous Data ---")
    val anomalousData = IoTSensorData(
      deviceId = "warehouse-a-001",
      temperature = 35.5,
      humidity = 90.2,
      pressure = 1015.0,
      motion = true,
      light = 500.0,
      acidity = 5.1,
      location = "warehouse-a",
      timestamp = Instant.now.toEpochMilli,
      metadata = Map.empty
    )

    val anomalies = SensorAlerting.checkForAnomalies(anomalousData, alertConfig)
    val maybeEmail = SensorAlerting.formatEmergencyAlert(anomalies, recipient)

    // The side-effect is handled by the chosen gateway
    maybeEmail.foreach(emailGateway.send)

    // Allow time for async email to be sent if using real email
    if (useRealEmail) {
      println("⏳ Waiting for email to be sent...")
      Thread.sleep(3000)
    }
  }
}
