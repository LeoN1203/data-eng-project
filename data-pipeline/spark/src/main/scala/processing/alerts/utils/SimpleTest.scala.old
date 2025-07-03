package scala.processing.alerts

import scala.processing.alerts.core._
import scala.processing.alerts.email._
import java.time.Instant

object SimpleTest {
  def main(args: Array[String]): Unit = {
    println("🧪 Test de AlertingExample - Version Simplifiée")
    println("=" * 60)
    
    try {
      // Test 1: Configuration
      println("1. Test de configuration...")
      val alertConfig = SensorAlertConfig()
      println(s"✅ Configuration créée: tempMax=${alertConfig.maxTemperature}°C")
      
      // Test 2: Gateway Console
      println("\n2. Test du ConsoleEmailGateway...")
      val emailGateway = new ConsoleEmailGateway()
      println("✅ Gateway console créé")
      
      // Test 3: Données de test
      println("\n3. Test avec données anomales...")
      val anomalousData = IoTSensorData(
        deviceId = "test-warehouse-001",
        temperature = 45.0,  // Anormal !
        humidity = 95.0,     // Anormal !
        pressure = 950.0,    // Anormal !
        motion = true,       // Anormal !
        light = 500.0,
        acidity = 9.5,       // Anormal !
        location = "warehouse-test",
        timestamp = Instant.now.toEpochMilli,
        metadata = Map("test" -> "mode")
      )
      
      // Test 4: Détection d'anomalies
      println("\n4. Détection des anomalies...")
      val anomalies = SensorAlerting.checkForAnomalies(anomalousData, alertConfig)
      println(s"✅ ${anomalies.size} anomalies détectées:")
      anomalies.foreach { anomaly =>
        println(s"   - ${anomaly.anomalyType}: ${anomaly.value}")
      }
      
      // Test 5: Formatage de l'email
      println("\n5. Formatage de l'email d'alerte...")
      val maybeEmail = SensorAlerting.formatEmergencyAlert(anomalies, "test@example.com")
      maybeEmail match {
        case Some(email) =>
          println("✅ Email formaté avec succès")
          println(s"   Destinataire: ${email.recipient}")
          println(s"   Sujet: ${email.subject}")
        case None =>
          println("❌ Aucun email généré")
      }
      
      // Test 6: Envoi via Console Gateway
      println("\n6. Test d'envoi via ConsoleGateway...")
      maybeEmail.foreach(emailGateway.send)
      
      // Test 7: Test avec données normales
      println("\n7. Test avec données normales...")
      val normalData = IoTSensorData(
        deviceId = "test-warehouse-002", 
        temperature = 22.0,
        humidity = 45.0,
        pressure = 1013.0,
        motion = false,
        light = 400.0,
        acidity = 7.0,
        location = "warehouse-test",
        timestamp = Instant.now.toEpochMilli,
        metadata = Map("test" -> "mode")
      )
      
      val normalAnomalies = SensorAlerting.checkForAnomalies(normalData, alertConfig)
      println(s"✅ Données normales: ${normalAnomalies.size} anomalies détectées (devrait être 0)")
      
      println("\n" + "=" * 60)
      println("🎉 Tous les tests sont passés avec succès !")
      
    } catch {
      case e: Exception =>
        println(s"❌ Erreur pendant les tests: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
