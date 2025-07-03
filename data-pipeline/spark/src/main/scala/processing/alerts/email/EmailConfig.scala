package scala.processing.alerts.email

import scala.util.Try

/**
 * Configuration sécurisée pour les emails SMTP
 * Charge les credentials depuis les variables d'environnement
 */
case class SmtpConfig(
  host: String,
  port: Int,
  user: String,
  password: String,
  fromEmail: String
)

object EmailConfig {
  
  /**
   * Charge la configuration SMTP depuis les variables d'environnement
   * Variables requises :
   * - SMTP_HOST (ex: smtp.gmail.com)
   * - SMTP_PORT (ex: 587)
   * - SMTP_USER (votre email)
   * - SMTP_PASSWORD (votre mot de passe d'application)
   * - FROM_EMAIL (email expéditeur, souvent identique à SMTP_USER)
   */
  def loadFromEnvironment(): Try[SmtpConfig] = {
    Try {
      val host = sys.env.getOrElse("SMTP_HOST", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_HOST manquante"))
      
      val port = sys.env.get("SMTP_PORT")
        .map(_.toInt)
        .getOrElse(throw new IllegalArgumentException("Variable d'environnement SMTP_PORT manquante"))
      
      val user = sys.env.getOrElse("SMTP_USER", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_USER manquante"))
      
      val password = sys.env.getOrElse("SMTP_PASSWORD", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_PASSWORD manquante"))
      
      val fromEmail = sys.env.getOrElse("FROM_EMAIL", user) // Par défaut, utilise SMTP_USER
      
      SmtpConfig(host, port, user, password, fromEmail)
    }
  }
  
  /**
   * Charge la configuration avec des valeurs par défaut pour Gmail
   * Nécessite seulement SMTP_USER et SMTP_PASSWORD en variables d'environnement
   */
  def loadGmailConfig(): Try[SmtpConfig] = {
    Try {
      val user = sys.env.getOrElse("SMTP_USER", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_USER manquante"))
      
      val password = sys.env.getOrElse("SMTP_PASSWORD", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_PASSWORD manquante"))
      
      val fromEmail = sys.env.getOrElse("FROM_EMAIL", user)
      
      SmtpConfig(
        host = "smtp.gmail.com",
        port = 587,
        user = user,
        password = password,
        fromEmail = fromEmail
      )
    }
  }
  
  /**
   * Crée un CourrierEmailGateway configuré depuis l'environnement
   */
  def createEmailGateway(): Try[CourrierEmailGateway] = {
    loadGmailConfig().map { config =>
      new CourrierEmailGateway(
        smtpHost = config.host,
        smtpPort = config.port,
        smtpUser = config.user,
        smtpPassword = config.password,
        fromEmail = config.fromEmail
      )
    }
  }
  
  /**
   * Utilitaire pour afficher les variables d'environnement requises (sans exposer les valeurs)
   */
  def printRequiredEnvVars(): Unit = {
    println("Variables d'environnement requises pour l'envoi d'emails :")
    println("  SMTP_USER=votre.email@gmail.com")
    println("  SMTP_PASSWORD=votre_mot_de_passe_application")
    println("  FROM_EMAIL=votre.email@gmail.com (optionnel, utilise SMTP_USER par défaut)")
    println("")
    println("Pour Gmail, vous devez :")
    println("  1. Activer l'authentification à 2 facteurs")
    println("  2. Générer un mot de passe d'application")
    println("  3. Utiliser ce mot de passe d'application (pas votre mot de passe normal)")
  }
  
  /**
   * Vérifie si toutes les variables d'environnement sont configurées
   */
  def areCredentialsConfigured(): Boolean = {
    sys.env.contains("SMTP_USER") && sys.env.contains("SMTP_PASSWORD")
  }
}
