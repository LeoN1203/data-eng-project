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
   * Charge la configuration avec des valeurs par défaut pour Outlook
   * Nécessite seulement SMTP_USER et SMTP_PASSWORD en variables d'environnement
   */
  def loadOutlookConfig(): Try[SmtpConfig] = {
    Try {
      val user = sys.env.getOrElse("SMTP_USER", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_USER manquante"))
      
      val password = sys.env.getOrElse("SMTP_PASSWORD", 
        throw new IllegalArgumentException("Variable d'environnement SMTP_PASSWORD manquante"))
      
      val fromEmail = sys.env.getOrElse("FROM_EMAIL", user)
      
      SmtpConfig(
        host = "smtp.office365.com",
        port = 587,
        user = user,
        password = password,
        fromEmail = fromEmail
      )
    }
  }
  
  /**
   * Crée un CourrierEmailGateway configuré depuis l'environnement
   * Essaie d'abord Gmail, puis Outlook si échec
   */
  def createEmailGateway(): Try[CourrierEmailGateway] = {
    // Détecte automatiquement le provider basé sur l'email
    val userEmail = sys.env.get("SMTP_USER").getOrElse("")
    
    if (userEmail.contains("@gmail.com")) {
      loadGmailConfig().map { config =>
        new CourrierEmailGateway(
          smtpHost = config.host,
          smtpPort = config.port,
          smtpUser = config.user,
          smtpPassword = config.password,
          fromEmail = config.fromEmail
        )
      }
    } else if (userEmail.contains("@outlook.com") || userEmail.contains("@hotmail.com")) {
      loadOutlookConfig().map { config =>
        new CourrierEmailGateway(
          smtpHost = config.host,
          smtpPort = config.port,
          smtpUser = config.user,
          smtpPassword = config.password,
          fromEmail = config.fromEmail
        )
      }
    } else {
      // Par défaut, essaie Gmail
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
  }
  
  /**
   * Utilitaire pour afficher les variables d'environnement requises (sans exposer les valeurs)
   */
  def printRequiredEnvVars(): Unit = {
    println("Variables d'environnement requises pour l'envoi d'emails :")
    println("  SMTP_USER=votre.email@outlook.com")
    println("  SMTP_PASSWORD=votre_mot_de_passe_outlook")
    println("  FROM_EMAIL=votre.email@outlook.com (optionnel, utilise SMTP_USER par défaut)")
    println("")
    println("Pour Outlook/Office365, vous devez :")
    println("  1. Utiliser votre email Outlook/Hotmail complet")
    println("  2. Utiliser votre mot de passe Outlook normal")
    println("  3. S'assurer que SMTP est activé dans vos paramètres Outlook")
    println("  4. Le serveur SMTP utilisé sera : smtp.office365.com:587")
  }
  
  /**
   * Vérifie si toutes les variables d'environnement sont configurées
   */
  def areCredentialsConfigured(): Boolean = {
    sys.env.contains("SMTP_USER") && sys.env.contains("SMTP_PASSWORD")
  }
}
