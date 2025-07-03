package scala.processing.alerts.email

/**
 * Email configuration for SMTP settings.
 * Loads credentials securely from environment variables.
 */
case class EmailConfig(
  smtpHost: String,
  smtpPort: Int,
  smtpUser: String,
  smtpPassword: String,
  fromAddress: String
)

object EmailConfig {
  
  /**
   * Load email configuration from environment variables.
   * Required variables:
   * - SMTP_HOST (e.g., smtp.gmail.com)
   * - SMTP_PORT (e.g., 587)
   * - SMTP_USER (your email)
   * - SMTP_PASSWORD (your app password)
   * - FROM_EMAIL (sender email, defaults to SMTP_USER)
   */
  def loadConfig(): Option[EmailConfig] = {
    for {
      host <- sys.env.get("SMTP_HOST")
      portStr <- sys.env.get("SMTP_PORT")
      port <- scala.util.Try(portStr.toInt).toOption
      user <- sys.env.get("SMTP_USER")
      password <- sys.env.get("SMTP_PASSWORD")
    } yield {
      val fromAddress = sys.env.getOrElse("FROM_EMAIL", user)
      
      // Auto-detect provider and set appropriate defaults
      val (finalHost, finalPort) = if (user.contains("@gmail.com")) {
        ("smtp.gmail.com", 587)
      } else if (user.contains("@outlook.com") || user.contains("@hotmail.com")) {
        ("smtp.office365.com", 587)
      } else {
        // Default to Mailtrap for testing
        ("sandbox.smtp.mailtrap.io", port)
      }
      
      EmailConfig(
        smtpHost = sys.env.getOrElse("SMTP_HOST", finalHost),
        smtpPort = sys.env.get("SMTP_PORT").map(_.toInt).getOrElse(finalPort),
        smtpUser = user,
        smtpPassword = password,
        fromAddress = fromAddress
      )
    }
  }
  
  /**
   * Check if email credentials are configured
   */
  def areCredentialsConfigured(): Boolean = {
    sys.env.contains("SMTP_USER") && sys.env.contains("SMTP_PASSWORD")
  }
  
  /**
   * Print required environment variables (without exposing values)
   */
  def printRequiredEnvVars(): Unit = {
    println("Required environment variables for email sending:")
    println("  SMTP_USER=your_mailtrap_username")
    println("  SMTP_PASSWORD=your_mailtrap_password")
    println("  FROM_EMAIL=test@example.com (optional for Mailtrap)")
    println("")
    println("Mailtrap configuration (default):")
    println("  1. Create account at https://mailtrap.io")
    println("  2. Get credentials from your Mailtrap inbox")
    println("  3. Use the username and password provided by Mailtrap")
    println("  4. SMTP server used: sandbox.smtp.mailtrap.io:587")
    println("")
    println("For Gmail (@gmail.com):")
    println("  - Enable 2FA and generate an app password")
    println("For Outlook (@outlook.com/@hotmail.com):")
    println("  - Use regular password or app password")
  }
}
