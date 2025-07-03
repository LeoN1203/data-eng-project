package scala.processing.alerts.email

import scala.processing.alerts.core.Email
import courier._
import javax.mail.internet.InternetAddress
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
 * An implementation of EmailGateway that sends real emails using the courier library.
 */
class CourrierEmailGateway(
  smtpHost: String,
  smtpPort: Int,
  smtpUser: String,
  smtpPassword: String,
  fromEmail: String,
  useTLS: Boolean = true
) extends EmailGateway {

  implicit val ec: ExecutionContext = ExecutionContext.global

  // Configuration correcte du mailer avec authentification
  private val mailer = Mailer(smtpHost, smtpPort)
    .auth(true)
    .as(smtpUser, smtpPassword)
    .startTls(useTLS)()

  override def send(email: Email): Unit = {
    println(s"📧 Attempting to send real email to ${email.recipient}")
    println(s"📧 Subject: ${email.subject}")
    
    try {
      // Création de l'envelope avec les bonnes classes
      val envelope = Envelope
        .from(new InternetAddress(fromEmail))
        .to(new InternetAddress(email.recipient))
        .subject(email.subject)
        .content(Text(email.body))

      // Envoi synchrone pour simplifier - syntaxe correcte pour Courier
      val future = mailer(envelope)
      Await.result(future, 30.seconds)
      
      println("✅ Email sent successfully!")
      
    } catch {
      case ex: Exception =>
        println(s"❌ Failed to send email: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
}
