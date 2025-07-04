package processing.alerts.email

import processing.alerts.core.Email

/**
 * A console-based implementation of EmailGateway for development/testing.
 * Instead of sending real emails, it prints the email content to the console.
 */
class ConsoleEmailGateway extends EmailGateway {
  override def send(email: Email): Unit = {
    println("=" * 80)
    println("📧 EMAIL NOTIFICATION (Console Gateway)")
    println("=" * 80)
    println(s"To: ${email.recipient}")
    println(s"Subject: ${email.subject}")
    println("-" * 80)
    println(email.body)
    println("=" * 80)
    println("Email sent successfully via Console Gateway!")
    println()
  }
}
