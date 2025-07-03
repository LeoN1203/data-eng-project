package scala.processing.alerts.email

import scala.processing.alerts.core.Email

/**
 * Gateway trait for sending emails. This allows for different implementations
 * (console output for testing, real email for production).
 */
trait EmailGateway {
  def send(email: Email): Unit
}
