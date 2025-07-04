package processing.alerts.email

import processing.alerts.core.Email

/**
 * Gateway trait for sending emails. This allows for different implementations
 * (console output for testing, real email for production).
 */
trait EmailGateway {
  def send(email: Email): Unit
}
