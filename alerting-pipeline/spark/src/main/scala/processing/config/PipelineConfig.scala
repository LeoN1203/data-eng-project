package processing.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Configuration loader for the Kafka alerting pipeline.
 * Loads configuration from application.conf with environment variable overrides.
 */
object PipelineConfig {
  
  private val config: Config = ConfigFactory.load()
  
  case class KafkaConfig(
    bootstrapServers: String,
    topic: String,
    consumerGroupId: String,
    startingOffsets: String
  )
  
  case class SparkConfig(
    appName: String,
    checkpointLocation: String,
    processingTimeSeconds: Int,
    logLevel: String
  )
  
  case class AlertingThresholds(
    temperatureMin: Double,
    temperatureMax: Double,
    humidityMin: Double,
    humidityMax: Double,
    pressureMin: Double,
    pressureMax: Double,
    lightMin: Double,
    lightMax: Double,
    acidityMin: Double,
    acidityMax: Double,
    batteryLowThreshold: Int
  )
  
  case class EmailConfig(
    smtpHost: String,
    smtpPort: Int,
    smtpUser: String,
    smtpPassword: String,
    tlsEnabled: Boolean,
    sslEnabled: Boolean,
    fromAddress: String,
    subjectPrefix: String
  )
  
  case class AlertingConfig(
    recipientEmail: String,
    thresholds: AlertingThresholds
  )
  
  case class PipelineConfiguration(
    kafka: KafkaConfig,
    spark: SparkConfig,
    alerting: AlertingConfig,
    email: EmailConfig
  )
  
  /**
   * Load the complete pipeline configuration
   */
  def loadConfiguration(): PipelineConfiguration = {
    PipelineConfiguration(
      kafka = loadKafkaConfig(),
      spark = loadSparkConfig(),
      alerting = loadAlertingConfig(),
      email = loadEmailConfig()
    )
  }
  
  private def loadKafkaConfig(): KafkaConfig = {
    KafkaConfig(
      bootstrapServers = config.getString("kafka.bootstrap.servers"),
      topic = config.getString("kafka.topic"),
      consumerGroupId = config.getString("kafka.consumer.group.id"),
      startingOffsets = config.getString("kafka.starting.offsets")
    )
  }
  
  private def loadSparkConfig(): SparkConfig = {
    SparkConfig(
      appName = config.getString("spark.app.name"),
      checkpointLocation = config.getString("spark.checkpoint.location"),
      processingTimeSeconds = config.getInt("spark.processing.time.seconds"),
      logLevel = config.getString("spark.log.level")
    )
  }
  
  private def loadAlertingConfig(): AlertingConfig = {
    AlertingConfig(
      recipientEmail = config.getString("alerting.recipient.email"),
      thresholds = AlertingThresholds(
        temperatureMin = config.getDouble("alerting.temperature.min"),
        temperatureMax = config.getDouble("alerting.temperature.max"),
        humidityMin = config.getDouble("alerting.humidity.min"),
        humidityMax = config.getDouble("alerting.humidity.max"),
        pressureMin = config.getDouble("alerting.pressure.min"),
        pressureMax = config.getDouble("alerting.pressure.max"),
        lightMin = config.getDouble("alerting.light.min"),
        lightMax = config.getDouble("alerting.light.max"),
        acidityMin = config.getDouble("alerting.acidity.min"),
        acidityMax = config.getDouble("alerting.acidity.max"),
        batteryLowThreshold = config.getInt("alerting.battery.low.threshold")
      )
    )
  }
  
  private def loadEmailConfig(): EmailConfig = {
    EmailConfig(
      smtpHost = config.getString("email.smtp.host"),
      smtpPort = config.getInt("email.smtp.port"),
      smtpUser = config.getString("email.smtp.user"),
      smtpPassword = config.getString("email.smtp.password"),
      tlsEnabled = config.getBoolean("email.smtp.tls.enabled"),
      sslEnabled = config.getBoolean("email.smtp.ssl.enabled"),
      fromAddress = config.getString("email.from.address"),
      subjectPrefix = config.getString("email.subject.prefix")
    )
  }
  
  /**
   * Convert pipeline alerting thresholds to alerting config format
   */
  def toSensorAlertConfig(thresholds: AlertingThresholds): processing.alerts.core.SensorAlertConfig = {
    processing.alerts.core.SensorAlertConfig(
      temperatureRange = (thresholds.temperatureMin, thresholds.temperatureMax),
      humidityRange = (thresholds.humidityMin, thresholds.humidityMax),
      pressureRange = (thresholds.pressureMin, thresholds.pressureMax),
      lightRange = (thresholds.lightMin, thresholds.lightMax),
      acidityRange = (thresholds.acidityMin, thresholds.acidityMax),
      batteryLowThreshold = thresholds.batteryLowThreshold
    )
  }
}
