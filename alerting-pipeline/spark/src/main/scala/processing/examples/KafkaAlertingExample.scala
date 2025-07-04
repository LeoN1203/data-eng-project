package processing.examples

import processing.KafkaAlertingPipeline
import processing.alerts.core._
import processing.alerts.email._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.util.{Random, Try}
import scala.concurrent.duration._
import java.util.concurrent.{Executors, TimeUnit}

/**
 * Example demonstrating the Kafka alerting pipeline with sample data generation.
 * This creates a Kafka producer that sends sample IoT sensor data that will trigger
 * various types of anomalies for testing the alerting system.
 */
object KafkaAlertingExample {

  def main(args: Array[String]): Unit = {
    println("=== Kafka Alerting Pipeline Example ===")
    
    val mode = args.headOption.getOrElse("help")
    
    mode match {
      case "producer" => runSampleDataProducer()
      case "consumer" => runAlertingPipeline()
      case "both" => runBoth()
      case _ => printUsage()
    }
  }

  private def printUsage(): Unit = {
    println("""
      |Usage: KafkaAlertingExample <mode>
      |
      |Modes:
      |  producer  - Start a sample data producer that sends IoT sensor data to Kafka
      |  consumer  - Start the alerting pipeline consumer
      |  both      - Run producer in background and then start consumer
      |  help      - Show this usage information
      |
      |Environment Variables:
      |  KAFKA_BOOTSTRAP_SERVERS  - Kafka brokers (default: localhost:9092)
      |  KAFKA_TOPIC             - Kafka topic (default: iot-sensor-data)
      |  ALERT_RECIPIENT_EMAIL   - Email to send alerts (default: admin@company.com)
      |  SMTP_HOST               - SMTP server host (for Mailtrap: sandbox.smtp.mailtrap.io)
      |  SMTP_PORT               - SMTP server port (for Mailtrap: 587)
      |  SMTP_USER               - SMTP username (Mailtrap inbox username)
      |  SMTP_PASSWORD           - SMTP password (Mailtrap inbox password)
      |
      |Example setup for Mailtrap:
      |  export SMTP_HOST=sandbox.smtp.mailtrap.io
      |  export SMTP_PORT=587
      |  export SMTP_USER=your_mailtrap_username
      |  export SMTP_PASSWORD=your_mailtrap_password
      |  export ALERT_RECIPIENT_EMAIL=test@example.com
      |
      """.stripMargin)
  }

  private def runBoth(): Unit = {
    println("Starting producer in background...")
    val executor = Executors.newSingleThreadExecutor()
    executor.submit(new Runnable {
      def run(): Unit = runSampleDataProducer()
    })
    
    // Give producer time to start
    Thread.sleep(3000)
    
    println("Starting alerting pipeline...")
    runAlertingPipeline()
  }

  private def runAlertingPipeline(): Unit = {
    println("Starting Kafka Alerting Pipeline...")
    KafkaAlertingPipeline.main(Array.empty)
  }

  private def runSampleDataProducer(): Unit = {
    val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "iot-sensor-data")
    
    println(s"Starting sample data producer...")
    println(s"Kafka servers: $bootstrapServers")
    println(s"Topic: $topic")

    val producer = createKafkaProducer(bootstrapServers)
    val random = new Random()
    
    try {
      var messageCount = 0
      
      while (true) {
        val sampleData = generateSampleIoTData(random, messageCount)
        val json = sampleDataToJson(sampleData)
        
        val record = new ProducerRecord[String, String](
          topic,
          sampleData.deviceId,
          json
        )
        
        producer.send(record)
        messageCount += 1
        
        println(s"Sent message $messageCount: ${sampleData.deviceId} - ${sampleData.location}")
        
        // Send data every 5 seconds
        Thread.sleep(5000)
      }
    } catch {
      case e: Exception =>
        println(s"Error in producer: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      producer.close()
    }
  }

  private def createKafkaProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    
    new KafkaProducer[String, String](props)
  }

  /**
   * Generate sample IoT sensor data with intentional anomalies for testing
   */
  private def generateSampleIoTData(random: Random, messageCount: Int): IoTSensorData = {
    val deviceId = s"sensor-${random.nextInt(10) + 1}"
    val location = s"Room-${random.nextInt(5) + 1}"
    val timestamp = System.currentTimeMillis()
    
    // Create some intentional anomalies every few messages
    val shouldCreateAnomaly = messageCount % 15 == 0
    
    if (shouldCreateAnomaly) {
      // Generate anomalous data
      random.nextInt(4) match {
        case 0 => // Temperature anomaly
          IoTSensorData(
            deviceId = deviceId,
            temperature = Some(85.0 + random.nextDouble() * 15), // Very high temperature
            humidity = Some(40.0 + random.nextDouble() * 20),
            pressure = Some(1000.0 + random.nextDouble() * 50),
            motion = Some(random.nextBoolean()),
            light = Some(200.0 + random.nextDouble() * 300),
            acidity = Some(6.5 + random.nextDouble() * 1.5),
            location = location,
            timestamp = timestamp,
            metadata = SensorMetadata(
              batteryLevel = Some(15 + random.nextInt(10)), // Low battery
              signalStrength = Some(40 + random.nextInt(30)),
              firmwareVersion = Some("v1.2.3")
            )
          )
        case 1 => // Humidity anomaly
          IoTSensorData(
            deviceId = deviceId,
            temperature = Some(20.0 + random.nextDouble() * 10),
            humidity = Some(95.0 + random.nextDouble() * 5), // Very high humidity
            pressure = Some(1000.0 + random.nextDouble() * 50),
            motion = Some(random.nextBoolean()),
            light = Some(200.0 + random.nextDouble() * 300),
            acidity = Some(6.5 + random.nextDouble() * 1.5),
            location = location,
            timestamp = timestamp,
            metadata = SensorMetadata(
              batteryLevel = Some(80 + random.nextInt(20)),
              signalStrength = Some(70 + random.nextInt(30)),
              firmwareVersion = Some("v1.2.3")
            )
          )
        case 2 => // Pressure anomaly
          IoTSensorData(
            deviceId = deviceId,
            temperature = Some(20.0 + random.nextDouble() * 10),
            humidity = Some(40.0 + random.nextDouble() * 20),
            pressure = Some(1200.0 + random.nextDouble() * 100), // Very high pressure
            motion = Some(random.nextBoolean()),
            light = Some(200.0 + random.nextDouble() * 300),
            acidity = Some(6.5 + random.nextDouble() * 1.5),
            location = location,
            timestamp = timestamp,
            metadata = SensorMetadata(
              batteryLevel = Some(80 + random.nextInt(20)),
              signalStrength = Some(70 + random.nextInt(30)),
              firmwareVersion = Some("v1.2.3")
            )
          )
        case 3 => // Light anomaly
          IoTSensorData(
            deviceId = deviceId,
            temperature = Some(20.0 + random.nextDouble() * 10),
            humidity = Some(40.0 + random.nextDouble() * 20),
            pressure = Some(1000.0 + random.nextDouble() * 50),
            motion = Some(random.nextBoolean()),
            light = Some(1500.0 + random.nextDouble() * 500), // Very bright light
            acidity = Some(6.5 + random.nextDouble() * 1.5),
            location = location,
            timestamp = timestamp,
            metadata = SensorMetadata(
              batteryLevel = Some(80 + random.nextInt(20)),
              signalStrength = Some(70 + random.nextInt(30)),
              firmwareVersion = Some("v1.2.3")
            )
          )
      }
    } else {
      // Generate normal data
      IoTSensorData(
        deviceId = deviceId,
        temperature = Some(20.0 + random.nextDouble() * 15), // 20-35°C
        humidity = Some(30.0 + random.nextDouble() * 40), // 30-70%
        pressure = Some(980.0 + random.nextDouble() * 40), // 980-1020 hPa
        motion = Some(random.nextBoolean()),
        light = Some(100.0 + random.nextDouble() * 400), // 100-500 lux
        acidity = Some(6.0 + random.nextDouble() * 2.0), // pH 6-8
        location = location,
        timestamp = timestamp,
        metadata = SensorMetadata(
          batteryLevel = Some(70 + random.nextInt(30)), // 70-100%
          signalStrength = Some(50 + random.nextInt(50)), // 50-100%
          firmwareVersion = Some("v1.2.3")
        )
      )
    }
  }

  /**
   * Convert IoTSensorData to JSON string for Kafka message
   */
  private def sampleDataToJson(data: IoTSensorData): String = {
    s"""{
      "deviceId": "${data.deviceId}",
      "temperature": ${data.temperature.map(_.toString).getOrElse("null")},
      "humidity": ${data.humidity.map(_.toString).getOrElse("null")},
      "pressure": ${data.pressure.map(_.toString).getOrElse("null")},
      "motion": ${data.motion.map(_.toString).getOrElse("null")},
      "light": ${data.light.map(_.toString).getOrElse("null")},
      "acidity": ${data.acidity.map(_.toString).getOrElse("null")},
      "location": "${data.location}",
      "timestamp": ${data.timestamp},
      "metadata": {
        "battery_level": ${data.metadata.batteryLevel.map(_.toString).getOrElse("null")},
        "signal_strength": ${data.metadata.signalStrength.map(_.toString).getOrElse("null")},
        "firmware_version": "${data.metadata.firmwareVersion.getOrElse("unknown")}"
      }
    }"""
  }
}
