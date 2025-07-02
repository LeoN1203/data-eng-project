package scala

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.time.Duration
import scala.util.{Random, Try}
import scala.collection.mutable

/** IoT Device Data Producer
  *
  * Scala version of the Kafka producer that simulates IoT devices sending
  * sensor data. Uses more idiomatic Scala patterns while maintaining the same
  * functionality.
  */
class IoTDataProducer(topicName: String) {
  private val producer: KafkaProducer[String, String] = createProducer()
  private val objectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
  private val random = new Random

  // Device types and locations
  private val deviceTypes =
    Seq("temperature", "humidity", "pressure", "motion", "light")
  private val locations =
    Seq("warehouse-a", "warehouse-b", "field-a", "field-b")

  /** Creates and configures the Kafka producer
    */
  private def createProducer(): KafkaProducer[String, String] = {
    val props = new java.util.Properties()

    // Basic configuration
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    // Reliability settings
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    // Performance tuning
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")

    // Compression
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

    new KafkaProducer[String, String](props)
  }

  /** IoT Sensor Data case class
    *
    * Using a case class gives us immutability, pattern matching, and automatic
    * equals/hashCode/toString
    */
  case class IoTSensorData(
      deviceId: String,
      temperature: Double,
      humidity: Double,
      pressure: Double,
      motion: Boolean,
      light: Double,
      acidity: Double,
      location: String,
      timestamp: Long,
      metadata: Map[String, Any]
  )

  /** Generate realistic IoT sensor data
    */
  private def generateSensorData(): IoTSensorData = {
    val location = locations(random.nextInt(locations.length))
    val deviceId =
      s"$location-${random.nextInt(100).formatted("%03d")}"

    val metadata = Map(
      "battery_level" -> (85 + random.nextInt(15)),
      "signal_strength" -> (-70 + random.nextInt(20)),
      "firmware_version" -> s"1.2.${random.nextInt(10)}"
    )

    IoTSensorData(
      deviceId,
      18.0 + (random.nextDouble() * 12.0),
      30.0 + (random.nextDouble() * 40.0),
      1010.0 + (random.nextDouble() * 20.0),
      random.nextBoolean(),
      random.nextDouble() * 1000.0,
      6.0 + (random.nextDouble() * 3.0),
      location,
      Instant.now.toEpochMilli,
      metadata
    )
  }

  /** Send data to Kafka with proper error handling
    */
  def sendData(asyncMode: Boolean): Unit = {
    Try {
      val sensorData = generateSensorData()
      val jsonData = objectMapper.writeValueAsString(sensorData)
      val record = new ProducerRecord[String, String](
        topicName,
        sensorData.deviceId,
        jsonData
      )

      if (asyncMode) {
        // Asynchronous send with callback
        producer.send(
          record,
          (metadata: RecordMetadata, exception: Exception) => {
            if (exception != null) {
              System.err.println(
                s"Error sending message: ${exception.getMessage}"
              )
            } else {
              println(
                s"Message sent successfully to topic: ${metadata.topic}, " +
                  s"partition: ${metadata.partition}, offset: ${metadata.offset}"
              )
            }
          }
        )
      } else {
        // Synchronous send
        val metadata = producer.send(record).get()
        println(
          s"Message sent to partition: ${metadata.partition}, offset: ${metadata.offset}"
        )
      }
    }.recover {
      case e: com.fasterxml.jackson.core.JsonProcessingException =>
        System.err.println(s"Error serializing data: ${e.getMessage}")
      case e: Exception =>
        System.err.println(s"Error sending message: ${e.getMessage}")
    }
  }

  /** Simulate continuous data generation
    */
  def startDataGeneration(
      messagesPerSecond: Int,
      durationSeconds: Int
  ): Unit = {
    println(
      s"Starting IoT data generation...\nRate: $messagesPerSecond messages/second\nDuration: $durationSeconds seconds"
    )

    val intervalMs = 1000 / messagesPerSecond
    val endTime = System.currentTimeMillis() + (durationSeconds * 1000)

    Iterator
      .continually(System.currentTimeMillis())
      .takeWhile(now => now < endTime && !Thread.currentThread.isInterrupted)
      .foreach { _ =>
        sendData(asyncMode = true)
        Thread.sleep(intervalMs)
      }

    println("Data generation completed")
  }

  /** Clean shutdown
    */
  def close(): Unit = {
    println("Shutting down producer...")
    producer.flush()
    producer.close(Duration.ofSeconds(5))
    println("Producer shutdown complete")
  }
}

object IoTDataProducer {
  def main(args: Array[String]): Unit = {
    val producer = new IoTDataProducer("iot-sensor-data")

    // Add shutdown hook for graceful cleanup
    sys.addShutdownHook {
      producer.close()
    }

    try {
      // Generate data for 30 seconds at 5 messages per second
      producer.startDataGeneration(5, 30)
    } finally {
      producer.close()
    }
  }
}
