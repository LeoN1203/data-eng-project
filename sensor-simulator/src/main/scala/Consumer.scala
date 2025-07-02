package scala

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.time.Duration
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicBoolean

/** Data Processing Trait
  *
  * Using a trait instead of Java interface for more flexibility
  */
trait DataProcessor {

  /** Process a batch of sensor readings
    * @param records
    *   The batch of records to process
    * @return
    *   true if processing was successful, false otherwise
    */
  def processRecords(records: List[ConsumerRecord[String, String]]): Boolean
}

/** IoT Data Consumer in Scala
  *
  * Demonstrates Kafka consumer patterns with:
  *   - Proper configuration
  *   - Offset management
  *   - Error handling
  *   - Partition-aware processing
  */
class IoTDataConsumer(topicName: String, consumerGroup: String) {
  private val consumer: KafkaConsumer[String, String] = createConsumer()
  private val objectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
  private val running = new AtomicBoolean(false)

  /** Creates and configures the Kafka consumer
    */
  private def createConsumer(): KafkaConsumer[String, String] = {
    val props = new java.util.Properties()

    // Basic configuration
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)

    // Deserialization
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[StringDeserializer].getName
    )
    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[StringDeserializer].getName
    )

    // Offset management
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    // Performance tuning
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024")
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

    // Session management
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000")

    new KafkaConsumer[String, String](props)
  }

  /** Example processor demonstrating alerting logic
    */
  class BasicDataProcessor extends DataProcessor {
    private val objectMapper =
      new ObjectMapper().registerModule(DefaultScalaModule)

    override def processRecords(
        records: List[ConsumerRecord[String, String]]
    ): Boolean = {
      Try {
        records.foreach { record =>
          val sensorData =
            objectMapper.readValue(record.value(), classOf[Map[String, Any]])

          // Extract data with pattern matching for safety
          (
            sensorData.get("deviceId"),
            sensorData.get("tempature"),
            sensorData.get("humidity"),
            sensorData.get("pressure"),
            sensorData.get("timestamp")
          ) match {

            case (
                  Some(deviceId: String),
                  Some(temperature: Double),
                  Some(humidity: Double),
                  Some(pressure: Double),
                  Some(timestamp: Long)
                ) =>
              // Check for alerts
              if (shouldTriggerAlert(temperature, humidity, pressure)) {
                println(
                  s"ALERT: Device $deviceId reported abnormal readings - " +
                    s"Temperature: $temperature, Humidity: $humidity, Pressure: $pressure"
                )
              }

              println(
                s"Processed: $deviceId at $timestamp - " +
                  s"Temperature: $temperature, Humidity: $humidity, Pressure: $pressure"
              )

            case _ =>
              System.err.println(
                s"Invalid data format in record: ${record.value()}"
              )
          }
        }
        true
      }.recover { case e: Exception =>
        System.err.println(s"Error processing records: ${e.getMessage}")
        false
      }.get
    }

    /** Alert logic using pattern matching
      */
    private def shouldTriggerAlert(
        temperature: Double,
        humidity: Double,
        pressure: Double
    ): Boolean =
      if (
        temperature > 35.0 || temperature < 10.0 ||
        humidity > 80.0 || humidity < 20.0 ||
        pressure > 1040.0 || pressure < 1000.0
      ) {
        println(
          s"ALERT: Abnormal readings detected - Temperature: $temperature, Humidity:" +
            s" $humidity, Pressure: $pressure"
        )
        true
      } else {
        false
      }
  }

  /** Start consuming data with proper error handling
    */
  def startConsuming(processor: DataProcessor): Unit = {
    try {
      Try {
        consumer.subscribe(util.Collections.singletonList(topicName))
        running.set(true)

        println(s"Starting consumer for topic: $topicName")

        while (running.get()) {
          Try {
            val records = consumer.poll(Duration.ofMillis(1000))

            if (!records.isEmpty) {
              println(s"Received ${records.count()} records")

              // Group records by partition using Scala collections
              val partitionRecords = records.asScala.groupBy { record =>
                new TopicPartition(record.topic(), record.partition())
              }

              // Process each partition separately
              val allSuccess = partitionRecords.forall {
                case (partition, records) =>
                  val recordList = records.toList
                  println(
                    s"Processing ${recordList.size} records from partition ${partition.partition()}"
                  )

                  if (processor.processRecords(recordList)) {
                    // Commit offset if successful
                    val lastOffset = recordList.last.offset() + 1
                    val offsets =
                      Map(partition -> new OffsetAndMetadata(lastOffset)).asJava
                    consumer.commitSync(offsets)
                    println(
                      s"Committed offset $lastOffset for partition ${partition.partition()}"
                    )
                    true
                  } else {
                    System.err.println(
                      s"Processing failed for partition ${partition.partition()}"
                    )
                    false
                  }
              }

              if (!allSuccess) {
                System.err.println(
                  "Some partitions failed processing, will retry"
                )
              }
            }
          }.recover { case e: Exception =>
            System.err.println(s"Error in consumer loop: ${e.getMessage}")
            e.printStackTrace()
            Thread.sleep(5000)
          }
        }
      }.recover { case e: Exception =>
        System.err.println(s"Fatal error in consumer: ${e.getMessage}")
        e.printStackTrace()
      }.get
    } finally {
      cleanup()
    }
  }

  /** Graceful shutdown
    */
  def shutdown(): Unit = {
    println("Shutting down consumer...")
    running.set(false)
  }

  /** Clean up resources
    */
  private def cleanup(): Unit = {
    Try {
      println("Cleaning up consumer resources...")
      consumer.close(Duration.ofSeconds(10))
      println("Consumer cleanup complete")
    }.recover { case e: Exception =>
      System.err.println(s"Error during cleanup: ${e.getMessage}")
    }
  }
}

object IoTDataConsumer {
  def main(args: Array[String]): Unit = {
    val topicName = "iot-sensor-data"
    val consumerGroup = "iot-processing-group"

    val consumer = new IoTDataConsumer(topicName, consumerGroup)
    val processor = new consumer.BasicDataProcessor()

    // Add shutdown hook
    sys.addShutdownHook {
      consumer.shutdown()
    }

    // Start consuming
    consumer.startConsuming(processor)
  }
}
