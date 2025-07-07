package test

import scala.IoTDataProducer
import scala.IoTDataConsumer
import scala.DataProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Try, Success, Failure}
import scala.collection.mutable

/** Integration test for IoT Kafka Producer and Consumer
  *
  * This test verifies that:
  *   1. Producer can send messages to Kafka 2. Consumer can receive and process
  *      those messages 3. Data integrity is maintained end-to-end
  */
object IoTKafkaIntegrationTest {

  implicit val ec: ExecutionContext = ExecutionContext.global

  // Test configuration
  private val TEST_TOPIC = "iot-sensor-data"
  private val TEST_CONSUMER_GROUP = "iot-sensor-spark-consumer"
  private val MESSAGE_COUNT = 10
  private val TEST_TIMEOUT_SECONDS = 30

  /** Test Data Processor that captures received messages for verification
    */
  class TestDataProcessor extends DataProcessor {
    private val receivedMessages = mutable.ListBuffer[String]()
    private val processedCount = new AtomicInteger(0)
    private val latch = new CountDownLatch(MESSAGE_COUNT)

    def getReceivedMessages: List[String] = receivedMessages.toList
    def getProcessedCount: Int = processedCount.get()
    def awaitCompletion(timeout: Long, unit: TimeUnit): Boolean =
      latch.await(timeout, unit)

    override def processRecords(
        records: List[ConsumerRecord[String, String]]
    ): Boolean = {
      Try {
        records.foreach { record =>
          println(
            s"Test Consumer received: ${record.key()} -> ${record.value()}"
          )
          receivedMessages += record.value()
          processedCount.incrementAndGet()
          latch.countDown()
        }
        true
      }.recover { case e: Exception =>
        System.err.println(s"Test processor error: ${e.getMessage}")
        false
      }.get
    }
  }

  /** Simple integration test function
    */
  def runSimpleTest(): Boolean = {
    println("=" * 60)
    println("Starting IoT Kafka Integration Test")
    println("=" * 60)

    var producer: IoTDataProducer = null
    var consumer: IoTDataConsumer = null
    var testProcessor: TestDataProcessor = null

    try {
      // Step 1: Initialize components
      println("\n1. Initializing Producer and Consumer...")
      producer = new IoTDataProducer(TEST_TOPIC)
      consumer = new IoTDataConsumer(TEST_TOPIC, TEST_CONSUMER_GROUP)
      testProcessor = new TestDataProcessor()

      // Step 2: Start consumer in a separate thread
      println("2. Starting Consumer...")
      val consumerFuture = Future {
        consumer.startConsuming(testProcessor)
      }

      // Give consumer time to subscribe and be ready
      Thread.sleep(3000)

      // Step 3: Send test messages
      println(s"3. Sending $MESSAGE_COUNT test messages...")
      for (i <- 1 to MESSAGE_COUNT) {
        producer.sendData(asyncMode = false) // Use sync mode for testing
        println(s"   Sent message $i/$MESSAGE_COUNT")
        Thread.sleep(200) // Small delay between messages
      }

      // Step 4: Wait for all messages to be consumed
      println("4. Waiting for messages to be consumed...")
      val allReceived =
        testProcessor.awaitCompletion(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)

      // Step 5: Verify results
      println("5. Verifying results...")
      val processedCount = testProcessor.getProcessedCount
      val receivedMessages = testProcessor.getReceivedMessages

      println(s"   Messages sent: $MESSAGE_COUNT")
      println(s"   Messages processed: $processedCount")
      println(s"   Messages received: ${receivedMessages.size}")

      val success =
        processedCount == MESSAGE_COUNT && receivedMessages.size == MESSAGE_COUNT

      if (success) {
        println("\n✅ TEST PASSED: Producer and Consumer are working correctly!")

        // Show sample of received data
        println("\nSample received messages:")
        receivedMessages.take(3).zipWithIndex.foreach { case (msg, idx) =>
          println(s"   Message ${idx + 1}: ${msg.take(100)}...")
        }
      } else {
        println(
          s"\n❌ TEST FAILED: Expected $MESSAGE_COUNT messages, but processed $processedCount"
        )
      }

      success

    } catch {
      case e: Exception =>
        println(s"\n❌ TEST ERROR: ${e.getMessage}")
        e.printStackTrace()
        false
    } finally {
      // Cleanup
      println("\n6. Cleaning up...")
      if (consumer != null) consumer.shutdown()
      if (producer != null) producer.close()
      Thread.sleep(2000) // Give time for cleanup
      println("Cleanup complete")
    }
  }

  /** Test with specific data validation
    */
  def runComprehensiveTest(): Boolean = {
    println("=" * 60)
    println("Starting Comprehensive IoT Kafka Test")
    println("=" * 60)

    // Test with custom processor that validates data structure
    class ValidatingTestProcessor extends DataProcessor {
      private val validMessages = new AtomicInteger(0)
      private val invalidMessages = new AtomicInteger(0)
      private val latch = new CountDownLatch(MESSAGE_COUNT)

      def getValidCount: Int = validMessages.get()
      def getInvalidCount: Int = invalidMessages.get()
      def awaitCompletion(timeout: Long, unit: TimeUnit): Boolean =
        latch.await(timeout, unit)

      override def processRecords(
          records: List[ConsumerRecord[String, String]]
      ): Boolean = {
        Try {
          records.foreach { record =>
            // Try to parse and validate the JSON structure
            val jsonStr = record.value()
            if (isValidIoTMessage(jsonStr)) {
              validMessages.incrementAndGet()
              println(s"✅ Valid message from device: ${record.key()}")
            } else {
              invalidMessages.incrementAndGet()
              println(s"❌ Invalid message format: ${jsonStr.take(50)}...")
            }
            latch.countDown()
          }
          true
        }.recover { case e: Exception =>
          System.err.println(s"Validation processor error: ${e.getMessage}")
          false
        }.get
      }

      private def isValidIoTMessage(jsonStr: String): Boolean = {
        Try {
          val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(
              com.fasterxml.jackson.module.scala.DefaultScalaModule
            )
          val data = objectMapper.readValue(jsonStr, classOf[Map[String, Any]])

          // Check required fields
          val requiredFields =
            Set("deviceId", "deviceType", "location", "value", "timestamp")
          requiredFields.forall(data.contains)
        }.getOrElse(false)
      }
    }

    var producer: IoTDataProducer = null
    var consumer: IoTDataConsumer = null
    var validator: ValidatingTestProcessor = null

    try {
      println("\n1. Initializing components for comprehensive test...")
      producer = new IoTDataProducer(TEST_TOPIC)
      consumer =
        new IoTDataConsumer(TEST_TOPIC, TEST_CONSUMER_GROUP + "-comprehensive")
      validator = new ValidatingTestProcessor()

      println("2. Starting consumer with validator...")
      val consumerFuture = Future {
        consumer.startConsuming(validator)
      }

      Thread.sleep(3000)

      println(s"3. Producing $MESSAGE_COUNT messages...")
      for (i <- 1 to MESSAGE_COUNT) {
        producer.sendData(asyncMode = true)
        if (i % 3 == 0) Thread.sleep(100) // Vary timing
      }

      println("4. Waiting for validation to complete...")
      val completed =
        validator.awaitCompletion(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)

      println("5. Validation results:")
      val validCount = validator.getValidCount
      val invalidCount = validator.getInvalidCount

      println(s"   Valid messages: $validCount")
      println(s"   Invalid messages: $invalidCount")
      println(s"   Total processed: ${validCount + invalidCount}")

      val success =
        completed && validCount == MESSAGE_COUNT && invalidCount == 0

      if (success) {
        println(
          "\n✅ COMPREHENSIVE TEST PASSED: All messages are valid IoT data!"
        )
      } else {
        println(
          s"\n❌ COMPREHENSIVE TEST FAILED: Issues with message validation"
        )
      }

      success

    } catch {
      case e: Exception =>
        println(s"\n❌ COMPREHENSIVE TEST ERROR: ${e.getMessage}")
        e.printStackTrace()
        false
    } finally {
      println("\n6. Cleaning up comprehensive test...")
      if (consumer != null) consumer.shutdown()
      if (producer != null) producer.close()
      Thread.sleep(2000)
    }
  }

  /** Main test runner
    */
  def main(args: Array[String]): Unit = {
    println("IoT Kafka Integration Test Suite")
    println("Make sure Kafka is running on localhost:9092")
    println("Press Enter to continue or Ctrl+C to cancel...")
    scala.io.StdIn.readLine()

    val simpleTestResult = runSimpleTest()
    Thread.sleep(5000) // Pause between tests

    val comprehensiveTestResult = runComprehensiveTest()

    println("\n" + "=" * 60)
    println("TEST SUITE SUMMARY")
    println("=" * 60)
    println(s"Simple Test: ${if (simpleTestResult) "PASSED ✅" else "FAILED ❌"}")
    println(s"Comprehensive Test: ${if (comprehensiveTestResult) "PASSED ✅"
      else "FAILED ❌"}")

    if (simpleTestResult && comprehensiveTestResult) {
      println(
        "\n🎉 ALL TESTS PASSED! Your Kafka Producer and Consumer are working correctly."
      )
    } else {
      println(
        "\n⚠️  Some tests failed. Please check your Kafka setup and code."
      )
      System.exit(1)
    }
  }
}
