package processing.performance

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import processing.alerts.core._
import processing.alerts.email._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import scala.util.Random
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.JavaConverters._

/**
 * Performance and concurrent processing tests for the alerting system.
 * Tests high-throughput scenarios, concurrent batch processing, and memory efficiency.
 */
class ConcurrentProcessingTest extends AnyFlatSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  "AlertDetection" should "handle high-throughput sensor data processing" in {
    val config = SensorAlertConfig()
    val batchSize = 1000
    val numBatches = 10
    
    val startTime = System.nanoTime()
    
    val results = (1 to numBatches).map { batchId =>
      Future {
        val sensorDataBatch = generateSensorDataBatch(batchSize, batchId)
        
        val anomalies = sensorDataBatch.flatMap { sensorData =>
          SensorAlerting.checkForAnomalies(sensorData, config)
        }
        
        (batchId, anomalies.size)
      }
    }
    
    val batchResults = Await.result(Future.sequence(results), 30.seconds)
    val endTime = System.nanoTime()
    
    val totalProcessed = batchSize * numBatches
    val totalAnomalies = batchResults.map(_._2).sum
    val processingTimeMs = (endTime - startTime) / 1000000
    val throughputPerSecond = (totalProcessed * 1000.0) / processingTimeMs
    
    println(s"High-throughput test results:")
    println(s"  - Processed: $totalProcessed sensor readings")
    println(s"  - Detected: $totalAnomalies anomalies") 
    println(s"  - Time: ${processingTimeMs}ms")
    println(s"  - Throughput: ${throughputPerSecond.toInt} readings/second")
    
    // Performance assertions
    totalProcessed shouldBe (batchSize * numBatches)
    totalAnomalies should be > 0 // Should detect some anomalies
    throughputPerSecond should be > 100.0 // Should process at least 100 readings/second
  }

  it should "handle concurrent email sending without blocking" in {
    val emailQueue = new ConcurrentLinkedQueue[Email]()
    val asyncEmailGateway = new AsyncEmailGateway(emailQueue)
    val sentCounter = new AtomicInteger(0)
    val errorCounter = new AtomicInteger(0)
    
    val numConcurrentEmails = 100
    
    val emailTasks = (1 to numConcurrentEmails).map { i =>
      Future {
        try {
          val anomaly = Anomaly(
            deviceId = s"load-test-sensor-$i",
            anomalyType = "Temperature",
            value = 95.0 + Random.nextDouble() * 10,
            threshold = "85.0",
            timestamp = System.currentTimeMillis(),
            location = s"LoadTestZone-${i % 10}"
          )
          
          val email = SensorAlerting.formatEmergencyAlert(List(anomaly), s"test$i@example.com")
          email.foreach { e =>
            asyncEmailGateway.send(e)
            sentCounter.incrementAndGet()
          }
        } catch {
          case _: Exception => errorCounter.incrementAndGet()
        }
      }
    }
    
    val startTime = System.nanoTime()
    Await.result(Future.sequence(emailTasks), 15.seconds)
    val endTime = System.nanoTime()
    
    val processingTimeMs = (endTime - startTime) / 1000000
    
    println(s"Concurrent email test results:")
    println(s"  - Emails sent: ${sentCounter.get()}")
    println(s"  - Errors: ${errorCounter.get()}")
    println(s"  - Time: ${processingTimeMs}ms")
    println(s"  - Queue size: ${emailQueue.size()}")
    
    sentCounter.get() shouldBe numConcurrentEmails
    errorCounter.get() shouldBe 0
    emailQueue.size() shouldBe numConcurrentEmails
  }

  it should "maintain memory efficiency under load" in {
    val runtime = Runtime.getRuntime
    val config = SensorAlertConfig()
    
    // Force garbage collection and measure initial memory
    System.gc()
    Thread.sleep(100)
    val initialMemory = runtime.totalMemory() - runtime.freeMemory()
    
    // Process large amounts of data
    val largeDataSets = (1 to 50).map { setId =>
      Future {
        val sensorDataBatch = generateSensorDataBatch(1000, setId)
        
        val anomalies = sensorDataBatch.flatMap { sensorData =>
          SensorAlerting.checkForAnomalies(sensorData, config)
        }
        
        // Simulate processing each anomaly
        anomalies.foreach { anomaly =>
          SensorAlerting.formatEmergencyAlert(List(anomaly), "memory-test@example.com")
        }
        
        anomalies.size
      }
    }
    
    val results = Await.result(Future.sequence(largeDataSets), 45.seconds)
    
    // Force garbage collection and measure final memory
    System.gc()
    Thread.sleep(100)
    val finalMemory = runtime.totalMemory() - runtime.freeMemory()
    
    val memoryIncreaseMB = (finalMemory - initialMemory) / (1024 * 1024)
    val totalAnomalies = results.sum
    
    println(s"Memory efficiency test results:")
    println(s"  - Initial memory: ${initialMemory / (1024 * 1024)}MB")
    println(s"  - Final memory: ${finalMemory / (1024 * 1024)}MB")
    println(s"  - Memory increase: ${memoryIncreaseMB}MB")
    println(s"  - Total anomalies processed: $totalAnomalies")
    
    // Memory increase should be reasonable (less than 100MB for this workload)
    memoryIncreaseMB should be < 100L
    totalAnomalies should be > 0
  }

  it should "handle burst traffic patterns" in {
    val config = SensorAlertConfig()
    val processedCounter = new AtomicLong(0)
    val errorCounter = new AtomicInteger(0)
    
    // Simulate burst patterns: quiet periods followed by high-activity bursts
    val burstPattern = List(
      (10, 100),   // 10 readings at 100ms intervals (quiet)
      (500, 10),   // 500 readings at 10ms intervals (burst)
      (20, 200),   // 20 readings at 200ms intervals (quiet)
      (1000, 5),   // 1000 readings at 5ms intervals (major burst)
      (50, 100)    // 50 readings at 100ms intervals (quiet)
    )
    
    val startTime = System.nanoTime()
    
    val burstTasks = burstPattern.zipWithIndex.map { case ((count, intervalMs), burstId) =>
      Future {
        try {
          (1 to count).foreach { i =>
            val sensorData = generateAnomalousSensorData(s"burst-$burstId-sensor-$i")
            val anomalies = SensorAlerting.checkForAnomalies(sensorData, config)
            processedCounter.addAndGet(anomalies.size)
            
            if (i < count) Thread.sleep(intervalMs)
          }
        } catch {
          case _: Exception => errorCounter.incrementAndGet()
        }
      }
    }
    
    Await.result(Future.sequence(burstTasks), 60.seconds)
    val endTime = System.nanoTime()
    
    val totalProcessingTimeMs = (endTime - startTime) / 1000000
    val totalReadings = burstPattern.map(_._1).sum
    
    println(s"Burst traffic test results:")
    println(s"  - Total readings: $totalReadings")
    println(s"  - Anomalies processed: ${processedCounter.get()}")
    println(s"  - Errors: ${errorCounter.get()}")
    println(s"  - Total time: ${totalProcessingTimeMs}ms")
    println(s"  - Average processing rate: ${(totalReadings * 1000.0) / totalProcessingTimeMs} readings/second")
    
    errorCounter.get() shouldBe 0
    processedCounter.get() should be > 0L
  }

  it should "maintain accuracy under concurrent load" in {
    val config = SensorAlertConfig()
    val accuracyResults = new ConcurrentLinkedQueue[(String, Boolean)]()
    
    // Generate test cases with known expected results
    val testCases = (1 to 200).map { i =>
      val (sensorData, expectedAnomalyCount) = generateKnownTestCase(i)
      (sensorData, expectedAnomalyCount, s"test-case-$i")
    }
    
    // Process all test cases concurrently
    val accuracyTasks = testCases.map { case (sensorData, expectedCount, testId) =>
      Future {
        val anomalies = SensorAlerting.checkForAnomalies(sensorData, config)
        val isAccurate = anomalies.size == expectedCount
        accuracyResults.offer((testId, isAccurate))
        isAccurate
      }
    }
    
    val results = Await.result(Future.sequence(accuracyTasks), 30.seconds)
    val accurateResults = results.count(identity)
    val accuracyPercentage = (accurateResults.toDouble / results.size) * 100
    
    println(s"Concurrent accuracy test results:")
    println(s"  - Test cases: ${results.size}")
    println(s"  - Accurate results: $accurateResults")
    println(s"  - Accuracy: ${accuracyPercentage}%")
    
    // Should maintain 100% accuracy under concurrent load
    accuracyPercentage shouldBe 100.0
  }

  // Helper methods for generating test data
  private def generateSensorDataBatch(size: Int, batchId: Int): List[IoTSensorData] = {
    (1 to size).map { i =>
      val isAnomalous = Random.nextBoolean()
      
      IoTSensorData(
        deviceId = s"batch-$batchId-sensor-$i",
        temperature = Some(if (isAnomalous) 95.0 + Random.nextDouble() * 20 else 20.0 + Random.nextDouble() * 15),
        humidity = Some(if (isAnomalous && Random.nextBoolean()) 105.0 + Random.nextDouble() * 20 else 30.0 + Random.nextDouble() * 40),
        pressure = Some(1000.0 + Random.nextDouble() * 50),
        motion = Some(Random.nextBoolean()),
        light = Some(200.0 + Random.nextDouble() * 400),
        acidity = Some(6.0 + Random.nextDouble() * 3),
        location = s"Zone-${batchId % 5}",
        timestamp = System.currentTimeMillis() + i,
        metadata = SensorMetadata(
          batteryLevel = Some(if (isAnomalous && Random.nextBoolean()) 15 + Random.nextInt(10) else 50 + Random.nextInt(50)),
          signalStrength = Some(70 + Random.nextInt(30)),
          firmwareVersion = Some(s"v${Random.nextInt(3) + 1}.${Random.nextInt(10)}.0")
        )
      )
    }.toList
  }

  private def generateAnomalousSensorData(deviceId: String): IoTSensorData = {
    IoTSensorData(
      deviceId = deviceId,
      temperature = Some(90.0 + Random.nextDouble() * 20), // Always anomalous
      humidity = Some(50.0 + Random.nextDouble() * 30),
      pressure = Some(1000.0 + Random.nextDouble() * 50),
      motion = Some(Random.nextBoolean()),
      light = Some(300.0 + Random.nextDouble() * 200),
      acidity = Some(7.0 + Random.nextDouble() * 2),
      location = "LoadTestZone",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(80 + Random.nextInt(20)),
        signalStrength = Some(70 + Random.nextInt(30)),
        firmwareVersion = Some("v2.0.0")
      )
    )
  }

  private def generateKnownTestCase(caseId: Int): (IoTSensorData, Int) = {
    // Generate test cases with predictable anomaly counts
    val anomalyCount = caseId % 4 // 0, 1, 2, or 3 anomalies
    
    val sensorData = IoTSensorData(
      deviceId = s"known-test-$caseId",
      temperature = Some(if (anomalyCount >= 1) 95.0 else 25.0),
      humidity = Some(if (anomalyCount >= 2) 105.0 else 50.0),
      pressure = Some(if (anomalyCount >= 3) 800.0 else 1013.0),
      motion = Some(false),
      light = Some(400.0),
      acidity = Some(7.0),
      location = "KnownTestZone",
      timestamp = System.currentTimeMillis(),
      metadata = SensorMetadata(
        batteryLevel = Some(if (anomalyCount >= 4) 15 else 80), // This would be the 4th anomaly, but we max at 3
        signalStrength = Some(90),
        firmwareVersion = Some("v1.0.0")
      )
    )
    
    (sensorData, anomalyCount)
  }
}

/**
 * Asynchronous email gateway for performance testing
 */
class AsyncEmailGateway(emailQueue: ConcurrentLinkedQueue[Email]) extends EmailGateway {
  override def send(email: Email): Unit = {
    // Simulate async processing by adding to queue
    emailQueue.offer(email)
    
    // Simulate minimal processing time
    Thread.sleep(1)
  }
}
