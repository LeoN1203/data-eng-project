// Basic project information
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17" // Compatible with Spark 3.5.0
ThisBuild / organization := "scala"

// Project definition
lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "data-pipeline-ingestion",

    // Compiler options for better code quality and performance
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8", // Specify character encoding
      "-deprecation", // Warn about deprecated features
      "-unchecked", // Warn about unchecked operations
      "-feature", // Warn about features that should be imported explicitly
      "-Xlint", // Enable additional warnings
      "-Ywarn-dead-code", // Warn about dead code
      "-Ywarn-numeric-widen", // Warn about numeric widening
      "-Ywarn-value-discard" // Warn about discarded values
      // "-Xfatal-warnings" // Turn warnings into errors (remove for development)
    ),

    // JVM options for better performance
    javaOptions ++= Seq(
      "-Xmx2G", // Maximum heap size
      "-XX:+UseG1GC", // Use G1 garbage collector
      "-XX:+UseStringDeduplication" // Reduce memory usage
    ),

    // Dependency management
    libraryDependencies ++= Seq(
      // Apache Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-streaming" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
      
      // Hadoop/AWS dependencies for S3 support
      "org.apache.hadoop" % "hadoop-aws" % "3.3.6",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.565",

      // Kafka dependencies for message streaming
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.6.0",

      // JSON processing
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Configuration management
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

      // Testing dependencies
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % "3.6.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.15" % Test,
      "com.dimafeng" %% "testcontainers-scala-kafka" % "0.40.15" % Test,

      // Metrics and monitoring
      "io.micrometer" % "micrometer-core" % "1.12.0",
      "io.micrometer" % "micrometer-registry-prometheus" % "1.12.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
    ),    // Test configuration
    Test / parallelExecution := false, // Run tests sequentially for integration tests
    Test / testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-oD"
    ) // Show test durations
  )

// Additional sub-projects for modular architecture
lazy val common = (project in file("modules/common"))
  .settings(
    name := "data-pipeline-common",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6"
    )
  )

lazy val producers = (project in file("modules/producers"))
  .dependsOn(common)
  .settings(
    name := "data-pipeline-producers",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
    )
  )

lazy val consumers = (project in file("modules/consumers"))
  .dependsOn(common)
  .settings(
    name := "data-pipeline-consumers",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.6.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
    )
  )
