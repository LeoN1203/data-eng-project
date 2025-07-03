import sbtassembly.AssemblyPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import sbt.Keys._

// Basic project information
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.13" // Latest stable Scala 2.13
ThisBuild / organization := "scala"

// Project definition
lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "data-pipeline-scala",

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
      // Kafka dependencies for message streaming
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.6.0",

      // Spark dependencies for data processing
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",

      // Akka for actor-based concurrency (useful for IoT device simulation)
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
      "com.typesafe.akka" %% "akka-stream" % "2.8.5",

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
      "com.typesafe.akka" %% "akka-testkit" % "2.8.5" % Test,

      // Metrics and monitoring
      "io.micrometer" % "micrometer-core" % "1.12.0",
      "io.micrometer" % "micrometer-registry-prometheus" % "1.12.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
    ),

    // Test configuration
    Test / parallelExecution := false, // Run tests sequentially for integration tests
    Test / testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-oD"
    ), // Show test durations

    // Assembly plugin for creating fat JARs
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf"            => MergeStrategy.concat
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    },

    // Docker configuration for containerization
    Docker / packageName := "data-pipeline-scala",
    Docker / version := version.value,
    dockerBaseImage := "openjdk:11-jre-slim",
    dockerExposedPorts := Seq(8080, 9092)
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
