import sbtassembly.AssemblyPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import sbt.Keys._

// Basic project information
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18" // Match bitnami Spark containers
ThisBuild / organization := "scala"

// Project definition
lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "data-pipeline-scala",
    mainClass in assembly := Some(
      "ingestion.KafkaS3DataLakePipeline" // Updated to correct main class
    ),
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
    ),

    // JVM options for better performance
    javaOptions ++= Seq(
      "-Xmx2G", // Maximum heap size
      "-XX:+UseG1GC", // Use G1 garbage collector
      "-XX:+UseStringDeduplication" // Reduce memory usage
    ),

    // Dependency management - keeping their versions but compatible with Scala 2.12
    libraryDependencies ++= Seq(
      // Kafka dependencies for message streaming
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.6.0",

      // Spark dependencies for data processing
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",

      // Delta Lake for data lake operations
      "io.delta" %% "delta-core" % "2.4.0",

      // AWS S3 support (from your original config)
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.565" exclude("com.fasterxml.jackson.core", "jackson-databind"),

      // PostgreSQL JDBC driver for Grafana analytics (from your original config)
      "org.postgresql" % "postgresql" % "42.7.1",

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

      // Metrics and monitoring
      "io.micrometer" % "micrometer-core" % "1.12.0",
      "io.micrometer" % "micrometer-registry-prometheus" % "1.12.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
    ),

    // Test configuration
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-oD"
    ),

    // Assembly plugin for creating fat JARs
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf"            => MergeStrategy.concat
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    },

    // Docker configuration for containerization
    Docker / packageName := "data-pipeline-spark",
    Docker / version := version.value,
    dockerBaseImage := "openjdk:11-jre-slim",
    dockerExposedPorts := Seq(8080, 9092)
  )
