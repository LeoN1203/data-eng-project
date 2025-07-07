# producer-app/Dockerfile
FROM openjdk:11-jre-slim

ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
ARG SCALA_VERSION=2.13

WORKDIR /app

# Copy the built JAR file from the build context
COPY sensor-simulator/target/scala-${SCALA_VERSION}/${JAR_NAME} ./app.jar

# Env vars to override at runtime:
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9093
ENV TOPIC=iot-sensor-data

ENTRYPOINT ["java", "-jar", "app.jar"]