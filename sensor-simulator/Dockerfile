# producer-app/Dockerfile
FROM openjdk:11-jre-slim

ARG JAR_NAME=data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar
ARG SCALA_VERSION=2.13

WORKDIR /app
COPY target/scala-${SCALA_VERSION}/${JAR_NAME} ./app.jar

# Env vars to override at runtime:
#   KAFKA_BOOTSTRAP_SERVER, TOPIC
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
#   (any other custom vars your producer uses)
# ENV KAFKA_BOOTSTRAP_SERVER=kafka:9093
ENV TOPIC=iot-sensor-data

ENTRYPOINT ["java", "-jar", "app.jar"]
