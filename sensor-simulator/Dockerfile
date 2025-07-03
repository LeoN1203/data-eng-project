# producer-app/Dockerfile
FROM openjdk:11-jre-slim

ARG JAR_NAME=

WORKDIR /app
COPY target/scala-2.12/${JAR_NAME} ./app.jar

# Env vars to override at runtime:
#   KAFKA_BOOTSTRAP_SERVER, TOPIC
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
#   (any other custom vars your producer uses)
# ENV KAFKA_BOOTSTRAP_SERVER=kafka:9093
ENV TOPIC=iot-sensor-data

ENTRYPOINT ["java", "-jar", "app.jar"]
