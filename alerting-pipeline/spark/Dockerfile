# Multi-stage Dockerfile for IoT Alerting Pipeline
FROM openjdk:11-jdk-slim as builder

# Install necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install SBT
RUN curl -L -o sbt.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-1.9.9.deb && \
    dpkg -i sbt.deb && \
    rm sbt.deb

# Set working directory
WORKDIR /app

# Copy build files first (for better caching)
COPY build.sbt .
COPY project/ ./project/

# Download dependencies (this layer will be cached if build.sbt doesn't change)
RUN sbt update

# Copy source code
COPY src/ ./src/

# Build the application
RUN sbt clean compile stage

# Runtime stage
FROM openjdk:11-jre-slim

# Install necessary runtime tools
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create app user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy the staged application from builder
COPY --from=builder /app/target/universal/stage/ ./

# Copy configuration files
COPY src/main/resources/application.conf ./conf/
COPY src/main/resources/log4j.properties ./conf/

# Create directories for checkpoints and logs
RUN mkdir -p /tmp/kafka-alerting-checkpoints /app/logs && \
    chown -R appuser:appuser /app /tmp/kafka-alerting-checkpoints

# Switch to non-root user
USER appuser

# Expose any ports if needed (not required for this Kafka consumer)
# EXPOSE 8080

# Environment variables with defaults
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=iot-sensor-data
ENV KAFKA_CONSUMER_GROUP_ID=iot-alerting-consumer
ENV KAFKA_STARTING_OFFSETS=earliest
ENV SPARK_APP_NAME=IoT-Kafka-Alerting-Pipeline-Docker
ENV SPARK_CHECKPOINT_LOCATION=/tmp/kafka-alerting-checkpoints/
ENV SPARK_PROCESSING_TIME_SECONDS=10
ENV SPARK_LOG_LEVEL=WARN
ENV ALERT_RECIPIENT_EMAIL=admin@company.com

# SMTP configuration (will be passed from host environment)
ENV SMTP_HOST=""
ENV SMTP_PORT=587
ENV SMTP_USER=""
ENV SMTP_PASSWORD=""
ENV EMAIL_FROM_ADDRESS=iot-alerts@company.com
ENV EMAIL_SUBJECT_PREFIX="[IoT Alert]"

# Health check - check for Java process instead of HTTP
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD pgrep -f "processing.KafkaAlertingPipeline" || exit 1

# Entry point
ENTRYPOINT ["./bin/data-pipeline-scala"]
CMD ["-main", "processing.KafkaAlertingPipeline"]
