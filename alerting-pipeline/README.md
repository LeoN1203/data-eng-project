# IoT Alerting Pipeline - Containerized Solution

This directory contains a containerized implementation of the IoT Alerting Pipeline using Docker and Docker Compose.

## Overview

The IoT Alerting Pipeline is a real-time data processing system that:
- Consumes sensor data from Kafka topics
- Detects anomalies in temperature, humidity, pressure, light, acidity, and battery levels
- Sends email alerts when anomalies are detected
- Runs entirely in Docker containers for easy deployment and testing

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Zookeeper     │    │      Kafka      │    │  IoT Alerting   │
│   Container     │◄───┤   Container     │◄───┤   Pipeline      │
│                 │    │                 │    │   Container     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────┐
                                              │   SMTP Server   │
                                              │   (External)    │
                                              └─────────────────┘
```

## Files Structure

```
alerting-pipeline/
├── README.md                          # This documentation
├── docker-compose.yml                 # Full containerized setup (Kafka + Pipeline)
├── docker-compose-kafka.yml           # Kafka-only setup (for local pipeline testing)
├── test-compose.sh                    # Docker Compose test script
└── spark/                             # Spark processing pipeline
    ├── Dockerfile                     # Pipeline container definition
    ├── build.sbt                      # SBT build configuration
    ├── project/
    │   └── plugins.sbt               # SBT plugins
    └── src/
        └── main/
            ├── scala/
            │   └── processing/
            │       └── KafkaAlertingPipeline.scala
            └── resources/
                └── application.conf  # Configuration file
```

## Quick Start

### Option 1: Full Containerized Setup (Recommended)

Use this for complete containerized deployment and testing:

1. **Start the entire pipeline:**
   ```bash
   ./test-compose.sh start
   ```

2. **Send test messages and check results:**
   ```bash
   ./test-compose.sh test
   ```

3. **View live logs:**
   ```bash
   ./test-compose.sh logs
   ```

4. **Stop all services:**
   ```bash
   ./test-compose.sh stop
   ```

### Option 2: Hybrid Setup (Kafka in Docker, Pipeline Local)

Use this for development when you want to run the Spark pipeline locally but use Kafka in Docker:

1. **Start Kafka services only:**
   ```bash
   docker-compose -f docker-compose-kafka.yml up -d
   ```

2. **Run the pipeline locally:**
   ```bash
   cd spark
   sbt "runMain processing.KafkaAlertingPipeline"
   ```

3. **Send test messages:**
   ```bash
   # Use the robust test script from the project root
   ../test-alerting-pipeline-robust.sh
   ```

4. **Stop Kafka services:**
   ```bash
   docker-compose -f docker-compose-kafka.yml down
   ```

5. **Build the Docker image:**
   ```bash
   ./test-compose.sh build
   ```

## Configuration

### Environment Variables

The pipeline can be configured using environment variables:

#### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: `kafka:29092`)
- `KAFKA_TOPIC`: Kafka topic name (default: `iot-sensor-data`)
- `KAFKA_CONSUMER_GROUP_ID`: Consumer group ID (default: `iot-alerting-consumer`)
- `KAFKA_STARTING_OFFSETS`: Starting offset strategy (default: `earliest`)

#### Spark Configuration
- `SPARK_APP_NAME`: Spark application name
- `SPARK_CHECKPOINT_LOCATION`: Checkpoint directory (default: `/tmp/kafka-alerting-checkpoints/`)
- `SPARK_PROCESSING_TIME_SECONDS`: Processing interval in seconds (default: `5`)
- `SPARK_LOG_LEVEL`: Spark log level (default: `WARN`)

#### Email Configuration
- `SMTP_HOST`: SMTP server hostname (default: `sandbox.smtp.mailtrap.io`)
- `SMTP_PORT`: SMTP server port (default: `587`)
- `SMTP_USER`: SMTP username (required for email alerts)
- `SMTP_PASSWORD`: SMTP password (required for email alerts)
- `EMAIL_FROM_ADDRESS`: From email address (default: `iot-alerts@company.com`)
- `EMAIL_SUBJECT_PREFIX`: Email subject prefix (default: `[IoT Alert]`)
- `ALERT_RECIPIENT_EMAIL`: Alert recipient email (default: `admin@company.com`)

### SMTP Configuration for Email Alerts

To enable email alerts, set the SMTP credentials:

```bash
export SMTP_USER="your_smtp_username"
export SMTP_PASSWORD="your_smtp_password"
export SMTP_HOST="your_smtp_server.com"
export SMTP_PASSWORD="your_password"
./test-compose.sh start
```

#### Using Mailtrap for Testing

1. Sign up for a free [Mailtrap](https://mailtrap.io/) account
2. Get your SMTP credentials from the inbox settings
3. Set the environment variables:
   ```bash
   export SMTP_HOST="sandbox.smtp.mailtrap.io"
   export SMTP_PORT="587"
   export SMTP_USER="your_mailtrap_username"
   export SMTP_PASSWORD="your_mailtrap_password"
   ```

## Anomaly Detection Rules

The pipeline detects the following anomalies:

| Sensor Type | Normal Range | Anomaly Conditions |
|-------------|--------------|-------------------|
| Temperature | -10°C to 80°C | < -10°C or > 80°C |
| Humidity | 0% to 100% | < 0% or > 100% |
| Pressure | 900 hPa to 1100 hPa | < 900 hPa or > 1100 hPa |
| Light Level | 0 to 1000 lux | < 0 or > 1000 lux |
| Acidity (pH) | 4.0 to 10.0 | < 4.0 or > 10.0 |
| Battery Level | 20% to 100% | < 20% |

## Sample Data Format

The pipeline expects JSON messages in the following format:

```json
{
  "sensorId": "TEMP_001",
  "timestamp": "2024-07-04T15:30:00Z",
  "temperature": 22.5,
  "humidity": 45.0,
  "pressure": 1013.25,
  "lightLevel": 300.0,
  "acidity": 7.0,
  "batteryLevel": 85
}
```

## Testing

### Manual Testing

1. Start the pipeline:
   ```bash
   ./test-compose.sh start
   ```

2. Send a normal message:
   ```bash
   docker exec kafka-iot kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:29092
   {"sensorId":"TEST_001","timestamp":"2024-07-04T15:30:00Z","temperature":22.5,"humidity":45.0,"pressure":1013.25,"lightLevel":300.0,"acidity":7.0,"batteryLevel":85}
   ```

3. Send an anomalous message:
   ```bash
   docker exec kafka-iot kafka-console-producer --topic iot-sensor-data --bootstrap-server localhost:29092
   {"sensorId":"TEST_001","timestamp":"2024-07-04T15:31:00Z","temperature":95.5,"humidity":45.0,"pressure":1013.25,"lightLevel":300.0,"acidity":7.0,"batteryLevel":15}
   ```

4. Check the logs:
   ```bash
   ./test-compose.sh logs
   ```

### Automated Testing

The test scripts automatically:
1. Start all required services (Zookeeper, Kafka, Pipeline)
2. Create the Kafka topic
3. Send both normal and anomalous test data
4. Verify that anomalies are detected
5. Check for email alerts (if SMTP is configured)
6. Display comprehensive results

## Troubleshooting

### Common Issues

1. **Container fails to start:**
   - Check Docker is running: `docker ps`
   - Check available disk space: `df -h`
   - View container logs: `docker logs <container_name>`

2. **No anomalies detected:**
   - Verify data format matches expected schema
   - Check Spark logs for processing errors
   - Ensure Kafka topic is created and accessible

3. **Email alerts not working:**
   - Verify SMTP credentials are set correctly
   - Check SMTP server connectivity
   - Review email logs in the pipeline container

4. **Performance issues:**
   - Adjust `SPARK_PROCESSING_TIME_SECONDS` for your workload
   - Monitor container resource usage: `docker stats`
   - Check checkpoint directory for disk space

### Debugging Commands

```bash
# View all container logs
docker-compose logs

# View specific service logs
docker-compose logs iot-alerting-pipeline

# Execute commands in running containers
docker exec -it kafka-iot bash
docker exec -it iot-alerting-consumer bash

# Check container resource usage
docker stats

# Inspect container configuration
docker inspect iot-alerting-consumer
```

## Building from Source

To rebuild the Docker image:

```bash
cd spark
docker build -t iot-alerting-pipeline:latest .
```

## Production Deployment

For production deployment:

1. **Configure persistent volumes:**
   - Map checkpoint directory to persistent storage
   - Configure Kafka data persistence

2. **Set resource limits:**
   - Define CPU and memory limits in docker-compose.yml
   - Configure JVM heap sizes appropriately

3. **Security considerations:**
   - Use secrets management for SMTP credentials
   - Configure network policies
   - Enable TLS for Kafka communication

4. **Monitoring and observability:**
   - Add Prometheus metrics collection
   - Configure centralized logging
   - Set up health checks and alerting

## License

This project is part of the IoT Data Engineering pipeline demonstration.
