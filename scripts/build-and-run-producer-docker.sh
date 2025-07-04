# from project root
docker build -f sensor-simulator/Dockerfile -t sensor-producer:latest .

# # Create if doesnt exist "iot-net" network
# if ! docker network ls | grep -q iot-net; then
#   docker network create iot-net
# fi


# Use the AWS credentials from the .env file
docker run -d \
  --name producer \
  --network kafka-broker_iot-net \
  -e KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION=$AWS_REGION \
  sensor-producer:latest
