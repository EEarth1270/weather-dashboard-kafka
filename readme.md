# Weather Data Streaming and Dashboard

This project demonstrates how to stream weather data using Kafka, store it in PostgreSQL, and visualize it in real-time using a Dash web application.

## Project Structure

- `kafka_producer.py`: Generates fake weather data and sends it to a Kafka topic.
- `kafka_consumer.py`: Consumes weather data from Kafka and stores it in a PostgreSQL database.
- `weather_dashboard.py`: Displays the weather data on a live updating dashboard.
- `docker-compose.yaml`: Sets up the necessary services (Kafka, Zookeeper, PostgreSQL) using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- `pip` (Python package installer)

 

Create a .env file in the root directory with the following content:

KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=weather_data
KAFKA_GROUP_ID=weather_consumer_group
DATABASE_URL=postgresql+psycopg2://{username}:{password}@postgres/{database}
UPDATE_INTERVAL=5000
MESSAGE_FREQUENCY=1