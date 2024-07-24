# Weather Data Streaming and Dashboard

This project demonstrates how to stream generated weather data using Kafka, store it in PostgreSQL, and visualize it in real-time using a Dash web application.

## Project Structure

- `kafka_producer.py`: Generates fake weather data and sends it to a Kafka topic.
- `kafka_consumer.py`: Consumes weather data from Kafka and stores it in a PostgreSQL database.
- `weather_dashboard.py`: Displays the weather data on a live updating dashboard.
- `docker-compose.yaml`: Sets up the necessary services (Kafka, Zookeeper, PostgreSQL) using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- `pip` (Python package installer)

Change .env.example to a .env file in the root directory  

latlong data from https://github.com/spicydog/thailand-province-district-subdistrict-zipcode-latitude-longitude/tree/master
Weather API using https://openweathermap.org/ 
