import os
import json
import logging
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from signal import signal, SIGINT
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
DATABASE_URL = os.getenv('DATABASE_URL')

print(f"KAFKA_BROKER: {KAFKA_BROKER}")
print(f"KAFKA_GROUP_ID: {KAFKA_GROUP_ID}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
print(f"DATABASE_URL: {DATABASE_URL}")

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka_consumer')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
}

try:
    consumer = Consumer(kafka_config)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Successfully connected to Kafka")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    exit(1)

# PostgreSQL configuration
Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    city = Column(String)
    temperature = Column(Float)
    timestamp = Column(Integer)

try:
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Successfully connected to PostgreSQL")
except Exception as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

def consume_messages():
    """
    Consumes messages every 1 seconds from Kafka and yields them.
    """
    while True:
        logger.info("Polling for messages...")
        msg = consumer.poll(1.0)
        if msg is None:
            logger.info("No message received")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info(f"End of partition reached {msg.partition()}")
            elif msg.error():
                logger.error(f"Error: {msg.error()}")
                break
        else:
            message = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received message: {message}")
            yield message

def save_message_to_db(message):
    """
    Saves the consumed message to the PostgreSQL database.
    """
    try:
        weather_data = WeatherData(
            city=message['city'],
            temperature=message['temperature'],
            timestamp=message['timestamp']
        )
        session.add(weather_data)
        session.commit()
        logger.info("Message saved to DB")
    except Exception as e:
        logger.error(f"Failed to save message to DB: {e}")
        session.rollback()

def shutdown_consumer(signal_received, frame):
    logger.info("Shutting down consumer...")
    consumer.close()
    exit(0)

# Register SIGINT handler for graceful shutdown
signal(SIGINT, shutdown_consumer)

if __name__ == "__main__":
    for message in consume_messages():
        save_message_to_db(message)
