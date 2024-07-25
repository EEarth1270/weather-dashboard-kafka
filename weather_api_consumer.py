import os
import time
import json
import logging
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Float, BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker
from signal import signal, SIGINT
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER2')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID2')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC2')
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
    'auto.offset.reset': 'earliest',
    'max.poll.interval.ms': 1860000 
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
    __tablename__ = 'weather_data_real'
    id = Column(Integer, primary_key=True)
    city = Column(String(255), nullable=False)
    district = Column(String(255), nullable=False)
    cross_check_name = Column(String(255))
    temperature = Column(Float)
    real_feel = Column(Float)
    timestamp = Column(BigInteger, nullable=False)
    lat = Column(Float)
    long = Column(Float)

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
    Consumes messages from Kafka and yields them.
    """
    last_message_time = time.time()
    while True:
        current_time = time.time()
        if current_time - last_message_time >= 60:
            logger.info("No messages received for 1 minute. Waiting for 30 minutes before polling again...")
            time.sleep(1800)  # 30-minute delay
            last_message_time = time.time()  # Reset the last message time

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
            last_message_time = time.time()  # Reset the last message time
            yield message

def save_message_to_db(message):
    """
    Saves the consumed message to the PostgreSQL database.
    """
    try:
        weather_data = WeatherData(
            city=message['city'],
            district=message['district'],
            cross_check_name=message.get('cross_check_name'),
            temperature=message.get('temperature'),
            real_feel=message.get('real_feel'),
            timestamp=message['timestamp'],
            lat=message.get('lat'),
            long=message.get('long')
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
