import os
import json
import time
import logging
from kafka import KafkaProducer
from faker import Faker
from signal import signal, SIGINT
from dotenv import load_dotenv
import random

# load env variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER' )
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC' )
MESSAGE_FREQUENCY = float(os.getenv('MESSAGE_FREQUENCY'))

# Setup Faker
fake = Faker()

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka_producer')

thailand_city_list = [
    'Bangkok','Samut Prakan','Nonthaburi','Pathum Thani','Phra Nakhon Si Ayutthaya','Ang Thong','Loburi','Sing Buri','Chai Nat','Saraburi','Nakhon Nayok','Nakhon Sawan','Uthai Thani','Kamphaeng Phet','Sukhothai','Phitsanulok','Phichit','Phetchabun','Suphan Buri','Nakhon Pathom','Samut Sakhon','Samut Songkhram','Chon Buri','Rayong','Chanthaburi','Trat','Chachoengsao','Prachin Buri','Sa Kaeo','Nakhon Ratchasima','Buri Ram','Surin','Si Sa Ket','Ubon Ratchathani','Yasothon','Chaiyaphum','Amnat Charoen','buogkan','Nong Bua Lam Phu','Khon Kaen','Udon Thani','Loei','Nong Khai','Maha Sarakham','Roi Et','Kalasin','Sakon Nakhon','Nakhon Phanom','Mukdahan','Chiang Mai','Lamphun','Lampang','Uttaradit','Phrae','Nan','Phayao','Chiang Rai','Mae Hong Son','Tak','Ratchaburi','Kanchanaburi','Phetchaburi','Prachuap Khiri Khan','Nakhon Si Thammarat','Krabi','Phangnga','Phuket','Surat Thani','Ranong','Chumphon','Songkhla','Satun','Trang','Phatthalung','Pattani','Yala','Narathiwat'
]

def produce_message(current_list):
    """
    Produces a message with a random city in Thailand and random temperature, and sends it to Kafka.
    """
    try:
        # print("before remove", len(current_list))
        if not current_list:  # If the list is empty, reset it
            current_list.extend(thailand_city_list)

        # Randomly choose and remove one item from the list 
        city = random.choice(current_list)
        # print(city)
        current_list.remove(city)
        # print("after remove" , len(current_list))
        temperature = fake.pyfloat(left_digits=2, right_digits=1, min_value=25, max_value=50)
        message = {'city': city, 'temperature': temperature, 'timestamp': int(time.time())}
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"Message sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def shutdown_producer(signal_received, frame):
    logger.info("Shutting down producer...")
    producer.close()
    exit(0)

# Register SIGINT handler for graceful shutdown
signal(SIGINT, shutdown_producer)

# Main loop
if __name__ == "__main__":
    current_list = thailand_city_list.copy()
    while True:
        produce_message(current_list)
        time.sleep(MESSAGE_FREQUENCY)
