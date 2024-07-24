import requests
from dotenv import load_dotenv
import os  
import json
import time
import logging
from kafka import KafkaProducer
from signal import signal, SIGINT

# Load configuration
load_dotenv()
api_key = os.getenv('API_KEY') 
KAFKA_BROKER = os.getenv('KAFKA_BROKER2')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC2')
MESSAGE_FREQUENCY = float(os.getenv('MESSAGE_FREQUENCY_HOUR'))

print(f"KAFKA_BROKER: {KAFKA_BROKER}")
# print(f"KAFKA_GROUP_ID: {KAFKA_GROUP_ID}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka_producer')


def call_api(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()  # Assuming the response is in JSON format
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

def request_api(api_key, call_api, lat, lon):
    base_url = 'https://api.openweathermap.org/data/2.5/weather'
    params = {'lat': f'{lat}', 'lon': f'{lon}','appid':f'{api_key}','units':'metric'}   
    response_data = call_api(base_url, params=params )
    return response_data

def call_api_prepare_msg(api_key, call_api, request_api, province, district, lat, lon):
    response_data = request_api(api_key, call_api, lat, lon)
    temp = response_data['main']['temp'] 
    real_feel = response_data['main']['feels_like'] 
    name_api = response_data['name']
    timestamp = response_data['dt']
    message = {'city': province,'district': district, 'cross_check_name':name_api, 'temperature': temp, 'real_feel':real_feel, 
               'timestamp': timestamp,'cross_check_name':name_api}
    return message

def produce_message(message):
    """
    Produces a message with a random city in Thailand and random temperature, and sends it to Kafka.
    """
    try: 
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"Message sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def read_input_city(input_file_path):
    with open(input_file_path, 'r',encoding='utf-8') as file:
        input_city = json.load(file)
    return input_city

# loop run through input city dict # current bangkok 
def extract_city_coor(city, coor):
    province = city.split(',')[0]
    district = city.split(',')[1]
    lat = coor['lat']
    lon = coor['long']
    return province,district,lat,lon

def shutdown_producer(signal_received, frame):
    logger.info("Shutting down producer...")
    producer.close()
    exit(0)

# Register SIGINT handler for graceful shutdown
signal(SIGINT, shutdown_producer)

# Main loop
if __name__ == "__main__":
    input_file_path ='bangkok_city_avg_latlon.json' 
    city_file = read_input_city(input_file_path)
    while True:
        logger.info("Wake up ... Start Call API ...")
        for city, coor in city_file.items():
            province, district, lat, lon = extract_city_coor(city, coor)
            message = call_api_prepare_msg(api_key, call_api, request_api, province, district, lat, lon)
            produce_message(message=message)
            time.sleep(1)
        logger.info("Finish Call API Sleeping now...")
        time.sleep(MESSAGE_FREQUENCY)
