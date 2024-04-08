import os
import time
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random

SINGAPORE_COORDINATES = { "latitude": 1.250111, "longitude": 103.830933 } #LONDON
MALACCA_COORDINATES = { "latitude": 2.200844, "longitude": 102.240143 } #BIRMINGHAM

#Calculate movement of the increments
LATITUDE_INCREMENT = (MALACCA_COORDINATES['latitude'] - SINGAPORE_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (MALACCA_COORDINATES['longitude'] - SINGAPORE_COORDINATES['longitude']) / 100

#Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')

start_time = datetime.now()
start_location = SINGAPORE_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60 ))
    return start_time

def simulate_vehicle_movement():
    global start_location

    #move towards malacca
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #simulate road travel (random)
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make' : 'BMW',
        'model' : 'C500',
        'year' : 2024,
        'fuelType' : 'Hybrid'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40), #km/h
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'camera_id': camera_id,
        'location' : location,
        'timestamp': timestamp,
        'snapshot' : 'Base64EncodedString'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError (f'object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'message fail to deliver: {err}')
    else:
        print(f'message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery= delivery_report
    )

    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='Nikon DSLR')
        #print(vehicle_data)
        #print(gps_data)
        #print(traffic_camera_data)

        if (vehicle_data['location'][0] >= MALACCA_COORDINATES['latitude']
        and vehicle_data['location'][1] >= MALACCA_COORDINATES['longitude']):
            print('WE HAVE ARRIVED IN MALACCA. HAVE A FUN TRIP!!!')
            break
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)

        time.sleep(5)

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Simulated-Journey')
    except KeyboardInterrupt:
        print('simulation terminated by Elton Taylor Chua Hao Yu')
    except Exception as e:
        print(f'Unexpected Error: {e}')
