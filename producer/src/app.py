import logging
import requests

import json

from config.constants import (
    EVENT_TYPE,
    SCHEMA_VERSION
)

from confluent_kafka import Consumer 

# Configuration for Kafka Consumer
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sourcer-fastapi',
    'group.id' : 'producer-fastapi',
    'auto.offset.reset': 'earliest',
}

# Configuration for Kafka Consumer
consumer = Consumer(kafka_conf)
consumer.subscribe(['orders'])

if __name__ == '__main__':

    TOP_LEVEL_FIELDS = {
        'event_type': EVENT_TYPE,
        'schema_version': SCHEMA_VERSION,
    }
    # URL = os.environ['COLLECTOR_URL']
    URL = 'http://10.109.94.250:8095/api/v1/collect'

    while True:
        order_data = consumer.poll(1.0)
        if order_data is None:
            continue
        if order_data.error():
            print("Consumer error: {}".format(order_data.error()))
            continue
        try:
            order_data_str = order_data.value()
            order_data_str = order_data_str.decode('utf-8')
            order_data_dict = json.loads(order_data_str)
            processed_event = {**TOP_LEVEL_FIELDS, 'payload': order_data_dict}
            print(processed_event)
            response = requests.post(
                URL,
                data=json.dumps(processed_event),
                headers={'Content-Type': 'application/json'},
                timeout=10,
            )
            response.raise_for_status()
            if response.status_code == 200:
                print(
                    f'Data sent successfully to the collector: {processed_event}'
                )
            else:
                print(
                    f'Failed to send data to the collector: {response.content}'
                )
        except requests.exceptions.HTTPError as errh:
            logging.error('Http Error:', errh)
        except requests.exceptions.ConnectionError as errc:
            logging.error('Error Connecting:', errc)
        except requests.exceptions.Timeout as errt:
            logging.error('Timeout Error:', errt)
        except requests.exceptions.RequestException as err:
            logging.error('Something went wrong:', err)