import logging
import requests

import json

from producer.src.config.constants import (
    EVENT_TYPE,
    MAX_NUM_ITEMS,
    MAX_NUM_USERS,
    PRODUCTS_JSON_FILE,
    SCHEMA_VERSION,
)

from helpers.sourcer import Sourcer

if __name__ == '__main__':

    TOP_LEVEL_FIELDS = {
        'event_type': EVENT_TYPE,
        'schema_version': SCHEMA_VERSION,
    }
    # URL = os.environ['COLLECTOR_URL']
    URL = 'http://10.105.95.164:8095/api/v1/collect'

    source = Sourcer(
        MAX_NUM_USERS,
        MAX_NUM_ITEMS,
        PRODUCTS_JSON_FILE,
    )

    for order_data in source.source_random_order_data():
        try:
            order_data_dict = json.loads(order_data)
            processed_event = {**TOP_LEVEL_FIELDS, 'payload': order_data_dict}
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