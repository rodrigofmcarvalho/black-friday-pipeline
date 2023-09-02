import io
import os
from typing import Tuple

from avro.io import BinaryEncoder, DatumWriter
from avro.schema import parse
from confluent_kafka import Producer, schema_registry  # type: ignore
from dotenv import load_dotenv

from etl_black_friday.constants import (AVRO_SCHEMA_SUBJECT_NAME,
                                        DEAD_LETTER_TOPIC, RAW_EVENTS_TOPIC)


import config.logger as logger

def _load_env_variables() -> None:
    load_dotenv()



def collect(payload: DataPayload):
    data = payload.dict() # Convert Pydantic object to dictionary

    if not data:
        raise HTTPException(status_code=400, detail="No data found in the request.")

    if _validate_avro(data, avro_schema):
        writer = DatumWriter(avro_schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
        response_message, status_code = _produce_message(RAW_EVENTS_TOPIC, raw_bytes)
        
        return {'message': response_message}, status_code
    else:
        logger.error(f'Invalid data: {data}')
        response_message, status_code = _produce_message(DEAD_LETTER_TOPIC, str(data).encode('utf-8'))
        return (
            {
                'message': f'Invalid data: Data successfully published to dead letter queue. {response_message}'
            },
            400 if status_code == 200 else status_code
        )

if __name__ == '__main__':
    _load_env_variables()
    producer = _create_producer()
    sr = _create_schema_registry_client()
    avro_schema_str = _fetch_schema(sr, AVRO_SCHEMA_SUBJECT_NAME)