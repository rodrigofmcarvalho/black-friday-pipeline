import os

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException


import sourcer.src.config.logger as logger
import sourcer.src.config.config as config
from sourcer.src.helpers.sourcer import Sourcer
from sourcer.src.config.constants import MAX_NUMBER_USERS, MAX_NUM_ITEMS_PER_USER, PRODUCTS_JSON_FILE

logger = logger.setup_logger()
# conf = getattr(config, f'{os.environ["APP_ENV"].title()}Config')
conf = getattr(config, 'DevConfig')

app = FastAPI(docs_url=f"/api/{conf.V_API}/docs",
              redoc_url=f"/api/{conf.V_API}/redoc",
              openapi_url=f"/api/{conf.V_API}/openapi.json")

# Configuration for Kafka Producer
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sourcer-fastapi',
}

producer = Producer(kafka_conf)


@app.get(f"/api/{conf.V_API}")
async def root():
    return {"message": f"Welcome to the sourcer {os.environ['HOSTNAME']}!"}


@app.post(f"/api/{conf.V_API}{conf.SOURCER_PATH}")
async def source_order():
    try:
        sourcer = Sourcer(MAX_NUMBER_USERS, MAX_NUM_ITEMS_PER_USER, PRODUCTS_JSON_FILE)
        
        for order_data in sourcer.source_random_order_data():
            producer.produce('orders', key="order_key", value=order_data)
            producer.flush()

        return {"status": "success", "message": "Data generated and pushed to Kafka"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)