from fastapi import FastAPI, HTTPException
from pykafka import KafkaClient
import json
from typing import List
from dags.models.models import *
from utils import *

app = FastAPI()

kafka_broker = "localhost:9092"
client = KafkaClient(hosts=kafka_broker)

@app.put("/SendEvent/")
async def send_event(event: Event):
    if event.event_name not in ["PageVisited", "AddedBasket", "CheckedProductReviews"]:
        raise HTTPException(status_code=400, detail="Invalid EventName")

    topic = client.topics[b'UserEvents']

    event_data = generate_event().model_dump()

    try:
        with topic.get_sync_producer() as producer:
            producer.produce(json.dumps(event_data).encode('utf-8'))
    except Exception as e:
        print(e)
    

@app.post("/PurchasedItems/")
async def send_purchased_items():
    topic = client.topics[b'PurchasedItems']

    items_data = generate_purchased_items().model_dump()

    try:
        with topic.get_sync_producer() as producer:
            producer.produce(json.dumps(items_data).encode('utf-8'))
    except Exception as e:
        print(e)
