from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pykafka import KafkaClient
from pymongo import MongoClient
import json

kafka_broker = "broker:29092"
mongo_uri = 'mongodb+srv://gokhan:gokhan123456@big-data.dinrnbf.mongodb.net/'

mongo_db = 'big-data'
inserted_data_collection = 'inserted_data'
updated_data_collection = 'updated_data'

def consume_from_kafka_and_insert_to_mongodb():
    try:
        client = KafkaClient(hosts=kafka_broker)
        topic = client.topics[b'UserEvents']
        consumer = topic.get_simple_consumer()

        mongo_client = MongoClient(mongo_uri)
        db = mongo_client[mongo_db]
        collection = db[inserted_data_collection]
        message_count=0
        for message in consumer:
            if message is not None:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    collection.insert_one(data)
                    message_count+=1
                    if message_count >= 3:
                        break
                except Exception as e:
                    print("Hata Kafka mesaji işlenirken:", e)
    except Exception as e:
        print("Hata oluştu:", e)
    

def calculate_event_count_and_update_mongodb():
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client[mongo_db]
    collection = db[inserted_data_collection]
    
    user_ids = collection.distinct("user_id")

    for user_id in user_ids:
        pipeline = [
            {"$match": {"user_id": user_id}},
            {"$group": {"_id": "$event_name", "EventCount": {"$sum": 1}}}
        ]
        events = collection.aggregate(pipeline)

        for event in events:
            event_name = event["_id"]
            event_count = event["EventCount"]

            query = {"UserId": user_id, "EventName": event_name}
            update = {"$set": {"EventCount": event_count}}

            db[updated_data_collection].update_one(query, update, upsert=True)

    mongo_client.close()

        
def start():
    print("DAG started")

def end():
    print("DAG finished")

with DAG(
    dag_id="insert_data",
    start_date=datetime(2024, 7, 8),
    catchup=False,
    schedule_interval="*/2 * * * *") as dag:

    start_task = PythonOperator(
        task_id='start',
        python_callable=start
    )

    consume_task = PythonOperator(
        task_id='consume_from_kafka_and_insert_to_mongodb',
        python_callable=consume_from_kafka_and_insert_to_mongodb,
    )

    calculate_task = PythonOperator(
        task_id='calculate_event_count_and_update_mongodb',
        python_callable=calculate_event_count_and_update_mongodb,
    )

    end_task = PythonOperator(
        task_id='end',
        python_callable=end
    )

    start_task >> consume_task >> calculate_task >> end_task
