import asyncio
from aiokafka import AIOKafkaConsumer
from kafka.errors import KafkaError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
import json

# MongoDB configuration
MONGO_DETAILS = "mongodb://localhost:27017"
DATABASE_NAME = "deribit111"
COLLECTION_NAME = "ticker111"

# Kafka configuration
KAFKA_TOPIC = 'real_time_data'
KAFKA_SERVERS = 'localhost:9093'

async def insert_document(db, document):
    try:
        # Attempt to insert a document into MongoDB
        await db[COLLECTION_NAME].insert_one(document)
        print(f"Inserted into mongodb.................................")
    except PyMongoError as e:
        print(f"MongoDB error: {e}, document could not be inserted.")
        # Implement retry logic or error handling as needed

async def consume_messages(consumer, db):
    while True:
        try:
            # Fetch messages
            async for message in consumer:
                try:
                    # Decode the message to a Python dictionary
                    data = json.loads(message.value.decode('utf-8'))
                    print(f"Consumed message: {data}")
                    # Insert the data into MongoDB, with error handling
                    await insert_document(db, data)
                except json.JSONDecodeError:
                    print("Error decoding JSON")
        except KafkaError as e:
            print(f"Kafka error: {e}, attempting to reconnect...")
            await asyncio.sleep(5)  # Wait a bit before trying to consume again
        except Exception as e:
            print(f"Unexpected error: {e}, attempting to reconnect...")
            await asyncio.sleep(5)  # Wait a bit before trying to consume again

async def main():
    mongo_client = AsyncIOMotorClient(MONGO_DETAILS)
    db = mongo_client[DATABASE_NAME]

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest'  # Start reading at the earliest message
    )
    await consumer.start()
    try:
        await consume_messages(consumer, db)
    finally:
        await consumer.stop()
        mongo_client.close()

if __name__ == '__main__':
    asyncio.run(main())
