import asyncio
from aiokafka import AIOKafkaConsumer
from kafka.errors import KafkaError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
import json

# MongoDB configuration
MONGO_DETAILS = "mongodb://localhost:27018"
DATABASE_NAME = "deribit_options"
# COLLECTION_NAME = "ticker111"

# Kafka configuration
KAFKA_TOPIC = 'real_time_data'
KAFKA_SERVERS = 'localhost:9093'

async def insert_document(db, document, ticker):
    try:
        # Attempt to insert a document into MongoDB
        await db[ticker].insert_one(document)
        print(f"Inserted into mongodb.............{ticker}...................")
    except PyMongoError as e:
        print(f"MongoDB error: {e}, document could not be inserted.")
        # Implement retry logic or error handling as needed


    # {'jsonrpc': '2.0', 'method': 'subscription', 'params': {'channel': 'ticker.BTC-PERPETUAL.agg2',
    #                                                         'data': {'funding_8h': 5.793e-05, 'current_funding': 0.0,
    #                                                                  'estimated_delivery_price': 47220.96,
    #                                                                  'best_bid_amount': 187500.0,
    #                                                                  'best_ask_amount': 79180.0,
    #                                                                  'best_bid_price': 47233.5,
    #                                                                  'best_ask_price': 47234.0, 'mark_price': 47225.0,
    #                                                                  'open_interest': 559267830, 'max_price': 47933.71,
    #                                                                  'min_price': 46516.95,
    #                                                                  'settlement_price': 47191.27,
    #                                                                  'last_price': 47231.0,
    #                                                                  'interest_value': 0.000941942703226156,
    #                                                                  'instrument_name': 'BTC-PERPETUAL',
    #                                                                  'index_price': 47220.96,
#                                                                           'state': 'open',
    #                                                                  'timestamp': 1707554242049}}}


# {"jsonrpc":"2.0","method":"subscription",
#  "params":{"channel":"ticker.BTC-23FEB24-45000-C.agg2",
#            "data":
#            {"estimated_delivery_price":48321.99,"best_bid_amount":32.3,"best_ask_amount":32.4,"bid_iv":37.0,"ask_iv":56.44,"underlying_index":"BTC-23FEB24","underlying_price":48559.14,"mark_iv":46.17,"best_bid_price":0.0775,"best_ask_price":0.0865,"interest_rate":0.0,"mark_price":0.0815,"open_interest":1716.9,"max_price":0.117,"min_price":0.0515,"settlement_price":0.06076527,"last_price":0.085,"instrument_name":"BTC-23FEB24-45000-C","index_price":48321.99,
#            "greeks":{"rho":12.04475,"theta":-42.9504,"vega":22.55662,"gamma":0.00006,"delta":0.82809},
#            "stats":{"volume_usd":154419.3,"volume":52.3,"price_change":36.0,"low":0.0605,"high":0.085},
#            "state":"open","timestamp":1707627659690}}}

async def consume_messages(consumer, db):
    while True:
        try:
            # Fetch messages
            async for message in consumer:
                try:
                    # Decode the message to a Python dictionary
                    data = json.loads(message.value.decode('utf-8'))
                    # print(f"Consumed message: {data}")

                    if 'id' in data:
                        print( 'setup data only.........')
                        continue


                    tick_data = data['params']['data']
                    # Remove the 'stats' entry and capture its value
                    stats = tick_data.pop('stats')
                    tick_data.update(stats)

                    if 'greeks' in tick_data:
                        greeks = tick_data.pop('greeks')
                        tick_data.update(greeks)

                    # Merge the 'stats' dictionary with the original dictionary


                    ticker_name =  data['params']['channel']
                    # Insert the data into MongoDB, with error handling
                    await insert_document(db, tick_data, ticker_name)
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
