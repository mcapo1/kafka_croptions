import asyncio
import json
from aiohttp import ClientSession, WSMsgType, WSServerHandshakeError, ClientConnectorError
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
KAFKA_TOPIC = 'real_time_data'
KAFKA_SERVERS = 'localhost:9093'

uri = 'wss://deribit.com/ws/api/v2'
API_KEY = "E3vOgLRO"
API_SECRET = "vQKH1s1OIFer4FI28j_hP1oEA5mSz2qJ1iw1IhlXyNg"

# Deribit WebSocket API endpoint
DERIBIT_WS_API = uri # "wss://www.deribit.com/ws/api/v2"

async def produce_to_kafka(producer, topic, message):
    try:
        # Send a message to Kafka
        await producer.send_and_wait(topic, message.encode('utf-8'))
    except KafkaError as e:
        print(f"Failed to send message to Kafka due to: {e}")
        # Implement retry logic here if needed

async def consume_deribit_and_produce_kafka(producer, session):
    retry_delay = 5  # Seconds to wait before retrying connection
    #
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


    while True:
        try:
            async with session.ws_connect(DERIBIT_WS_API) as ws:
                # Subscribe to a Deribit channel using ws.send_json for easier JSON handling
                await ws.send_json({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "public/subscribe",
                    "params": {
                        "channels": ["ticker.BTC-PERPETUAL.agg2"]
                    }
                })

                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        data = msg.data.strip()
                        print(f"Received data from Deribit: {data}")
                        # Produce the received message to Kafka
                        await produce_to_kafka(producer, KAFKA_TOPIC, data)
                    elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
        except (WSServerHandshakeError, ClientConnectorError) as e:
            print(f"WebSocket connection error: {e}, retrying in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)
        except Exception as e:
            print(f"Unexpected error: {e}, attempting to reconnect in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVERS)
    await producer.start()
    try:
        async with ClientSession() as session:
            await consume_deribit_and_produce_kafka(producer, session)
    finally:
        # Ensure the producer is properly closed
        await producer.stop()

if __name__ == '__main__':

    asyncio.run(main())
