import asyncio
import json
from aiohttp import ClientSession, WSMsgType, WSServerHandshakeError, ClientConnectorError
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaError
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# Kafka configuration
KAFKA_TOPIC = 'real_time_data_options'
KAFKA_SERVERS = 'localhost:9093'

uri = 'wss://deribit.com/ws/api/v2'
API_KEY = "E3vOgLRO"
API_SECRET = "vQKH1s1OIFer4FI28j_hP1oEA5mSz2qJ1iw1IhlXyNg"

# Deribit WebSocket API endpoint
DERIBIT_WS_API = uri # "wss://www.deribit.com/ws/api/v2"
DERIBIT_WS_API = "wss://www.deribit.com/ws/api/v2"

async def produce_to_kafka(producer, topic, message):
    try:
        # Send a message to Kafka
        await producer.send_and_wait(topic, message.encode('utf-8'))
    except KafkaError as e:
        print(f"Failed to send message to Kafka due to: {e}")
        # Implement retry logic here if needed


def get_channels():

    # channels_call_23feb = [f'ticker.BTC-23FEB24-{str(int(k*1000))}-C.agg2' for k in range(40, 60, 2)]
    # channels_put_23_feb = [f'ticker.BTC-23FEB24-{str(int(k*1000))}-P.agg2' for k in range(40, 60, 2)]
    channels_call_29mar = [f'ticker.BTC-29MAR24-{str(int(k*1000))}-C.agg2' for k in range(50, 72, 2)]
    channels_put_29mar = [f'ticker.BTC-29MAR24-{str(int(k*1000))}-P.agg2' for k in range(50, 72, 2)]
    # return  channels_call_29mar + channels_put_29mar + ['ticker.BTC-PERPETUAL.agg2']
    return   ['ticker.BTC-PERPETUAL.agg2']
#
# {"jsonrpc":"2.0","method":"subscription",
#  "params":{"channel":"ticker.BTC-23FEB24-45000-C.agg2",
#            "data":
#            {"estimated_delivery_price":48321.99,"best_bid_amount":32.3,"best_ask_amount":32.4,"bid_iv":37.0,"ask_iv":56.44,"underlying_index":"BTC-23FEB24","underlying_price":48559.14,"mark_iv":46.17,"best_bid_price":0.0775,"best_ask_price":0.0865,"interest_rate":0.0,"mark_price":0.0815,"open_interest":1716.9,"max_price":0.117,"min_price":0.0515,"settlement_price":0.06076527,"last_price":0.085,"instrument_name":"BTC-23FEB24-45000-C","index_price":48321.99,
#            "greeks":{"rho":12.04475,"theta":-42.9504,"vega":22.55662,"gamma":0.00006,"delta":0.82809},
#            "stats":{"volume_usd":154419.3,"volume":52.3,"price_change":36.0,"low":0.0605,"high":0.085},
#            "state":"open","timestamp":1707627659690}}}


async def consume_deribit_and_produce_kafka(producer, session):
    retry_delay = 5  # Seconds to wait before retrying connection
    while True:
        cnt = 0
        try:
            print('tryng to setup cx')
            async with session.ws_connect(DERIBIT_WS_API) as ws:
                # Subscribe to a Deribit channel using ws.send_json for easier JSON handling
                print('setup session')
                await ws.send_json({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "public/subscribe",
                    "params": {
                        "channels": get_channels()
                    }
                })
                print('sent requet.....')
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        data = msg.data.strip()
                        print(data)
                        # Produce the received message to Kafka
                        await produce_to_kafka(producer, KAFKA_TOPIC, data)
                        cnt +=1
                        if cnt%50 ==0:
                            print(f"Received data from Deribit: {data}")
                            cnt =0
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
