import asyncio
import json
from aiohttp import ClientSession, WSMsgType, WSServerHandshakeError, ClientConnectorError
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import math
import requests
import math
from pprint import pprint

from utils.option_utils import get_startup_channels,get_day_n_options

# Kafka configuration
KAFKA_TOPIC = 'real_time_data_options'
# KAFKA_SERVERS = 'localhost:9093'
KAFKA_SERVERS = 'kafka:9092'

uri = 'wss://deribit.com/ws/api/v2'
API_KEY = "E3vOgLRO"
API_SECRET = "vQKH1s1OIFer4FI28j_hP1oEA5mSz2qJ1iw1IhlXyNg"

# Deribit WebSocket API endpoint
DERIBIT_WS_API = uri # "wss://www.deribit.com/ws/api/v2"
DERIBIT_WS_API = "wss://www.deribit.com/ws/api/v2"


scheduler = AsyncIOScheduler()

def get_channels():

    return get_startup_channels(raw_or_agg2='agg2')


async def produce_to_kafka(producer, topic, message):
    try:
        # Send a message to Kafka
        await producer.send_and_wait(topic, message.encode('utf-8'))
    except KafkaError as e:
        print(f"Failed to send message to Kafka due to: {e}")
        # Implement retry logic here if needed

async def clear_and_subscribe_to_deribit(ws):  #takes ws as setup previously

    print(f"******************************  UNSUB  ****************************")
    print(f"******************************  UNSUB  ****************************")
    print(f"******************************  UNSUB  ****************************")
    print(f"******************************  UNSUB  ****************************")
    print(f"******************************  UNSUB  ****************************")
    unsub_json = {
              "jsonrpc" : "2.0",
              "id" : 153,
              "method" : "public/unsubscribe_all",
              "params" : {  }
            }

    await ws.send_json(unsub_json)

    await asyncio.sleep(1)

    print(f"******************************      SUB       ****************************")
    print(f"******************************      SUB       ****************************")
    print(f"******************************      SUB       ****************************")
    print(f"******************************      SUB       ****************************")
    print(f"******************************      SUB       ****************************")

    # new_options = get_day_n_options(day=3)
    new_options = get_startup_channels(days=3)

    print(f"New options: {new_options}")
    sub_json = {
              "method": "public/subscribe",
              "params": {
                "channels": new_options
              },
              "jsonrpc": "2.0",
              "id": 3
            }
    await ws.send_json(sub_json)

job_id = 'daily_option_refresh'
def add_job_if_not_exists(scheduler, job_id, func, trigger, args):
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        print(f"Removed existing job with ID '{job_id}'")
    scheduler.add_job(func, trigger=trigger, id=job_id, args=args)
    print(f"Added new job with ID '{job_id}'")
async def consume_deribit_and_produce_kafka(producer, session):
    retry_delay = 1  # Seconds to wait before retrying connection
    #


    while True:
        cnt = 0
        print( f'Consuming Deribit.....starting loop........................................')
        try:
            print('tryng to setup cx')
            async with session.ws_connect(DERIBIT_WS_API) as ws:
                # Subscribe to a Deribit channel using ws.send_json for easier JSON handling
                print('setup session')

                # Add the job if it doesn't already exist
                add_job_if_not_exists(
                    scheduler,
                    job_id,
                    clear_and_subscribe_to_deribit,
                    trigger=CronTrigger(hour=8, minute=0, second=10, timezone= 'UTC'),
                    args=[ws]
                )

                if not scheduler.running:
                    scheduler.start()
                else:
                    print("Scheduler is already running")

                try:
                    await ws.send_json({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "public/subscribe",
                        "params": {
                            "channels": get_channels()
                        }
                    })
                    print('sent subscribe requet.....')
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            data = msg.data.strip()
                            await produce_to_kafka(producer, KAFKA_TOPIC, data)
                            cnt += 1
                            if cnt % 200 == 0:
                                print(f"{cnt}: {data}")
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            print('Non-text message received - error or closed')
                            break
                    print('FOR loop exited!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                except Exception as e:
                    print(f"Unexpected error in message loop: {e}, attempting to reconnect...")
                    break  # Break out of the inner while loop to reconnect
                print('WHILE loop exited!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
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



#
# {"jsonrpc":"2.0","method":"subscription",
#  "params":{"channel":"ticker.BTC-23FEB24-45000-C.agg2",
#            "data":
#            {"estimated_delivery_price":48321.99,"best_bid_amount":32.3,"best_ask_amount":32.4,"bid_iv":37.0,"ask_iv":56.44,"underlying_index":"BTC-23FEB24","underlying_price":48559.14,"mark_iv":46.17,"best_bid_price":0.0775,"best_ask_price":0.0865,"interest_rate":0.0,"mark_price":0.0815,"open_interest":1716.9,"max_price":0.117,"min_price":0.0515,"settlement_price":0.06076527,"last_price":0.085,"instrument_name":"BTC-23FEB24-45000-C","index_price":48321.99,
#            "greeks":{"rho":12.04475,"theta":-42.9504,"vega":22.55662,"gamma":0.00006,"delta":0.82809},
#            "stats":{"volume_usd":154419.3,"volume":52.3,"price_change":36.0,"low":0.0605,"high":0.085},
#            "state":"open","timestamp":1707627659690}}}