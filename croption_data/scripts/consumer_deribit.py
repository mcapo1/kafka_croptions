import asyncio
from aiokafka import AIOKafkaConsumer
from kafka.errors import KafkaError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
import json
import sys

from sqlalchemy import Column, DateTime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Column, Float, String, BigInteger, Numeric, TIMESTAMP, Boolean
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base

from aiologger import Logger
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from aiologger.handlers.streams import AsyncStreamHandler


from utils.option_utils import get_next_8_UTC_date_str, get_startup_channels
import re


from sqlalchemy.orm import sessionmaker
import asyncio
# MongoDB configuration
MONGO_DETAILS = "mongodb://team:Python123@mongo:27017/"
DATABASE_NAME = "deribit_options_data"
# COLLECTION_NAME = "ticker111"

from datetime import datetime, timezone
# Kafka configuration
KAFKA_TOPIC = 'real_time_data_options'
# KAFKA_SERVERS = 'localhost:9093'
KAFKA_SERVERS = 'kafka:9092'

from sqlalchemy.ext.asyncio import async_scoped_session

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.schema import UniqueConstraint

# DATABASE_URI = f'postgresql+asyncpg://team:Python123@localhost/postgres'
DATABASE_URI = f'postgresql+asyncpg://team:Python123@timescaledb/postgres'
# async_engine = create_async_engine(DATABASE_URI, echo=True)
async_engine: AsyncEngine = create_async_engine(DATABASE_URI)
AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

DEFAULT_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logger = Logger(name="MyAsyncLogger")

# Cache for storing model classes
model_cache = {}

pattern = re.compile(r'\d{1,2}[A-Za-z]{3}\d{2}')
async def configure_logger():
    # Create and add the custom file handler
    # file_handler = AsyncFileHandler(filename=log_file_path)
    # file_handler.formatter = Formatter(fmt=DEFAULT_FORMAT)
    # logger.add_handler(file_handler)

    # Create and add a customized shell (stdout) streaming handler
    formatter = Formatter(fmt=DEFAULT_FORMAT)
    stream_handler = AsyncStreamHandler(stream=sys.stdout, formatter=formatter)
    logger.add_handler(stream_handler)

    await logger.info('Logger setup!!!!!!!!!!!!!!!!')

    # Ensure the log file is deleted if it exists
    # if os.path.exists(log_file_path):
    #     os.remove(log_file_path)
    #     await logger.info(f'Removing existing log file: {log_file_path}')

async def create_tables(tickers):
    async with async_engine.begin() as conn:
        # Dynamically create a model for each ticker and create table
        for ticker in tickers:
            model = option_ticker_model(ticker)  # Synchronous call within an async block
            await conn.run_sync(model.__table__.create, checkfirst=True)
            await logger.info(f"{ticker} table crated!")

async def convert_to_hypertables(tickers):
    async with async_engine.connect() as conn:
        await conn.begin()
        for ticker in tickers:
            tablename = ticker.replace('.', '_').lower()
            try:
                # Use the text() construct to ensure the SQL command is executed as a raw string
                await conn.execute(text(f"SELECT create_hypertable('{tablename}', 'timestamp_ms');"))
                await conn.commit()  # Commit the transaction
                await logger.info(f"{ticker} HYPER TABLE  created!")
            except Exception as e:
                await conn.rollback()  # Rollback in case of an error
                print(f"Could not convert {tablename} to hypertable: {e}")

# async def convert_to_hypertables(tickers):
#     async with async_engine.connect() as conn:
#         for ticker in tickers:
#             tablename = ticker.replace('.', '_').lower()
#             try:
#                 await conn.execute(f"SELECT create_hypertable('{tablename}', 'timestamp');")
#             except Exception as e:
#                 print(f"Could not convert {tablename} to hypertable: {e}")
# def get_channels():
#
#     # channels_call_23feb = [f'ticker.BTC-23FEB24-{str(int(k*1000))}-C.agg2' for k in range(40, 60, 2)]
#     # channels_put_23_feb = [f'ticker.BTC-23FEB24-{str(int(k*1000))}-P.agg2' for k in range(40, 60, 2)]
#     channels_call_29mar = [f'ticker.BTC-29MAR24-{str(int(k*1000))}-C.agg2' for k in range(50, 72, 2)]
#     channels_put_29mar = [f'ticker.BTC-29MAR24-{str(int(k*1000))}-P.agg2' for k in range(50, 72, 2)]
#     # return  channels_call_29mar + channels_put_29mar + ['ticker.BTC-PERPETUAL.agg2']
#     # return   ['ticker.BTC-PERPETUAL.agg2']
#     return get_startup_channels(raw_or_agg2='agg2')

async def insert_into_timescale(data, ticker_name):
    # Dynamically create a model class based on the ticker name
    MarketDataModel = option_ticker_model(ticker_name)  # Adjust based on your actual factory function name

    # Use the async session for database operations
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Create an instance of the model with the provided data
            new_entry = MarketDataModel(**data)
            # print(data)
            session.add(new_entry)
            # No need for explicit session.commit() due to the context manager

def convert_timestamp(timestamp_ms):
    """Converts a timestamp in milliseconds to a timezone-aware datetime object."""
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

def option_ticker_model(ticker):

    if ticker in model_cache:
        return model_cache[ticker]
    # {'estimated_delivery_price': 48155.23, 'best_bid_amount': 0.9, 'best_ask_amount': 0.9, 'bid_iv': 0.0,
    #  'ask_iv': 999.0, 'underlying_index': 'BTC-23FEB24', 'underlying_price': 48390.5, 'mark_iv': 49.38,
    #  'best_bid_price': 0.0001, 'best_ask_price': 15.0, 'interest_rate': 0.0, 'mark_price': 0.0697, 'open_interest': 0.5,
    #  'max_price': 0.114, 'min_price': 0.038, 'settlement_price': 0.06846742, 'last_price': 0.276,
    #  'instrument_name': 'BTC-23FEB24-51000-P', 'index_price': 48155.23, 'state': 'open',
    #  'timestamp_utc': datetime.datetime(2024, 2, 11, 9, 1, 28, 641000, tzinfo=datetime.timezone.utc),
    #  'timestamp_ms': 1707642088641, 'volume_usd': 0.0, 'volume': 0.0, 'price_change': None, 'low': None, 'high': None,
    #  'rho': -12.30382, 'theta': -62.26424, 'vega': 30.15361, 'gamma': 8e-05, 'delta': -0.70641,
    #  '_id': ObjectId('65c88cdf02d59f62074a43a8')}

    class OptionsData(Base):
        __tablename__ = ticker.replace('.', '_').lower()

        # __table_args__ = (UniqueConstraint('timestamp_ms', name=f"uix_ts_instrument_{ticker.replace('.', '_').lower()}"),  {'extend_existing': True},)

        __table_args__ = (
            UniqueConstraint('timestamp_ms', 'instrument_name', name=f"uix_ts_instrument_{ticker.replace('.', '_').lower()}"),
            {'extend_existing': True},
        )

        timestamp_ms = Column(BIGINT, primary_key=True)
        instrument_name = Column(String, primary_key=True)
        funding_8h = Column(Numeric)
        current_funding = Column(Numeric)
        interest_value = Column(Numeric)
        estimated_delivery_price = Column(Numeric)
        best_bid_amount = Column(Float)
        best_ask_amount = Column(Float)
        bid_iv = Column(Float)
        ask_iv = Column(Float)
        underlying_index = Column(String)
        underlying_price = Column(Numeric)
        mark_iv = Column(Float)
        best_bid_price = Column(Numeric)
        best_ask_price = Column(Numeric)
        interest_rate = Column(Float)
        mark_price = Column(Numeric)
        open_interest = Column(Float)
        max_price = Column(Numeric)
        min_price = Column(Numeric)
        settlement_price = Column(Numeric)
        last_price = Column(Numeric)
        index_price = Column(Numeric)
        rho = Column(Float)
        theta = Column(Float)
        vega = Column(Float)
        gamma = Column(Float)
        delta = Column(Float)
        volume_usd = Column(Numeric)
        volume = Column(Float)
        volume_notional = Column(Float)
        price_change = Column(Float)
        low = Column(Numeric)
        high = Column(Numeric)
        state = Column(String)
        timestamp_utc = Column(DateTime(timezone=True))

    model_cache[ticker] = OptionsData


    return OptionsData

async def insert_document(db, document, ticker):
    try:
        # Attempt to insert a document into MongoDB
        await db[ticker].insert_one(document)
        # print(f"Inserted into mongodb.............{ticker}...................")
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
#            {"estimated_delivery_price":48321.99,"best_bid_amount":32.3,"best_ask_amount":32.4,"bid_iv":37.0,"ask_iv":56.44,"underlying_index":"BTC-23FEB24","underlying_price":48559.14,"mark_iv":46.17,"best_bid_price":0.0775,"best_ask_price":0.0865,"interest_rate":0.0,"mark_price":0.0815,"open_interest":1716.9,"max_price":0.117,"min_price":0.0515,"settlement_price":0.06076527,"last_price":0.085,"instrument_name":"BTC-23FEB24-45000-C","index_price":48321.99, "rho":12.04475,"theta":-42.9504,"vega":22.55662,"gamma":0.00006,"delta":0.82809, "volume_usd":154419.3,"volume":52.3,"price_change":36.0,"low":0.0605,"high":0.085, "state":"open","timestamp":1707627659690}
async def consume_messages(consumer, db):
    while True:
        cnt = 0
        cnt_error = 0
        try:
            # Fetch messages
            async for message in consumer:
                try:
                    # Decode the message to a Python dictionary
                    data = json.loads(message.value.decode('utf-8'))
                    # print(f"Consumed message: {data}")

                    if 'id' in data:
                        print( f'setup data only.........{data}')
                        continue

                    tick_data = data['params']['data']

                    tick_data['timestamp_utc'] = convert_timestamp(tick_data['timestamp'])
                    tick_data['timestamp_ms'] = tick_data.pop('timestamp')
                    # Remove the 'stats' entry and capture its value
                    stats = tick_data.pop('stats')
                    tick_data.update(stats)

                    # if 'funding_8h' in tick_data:
                    #     tick_data.pop('funding_8h')

                    if 'greeks' in tick_data:
                        greeks = tick_data.pop('greeks')
                        tick_data.update(greeks)
                        ticker_in = data['params']['channel'].lower()

                        match = pattern.search(ticker_in)
                        if match:
                            ticker_name = 'btc_opt_' + match.group().lower()
                        else:
                            ticker_name = 'btc_fut_perp'

                    else:
                        # print(tick_data)
                        ticker_name = 'btc_fut_perp'

                    try:
                        await insert_into_timescale(tick_data, ticker_name)
                        # print("TIMESCALE inserted successfully.", ticker_name)
                    except Exception as e:
                        # Basic error checking
                        # cnt_error += 1
                        # if cnt_error%100==0:
                        await logger.error(f"Timescale - An error occurred during data insertion: {e}")
                    try:
                        pass
                        # await insert_document(db, tick_data, ticker_name)
                        # print("MONGO inserted successfully.", ticker_name)
                    except PyMongoError as e:
                        await logger.error(f"MongoDB error: {e}")

                    cnt = cnt + 1

                    await consumer.commit()

                    if cnt % 200 == 0:
                        logger.info(f'{cnt}: {tick_data}')
                except json.JSONDecodeError:
                    logger.error("Error decoding JSON")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}, attempting to reconnect...")
            await asyncio.sleep(5)  # Wait a bit before trying to consume again
        except Exception as e:
            logger.error(f"Unexpected error: {e}, attempting to reconnect...")
            logger.error(data)
            await asyncio.sleep(5)  # Wait a bit before trying to consume again

async def setup_ts_index_mongo(db):
    from pymongo import MongoClient
    # cls = get_channels()
    # loop thorugh collections and crate index
    for cl in cls:
        cl_name = cl.replace('.', '_').lower()
        collection = db[cl_name]  # Replace with your actual collection name
       # Create a unique index on the 'timestamp_ms' field
        index_name  = await collection.create_index([('timestamp_ms', 1)], unique=True)

        await logger.info(f'Index created for {cl} with name {index_name}')

async def main():

    ccy = 'BTC'
    await configure_logger()

    # tickers = get_channels()

    mat_array = [ccy.lower() + '_opt_' + get_next_8_UTC_date_str(i) for i in range(10)] + [ccy.lower() +  '_fut_perp']
    print(mat_array)


    await create_tables(mat_array)
    await convert_to_hypertables(mat_array)

    mongo_client = AsyncIOMotorClient(MONGO_DETAILS)
    db = mongo_client[DATABASE_NAME]

    # setup indexs and tables
    # await setup_ts_index_mongo(db)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        # group_id="your-group-id",
        auto_offset_reset='latest',
        enable_auto_commit=False,  # Disable auto commit
        session_timeout_ms=30000,  # 30 seconds
        heartbeat_interval_ms=10000,  # 10 seconds
        max_poll_interval_ms=300000,  # 5 minutes
        request_timeout_ms=305000,  # 5 minutes + 5 seconds
        max_poll_records=10,  # Maximum number of records returned in a single poll
        fetch_min_bytes=1,  # Minimum amount of data to fetch
        fetch_max_wait_ms=500,  # Maximum wait time for fetching data
        max_partition_fetch_bytes=1 * 1024 * 1024,  # 1MB per partition
    )

    await consumer.start()
    await logger.info('Consumer started................')

    await logger.info('Started consumer.......................................')
    try:
        await consume_messages(consumer, db)
    finally:
        await consumer.stop()
        mongo_client.close()

if __name__ == '__main__':

    asyncio.run(main())
