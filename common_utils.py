# common_utils.py
import hashlib
import hmac
import time
import aiohttp
import asyncio
import logging

from binance_endpoints import TIME_ENDPOINT_V1, TIME_ENDPOINT_V3

logger = logging.getLogger(__name__)

def hashing(query_string, secret):
    return hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

class ServerTimestampCache:
    offset = None
    is_initialized = False
    is_maintenance_task_started = False
    sync_interval = 600  # Sync every 10 minutes
    lock = asyncio.Lock()  # Add a lock to prevent concurrent fetches
    buffer_ms = None  # Buffer will be set when syncing with Binance API

    @classmethod
    async def fetch_server_time(cls, resync=False):
        logger.debug("Fetching server time...")
        async with cls.lock:
            logger.debug("Acquired lock for fetching server time")
            if cls.is_initialized and not resync:
                logger.debug("Server timestamp is already initialized. Skipping fetch.")
                return 

            async with aiohttp.ClientSession() as session:
                endpoints = [TIME_ENDPOINT_V3, TIME_ENDPOINT_V1]
                for attempt in range(3):
                    for endpoint in endpoints:
                        try:
                            logger.info(f"Attempting to fetch server time from {endpoint}...")
                            start_time = int(time.time() * 1000)
                            async with session.get(endpoint) as response:
                                if response.status == 200:
                                    logger.info(f"Successfully fetched server time from {endpoint}")
                                    data = await response.json()
                                    server_time = data['serverTime']
                                    end_time = int(time.time() * 1000)
                                    round_trip_time = end_time - start_time
                                    current_time = int(time.time() * 1000)
                                    cls.offset = server_time - current_time
                                    cls.buffer_ms = round_trip_time + 500  # Set buffer based on round-trip time
                                    cls.is_initialized = True
                                    logger.debug(f"Updated server timestamp: {server_time} (Offset: {cls.offset}, Buffer: {cls.buffer_ms} ms)")
                                    return
                        except Exception as e:
                            logger.error(f"Attempt {attempt + 1}: Failed to fetch server time from {endpoint}: {e}")
                            await asyncio.sleep(0.2 * (2 ** attempt))  # Exponential backoff

                cls.is_initialized = False
                logger.error("Failed to update server timestamp from all endpoints. Using local time instead.")
                cls.offset = 0

    @classmethod
    async def maintain_timestamp(cls):
        while True:
            await cls.fetch_server_time()
            await asyncio.sleep(cls.sync_interval)

    @classmethod
    async def ensure_initialized(cls):
        await cls.fetch_server_time()

    @classmethod
    async def ensure_maintenance_task_started(cls):
        if not cls.is_maintenance_task_started:
            cls.is_maintenance_task_started = True
            asyncio.create_task(cls.maintain_timestamp())

async def get_server_timestamp(resync=False):
    if resync:
        logger.info("Resyncing server timestamp...")
        await ServerTimestampCache.fetch_server_time(resync=True)
    else:
        await ServerTimestampCache.ensure_initialized()
        await ServerTimestampCache.ensure_maintenance_task_started()

    if ServerTimestampCache.offset is None:
        logger.error("Server timestamp offset is not initialized. Using local time.")
        return int(time.time() * 1000)

    current_timestamp = int(time.time() * 1000) + ServerTimestampCache.offset + ServerTimestampCache.buffer_ms
    logger.debug(f"Returning server timestamp: {current_timestamp} (Local time: {int(time.time() * 1000)}, Offset: {ServerTimestampCache.offset}, Buffer: {ServerTimestampCache.buffer_ms} ms)")
    return current_timestamp
