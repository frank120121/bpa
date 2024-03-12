import hashlib
import hmac
import time
import aiohttp
import asyncio
from binance_endpoints import TIME_ENDPOINT_V1, TIME_ENDPOINT_V3
import logging
logger = logging.getLogger(__name__)

def hashing(query_string, secret):
    return hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

class ServerTimestampCache:
    offset = None
    is_initialized = False
    is_maintenance_task_started = False
    sync_interval = 1800  # Sync every 30 minutes

    @classmethod
    async def fetch_server_time(cls):
        async with aiohttp.ClientSession() as session:
            endpoints = [TIME_ENDPOINT_V3, TIME_ENDPOINT_V1]
            for attempt in range(3):
                for endpoint in endpoints:
                    try:
                        async with session.get(endpoint) as response:
                            if response.status == 200:
                                data = await response.json()
                                server_time = data['serverTime']
                                cls.offset = server_time - int(time.time() * 1000)
                                cls.is_initialized = True
                                logger.info(f"Updated server timestamp: {server_time}")
                                return
                    except Exception as e:
                        logger.error(f"Attempt {attempt + 1}: Failed to fetch server time from {endpoint}: {e}")
                        await asyncio.sleep(1)  # Pause for 1 second before retrying

        cls.is_initialized = False
        logger.error("Failed to update server timestamp from all endpoints. Using local time instead.")
        cls.offset = 0  # Fallback to local system time

    @classmethod
    async def maintain_timestamp(cls):
        while True:
            await cls.fetch_server_time()
            await asyncio.sleep(cls.sync_interval)

    @classmethod
    async def ensure_initialized(cls):
        if not cls.is_initialized:
            await cls.fetch_server_time()

    @classmethod
    async def ensure_maintenance_task_started(cls):
        if not cls.is_maintenance_task_started:
            cls.is_maintenance_task_started = True
            asyncio.create_task(cls.maintain_timestamp())

async def get_server_timestamp():
    await ServerTimestampCache.ensure_initialized()
    await ServerTimestampCache.ensure_maintenance_task_started()
    if ServerTimestampCache.offset is None:
        logger.error("Server timestamp offset is not initialized. Using local time.")
        return int(time.time() * 1000)
    return int(time.time() * 1000) + ServerTimestampCache.offset