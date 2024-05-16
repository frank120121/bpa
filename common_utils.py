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
    sync_interval = 900  # Sync every 15 minutes
    lock = asyncio.Lock()  # Add a lock to prevent concurrent fetches
    buffer_ms = 1000  # Initial buffer of 1000ms
    max_buffer_ms = 3000  # Maximum buffer limit

    @classmethod
    async def fetch_server_time(cls):
        async with cls.lock:  # Ensure only one fetch is happening at a time
            if cls.is_initialized:
                return  # If already initialized, no need to fetch again
            async with aiohttp.ClientSession() as session:
                endpoints = [TIME_ENDPOINT_V3, TIME_ENDPOINT_V1]
                for attempt in range(3):
                    for endpoint in endpoints:
                        try:
                            start_time = int(time.time() * 1000)
                            async with session.get(endpoint) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    server_time = data['serverTime']
                                    end_time = int(time.time() * 1000)
                                    round_trip_time = end_time - start_time
                                    current_time = int(time.time() * 1000)
                                    cls.offset = server_time - current_time
                                    cls.buffer_ms = min(cls.max_buffer_ms, round_trip_time + 500)
                                    cls.is_initialized = True
                                    logger.debug(f"Updated server timestamp: {server_time} (Offset: {cls.offset}, Buffer: {cls.buffer_ms} ms)")
                                    return
                        except Exception as e:
                            logger.error(f"Attempt {attempt + 1}: Failed to fetch server time from {endpoint}: {e}")
                            await asyncio.sleep(0.2)  # Pause for 1 second before retrying

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
        await cls.fetch_server_time()

    @classmethod
    async def ensure_maintenance_task_started(cls):
        if not cls.is_maintenance_task_started:
            cls.is_maintenance_task_started = True
            asyncio.create_task(cls.maintain_timestamp())

async def get_server_timestamp(increment_buffer=False):
    await ServerTimestampCache.ensure_initialized()
    await ServerTimestampCache.ensure_maintenance_task_started()
    if ServerTimestampCache.offset is None:
        logger.error("Server timestamp offset is not initialized. Using local time.")
        return int(time.time() * 1000)
    
    if increment_buffer:
        ServerTimestampCache.buffer_ms = min(ServerTimestampCache.max_buffer_ms, ServerTimestampCache.buffer_ms + 200)  # Increment buffer by 200ms if required
        logger.debug(f"Incrementing buffer to: {ServerTimestampCache.buffer_ms} ms")

    current_timestamp = int(time.time() * 1000) + ServerTimestampCache.offset + ServerTimestampCache.buffer_ms
    logger.debug(f"Returning server timestamp: {current_timestamp} (Local time: {int(time.time() * 1000)}, Offset: {ServerTimestampCache.offset}, Buffer: {ServerTimestampCache.buffer_ms} ms)")
    return current_timestamp

async def make_api_call(api_function, *args, **kwargs):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        timestamp = await get_server_timestamp(increment_buffer=(attempt > 0))
        try:
            # Pass the timestamp to the API function and make the call
            response = await api_function(timestamp, *args, **kwargs)
            return response
        except Exception as e:
            if 'recvWindow' in str(e):
                # Increment buffer and try again if recvWindow error occurs
                logger.warning(f"recvWindow error detected. Attempt {attempt + 1} of {retry_attempts}. Retrying...")
                await asyncio.sleep(1)  # Exponential backoff
            else:
                logger.error(f"API call failed: {e}")
                break

# Example usage:
# async def example_api_function(timestamp, *args, **kwargs):
#     # Simulated API call using the timestamp
#     pass

# asyncio.run(make_api_call(example_api_function))
