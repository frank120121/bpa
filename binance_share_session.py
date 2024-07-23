# shared_session.py
import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)

class SharedSession:
    _session = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_session(cls):
        async with cls._lock:
            if cls._session is None:
                cls._session = aiohttp.ClientSession()
                logger.info("Created new shared aiohttp session.")
            return cls._session

    @classmethod
    async def close_session(cls):
        async with cls._lock:
            if cls._session is not None:
                await cls._session.close()
                cls._session = None
                logger.info("Closed shared aiohttp session.")
