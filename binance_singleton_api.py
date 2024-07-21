# binance_singleton_api.py
import aiohttp
import asyncio
from binance_api import BinanceAPI
import logging

logger = logging.getLogger(__name__)

class SingletonBinanceAPI:
    _instances = {}
    _lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls, account, api_key, api_secret, client_type='WEB'):
        async with cls._lock:
            if account not in cls._instances:
                cls._instances[account] = BinanceAPI(api_key, api_secret, client_type)
                logger.info(f"Created new BinanceAPI instance for account: {account}. Number of instances: {len(cls._instances)}")
            else:
                logger.debug(f"Using existing BinanceAPI instance for account: {account}. Number of instances: {len(cls._instances)}")
            return cls._instances[account]

    @classmethod
    async def close_all(cls):
        async with cls._lock:
            for instance in cls._instances.values():
                await instance.close_session()
            cls._instances = {}
            logger.info("Closed all BinanceAPI instances.")

# Ensure proper session handling by having a shared session
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
