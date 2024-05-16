# binance_singleton_api.py
import aiohttp
import asyncio
from binance_api import BinanceAPI

class SingletonBinanceAPI:
    _instances = {}
    _lock = asyncio.Lock()
    last_call = 0  
    min_delay = 0.2  

    @classmethod
    async def get_instance(cls, account, KEY, SECRET):
        async with cls._lock:
            if account not in cls._instances:
                session = aiohttp.ClientSession()
                semaphore = asyncio.Semaphore(1)  
                cls._instances[account] = BinanceAPI(KEY, SECRET, session, semaphore, cls.min_delay, cls.last_call, cls._lock)
            return cls._instances[account]

    @classmethod
    async def close_all(cls):
        async with cls._lock:
            for instance in cls._instances.values():
                await instance.close_session()
            cls._instances = {}

