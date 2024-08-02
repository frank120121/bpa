import asyncio
import logging

logger = logging.getLogger(__name__)

class AsyncSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            logger.debug(f"Getting key {key} from AsyncSafeDict.")
            return self._dict.get(key)

    async def put(self, key, value):
        logger.debug(f"Attempting to put key {key} in AsyncSafeDict.")
        try:
            logger.debug(f"Acquiring lock for key {key}.")
            async with self._lock:
                logger.debug(f"Lock acquired for key {key}. Setting value.")
                self._dict[key] = value
                logger.debug(f"Key {key} set in AsyncSafeDict. Total items: {len(self._dict)}")
        except Exception as e:
            logger.error(f"Error putting key {key} in AsyncSafeDict: {e}")
            raise
        finally:
            logger.debug(f"Lock released for key {key} after setting value.")

    async def items(self):
        async with self._lock:
            return list(self._dict.items())

    async def len(self):
        async with self._lock:
            return len(self._dict)


    async def copy(self):
        async with self._lock:
            return self._dict.copy()