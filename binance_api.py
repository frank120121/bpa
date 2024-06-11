#binance_api.py
import hashlib
import hmac
from urllib.parse import urlencode
from common_utils import get_server_timestamp
import logging
import asyncio
from binance_search_ad import search_ads

logger = logging.getLogger(__name__)

class BinanceAPI:
    def __init__(self, KEY, SECRET, session, semaphore, min_delay, last_call, lock):
        self.KEY = KEY
        self.SECRET = SECRET
        self.session = session
        self.semaphore = semaphore
        self.min_delay = min_delay
        self.last_call = last_call
        self.lock = lock

    def hashing(self, query_string):
        return hmac.new(self.SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def api_call(self, method, endpoint, payload, max_retries=30, initial_retry_delay=0.1, max_retry_delay=1.5):
        async with self.semaphore:
            retry_delay = initial_retry_delay
            for retry_count in range(max_retries):
                async with self.lock:
                    current_time = asyncio.get_event_loop().time()
                    elapsed_time = current_time - self.last_call
                    if elapsed_time < self.min_delay:
                        await asyncio.sleep(self.min_delay - elapsed_time)
                    self.last_call = asyncio.get_event_loop().time()

                    try:
                        logger.info(f"API call to '{method} {endpoint}''")
                        payload["timestamp"] = await get_server_timestamp()
                        query_string = urlencode(payload)
                        signature = self.hashing(query_string)
                        headers = {
                            "Content-Type": "application/json;charset=utf-8",
                            "X-MBX-APIKEY": self.KEY,
                            "clientType": "WEB",
                        }
                        query_string += f"&signature={signature}"
                        async with self.session.post(f"{endpoint}?{query_string}", json=payload, headers=headers) as response:

                            if response.status != 200:
                                logger.info(f'response: {response}')
                            if response.status == 200:
                                #log a short and simple message
                                logger.debug(f"API call to '{method}' successful.")
                                return await response.json()
                            elif response.status == 429:  # Too many requests
                                logger.warning("Rate limit exceeded. Retrying after delay.")
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(retry_delay * 2, max_retry_delay)
                            elif response.status == 400:  # Timestamp error
                                logger.error(f"Timestamp error: {await response.text()}. Retrying after delay.")
                                await asyncio.sleep(retry_delay)
                                retry_delay = min(retry_delay * 2, max_retry_delay)
                            else:
                                logger.error(f"API call to '{method} {endpoint}' with payload '{str(payload)[:50]}...' failed with status code {response.status}: {await response.text()}")
                                return None
                    except Exception as e:
                        logger.error(f"API call to '{method} {endpoint}' with payload '{str(payload)[:50]}...' failed: {e}")

                    if retry_count < max_retries - 1:
                        logger.warning(f"Retrying API call in {retry_delay} seconds (attempt {retry_count + 2}/{max_retries})")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)
                    else:
                        logger.error("Max retries reached. Exiting.")
                        return None

    async def close_session(self):
        await self.session.close()

    async def get_ad_detail(self, advNo):
        logger.debug(f'calling get_ad_detail')
        return await self.api_call(
            'post',
            "https://api.binance.com/sapi/v1/c2c/ads/getDetailByNo",
            {
                "adsNo": advNo
            }
        )

    async def update_ad(self, advNo, priceFloatingRatio):
        if advNo in ['12590489123493851136', '12590488417885061120']:
            logger.debug(f"Ad: {advNo} is in the skip list")
            return
        logger.debug(f"Updating ad: {advNo} with rate: {priceFloatingRatio}")
        return await self.api_call(
            'post',
            "https://api.binance.com/sapi/v1/c2c/ads/update",
            {
                "advNo": advNo,
                "priceFloatingRatio": priceFloatingRatio
            }
        )

    async def fetch_ads_search(self, trade_type, asset_type, fiat, transAmount, payTypes=None):
        try:
            await asyncio.sleep(0.05)
            result = await search_ads(self.KEY, self.SECRET, trade_type, asset_type, fiat, transAmount, payTypes)
            await asyncio.sleep(0.05)
            if not result:
                logger.error("Failed to fetch ads data.")
            return result
        except Exception as e:
            logger.error(f"An error occurred: {e}")
