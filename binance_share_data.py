# binance_share_data.py
import aiohttp
import asyncio
import logging
from async_safe_dict import AsyncSafeDict

logger = logging.getLogger(__name__)

class SharedData:
    _ad_details_dict = AsyncSafeDict()
    _lock = asyncio.Lock()

    @classmethod
    async def get_ad_details_dict(cls):
        async with cls._lock:
            return cls._ad_details_dict

    @classmethod
    async def get_ad(cls, advNo):
        async with cls._lock:
            return await cls._ad_details_dict.get(advNo)

    @classmethod
    async def set_ad(cls, advNo, ad_details):
        logger.info(f"Attempting to set ad {advNo} in SharedData.")
        try:
            async def set_ad_with_lock():
                async with cls._lock:
                    logger.info(f"Acquired lock to set ad {advNo}.")
                    await cls._ad_details_dict.put(advNo, ad_details)
                    logger.info(f"Ad {advNo} set in SharedData.")

            await asyncio.wait_for(set_ad_with_lock(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout while setting ad {advNo} in SharedData")
            return False  # Indicate failure
        except Exception as e:
            logger.error(f"Error setting ad {advNo} in SharedData: {e}")
            return False  # Indicate failure
        return True  # Indicate success

    @classmethod
    async def get_all_ads(cls):
        async with cls._lock:
            return await cls._ad_details_dict.items()

    @classmethod
    async def len(cls):
        async with cls._lock:
            return await cls._ad_details_dict.len()

    @classmethod
    async def update_ad(cls, advNo, **kwargs):
        async with cls._lock:
            ad_details = await cls._ad_details_dict.get(advNo)
            if ad_details is not None:
                for key, value in kwargs.items():
                    if key in ad_details:
                        ad_details[key] = value
                await cls._ad_details_dict.put(advNo, ad_details)
                logger.info(f"Updated ad {advNo} with {kwargs}")
            else:
                logger.warning(f"Ad {advNo} not found in shared data.")
    @classmethod
    async def fetch_all_ads(cls, trade_type=None):
        try:
            async with cls._lock:
                ads = await cls._ad_details_dict.items()
                logger.info(f"Total ads in SharedData: {len(ads)}")
                if trade_type:
                    filtered_ads = [
                        ad for advNo, ad in ads if ad.get('trade_type') == trade_type
                    ]
                    logger.info(f"Filtered ads for trade_type {trade_type}: {len(filtered_ads)}")
                    return filtered_ads
                return [ad for advNo, ad in ads]
        except Exception as e:
            logger.error(f"Error fetching ads from SharedData: {e}")
            raise
            
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
