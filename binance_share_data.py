# binance_share_data.py
import aiohttp
import asyncio
import logging
from async_safe_dict import AsyncSafeDict
from ads_database import update_ad_in_database

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
            ad_details = await cls._ad_details_dict.get(advNo)
            if ad_details:
                logger.debug(f"Ad {advNo} retrieved from SharedData with ad_details: {ad_details}")
            else:
                logger.warning(f"Ad {advNo} not found in SharedData.")
            return ad_details

    @classmethod
    async def set_ad(cls, advNo, ad_details):
        logger.debug(f"Attempting to set ad {advNo} in SharedData.")
        try:
            async def set_ad_with_lock():
                async with cls._lock:
                    logger.debug(f"Acquired lock to set ad {advNo}.")
                    await cls._ad_details_dict.put(advNo, ad_details)
                    logger.debug(f"Ad {advNo} set in SharedData with ad_details: {ad_details}.")

            await asyncio.wait_for(set_ad_with_lock(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout while setting ad {advNo} in SharedData")
            return False  # Indicate failure
        except Exception as e:
            logger.error(f"Error setting ad {advNo} in SharedData: {e}")
            return False  # Indicate failure
        return True  # Indicate success

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
                logger.debug(f"Updated ad {advNo} with {kwargs}")
            else:
                logger.warning(f"Ad {advNo} not found in shared data.")
    @classmethod
    async def fetch_all_ads(cls, trade_type=None):
        try:
            async with cls._lock:
                ads = await cls._ad_details_dict.items()
                logger.debug(f"Total ads in SharedData: {len(ads)}")
                if trade_type:
                    filtered_ads = [
                        ad for advNo, ad in ads if ad.get('trade_type') == trade_type
                    ]
                    logger.debug(f"Filtered ads for trade_type {trade_type}: {len(filtered_ads)}")
                    return filtered_ads
                return [ad for advNo, ad in ads]
        except Exception as e:
            logger.error(f"Error fetching ads from SharedData: {e}")
            raise
    @classmethod
    async def save_all_ads_to_database(cls, trade_type=None):
        try:
            ads = await cls.fetch_all_ads(trade_type=trade_type)
            for ad in ads:
                await update_ad_in_database(
                    target_spot=ad.get('target_spot'),
                    advNo=ad.get('advNo'),
                    asset_type=ad.get('asset_type'),
                    floating_ratio=ad.get('floating_ratio'),
                    price=ad.get('price'),
                    surplusAmount=ad.get('surplused_amount'),
                    account=ad.get('account'),
                    fiat=ad.get('fiat'),
                    transAmount=ad.get('transAmount'),
                    minTransAmount=ad.get('minTransAmount')
                )
                logger.debug(f"Ad {ad.get('advNo')} saved to database.")
        except Exception as e:
            logger.error(f"Error saving ads to database: {e}")
            raise
            
class SharedSession:
    _session = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_session(cls):
        async with cls._lock:
            if cls._session is None:
                cls._session = aiohttp.ClientSession()
                logger.debug("Created new shared aiohttp session.")
            return cls._session

    @classmethod
    async def close_session(cls):
        async with cls._lock:
            if cls._session is not None:
                await cls._session.close()
                cls._session = None
                logger.debug("Closed shared aiohttp session.")
