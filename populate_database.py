# populate_database.py
import asyncio
import aiohttp
from ads_database import fetch_all_ads_from_database, update_ad_in_database
from common_vars import ads_dict
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI
import logging
import sys

logger = logging.getLogger(__name__)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

advNo_to_target_spot = {ad['advNo']: ad['target_spot'] for _, ads in ads_dict.items() for ad in ads}
advNo_to_fiat = {ad['advNo']: ad['fiat'] for _, ads in ads_dict.items() for ad in ads}
advNo_to_transAmount = {ad['advNo']: ad['transAmount'] for _, ads in ads_dict.items() for ad in ads}
advNo_to_minTransAmount = {ad['advNo']: ad['minTransAmount'] for _, ads in ads_dict.items() for ad in ads}

async def populate_ads_with_details():
    async with aiohttp.ClientSession() as session:
        try:
            ads_info = await fetch_all_ads_from_database()
            logger.debug(f"Fetched ads from database: {ads_info}")

            tasks = []
            for i, ad_info in enumerate(ads_info):
                account = ad_info['account']
                KEY = credentials_dict[account]['KEY']
                SECRET = credentials_dict[account]['SECRET']
                api_instance = await SingletonBinanceAPI.get_instance(account, KEY, SECRET)
                tasks.append(asyncio.create_task(delayed_process(i * 2, ad_info, api_instance)))
            await asyncio.gather(*tasks)
        finally:
            await SingletonBinanceAPI.close_all()
        logger.info("All ads processed successfully.")

async def delayed_process(delay, ad_info, api_instance):
    """Wait for the specified delay and then process the ad."""
    await process_ad(ad_info, api_instance)

async def process_ad(ad_info, api_instance):
    advNo = ad_info['advNo']
    ad_details = await api_instance.get_ad_detail(advNo)
    logger.debug(f"Ad details fetched from BinanceAPI for advNo {advNo}: {ad_details}")

    if ad_details and advNo in advNo_to_target_spot:
        # Update target_spot, fiat, and transAmount using the mappings
        ad_details['target_spot'] = advNo_to_target_spot[advNo]
        fiat = advNo_to_fiat.get(advNo)
        transAmount = advNo_to_transAmount.get(advNo)
        minTransAmount = advNo_to_minTransAmount.get(advNo)

        logger.debug(f"Updated target_spot for advNo {advNo} to {advNo_to_target_spot[advNo]}")
        await update_ad_in_database(
            target_spot=ad_details['target_spot'],
            advNo=advNo,
            asset_type=ad_details['data']['asset'],
            floating_ratio=ad_details['data']['priceFloatingRatio'],
            price=ad_details['data']['price'],
            surplusAmount=ad_details['data']['surplusAmount'],
            account=ad_info['account'],
            fiat=fiat,
            transAmount=transAmount,
            minTransAmount=minTransAmount
        )

if __name__ == "__main__":
    asyncio.run(populate_ads_with_details())
