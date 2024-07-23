# populate_database.py
import asyncio
from ads_database import fetch_all_ads_from_database, update_ad_in_database
from common_vars import ads_dict
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI
from binance_share_session import SharedSession
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
    try:
        logger.debug("Fetching ads from database...")
        ads_info = await fetch_all_ads_from_database()
        logger.debug(f"Fetched ads from database: {ads_info}")

        tasks = []
        for ad_info in ads_info:
            account = ad_info['account']
            KEY = credentials_dict[account]['KEY']
            SECRET = credentials_dict[account]['SECRET']
            tasks.append(process_ad(account, KEY, SECRET, ad_info))
        
        await asyncio.gather(*tasks)
    finally:
        logger.info("All ads processed successfully.")

async def process_ad(account, KEY, SECRET, ad_info):
    api_instance = await SingletonBinanceAPI.get_instance(account, KEY, SECRET)
    advNo = ad_info['advNo']
    ad_details = await api_instance.get_ad_detail(advNo)
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

async def main():
    try:
        await populate_ads_with_details()
    finally:
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())
