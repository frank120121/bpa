# populate_database.py
import asyncio
from ads_database import fetch_all_ads_from_database, update_ad_in_database
from common_vars import ads_dict
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI
from binance_share_data import SharedData, SharedSession
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
        logger.debug("Fetching ads from database to update...")
        ads_info = await fetch_all_ads_from_database()
        logger.debug(f"Fetched {len(ads_info)} ads from database.")

        # Update ads details and save to database
        tasks = []
        for ad_info in ads_info:
            account = ad_info['account']
            KEY = credentials_dict[account]['KEY']
            SECRET = credentials_dict[account]['SECRET']
            tasks.append(update_ad_detail(account, KEY, SECRET, ad_info))
        
        await asyncio.gather(*tasks)
        logger.debug("Ads details updated in the database.")
        
        # Fetch updated ads from database and populate SharedData
        logger.debug("Fetching updated ads from database...")
        updated_ads_info = await fetch_all_ads_from_database()
        logger.debug(f"Fetched {len(updated_ads_info)} updated ads from database.")

        # Populate SharedData with updated ads
        logger.debug("Populating SharedData with updated ads...")
        await populate_shared_data(updated_ads_info)
        ads_count = await SharedData.len()
        logger.debug(f"SharedData populated with {ads_count} ads.")
    finally:
        logger.debug("All ads processed successfully.")

async def update_ad_detail(account, KEY, SECRET, ad_info):
    logger.debug(f"Updating ad details for account: {account}, advNo: {ad_info['advNo']}")
    try:
        api_instance = await SingletonBinanceAPI.get_instance(account, KEY, SECRET)
        advNo = ad_info['advNo']
        ad_details_response = await api_instance.get_ad_detail(advNo)
        logger.debug(f"Ad details fetched for advNo {advNo}: {ad_details_response}")

        if ad_details_response and ad_details_response.get('data'):
            ad_details = ad_details_response['data']
            # Update ad details using the mappings and save to database
            ad_info['target_spot'] = advNo_to_target_spot.get(advNo)
            ad_info['fiat'] = advNo_to_fiat.get(advNo)
            ad_info['transAmount'] = advNo_to_transAmount.get(advNo)
            ad_info['minTransAmount'] = advNo_to_minTransAmount.get(advNo)

            # Ensure all necessary keys are present
            if 'priceFloatingRatio' in ad_details and 'price' in ad_details:
                logger.debug(f"Updating ad details for advNo {advNo} in the database with: {ad_details}")
                await update_ad_in_database(
                    target_spot=ad_info['target_spot'],
                    advNo=advNo,
                    asset_type=ad_info['asset_type'],
                    floating_ratio=ad_details['priceFloatingRatio'],
                    price=ad_details['price'],
                    surplusAmount=ad_details.get('surplused_amount'),
                    account=ad_info['account'],
                    fiat=ad_info['fiat'],
                    transAmount=ad_info['transAmount'],
                    minTransAmount=ad_info['minTransAmount']
                )
            else:
                logger.error(f"Missing required fields in ad details for advNo {advNo}: {ad_details}")
        else:
            logger.error(f"Failed to fetch ad details or missing 'data' for advNo {advNo}.")
    except Exception as e:
        logger.error(f"Error updating ad details for advNo {ad_info['advNo']}: {e}")


async def populate_shared_data(ads_info):
    logger.debug(f"Starting to populate SharedData with {ads_info} ads.")
    successful_additions = 0
    try:
        for ad_info in ads_info:
            advNo = ad_info['advNo']
            success = await SharedData.set_ad(advNo, ad_info)
            if success:
                successful_additions += 1
                logger.debug(f"Ad {advNo} successfully set in SharedData.")
            else:
                logger.warning(f"Failed to set ad {advNo} in SharedData.")
        
        ads_count = await SharedData.len()
        logger.debug(f"SharedData populated with {ads_count} ads. Successful additions: {successful_additions}")
    except Exception as e:
        logger.error(f"Error in populate_shared_data: {e}")
    finally:
        logger.debug("Exiting populate_shared_data.")
async def main():
    try:
        await populate_ads_with_details()
    finally:
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())
