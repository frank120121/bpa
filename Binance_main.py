# binance_main.py
import asyncio
import logging
import traceback

from logging_config import setup_logging
from binance_c2c import main_binance_c2c
from binance_update_ads import update_ads_main
from populate_database import populate_ads_with_details
from common_utils_db import create_connection
from ads_database import update_ad_in_database
from common_vars import DB_FILE
from binance_bank_deposit import initialize_account_cache
from binance_singleton_api import SingletonBinanceAPI
from binance_share_data import SharedData, SharedSession
from TESTbitso_price_listener import fetch_prices

setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
asyncio.get_event_loop().set_debug(True)

# Function to save ad_details_dict to the database
async def save_data_to_db():
    logger.info("Saving ad details to the database...")
    ads = await SharedData.get_all_ads()
    for advNo, details in ads:
        await update_ad_in_database(
            target_spot=details['target_spot'],
            advNo=advNo,
            asset_type=details['asset_type'],
            floating_ratio=details['floating_ratio'],
            price=details['price'],
            surplusAmount=details['surplusAmount'],
            account=details['account'],
            fiat=details['fiat'],
            transAmount=details['transAmount'],
            minTransAmount=details['minTransAmount']
        )
    logger.info("All ad details saved to the database.")

async def main():
    tasks = []
    try:
        # Start fetch_prices before other tasks
        tasks.append(asyncio.create_task(fetch_prices()))
        await asyncio.sleep(5)  # Ensure fetch_prices has time to update shared_data

        # Start other main tasks
        tasks.append(asyncio.create_task(main_binance_c2c()))
        tasks.append(asyncio.create_task(update_ads_main()))
        await asyncio.gather(*tasks)
    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"An error occurred: {tb_str}")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

async def run():
    conn = None
    try:
        conn = await create_connection(DB_FILE)
        await initialize_account_cache(conn)
        await populate_ads_with_details()
        await main()
    except Exception as e:
        logger.error(f"An error occurred during initialization: {e}")
    finally:
        if conn:
            await conn.close()
        await save_data_to_db()
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received exit signal, shutting down...")
        asyncio.run(save_data_to_db())  # Ensure data is saved if interrupted
