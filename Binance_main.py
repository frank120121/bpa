# binance_main.py
import asyncio
import logging
import traceback

from logging_config import setup_logging
from binance_c2c import main_binance_c2c
from binance_update_ads import update_ads_main
from populate_database import populate_ads_with_details
from common_utils_db import create_connection
from common_vars import DB_FILE
from binance_bank_deposit import initialize_account_cache
from binance_singleton_api import SingletonBinanceAPI
from binance_share_data import SharedData, SharedSession
from TESTBitsoOrderBook import start_bitso_order_book

setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
asyncio.get_event_loop().set_debug(True)

async def main():
    tasks = []
    try:
        tasks.append(asyncio.create_task(start_bitso_order_book()))
        
        await asyncio.sleep(5)

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
        await SharedData.save_all_ads_to_database()
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received exit signal, shutting down...")
        asyncio.run(SharedData.save_all_ads_to_database())
