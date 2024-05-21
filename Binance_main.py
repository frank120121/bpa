import asyncio
import logging
from logging_config import setup_logging
import traceback

from binance_c2c import main_binance_c2c
from binance_update_ads import run_ads_update
from populate_database import populate_ads_with_details
from common_utils_db import create_connection
from common_vars import DB_FILE
from binance_bank_deposit import initialize_account_cache

setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
asyncio.get_event_loop().set_debug(True)

async def main():
    tasks = []
    try:
        tasks.append(asyncio.create_task(main_binance_c2c()))
        tasks.append(asyncio.create_task(run_ads_update()))
        await asyncio.gather(*tasks)
    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"An error occurred: {tb_str}")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.error("KeyboardInterrupt received. Exiting...")
