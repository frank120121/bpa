import asyncio
import logging
from logging_config import setup_logging
import traceback

from binance_c2c import main_binance_c2c
from binance_update_ads import start_update_ads
from populate_database import populate_ads_with_details

setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
asyncio.get_event_loop().set_debug(True)

async def main():
    tasks = []
    try:
        tasks.append(asyncio.create_task(main_binance_c2c()))
        tasks.append(asyncio.create_task(start_update_ads()))
        await asyncio.gather(*tasks)
    except Exception as e:
        tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
        logger.error(f"An error occurred: {''.join(tb_str)}")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

async def run():
    await populate_ads_with_details()
    await main()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.error("KeyboardInterrupt received. Exiting...")