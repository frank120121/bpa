import logging
import asyncio

from binance_api import BinanceAPI
from credentials import credentials_dict
from binance_share_data import SharedSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def open_orders(binance_api, account, KEY, SECRET):
    try:
        response = await binance_api.list_orders(KEY, SECRET)
        if response:
            logger.info(f"Open orders for {account}: {response}")
        else:
            logger.error(f"Failed to get open orders for {account}")
    except Exception as e:
        logger.exception(f"An exception occurred for {account}: {e}")

async def main_list_orders(binance_api):
    tasks = []
    for account, cred in credentials_dict.items():
        task = asyncio.create_task(open_orders(binance_api, account, cred['KEY'], cred['SECRET']))
        tasks.append(task)
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt received. Exiting.")
    except Exception as e:
        logger.exception("An unexpected error occurred:")

async def main():
    try:
        binance_api = await BinanceAPI.get_instance()
        await main_list_orders(binance_api)
    finally:
        await binance_api.close_session()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())
