import logging
import asyncio
from binance_singleton_api import SingletonBinanceAPI
from credentials import credentials_dict
from binance_share_data import SharedSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def open_orders(account, KEY, SECRET):
    api_instance = await SingletonBinanceAPI.get_instance(account, KEY, SECRET)
    try:
        response = await api_instance.list_orders()
        if response:
            logger.info(f"Open orders for {account}: {response}")
        else:
            logger.error(f"Failed to get open orders for {account}")
    except Exception as e:
        logger.exception(f"An exception occurred for {account}: {e}")

async def main_list_orders():
    tasks = []
    for account, cred in credentials_dict.items():
        task = asyncio.create_task(open_orders(account, cred['KEY'], cred['SECRET']))
        tasks.append(task)
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt received. Exiting.")
    except Exception as e:
        logger.exception("An unexpected error occurred:")

async def main():
    try:
        await main_list_orders()
    finally:
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())
