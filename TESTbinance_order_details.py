import asyncio
import os
from dotenv import load_dotenv
import logging


from binance_db import insert_or_update_order, DB_FILE
from common_utils_db import create_connection
from binance_api import BinanceAPI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    load_dotenv()

    credentials_dict = {
        'account_1': {
            'KEY': os.environ.get('API_KEY_MFMP'),
            'SECRET': os.environ.get('API_SECRET_MFMP')
        }
    }

    account = 'account_1'
    if account in credentials_dict:
        KEY = credentials_dict[account]['KEY']
        SECRET = credentials_dict[account]['SECRET']
    else:
        logger.error(f"Credentials not found for account: {account}")
        return

    adOrderNo = "22652500280305926144"
    binance_api = await BinanceAPI.get_instance()
    result = await binance_api.fetch_order_details(KEY, SECRET, adOrderNo)
    print(result)

    if result:
        conn = await create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
        if conn:
            try:
                await insert_or_update_order(conn, result)
            except Exception as e:
                logger.error(f"Failed to insert or update order: {e}")
            finally:
                await conn.close()
        else:
            logger.error("Failed to connect to the database.")
    
    account_number = None
    if result and 'data' in result and 'payMethods' in result['data']:
        for method in result['data']['payMethods']:
            if 'fields' in method:
                for field in method['fields']:
                    if field['fieldName'] == 'Account number':
                        account_number = field['fieldValue']
                        break
                if account_number:
                    break

    if account_number:
        print(f"Account number: {account_number}")
    else:
        print("Account number not found.")
    await binance_api.close_session()

if __name__ == "__main__":
    asyncio.run(main())