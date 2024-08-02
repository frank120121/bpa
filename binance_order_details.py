import asyncio
import aiohttp
from urllib.parse import urlencode
from common_utils import get_server_timestamp, hashing
import os
from dotenv import load_dotenv
import logging
from binance_db import insert_or_update_order, DB_FILE
from common_utils_db import create_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from binance_endpoints import USER_ORDER_DETAIL

async def fetch_order_details(KEY, SECRET, order_no):
    attempt_count = 0
    max_attempts = 3
    backoff_time = 1  # seconds

    while attempt_count < max_attempts:
        try:
            timestamp = await get_server_timestamp()
            payload = {"adOrderNo": order_no, "timestamp": timestamp}
            query_string = urlencode(payload)
            signature = hashing(query_string, SECRET)
            headers = {
                "Content-Type": "application/json;charset=utf-8",
                "X-MBX-APIKEY": KEY,
                "clientType": "WEB",
            }
            query_string += f"&signature={signature}"
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{USER_ORDER_DETAIL}?{query_string}", json=payload, headers=headers) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.debug("Fetched order details: success")
                        return response_data
                    else:
                        logger.error(f"Request failed with status code {response.status}: {await response.text()}")
                        attempt_count += 1
                        await asyncio.sleep(backoff_time)
                        continue  # Proceed to the next attempt

        except Exception as e:
            logger.exception(f"An error occurred on attempt {attempt_count + 1}: {e}")
            attempt_count += 1
            if attempt_count < max_attempts:
                await asyncio.sleep(backoff_time)  # Wait before retrying
            else:
                logger.error("Maximum retry attempts reached. Aborting operation.")
                return None 

async def main():
    # Load environment variables
    load_dotenv()

    # Retrieve credentials
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

    # Fetch order details
    adOrderNo = "22652500280305926144"
    result = await fetch_order_details(KEY, SECRET, adOrderNo)
    print(result)

    if result:
        # Establish database connection
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

if __name__ == "__main__":
    asyncio.run(main())