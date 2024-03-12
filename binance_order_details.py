import asyncio
import aiohttp
from urllib.parse import urlencode
from common_utils import get_server_timestamp, hashing
import os
from dotenv import load_dotenv
import logging
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



if __name__ == "__main__":
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
        exit()
    adOrderNo = "20592632273051729920"
    result = asyncio.run(fetch_order_details(KEY, SECRET, adOrderNo))
    print(result)
    account_number = None
    for method in result['data']['payMethods']:
        for field in method['fields']:
            if field['fieldName'] == 'Account number':
                account_number = field['fieldValue']
                break

    if account_number:
        print(account_number)
    else:
        print("Account number not found.")
