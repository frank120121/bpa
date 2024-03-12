import aiohttp
import asyncio
from credentials import credentials_dict
from common_utils import get_server_timestamp, hashing
import logging
from binance_endpoints import SEARCH_ADS
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Initialize a cache dictionary
cache = {}

async def search_ads(KEY, SECRET, asset_type, fiat, transAmount, payTypes=None):
    global cache

    # Create a cache key based on the function's arguments
    cache_key = (asset_type, fiat, transAmount, tuple(sorted(payTypes)) if payTypes else None)

    # Check if these parameters are in the cache and if the cached result is less than 10 seconds old
    if cache_key in cache:
        cached_result, timestamp = cache[cache_key]
        if datetime.now() - timestamp < timedelta(seconds=30):
            logger.debug("Returning cached result")
            return cached_result

    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        attempts += 1
        try:
            timestamp = str(await get_server_timestamp())

            payload = {
                "asset": asset_type,
                "fiat": fiat,
                "page": 1,
                "publisherType": "merchant",
                "rows": 20,
                "tradeType": "BUY"
            }
            
            if payTypes:
                payload["payTypes"] = payTypes

            query_string = f"timestamp={timestamp}"
            signature = hashing(query_string, SECRET)

            full_url = f"{SEARCH_ADS}?{query_string}&signature={signature}"

            headers = {
                "Content-Type": "application/json;charset=utf-8",
                "X-MBX-APIKEY": KEY,
                "clientType": "WEB",
            }
            
            async with aiohttp.ClientSession() as session:
                logger.debug(f'Calling fetch_ads_search for {asset_type} {fiat} {transAmount} {payTypes}')
                async with session.post(full_url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.debug("Fetched ads search: success")

                        # Cache the result along with the current timestamp
                        cache[cache_key] = (response_data, datetime.now())
                        
                        return response_data
                    else:
                        logger.error(f"Request failed with status code {response.status}: {await response.text()}")
                        return None

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            if attempts >= max_attempts:
                logger.error("Maximum retry attempts reached, failing...")
                return None
            else:
                logger.info(f"Attempt {attempts}/{max_attempts} failed, retrying after delay...")
                await asyncio.sleep(2 ** attempts)  # Exponential backoff

    logger.error("Failed to complete request after maximum attempts.")
    return None
            

if __name__ == "__main__":
    import sys
    import asyncio

    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def get_credentials():
        account = 'account_1' 
        if account in credentials_dict:
            return credentials_dict[account]['KEY'], credentials_dict[account]['SECRET']
        else:
            logger.error("Account not found in credentials.")
            return None, None
    KEY, SECRET = get_credentials()

    if KEY and SECRET:
        async def main():
            # Define your asset types, fiat, and transAmount combinations
            search_params = [
                {'asset_type': 'USDT', 'fiat': 'USD', 'transAmount': 100, 'payTypes': None},
                # Add more combinations if necessary
            ]

            # Create tasks for each combination
            tasks = [search_ads(KEY, SECRET, param['asset_type'], param['fiat'], param['transAmount'], param['payTypes']) for param in search_params]
            
            results = await asyncio.gather(*tasks)

            # Print results
            for param, ads in zip(search_params, results):
                print(f"{param['asset_type']} Ads with fiat {param['fiat']} and transAmount {param['transAmount']}:\n", ads)
                print()

        asyncio.run(main())
