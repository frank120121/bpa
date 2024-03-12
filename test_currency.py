import aiohttp
import hashlib
import hmac
import asyncio
from credentials import credentials_dict
from common_utils import get_server_timestamp
import logging

logger = logging.getLogger(__name__)

url = "https://api.binance.com/sapi/v1/c2c/ads/search"

def get_credentials():
    account = 'account_1'
    if account in credentials_dict:
        return credentials_dict[account]['KEY'], credentials_dict[account]['SECRET']
    else:
        logger.error("Account not found in credentials.")
        return None, None

def hashing(query_string, secret):
    return hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

async def search_ads(session, KEY, SECRET, asset_type, fiat, trade_type, global_unique_users, fiat_unique_users):
    page = 1
    while True:
        timestamp = await get_server_timestamp()

        payload = {
            "asset": asset_type,
            "fiat": fiat,
            "page": page,
            "rows": 20,
            "tradeType": trade_type,  # Use the trade_type variable here
        }

        query_string = f"timestamp={timestamp}"
        signature = hashing(query_string, SECRET)

        full_url = f"{url}?{query_string}&signature={signature}"

        headers = {
            "Content-Type": "application/json;charset=utf-8",
            "X-MBX-APIKEY": KEY,
            "clientType": "WEB",
        }

        async with session.post(full_url, json=payload, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                ads = response_data.get('data', [])
                for ad in ads:
                    user_no = ad.get('advertiser', {}).get('userNo')
                    if user_no:
                        global_unique_users.add(user_no)  # Add unique userNo to the global set
                        fiat_unique_users.add(user_no)  # Add unique userNo to the current fiat set

                if len(ads) == 0:
                    break  # Exit loop if no ads found
            else:
                logger.error(f"Request failed with status code {response.status}: {await response.text()}")
                break  # Exit loop on error
        page += 1  # Increment page number for next iteration

async def main():
    KEY, SECRET = get_credentials()
    if not (KEY and SECRET):
        return
    trade_types = ['BUY', 'SELL']
    fiat_currencies = ['AFN', 'ALL', 'AOA', 'AZN', 'BAM', 'BIF', 'BND', 'BSD', 'BWP', 'BYN', 'BZD', 'CDF', 'CNY', 'CRC', 'CVE', 'DJF', 'DKK', 'ERN', 'ETB', 'GMD', 'GNF', 'GTQ', 'HNL', 'HTG', 'HUF', 'IQD', 'ISK', 'JMD', 'JOD', 'KGS', 'KMF', 'KWD', 'KYD', 'LRD', 'LYD', 'MDL', 'MGA', 'MOP', 'MRU', 'MWK', 'MZN', 'NAD', 'NIO', 'NOK', 'PGK', 'RSD', 'SCR', 'SLL', 'SOS', 'TJS', 'TMT', 'TTD', 'YER', 'ZMW', 'ZWD', 'VND', 'NGN', 'UAH', 'EUR', 'COP', 'BRL', 'ARS', 'PEN', 'ZAR', 'MXN', 'HKD', 'GBP', 'KES', 'AUD', 'CAD', 'VES', 'INR', 'IDR', 'KZT', 'USD', 'JPY', 'THB', 'PHP', 'TWD', 'SAR', 'BDT', 'EGP', 'AED', 'BGN', 'MAD', 'PLN', 'PKR', 'RON', 'CHF', 'CZK', 'SEK', 'TRY', 'UGX', 'GHS', 'LBP', 'AMD', 'GEL', 'UYU', 'CLP', 'XAF', 'DZD', 'PYG', 'BOB', 'LKR', 'PAB', 'NZD', 'KHR', 'LAK', 'MMK', 'DOP', 'QAR', 'BHD', 'OMR', 'TND', 'SDG', 'MNT', 'UZS', 'NPR', 'TZS', 'XOF', 'RWF']
    asset_types = ['ARS', 'BRL', 'C98', 'DOT', 'SOL', 'TRX', 'WRX', 'XRP', 'DASH', 'SHIB', 'TUSD', 'USDC', 'USDT', 'FDUSD', 'MATIC', 'BTC', 'BNB', 'ETH', 'SLP', 'DAI', 'EOS', 'RUB', 'BIDR', 'UAH', 'DOGE', 'NGN', 'ADA']  # Add more asset types as needed


    global_unique_users = set()  # Set to track unique userNos across all assets, fiats, and trade types
    fiat_user_counts = {}  # Dictionary to track unique userNos per fiat

    async with aiohttp.ClientSession() as session:
        for trade_type in trade_types:
            for fiat in fiat_currencies:
                print(f"Working on fiat currency: {fiat} for trade type: {trade_type}")
                fiat_unique_users = set()  # Reset for each fiat and trade type
                tasks = [search_ads(session, KEY, SECRET, asset, fiat, trade_type, global_unique_users, fiat_unique_users) for asset in asset_types]
                await asyncio.gather(*tasks)
                fiat_user_counts[fiat] = len(fiat_unique_users)  # Update the count of unique users for the current fiat
                await asyncio.sleep(5)  # Wait for 2 seconds before proceeding to the next fiat currency

    # Sort and print the fiats and their unique user counts
    sorted_fiats_by_users = sorted(fiat_user_counts.items(), key=lambda item: item[1], reverse=True)
    print(f"Total number of unique users across all asset types, fiats, and trade types: {len(global_unique_users)}")
    print("Fiats sorted by the number of unique users (descending):")
    for fiat, count in sorted_fiats_by_users:
        print(f"{fiat}: {count} unique users")

if __name__ == "__main__":
    asyncio.run(main())