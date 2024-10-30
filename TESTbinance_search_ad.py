import asyncio
import logging
import sys

from credentials import credentials_dict
from binance_api import BinanceAPI

logger = logging.getLogger(__name__)

if __name__ == "__main__":

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
            binance_api = await BinanceAPI.get_instance()
            search_params = [
                {'asset_type': 'USDT', 'fiat': 'USD', 'transAmount': '100', 'payTypes': ['SkrillMoneybookers'], 'page': 1},
            ]

            tasks = [binance_api.fetch_ads_search(KEY, SECRET, 'BUY', param['asset_type'], param['fiat'], param['transAmount'], param['payTypes'], param['page']) for param in search_params]
            
            results = await asyncio.gather(*tasks)

            for param, ads in zip(search_params, results):
                print(f"{param['asset_type']} Ads with fiat {param['fiat']} and transAmount {param['transAmount']}:\n", ads)
                print()
            await binance_api.close_session()

        asyncio.run(main())
