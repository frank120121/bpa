#TESTbinance_getAdDetails.py
import asyncio
from credentials import credentials_dict
from binance_singleton_api import SingletonBinanceAPI
from binance_share_data import SharedSession
import logging
import sys

logger = logging.getLogger(__name__)





async def main():
    account='account_1'
    api_key = credentials_dict[account]['KEY']
    api_secret = credentials_dict[account]['SECRET']
    try:
        api_instance = await SingletonBinanceAPI.get_instance(account, api_key, api_secret)
        response = await api_instance.ads_list()
        print(response)
    finally:
        await SingletonBinanceAPI.close_all()
        await SharedSession.close_session()

if __name__ == "__main__":
    asyncio.run(main())