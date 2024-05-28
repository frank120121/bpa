from credentials import credentials_dict
from common_utils import get_server_timestamp
from asset_balances import update_balance, get_balance
import hmac
import hashlib
import aiohttp
import asyncio
import platform


import logging
from logging_config import setup_logging
setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)

class BinanceWallets:
    def __init__(self):
        self.combined_balances = {}
        self.free_usd_assets_per_account = {}
        self.detailed_free_usd = {}
        self.credentials_dict = credentials_dict


    def generate_signature(self, api_secret, payload):
        signature = hmac.new(api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    async def get_user_assets(self, api_key, api_secret, account):
        try:
            timestamp = await get_server_timestamp()
            query_string = f"timestamp={timestamp}"

            signature = self.generate_signature(api_secret, query_string)
            url = f"https://api.binance.com/sapi/v3/asset/getUserAsset?{query_string}&signature={signature}"

            headers = {"X-MBX-APIKEY": api_key}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as response:
                    if response.status == 200:
                        assets_data = await response.json()
                        self.update_balances(assets_data, account, is_funding=False)
        except Exception as e:
            logger.error(f"An exception occurred in get_user_assets: {e}")

    async def get_funding_assets(self, api_key, api_secret, account):
        try:
            timestamp = await get_server_timestamp()
            query_string = f"timestamp={timestamp}"

            signature = self.generate_signature(api_secret, query_string)
            url = f"https://api.binance.com/sapi/v1/asset/get-funding-asset?{query_string}&signature={signature}"

            headers = {"X-MBX-APIKEY": api_key}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as response:
                    if response.status == 200:
                        funding_data = await response.json()
                        self.update_balances(funding_data, account, is_funding=True)
        except Exception as e:
            logger.error(f"An exception occurred in get_funding_assets: {e}")

    def update_balances(self, asset_data, account, is_funding):
        for data in asset_data:
            asset = data['asset']
            if asset not in self.combined_balances:
                self.combined_balances[asset] = 0

            for balance_type in ['free', 'locked', 'freeze']:
                if balance_type in data:
                    self.combined_balances[asset] += float(data[balance_type])

            if not is_funding:
                if asset == 'USDC' or asset == 'USDT':
                    if account not in self.detailed_free_usd:
                        self.detailed_free_usd[account] = {}
                    self.detailed_free_usd[account][asset] = float(data.get('free', 0))

        if not is_funding:
            usd_assets = ['USDC', 'USDT']
            free_usd = sum(float(data.get('free', 0)) for data in asset_data if data['asset'] in usd_assets)
            self.free_usd_assets_per_account[account] = free_usd

            self.detailed_free_usd[account] = {
                'USDC': sum(float(data.get('free', 0)) for data in asset_data if data['asset'] == 'USDC'),
                'USDT': sum(float(data.get('free', 0)) for data in asset_data if data['asset'] == 'USDT')
            }
        
    def check_asset_balance(self, asset):
        if asset == 'BTC':
            target = 0.25
        elif asset == 'ETH':
            target = 1.000
        balance = self.combined_balances.get(asset, 0)
        return target - balance

    def get_account_with_most_usd(self):
        max_free_usd = 0
        max_account = None
        most_usd_asset = None
        for account, assets in self.detailed_free_usd.items():
            total_usd = sum(assets.values())
            if total_usd > max_free_usd:
                max_free_usd = total_usd
                max_account = account
                most_usd_asset = max(assets, key=assets.get)
        return max_account, most_usd_asset
    async def place_order(self, api_key, api_secret, symbol, side, order_type, quantity=None, price=None, timeInForce=None, quoteOrderQty=None):
        try:
            timestamp = await get_server_timestamp()
            query_string = f"symbol={symbol}&side={side}&type={order_type}&timestamp={timestamp}"
            if quantity:
                query_string += f"&quantity={quantity}"
            if price:
                query_string += f"&price={price}"
            if timeInForce:
                query_string += f"&timeInForce={timeInForce}"
            signature = self.generate_signature(api_secret, query_string)
            url = f"https://api.binance.com/api/v3/order?{query_string}&signature={signature}"
            headers = {"X-MBX-APIKEY": api_key}
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as response:
                    if response.status == 200:
                        order_data = await response.json()
                        logger.debug(f"Order successfully placed: {order_data}")
                    else:
                        logger.error(f"Failed to place order: {await response.text()}")
        except Exception as e:
            logger.warning(f"An exception occurred in place_order: {e}")
    async def save_balances_to_db(self, account):
        exchange_id = 1
        logger.debug("Calling update_balance from binance_wallets.py")
        try:
            update_balance(exchange_id, account, self.combined_balances)
        except Exception as e:
            logger.error(f"Error in save_balances_to_db: {e}")
    def validate_balances(self, account):
        exchange_id = 1
        balance = get_balance(exchange_id, account)
        logger.debug(f"Balance for account {account} in exchange {exchange_id}: {balance}")
    async def main(self):
        for account, cred in credentials_dict.items():
            self.combined_balances = {}
            await self.get_user_assets(cred['KEY'], cred['SECRET'], account)
            await self.get_funding_assets(cred['KEY'], cred['SECRET'], account)
            await self.save_balances_to_db(account)
            self.validate_balances(account)
    async def balances(self):
        for account, cred in credentials_dict.items():
            await self.get_user_assets(cred['KEY'], cred['SECRET'], account)
            await self.get_funding_assets(cred['KEY'], cred['SECRET'], account)
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if __name__ == "__main__":
    wallets = BinanceWallets()
    asyncio.run(wallets.main())

