import json
import requests
import time
import hashlib
import hmac
from credentials import bitso_credentials, BITSO_BASE_URL
from asset_balances import update_balance
import logging
logger = logging.getLogger(__name__)
class BitsoWallets:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
    def generate_bitso_authorization(self, HTTP_METHOD, REQUEST_PATH, JSON_PAYLOAD=""):
        SECS = int(time.time())
        DNONCE = str(SECS * 1000)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            (DNONCE + HTTP_METHOD + REQUEST_PATH + JSON_PAYLOAD).encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        AUTH_HEADER = f"Bitso {self.api_key}:{DNONCE}:{signature}"
        return AUTH_HEADER
    def send_request(self, HTTP_METHOD, REQUEST_PATH, JSON_PAYLOAD=None):
        headers = {
            'Authorization': self.generate_bitso_authorization(HTTP_METHOD, REQUEST_PATH, json.dumps(JSON_PAYLOAD) if JSON_PAYLOAD else ""),
            'Content-Type': 'application/json'
        }
        full_url = BITSO_BASE_URL + REQUEST_PATH
        response = None
        if HTTP_METHOD == "GET":
            response = requests.get(full_url, headers=headers)
        elif HTTP_METHOD == "POST":
            response = requests.post(full_url, headers=headers, json=JSON_PAYLOAD)
        else:
            logger.warning(f"Unsupported HTTP method: {HTTP_METHOD}")
            return

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"An error occurred: {response.status_code} {response.reason}")
            return None
    def get_balances(self):
        balance_response = self.send_request("GET", "/v3/balance/")
        if balance_response:
            return balance_response['payload']['balances']
        else:
            logger.warning("Response is empty")
            return []
    async def save_balances_to_db(self, account):
        exchange_id = 2
        original_balances = self.get_balances()
        adjusted_balances = {}   
        for asset_balance in original_balances:
            asset = self._convert_currency_code(asset_balance['currency']).upper()
            balance = float(asset_balance['total'])

            if balance != 0.0:
                adjusted_balances[asset] = balance
        
        update_balance(exchange_id, account, adjusted_balances)
        return adjusted_balances
    def _convert_currency_code(self, currency_code):
        conversion_map = {
         "USD": "USDC"
        }
        return conversion_map.get(currency_code.upper(), currency_code)
async def main():
    for account_name, credentials in bitso_credentials.items():
        logger.info(f"Fetching balances for {account_name}")
        wallets = BitsoWallets(credentials['KEY'], credentials['SECRET'])
        await wallets.save_balances_to_db(account_name)
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())






