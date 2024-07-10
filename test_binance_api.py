import aiohttp
import asyncio
import hmac
import hashlib
import time
from urllib.parse import urlencode
from credentials import credentials_dict

class BinanceAPI:
    BASE_URL = "https://api.binance.com"
    
    def __init__(self, api_key, api_secret, client_type='WEB'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client_type = client_type
        self.session = aiohttp.ClientSession()

    def _generate_signature(self, query_string):
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _add_optional_headers(self, headers, x_gray_env, x_trace_id, x_user_id):
        if x_gray_env:
            headers['x-gray-env'] = x_gray_env
        if x_trace_id:
            headers['x-trace-id'] = x_trace_id
        if x_user_id:
            headers['x-user-id'] = x_user_id

    def _prepare_headers(self, x_gray_env=None, x_trace_id=None, x_user_id=None):
        headers = {
            'clientType': self.client_type,
            'X-MBX-APIKEY': self.api_key
        }
        self._add_optional_headers(headers, x_gray_env, x_trace_id, x_user_id)
        return headers

    async def _make_request(self, method, endpoint, params=None, headers=None, body=None):
        if params is None:
            params = {}

        params['timestamp'] = int(time.time() * 1000)
        query_string = urlencode(params)
        signature = self._generate_signature(query_string)
        query_string += f"&signature={signature}"
        
        url = f"{self.BASE_URL}{endpoint}?{query_string}"

        async with self.session.request(method, url, headers=headers, json=body) as response:
            resp_json = await response.json()
            if response.status == 200:
                print("Request success")
            else:
                print(f"Failed request to URL: {url} with headers: {headers} and body: {body}")
                print(f"Response: {resp_json}")
            return resp_json
    
    async def get_ads_detail_by_no(self, ads_no, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/ads/getDetailByNo"
        params = {
            'adsNo': ads_no
        }
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, params, headers)
    
    async def update_ads(self, ad_update_req, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/ads/update"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, headers=headers, body=ad_update_req)
    
    async def search_ads(self, ad_search_req, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/ads/search"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, headers=headers, body=ad_search_req)
    
    async def retrieve_chat_credential(self, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/chat/retrieveChatCredential"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('GET', endpoint, headers=headers)

    async def get_user_order_detail(self, ad_order_no_req, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/orderMatch/getUserOrderDetail"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, headers=headers, body=ad_order_no_req)

    async def check_if_can_release_coin(self, confirm_order_paid_req, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/orderMatch/checkIfCanReleaseCoin"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, headers=headers, body=confirm_order_paid_req)

    async def close_session(self):
        await self.session.close()

class BinanceWorker:
    def __init__(self, api_key, api_secret):
        self.binance_api = BinanceAPI(api_key, api_secret)
        self.task_queue = asyncio.Queue()

    async def add_task(self, task, *args):
        await self.task_queue.put((task, args))

    async def run(self):
        while True:
            task, args = await self.task_queue.get()
            await task(*args)
            self.task_queue.task_done()

    async def close(self):
        await self.binance_api.close_session()

# Usage example
async def main():
    account='account_1'
    api_key = credentials_dict[account]['KEY']
    api_secret = credentials_dict[account]['SECRET']
    worker = BinanceWorker(api_key, api_secret)

    # Start the worker
    worker_task = asyncio.create_task(worker.run())

    # Add tasks to the queue
    await worker.add_task(worker.binance_api.get_ads_detail_by_no, '12633943668966330368')

    ad_update_req = {
        "advNo": "12633943668966330368",
        "advStatus": 3,
        # other required fields...
    }
    await worker.add_task(worker.binance_api.update_ads, ad_update_req)

    ad_search_req = {
        "asset": "BTC",
        "fiat": "USD",
        "page": 1,
        "rows": 10,
        "tradeType": "BUY"
    }
    await worker.add_task(worker.binance_api.search_ads, ad_search_req)

    await worker.add_task(worker.binance_api.retrieve_chat_credential)

    ad_order_no_req = {
        "adOrderNo": "22644168169935429632"
    }
    await worker.add_task(worker.binance_api.get_user_order_detail, ad_order_no_req)

    confirm_order_paid_req = {
        "orderNumber": "22644168169935429632"
        # other required fields...
    }
    await worker.add_task(worker.binance_api.check_if_can_release_coin, confirm_order_paid_req)

    # Wait for all tasks to complete
    await worker.task_queue.join()

    # Close the worker and the session
    await worker.close()
    worker_task.cancel()

# Run the example
asyncio.run(main())
