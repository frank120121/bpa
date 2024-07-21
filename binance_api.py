#binance_api.py
import aiohttp
import asyncio
import hmac
import hashlib
import logging
from urllib.parse import urlencode
from common_utils import get_server_timestamp
import time
from datetime import datetime, timedelta
from asyncio import Lock

logger = logging.getLogger(__name__)

class BinanceAPI:
    BASE_URL = "https://api.binance.com"
    instance_count = 0  # Class-level variable to count instances
    last_request_time = 0  # Timestamp of the last request made
    rate_limit_delay = 0  # Default minimum delay between requests in seconds
    request_lock = Lock()  # Lock to ensure rate limiting is respected globally
    cache = {}

    def __init__(self, api_key, api_secret, client_type='WEB'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client_type = client_type
        self.session = aiohttp.ClientSession()
        BinanceAPI.instance_count += 1  # Increment instance count
        self.instance_id = BinanceAPI.instance_count  # Assign an instance ID
        logger.debug(f"Instance {self.instance_id} created. Number of BinanceAPI instances: {BinanceAPI.instance_count}")

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

    async def _make_request(self, method, endpoint, params=None, headers=None, body=None, retries=5, backoff_factor=2):
        for attempt in range(retries):
            try:
                async with BinanceAPI.request_lock:
                    # Implement global rate limiting for specific endpoints
                    if endpoint in ['/sapi/v1/c2c/ads/update', '/sapi/v1/c2c/ads/search']:
                        logger.debug(f"Instance {self.instance_id}: Rate limiting for endpoint: {endpoint}")
                        if endpoint == '/sapi/v1/c2c/ads/update':
                            logger.debug(f"Instance {self.instance_id}: Rate limiting for ad update endpoint")
                            BinanceAPI.rate_limit_delay = 0.5
                        else:
                            BinanceAPI.rate_limit_delay = 0.05

                        current_time = time.time()
                        time_since_last_request = current_time - BinanceAPI.last_request_time
                        if time_since_last_request < BinanceAPI.rate_limit_delay:
                            wait_time = BinanceAPI.rate_limit_delay - time_since_last_request
                            logger.debug(f"Instance {self.instance_id}: Rate limiting: waiting for {wait_time:.2f} seconds before next request.")
                            await asyncio.sleep(wait_time)
                        BinanceAPI.last_request_time = time.time()  # Update last request time after waiting

                if params is None:
                    params = {}

                params['timestamp'] = await get_server_timestamp()
                query_string = urlencode(params)
                signature = self._generate_signature(query_string)
                query_string += f"&signature={signature}"
                
                url = f"{self.BASE_URL}{endpoint}?{query_string}"
                logger.debug(f"Instance {self.instance_id}: Request: method={method}, url={url}, headers={headers}, body={body}")

                async with self.session.request(method, url, headers=headers, json=body) as response:
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' in content_type:
                        resp_json = await response.json()
                        logger.debug(f"Instance {self.instance_id}: Response status: {response.status}, Response body: {resp_json}")
                        
                        if response.status == 200:
                            logger.debug(f"Instance {self.instance_id}: Request success")
                            return resp_json
                        elif response.status == 400:
                            error_code = resp_json.get('code')
                            if error_code == -1021:
                                logger.info(f"Instance {self.instance_id}: Resyncing server timestamp due to error code -1021")
                                await get_server_timestamp(resync=True)
                                continue
                            else:
                                logger.error(f"Instance {self.instance_id}: Status: {response.status} Response: {resp_json}")
                                return resp_json
                        elif response.status in [429, 83628]:  # Handle rate limiting
                            logger.info(f"Instance {self.instance_id} Status: {response.status} Response: {resp_json}")
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                retry_after = int(retry_after)
                            else:
                                retry_after = 30
                            await asyncio.sleep(retry_after)
                            BinanceAPI.rate_limit_delay = max(BinanceAPI.rate_limit_delay, retry_after)
                        else:
                            logger.error(f"Instance {self.instance_id}: Status: {response.status} Response: {resp_json}")
                            return resp_json
                    else:
                        logger.error(f"Instance {self.instance_id}: Unexpected content type: {content_type} for URL: {url}")
                        text_response = await response.text()
                        logger.debug(f"Instance {self.instance_id}: Response status: {response.status}, Response body: {text_response}")
                        return text_response
            except aiohttp.ClientError as e:
                logger.error(f"Instance {self.instance_id}: Client error during request: {e}")
            except Exception as e:
                logger.error(f"Instance {self.instance_id}: Unexpected error during request: {e}")

            await asyncio.sleep(backoff_factor ** attempt)
        logger.error(f"Instance {self.instance_id}: Exceeded max retries for URL: {url}")
        return None

    
    async def get_ad_detail(self, ads_no, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/ads/getDetailByNo"
        params = {
            'adsNo': ads_no
        }
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('POST', endpoint, params, headers)
    
    async def update_ad(self, advNo, priceFloatingRatio, x_gray_env=None, x_trace_id=None, x_user_id=None):
        if advNo in ['12590489123493851136', '12590488417885061120']:
            logger.debug(f"Instance {self.instance_id}: Ad: {advNo} is in the skip list")
            return
        endpoint = "/sapi/v1/c2c/ads/update"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)
        body = {
            "advNo": advNo,
            "priceFloatingRatio": priceFloatingRatio
        }
        
        return await self._make_request('POST', endpoint, headers=headers, body=body)
    
    async def fetch_ads_search(self, trade_type, asset, fiat, trans_amount, pay_types, x_gray_env=None, x_trace_id=None, x_user_id=None):
        # Create a cache key based on the function's arguments
        cache_key = (trade_type, asset, fiat, trans_amount, tuple(sorted(pay_types)) if pay_types else None)

        # Check if these parameters are in the cache and if the cached result is less than 0.5 seconds old
        if cache_key in BinanceAPI.cache:
            cached_result, timestamp = BinanceAPI.cache[cache_key]
            if datetime.now() - timestamp < timedelta(seconds=0.25):
                logger.debug(f"Instance {self.instance_id}: Returning cached result for {asset} {fiat} {trans_amount} {pay_types}")
                return cached_result
            
        endpoint = "/sapi/v1/c2c/ads/search"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)
        body = {
            "asset": asset,
            "fiat": fiat,
            "page": 1,
            "publisherType": "merchant",
            "rows": 20,
            "tradeType": trade_type,
            "transAmount": trans_amount,
        }
        if pay_types:
            body['payTypes'] = pay_types

        response_data = await self._make_request('POST', endpoint, headers=headers, body=body)

        # Cache the result along with the current timestamp 
        BinanceAPI.cache[cache_key] = (response_data, datetime.now())

        return response_data
    
    async def retrieve_chat_credential(self, x_gray_env=None, x_trace_id=None, x_user_id=None):
        endpoint = "/sapi/v1/c2c/chat/retrieveChatCredential"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)

        return await self._make_request('GET', endpoint, headers=headers)
    

    async def get_counterparty_order_statistics(self, order_number, x_gray_env=None, x_trace_id=None, x_user_id=None):
        logger.debug(f"Instance {self.instance_id}: calling get_counterparty_order_statistics")
        endpoint = "/sapi/v1/c2c/orderMatch/queryCounterPartyOrderStatistic"
        headers = self._prepare_headers(x_gray_env, x_trace_id, x_user_id)
        body = {
            "orderNumber": order_number
        }
        return await self._make_request('POST', endpoint, headers=headers, body=body)
    
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
        BinanceAPI.instance_count -= 1  # Decrement instance count
        logger.info(f"Instance {self.instance_id}: BinanceAPI instance closed. Number of instances remaining: {BinanceAPI.instance_count}")  # Log the instance count
