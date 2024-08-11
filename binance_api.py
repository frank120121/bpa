import json
import aiohttp
import asyncio
import hmac
import hashlib
import logging
from urllib.parse import urlencode
import time
from datetime import datetime, timedelta
from asyncio import Lock
from traceback import format_exc
from common_utils import get_server_timestamp
from binance_share_data import SharedSession

logger = logging.getLogger(__name__)

class BinanceAPI:
    BASE_URL = "https://api.binance.com"
    last_request_time = 0
    rate_limit_delay = 0
    request_lock = Lock()
    cache = {}
    ads_list_cache = {}
    get_ad_detail_cache = {}
    
    _instance = None 
    _lock = Lock() 

    def __init__(self, client_type='WEB'):
        self.client_type = client_type
        self.session = None

    @classmethod
    async def get_instance(cls) -> 'BinanceAPI':
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    async def _init_session(self):
        if self.session is None:
            self.session = await SharedSession.get_session()

    def _generate_signature(self, query_string, api_secret):
        return hmac.new(
            api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _prepare_headers(self, api_key):
        return {
            'clientType': self.client_type,
            'X-MBX-APIKEY': api_key
        }

    async def _apply_rate_limit(self, endpoint):
        async with BinanceAPI.request_lock:
            if endpoint in ['/sapi/v1/c2c/ads/update', '/sapi/v1/c2c/ads/search']:
                BinanceAPI.rate_limit_delay = 0.6 if endpoint == '/sapi/v1/c2c/ads/update' else 0.1

                current_time = time.time()
                time_since_last_request = current_time - BinanceAPI.last_request_time
                if time_since_last_request < BinanceAPI.rate_limit_delay:
                    wait_time = BinanceAPI.rate_limit_delay - time_since_last_request
                    await asyncio.sleep(wait_time)

                BinanceAPI.last_request_time = time.time()

    async def _make_request(self, method, endpoint, api_key, api_secret, params=None, headers=None, body=None, retries=5, backoff_factor=2, timeout=30):
        await self._init_session()
        if params is None:
            params = {}

        for attempt in range(retries):
            try:
                await self._apply_rate_limit(endpoint)

                params['timestamp'] = await get_server_timestamp()
                query_string = urlencode(params)
                signature = self._generate_signature(query_string, api_secret)
                query_string += f"&signature={signature}"
                url = f"{self.BASE_URL}{endpoint}?{query_string}"

                headers = self._prepare_headers(api_key)

                async with self.session.request(method, url, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    content_type = response.headers.get('Content-Type', '')
                    try:
                        resp_json = await response.json()
                    except aiohttp.ContentTypeError:
                        text_response = await response.text()
                        try:
                            resp_json = json.loads(text_response)
                        except json.JSONDecodeError:
                            logger.error(f"Unexpected content type: {content_type} for URL: {url}")
                            return text_response

                    if response.status == 200:
                        return resp_json
                    else:
                        if await self._handle_error(resp_json):
                            logger.error(f"Status: {response.status} Response: {resp_json} Body: {body} Params: {params} Headers: {headers} Endpoint: {endpoint}")
                            continue
                        return resp_json

            except aiohttp.ClientConnectorError as e:
                logger.error(f"Connection error (attempt {attempt + 1}/{retries}): {e}")
                wait_time = backoff_factor ** attempt * 2 
            except asyncio.TimeoutError:
                logger.error(f"Request timed out (attempt {attempt + 1}/{retries})")
                wait_time = backoff_factor ** attempt
            except aiohttp.ClientOSError as e:
                logger.error(f"OS error during request: {e}")
                wait_time = backoff_factor ** attempt
            except aiohttp.ClientError as e:
                logger.error(f"Client error during request: {e}\n{format_exc()}")
                wait_time = backoff_factor ** attempt
            except Exception as e:
                logger.error(f"Unexpected error during request: {e}\n{format_exc()}")
                wait_time = backoff_factor ** attempt

            logger.info(f"Retrying in {wait_time:.2f} seconds...")
            await asyncio.sleep(wait_time)

        logger.error(f"Exceeded max retries for URL: {url}")
        return None

    async def _handle_error(self, resp_json):
        error_code = resp_json.get('code')
        if error_code == -1021:
            await get_server_timestamp(resync=True)
            return True
        elif error_code in [83628, -1003]:
            retry_after = 1
            await asyncio.sleep(retry_after)
            BinanceAPI.rate_limit_delay = max(BinanceAPI.rate_limit_delay, retry_after)
            return True
        elif error_code == 83015:
            return True
        else:
            return False

    async def _handle_cache(self, cache_dict, cache_key, func, ttl, *args, **kwargs):
        current_time = datetime.now()

        if cache_key in cache_dict:
            cached_result, timestamp = cache_dict[cache_key]
            if current_time - timestamp < timedelta(seconds=ttl):
                return cached_result

        response_data = await func(*args, **kwargs)
        cache_dict[cache_key] = (response_data, current_time)
        return response_data

    async def ads_list(self, api_key, api_secret):
        ads_cache_key = (api_key, "ads_list")
        endpoint = "/sapi/v1/c2c/ads/listWithPagination"
        body = {
            "page": 1,
            "rows": 20
        }
        return await self._handle_cache(BinanceAPI.ads_list_cache, ads_cache_key, self._make_request, 60, 'POST', endpoint, api_key=api_key, api_secret=api_secret, body=body)

    async def get_ad_detail(self, api_key, api_secret, ads_no):
        get_ad_detail_cache_key = (api_key, ads_no)
        endpoint = "/sapi/v1/c2c/ads/getDetailByNo"
        params = {
            'adsNo': ads_no
        }
        return await self._handle_cache(BinanceAPI.get_ad_detail_cache, get_ad_detail_cache_key, self._make_request, 0.001, 'POST', endpoint, api_key, api_secret, params)

    async def fetch_ads_search(self, api_key, api_secret, trade_type, asset, fiat, trans_amount, pay_types, page):
        cache_key = (api_key, page, trade_type, asset, fiat, trans_amount, tuple(sorted(pay_types)) if pay_types else None)
        endpoint = "/sapi/v1/c2c/ads/search"
        body = {
            "asset": asset,
            "fiat": fiat,
            "page": page,
            "publisherType": "merchant",
            "rows": 20,
            "tradeType": trade_type,
            "transAmount": trans_amount,
        }
        if pay_types:
            body['payTypes'] = pay_types
        return await self._handle_cache(BinanceAPI.cache, cache_key, self._make_request, 0.20, 'POST', endpoint, api_key, api_secret, body=body)
    
    async def fetch_order_detail(self, api_key, api_secret, order_no):
        endpoint = "/sapi/v1/c2c/orderMatch/getUserOrderDetail"
        body = {
            "adOrderNo": order_no
        }
        return await self._make_request('POST', endpoint, api_key, api_secret, body=body)

    async def update_ad(self, api_key, api_secret, advNo, priceFloatingRatio):
        if advNo in ['12590489123493851136', '12590488417885061120']:
            return
        endpoint = "/sapi/v1/c2c/ads/update"
        body = {
            "advNo": advNo,
            "priceFloatingRatio": priceFloatingRatio
        }
        return await self._make_request('POST', endpoint, api_key, api_secret, body=body)

    async def list_orders(self,  api_key, api_secret):
        endpoint = "/sapi/v1/c2c/orderMatch/listOrders"
        body = {
            "orderStatusList": [
                1,
                2,
                3
            ],
            "page": 1,
            "rows": 20
        }
        return await self._make_request('POST', endpoint, api_key, api_secret, body=body)
    
    async def retrieve_chat_credential(self, api_key, api_secret):
        endpoint = "/sapi/v1/c2c/chat/retrieveChatCredential"
        return await self._make_request('GET', endpoint, api_key, api_secret)

    async def get_counterparty_order_statistics(self, api_key, api_secret, order_number):
        logger.debug(f"Instance {self.instance_id}: calling get_counterparty_order_statistics")
        endpoint = "/sapi/v1/c2c/orderMatch/queryCounterPartyOrderStatistic"
        body = {
            "orderNumber": order_number
        }
        return await self._make_request('POST', endpoint, api_key, api_secret, body=body)
    
    async def get_user_order_detail(self, api_key, api_secret, ad_order_no_req):
        endpoint = "/sapi/v1/c2c/orderMatch/getUserOrderDetail"
        return await self._make_request('POST', endpoint, api_key, api_secret, body=ad_order_no_req)

    async def check_if_can_release_coin(self, api_key, api_secret, confirm_order_paid_req):
        endpoint = "/sapi/v1/c2c/orderMatch/checkIfCanReleaseCoin"
        return await self._make_request('POST', endpoint,  api_key, api_secret, body=confirm_order_paid_req)

    async def close_session(self):
        await SharedSession.close_session()
