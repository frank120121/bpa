import asyncio
import aiohttp
from urllib.parse import urlencode
import websockets
import logging
from common_utils import get_server_timestamp, hashing
from binance_endpoints import GET_CHAT_CREDENTIALS
from credentials import credentials_dict
from binance_c2c import on_message
from binance_merchants import fetch_merchant_credentials

logger = logging.getLogger(__name__)
async_sessions = {}

async def send_http_request(method, url, api_key, secret_key, params=None, body=None):
    params = params or {}
    params['timestamp'] = await get_server_timestamp()
    query_string = urlencode(params)
    signature = hashing(query_string, secret_key)
    final_url = f"{url}?{query_string}&signature={signature}"
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "X-MBX-APIKEY": api_key,
        "clientType": "WEB"
    }

    async with aiohttp.ClientSession() as session:
        async with session.request(method, final_url, json=body, headers=headers) as response:
            response_data = await response.json()
            if response.status != 200 or 'data' not in response_data:
                logger.error(f"Error {response.status} from API: {response_data}")
                return None
            return response_data['data']

async def get_websocket_url(api_key, secret_key):
    response_data = await send_http_request("GET", GET_CHAT_CREDENTIALS, api_key, secret_key)
    if response_data and 'chatWssUrl' in response_data and 'listenKey' in response_data:
        return f"{response_data['chatWssUrl']}/{response_data['listenKey']}?token={response_data['listenToken']}&clientType=web"
    return None

async def websocket_listener(uri, api_key, secret_key):
    async with websockets.connect(uri) as ws:
        async for message in ws:
            await on_message(ws, message, api_key, secret_key)

async def establish_websocket_connection(api_key, secret_key, retries=200, backoff=1, max_backoff=10):
    for attempt in range(retries):
        try:
            wss_url = await get_websocket_url(api_key, secret_key)
            if wss_url:
                await websocket_listener(wss_url, api_key, secret_key)
                return 
            else:
                logger.error("Failed to get WebSocket URL.")
        except Exception as e:
            logger.exception("An error occurred during WebSocket connection:")
        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)
    logger.error("Reached maximum retry limit. Exiting.")

async def start_merchant_session(merchant_id):
    credentials = await fetch_merchant_credentials(merchant_id)
    if credentials and merchant_id not in async_sessions:
        task = asyncio.create_task(establish_websocket_connection(merchant_id, credentials))
        async_sessions[merchant_id] = task
        logger.debug(f"Started bot session for merchant {merchant_id}")
    else:
        logger.warning(f"Could not start session for merchant {merchant_id} (already active or credentials missing)")

async def stop_merchant_session(merchant_id):
    if merchant_id in async_sessions:
        task = async_sessions.pop(merchant_id)
        task.cancel()
        logger.debug(f"Stopped bot session for merchant {merchant_id}")
    else:
        logger.warning(f"No active session found for merchant {merchant_id}")