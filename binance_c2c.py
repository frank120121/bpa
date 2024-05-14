
import aiohttp
import asyncio
import json
import logging
import websockets

from urllib.parse import urlencode

from binance_endpoints import GET_CHAT_CREDENTIALS
from binance_merchant_handler import MerchantAccount
from common_utils import get_server_timestamp, hashing
from common_utils_db import create_connection
from credentials import credentials_dict

logger = logging.getLogger(__name__)

async def send_http_request(method, url, api_key, secret_key, params=None, body=None):
    try:
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
    except Exception as e:
        logger.exception(f"An error occurred in send_http_request: {e}")
        return None

async def on_message(connection_manager, message, KEY, SECRET):
    merchant_account = MerchantAccount()
    try:
        msg_json = json.loads(message)
        is_self = msg_json.get('self', False)
        if is_self:
            logger.debug("message was from self")
            return
        msg_type = msg_json.get('type', '')
        if msg_type == 'auto_reply':
            logger.debug("Ignoring auto-reply message")
            return
        conn = await create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
        if conn:
            logger.debug(message)
            try:
                await merchant_account.handle_message_by_type(connection_manager, KEY, SECRET, msg_json, msg_type, conn)
                await conn.commit()               
            except Exception as e:
                await conn.rollback()
                logger.exception("Database operation failed, rolled back: %s", e)
            finally:
                await conn.close()
        else:
            logger.error("Failed to connect to the database.")
    except Exception as e:
        logger.exception("An exception occurred: %s", e)
class ConnectionManager:
    def __init__(self, uri, api_key, secret_key):
        self.uri = uri
        self.api_key = api_key
        self.secret_key = secret_key
        self.ws = None
        self.is_connected = False

    async def send_text_message(self, text, order_no):
        message = {
            'type': 'text',
            'uuid': f"self_{await get_server_timestamp()}",
            'orderNo': order_no,
            'content': text,
            'self': False,
            'clientType': 'web',
            'createTime': await get_server_timestamp(),
            'sendStatus': 4
        }
        message_json = json.dumps(message)

        if not self.is_connected:
            logger.info("WebSocket is not connected, reconnecting...")

        if self.is_connected:
            try:
                await self.ws.send(message_json)
                logger.info(f"Message sent")
            except Exception as e:
                logger.error(f"Message sending failed: {e}.")
        else:
            logger.error("Failed to send message: WebSocket not connected.")

async def run_websocket(KEY, SECRET):
    uri_path = GET_CHAT_CREDENTIALS
    backoff = 1
    max_backoff = 2  # Maximum backoff time set to 
    retry_count = 0
    max_retries = 2000  # Maximum of 200 retry attempts

    while retry_count < max_retries:
        try:
            response = await send_http_request("GET", uri_path, KEY, SECRET)
            if 'chatWssUrl' in response:
                wss_url = f"{response['chatWssUrl']}/{response['listenKey']}?token={response['listenToken']}&clientType=web"
            else:
                logger.error(f"Key 'chatWssUrl' not found in API response. Full response: {response}")
                retry_count += 1
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)
                continue
            connection_manager = ConnectionManager(wss_url, KEY, SECRET)
            logger.debug(f"Attempting to connect to WebSocket with URL: {wss_url}")
            async with websockets.connect(wss_url) as ws:
                connection_manager.ws = ws
                connection_manager.is_connected = True
                async for message in ws:
                    logger.debug(f"Received message: {message}")
                    await on_message(connection_manager, message, KEY, SECRET)
            connection_manager.is_connected = False
            logger.info("WebSocket connection closed gracefully.")
            backoff = 1
            retry_count = 0

        except Exception as e:
            logger.exception("An unexpected error occurred:")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
            retry_count += 1

    logger.error(f"Reached maximum retry limit of {max_retries}. Exiting.")

async def on_close(connection_manager, close_status_code, close_msg, KEY, SECRET):
    logger.debug(f"### closed ###")

async def main_binance_c2c():
    credentials = list(credentials_dict.values())
    tasks = []
    for cred in credentials:
        task = asyncio.create_task(
            run_websocket(cred['KEY'], cred['SECRET'])
        )
        tasks.append(task)
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt received. Exiting.")