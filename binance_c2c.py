#binance_c2c.py
import asyncio
import json
import logging
import websockets
from binance_singleton_api import SingletonBinanceAPI
from binance_merchant_handler import MerchantAccount
from common_utils import get_server_timestamp
from common_utils_db import create_connection
from credentials import credentials_dict

RETRY_DELAY = 0.1
MAX_RETRY_DELAY = 1 

logger = logging.getLogger(__name__)

async def on_message(connection_manager, message, KEY, SECRET):
    merchant_account = MerchantAccount()
    try:
        msg_json = json.loads(message)
        is_self = msg_json.get('self', False)
        if is_self:
            logger.debug("Message was from self")
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
                logger.debug("Message sent")
            except Exception as e:
                logger.error(f"Message sending failed: {e}.")
        else:
            logger.error("Failed to send message: WebSocket not connected.")

async def run_websocket(account, api_key, api_secret, connection_status):
    retry_delay = RETRY_DELAY
    while True:
        try:
            # Get the API instance and retrieve credentials
            api_instance = await SingletonBinanceAPI.get_instance(account, api_key, api_secret)
            logger.debug(f"Fetching chat credentials for account: {account}...")
            response = await api_instance.retrieve_chat_credential()
            logger.debug(f"Received response for account {account}: {response}")

            if response and 'data' in response:
                data = response['data']
                if 'chatWssUrl' in data and 'listenKey' in data and 'listenToken' in data:
                    wss_url = f"{data['chatWssUrl']}/{data['listenKey']}?token={data['listenToken']}&clientType=web"
                    logger.debug(f"WebSocket URL constructed for account {account}: {wss_url}")
                else:
                    raise ValueError(f"Missing expected keys in 'data' for account {account}. Full response: {response}")
            else:
                raise ValueError(f"Unexpected structure in API response for account {account}. Full response: {response}")

            # Establish WebSocket connection
            connection_manager = ConnectionManager(wss_url, api_key, api_secret)
            logger.debug(f"Attempting to connect to WebSocket with URL for account {account}: {wss_url}")
            async with websockets.connect(wss_url) as ws:
                connection_manager.ws = ws
                connection_manager.is_connected = True
                connection_status[account] = True
                logger.info(f"Updated connection status for {account}: {connection_status[account]}")
                
                # Log the current status for debugging
                logger.debug(f"Updated connection_status after connection for {account}: {connection_status}")
                
                # Listen for messages
                logger.info(f"Entering WebSocket message loop for account {account}")
                async for message in ws:
                    logger.debug(f"Received message for account {account}: {message}")
                    await on_message(connection_manager, message, api_key, api_secret)
                
                # Reset retry delay after a successful connection
                retry_delay = RETRY_DELAY

        except Exception as e:
            logger.exception(f"An unexpected error occurred for account {account}: {e}")
            connection_status[account] = False
            logger.info(f"Updated connection status for {account}: {connection_status[account]}")
            connection_manager.is_connected = False

            # Incremental backoff with a maximum limit
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
            logger.info(f"Retrying connection for account {account} in {retry_delay} seconds.")
            await asyncio.sleep(retry_delay)


async def check_connections(connection_status):
    while True:
        logger.debug(f"Current connection status: {connection_status}")
        failed_accounts = [account for account, status in connection_status.items() if not status]
        logger.debug(f"Failed accounts: {failed_accounts}")
        if failed_accounts:
            logger.warning(f"Detected {len(failed_accounts)} failed connections: {failed_accounts}")
            # Here you could implement additional logic to handle failed connections
        else:
            logger.info("All WebSocket connections are currently established.")
        await asyncio.sleep(30)  # Check every 30 seconds

async def main_binance_c2c():
    logger.debug("Starting main_binance_c2c function")
    connection_status = {}
    tasks = []

    # Initial connection attempt for all accounts
    for account, cred in credentials_dict.items():
        connection_status[account] = False
        logger.debug(f"Updated connection status for {account}: {connection_status[account]}")
        task = asyncio.create_task(
            run_websocket(account, cred['KEY'], cred['SECRET'], connection_status)
        )
        tasks.append(task)

    # Add the connection checker task
    checker_task = asyncio.create_task(check_connections(connection_status))
    tasks.append(checker_task)

    # Keep all tasks running
    await asyncio.gather(*tasks)