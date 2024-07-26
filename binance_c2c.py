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

RETRY_DELAY = 10  

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
    api_instance = await SingletonBinanceAPI.get_instance(account, api_key, api_secret)

    try:
        logger.debug(f"Fetching chat credentials for account: {account}...")
        response = await api_instance.retrieve_chat_credential()
        logger.debug(f"Received response for account {account}: {response}")
        
        if response and 'data' in response:
            data = response['data']
            if 'chatWssUrl' in data and 'listenKey' in data and 'listenToken' in data:
                wss_url = f"{data['chatWssUrl']}/{data['listenKey']}?token={data['listenToken']}&clientType=web"
                logger.debug(f"WebSocket URL constructed for account {account}: {wss_url}")
            else:
                logger.error(f"Missing expected keys in 'data' for account {account}. Full response: {response}")
                connection_status[account] = False
                return
                
        else:
            logger.error(f"Unexpected structure in API response for account {account}. Full response: {response}")
            connection_status[account] = False
            return

        connection_manager = ConnectionManager(wss_url, api_key, api_secret)
        logger.debug(f"Attempting to connect to WebSocket with URL for account {account}: {wss_url}")
        async with websockets.connect(wss_url) as ws:
            connection_manager.ws = ws
            connection_manager.is_connected = True
            connection_status[account] = True
            logger.info(f"WebSocket connection established for account {account}.")
            async for message in ws:
                logger.info(f"Received message for account {account}: {message}")
                await on_message(connection_manager, message, api_key, api_secret)
        connection_manager.is_connected = False
        logger.info(f"WebSocket connection closed gracefully for account {account}.")
        connection_status[account] = False

    except Exception as e:
        logger.exception(f"An unexpected error occurred for account {account}:")
        connection_status[account] = False

async def main_binance_c2c():
    connection_status = {}
    tasks = []

    # Initial connection attempt for all accounts
    for account, cred in credentials_dict.items():
        connection_status[account] = False
        task = asyncio.create_task(
            run_websocket(account, cred['KEY'], cred['SECRET'], connection_status)
        )
        tasks.append(task)
    
    try:
        await asyncio.gather(*tasks)
        
        while True:
            # Check for failed connections
            failed_accounts = [account for account, status in connection_status.items() if not status]
            if not failed_accounts:
                logger.info("All WebSocket connections established successfully.")
                break

            logger.error(f"Retrying failed WebSocket connections for the following accounts: {failed_accounts}")

            # Retry failed connections after a delay
            await asyncio.sleep(RETRY_DELAY)
            retry_tasks = []
            for account in failed_accounts:
                cred = credentials_dict[account]
                retry_task = asyncio.create_task(
                    run_websocket(account, cred['KEY'], cred['SECRET'], connection_status)
                )
                retry_tasks.append(retry_task)
            await asyncio.gather(*retry_tasks)

    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt received. Exiting.")
    except Exception as e:
        logger.exception("An unexpected error occurred:")

if __name__ == "__main__":
    asyncio.run(main_binance_c2c())
