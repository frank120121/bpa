import asyncio
import json
import logging
import websockets

from binance_merchant_handler import MerchantAccount
from common_utils import get_server_timestamp
from common_utils_db import create_connection
from credentials import credentials_dict
from binance_share_data import SharedSession

RETRY_DELAY = 0.1
MAX_RETRY_DELAY = 1 

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self, payment_manager, binance_api, credentials_dict):
        self.connections = {}
        self.payment_manager = payment_manager
        self.binance_api = binance_api
        self.credentials_dict = credentials_dict

    async def create_connection(self, account):
        try:
            api_key = self.credentials_dict[account]['KEY']
            api_secret = self.credentials_dict[account]['SECRET']
            
            response = await self.binance_api.retrieve_chat_credential(api_key, api_secret)

            if response and 'data' in response:
                data = response['data']
                if 'chatWssUrl' in data and 'listenKey' in data and 'listenToken' in data:
                    wss_url = f"{data['chatWssUrl']}/{data['listenKey']}?token={data['listenToken']}&clientType=web"
                else:
                    raise ValueError(f"Missing expected keys in 'data' for account {account}.")
            else:
                raise ValueError(f"Unexpected structure in API response for account {account}.")

            ws = await websockets.connect(wss_url)
            self.connections[account] = {
                'ws': ws,
                'is_connected': True,
                'api_key': api_key,
                'api_secret': api_secret
            }

        except Exception as e:
            logger.error(f"Failed to create connection for account {account}: {e}")
            self.connections[account] = {
                'ws': None,
                'is_connected': False,
                'api_key': api_key,
                'api_secret': api_secret
            }

    async def ensure_connection(self, account):
        if account not in self.connections or not self.connections[account]['is_connected']:
            try:
                await self.create_connection(account)
            except Exception as e:
                logger.error(f"Failed to establish/re-establish connection for account {account}: {e}")
        return self.connections[account]['is_connected']

    async def close_connection(self, account):
        if account in self.connections:
            if self.connections[account]['ws']:
                await self.connections[account]['ws'].close()
            self.connections[account]['is_connected'] = False
            logger.info(f"Connection closed for account {account}")

    async def send_text_message(self, account, text, order_no):
        max_retries = 3
        for attempt in range(max_retries):
            await self.ensure_connection(account)
            if self.connections[account]['is_connected']:
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

                try:
                    await asyncio.sleep(3)
                    await self.connections[account]['ws'].send(message_json)
                    return
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1}: Message sending failed for account {account}: {e}")
                    self.connections[account]['is_connected'] = False
            else:
                logger.error(f"Attempt {attempt + 1}: Failed to establish connection for account {account}")

            if attempt < max_retries - 1:
                await asyncio.sleep(1)

        logger.error(f"Failed to send message after {max_retries} attempts: No active connection for account {account}")

    async def get_session(self):
        return await SharedSession.get_session()

    async def run_websocket(self, account, merchant_account):
        while True:
            try:
                await self.ensure_connection(account)

                while self.connections[account]['is_connected']:
                    message = await self.connections[account]['ws'].recv()
                    await self.on_message(merchant_account, account, message)

            except websockets.exceptions.ConnectionClosed:
                logger.info(f"WebSocket connection closed for account {account}. Reconnecting...")
                
            except Exception as e:
                logger.exception(f"An unexpected error occurred for account {account}: {e}")
                
            finally:
                await self.close_connection(account)
            
            await asyncio.sleep(5)

    async def on_message(self, merchant_account, account, message):
        try:
            msg_json = json.loads(message)
            is_self = msg_json.get('self', False)
            if is_self:
                return
            msg_type = msg_json.get('type', '')
            if msg_type == 'auto_reply':
                return

            api_key = self.connections[account]['api_key']
            api_secret = self.connections[account]['api_secret']
            
            conn = await create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
            if conn:
                try:
                    await merchant_account.handle_message_by_type(self, account, api_key, api_secret, msg_json, msg_type, conn)
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

    async def check_connections(self):
        while True:
            failed_accounts = [account for account, conn in self.connections.items() if not conn['is_connected']]
            if failed_accounts:
                logger.warning(f"Detected {len(failed_accounts)} failed connections: {failed_accounts}")
                for account in failed_accounts:
                    logger.info(f"Attempting to reconnect account: {account}")
                    await self.ensure_connection(account)
            await asyncio.sleep(300)

async def main_binance_c2c(payment_manager, binance_api):
    connection_manager = ConnectionManager(payment_manager, binance_api, credentials_dict)
    merchant_account = MerchantAccount(payment_manager, binance_api)
    tasks = []

    for account in credentials_dict.keys():
        task = asyncio.create_task(
            connection_manager.run_websocket(account, merchant_account)
        )
        tasks.append(task)
    
    checker_task = asyncio.create_task(connection_manager.check_connections())
    tasks.append(checker_task)

    await asyncio.gather(*tasks)
