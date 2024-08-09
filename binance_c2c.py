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
from binance_share_data import SharedSession
from binance_bank_deposit import PaymentManager

RETRY_DELAY = 0.1
MAX_RETRY_DELAY = 1 

logger = logging.getLogger(__name__)

async def on_message(connection_manager, merchant_account, account, message, KEY, SECRET):
    try:
        msg_json = json.loads(message)
        is_self = msg_json.get('self', False)
        if is_self:
            return
        msg_type = msg_json.get('type', '')
        if msg_type == 'auto_reply':
            return
        conn = await create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
        if conn:
            try:
                await merchant_account.handle_message_by_type(connection_manager, account, KEY, SECRET, msg_json, msg_type, conn)
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
    def __init__(self, payment_manager):
        self.connections = {}
        self.payment_manager = payment_manager

    async def create_connection(self, account, api_key, api_secret):
        api_instance = await SingletonBinanceAPI.get_instance(account, api_key, api_secret)
        response = await api_instance.retrieve_chat_credential()

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
            'api_key': api_key,
            'api_secret': api_secret,
            'is_connected': True
        }

    async def close_connection(self, account):
        if account in self.connections:
            await self.connections[account]['ws'].close()
            del self.connections[account]

    async def send_text_message(self, account, text, order_no):
        if account not in self.connections or not self.connections[account]['is_connected']:
            logger.error(f"No active connection for account {account}")
            return

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
            await self.connections[account]['ws'].send(message_json)
            logger.debug(f"Message sent for account {account}")
        except Exception as e:
            logger.error(f"Message sending failed for account {account}: {e}.")
            self.connections[account]['is_connected'] = False

    async def get_session(self):
        return await SharedSession.get_session()

async def run_websocket(account, api_key, api_secret, connection_manager, merchant_account):
    retry_delay = RETRY_DELAY
    while True:
        try:
            await connection_manager.create_connection(account, api_key, api_secret)
            
            while True:
                message = await connection_manager.connections[account]['ws'].recv()
                await on_message(connection_manager, merchant_account, account, message, api_key, api_secret)
            
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WebSocket connection closed for account {account}. Retrying...")
        except Exception as e:
            logger.exception(f"An unexpected error occurred for account {account}: {e}")
        finally:
            await connection_manager.close_connection(account)

        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
        logger.info(f"Retrying connection for account {account} in {retry_delay} seconds.")
        await asyncio.sleep(retry_delay)


async def check_connections(connection_manager):
    while True:
        failed_accounts = [account for account, conn in connection_manager.connections.items() if not conn['is_connected']]
        if failed_accounts:
            logger.warning(f"Detected {len(failed_accounts)} failed connections: {failed_accounts}")
        await asyncio.sleep(30)

async def main_binance_c2c(payment_manager):
    connection_manager = ConnectionManager(payment_manager)
    merchant_account = MerchantAccount(payment_manager)
    tasks = []

    for account, cred in credentials_dict.items():
        task = asyncio.create_task(
            run_websocket(account, cred['KEY'], cred['SECRET'], connection_manager, merchant_account)
        )
        tasks.append(task)

    checker_task = asyncio.create_task(check_connections(connection_manager))
    tasks.append(checker_task)

    await asyncio.gather(*tasks)