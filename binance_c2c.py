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
    def __init__(self, payment_manager, credentials_dict):
        self.connections = {}
        self.payment_manager = payment_manager
        self.credentials_dict = credentials_dict
        
    async def create_connection(self, account, api_key, api_secret):
        try:
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
            logger.info(f"Connection established for account {account}")
        except Exception as e:
            logger.error(f"Failed to create connection for account {account}: {e}")
            self.connections[account] = {
                'ws': None,
                'api_key': api_key,
                'api_secret': api_secret,
                'is_connected': False
            }

    async def ensure_connection(self, account):
        if account not in self.connections or not self.connections[account]['is_connected']:
            logger.info(f"Establishing/Re-establishing connection for account {account}")
            try:
                # Check if we have the credentials for this account
                if account not in self.connections:
                    # If we don't have the credentials, we need to get them from somewhere
                    # This could be from a config file, database, or passed in when initializing the ConnectionManager
                    api_key, api_secret = self.get_account_credentials(account)
                else:
                    api_key = self.connections[account]['api_key']
                    api_secret = self.connections[account]['api_secret']

                await self.create_connection(account, api_key, api_secret)
            except Exception as e:
                logger.error(f"Failed to establish/re-establish connection for account {account}: {e}")
                self.connections[account] = {
                    'ws': None,
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'is_connected': False
                }
        return self.connections[account]['is_connected']
    
    def get_account_credentials(self, account):
        # This method should return the API key and secret for the given account
        # You'll need to implement this based on how you're storing your credentials
        # For example:
        if account in credentials_dict:
            return credentials_dict[account]['KEY'], credentials_dict[account]['SECRET']
        else:
            raise ValueError(f"No credentials found for account {account}")

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
                    await self.connections[account]['ws'].send(message_json)
                    logger.debug(f"Message sent for account {account}")
                    return  # Successfully sent the message, exit the method
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1}: Message sending failed for account {account}: {e}")
                    self.connections[account]['is_connected'] = False
                    # Connection failed, we'll retry in the next iteration
            else:
                logger.error(f"Attempt {attempt + 1}: Failed to establish connection for account {account}")

            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Short delay before retrying

        logger.error(f"Failed to send message after {max_retries} attempts: No active connection for account {account}")

    async def get_session(self):
        return await SharedSession.get_session()

async def run_websocket(account, api_key, api_secret, connection_manager, merchant_account):
    while True:
        try:
            await connection_manager.ensure_connection(account)
            
            while connection_manager.connections[account]['is_connected']:
                message = await connection_manager.connections[account]['ws'].recv()
                await on_message(connection_manager, merchant_account, account, message, api_key, api_secret)
            
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WebSocket connection closed for account {account}. Reconnecting...")
        except Exception as e:
            logger.exception(f"An unexpected error occurred for account {account}: {e}")
        finally:
            await connection_manager.close_connection(account)

        logger.info(f"Attempting to re-establish connection for account {account}")


async def check_connections(connection_manager):
    while True:
        failed_accounts = [account for account, conn in connection_manager.connections.items() if not conn['is_connected']]
        if failed_accounts:
            logger.warning(f"Detected {len(failed_accounts)} failed connections: {failed_accounts}")
            for account in failed_accounts:
                logger.info(f"Attempting to reconnect account: {account}")
                await connection_manager.ensure_connection(account)
        
        # Check if all connections are now established
        all_connected = all(conn['is_connected'] for conn in connection_manager.connections.values())
        if all_connected:
            logger.info("All connections are now established.")
        
        await asyncio.sleep(300) 

async def main_binance_c2c(payment_manager):
    connection_manager = ConnectionManager(payment_manager, credentials_dict)
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