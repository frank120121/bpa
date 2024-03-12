import asyncio
import aiohttp
import websockets
from credentials import credentials_dict, BASE_URL
import logging
logger = logging.getLogger(__name__)

async def send_signed_request(http_method, url_path, api_key, secret_key, params=None):
    async with aiohttp.ClientSession() as session:
        async with session.request(http_method, f"{BASE_URL}{url_path}", headers={'X-MBX-APIKEY': api_key}) as response:
            return await response.json()

async def on_error(ws, error):
    print(f"Error: {error}")

async def run_websocket(api_key, api_secret):
    try:
        listen_key_response = await send_signed_request("POST", "/api/v3/userDataStream", api_key, api_secret, params=None)
        listen_key = listen_key_response.get("listenKey", "")
        if not listen_key:
            logger.error("Failed to get listenKey")
            return
        ws_url = f"wss://stream.binance.com:9443/ws/{listen_key}"
        async with websockets.connect(ws_url) as ws:
            async for message in ws:
                logger.info(f"Incoming user_data_ws Message: {message}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

async def main_user_data_ws():
    tasks = []
    for account, creds in credentials_dict.items():
        api_key = creds['KEY']
        api_secret = creds['SECRET']
        task_binance = asyncio.create_task(run_websocket(api_key, api_secret))
        tasks.append(task_binance)
    await asyncio.gather(*tasks)




