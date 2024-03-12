import asyncio
import json
import websockets
import logging
from logging_config import setup_logging
setup_logging(log_filename='bitso_price_listener.log')
logger = logging.getLogger(__name__)
async def fetch_btcmxn():
    uri = "wss://ws.bitso.com"
    async with websockets.connect(uri) as websocket:
        # Subscribe to the 'orders' channel
        subscribe_message = json.dumps({
            "action": "subscribe",
            "book": "btc_mxn",
            "type": "orders"
        })
        await websocket.send(subscribe_message)

        while True:
            response = await websocket.recv()
            data = json.loads(response)
            
            # Handle incoming orders data
            if data['type'] == 'orders' and 'payload' in data:
                payload = data['payload']
                lowest_ask = min([float(order['r']) for order in payload['asks']], default=None)
                
                if lowest_ask is not None:
                    logger.debug(f"Lowest ask price for BTC/MXN: {lowest_ask}")

                # Process the data, you can send this to update_ad in binance_update_ads
                # This can be done using various ways, for example, through a shared data structure or message queue
                
            # Handle keep-alive messages
            elif data['type'] == 'ka':
                logger.debug("Received keep-alive message")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(fetch_btcmxn())