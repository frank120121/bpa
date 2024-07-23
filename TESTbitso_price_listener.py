# TESTbitso_price_listener.py
import asyncio
import json
import websockets
import logging
from logging_config import setup_logging

# Shared dictionary to store lowest ask price
shared_data = {"lowest_ask": None}
lowest_ask_lock = asyncio.Lock()

setup_logging(log_filename='bitso_price_listener.log')
logger = logging.getLogger(__name__)

async def fetch_lowest_ask():
    uri = "wss://ws.bitso.com"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                # Subscribe to the 'orders' channel
                subscribe_message = json.dumps({
                    "action": "subscribe",
                    "book": "usdt_mxn",
                    "type": "orders"
                })
                await websocket.send(subscribe_message)

                while True:
                    try:
                        response = await websocket.recv()
                        data = json.loads(response)
                        
                        # Handle incoming orders data
                        if data['type'] == 'orders' and 'payload' in data:
                            payload = data['payload']
                            lowest_ask = min([float(order['r']) for order in payload['asks']], default=None)
                            
                            if lowest_ask is not None:
                                async with lowest_ask_lock:
                                    shared_data["lowest_ask"] = lowest_ask
                                logger.debug(f"Lowest ask price for usdt/MXN plus commission: {lowest_ask}")

                        # Handle keep-alive messages
                        elif data['type'] == 'ka':
                            logger.debug("Received keep-alive message")

                    except websockets.ConnectionClosedError as e:
                        logger.error(f"WebSocket connection closed: {e}")
                        break  # Exit the inner loop and reconnect

                    except asyncio.IncompleteReadError as e:
                        logger.error(f"Incomplete read error: {e}")
                        break  # Exit the inner loop and reconnect

                    except Exception as e:
                        logger.error(f"An unexpected error occurred: {e}")
                        break  # Exit the inner loop and reconnect

        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")

        logger.info("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)  # Wait before retrying to connect
    
if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(fetch_lowest_ask())