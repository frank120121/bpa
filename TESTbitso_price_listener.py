# TESTbitso_price_listener.py
import asyncio
import json
import websockets
import logging
from logging_config import setup_logging

# Shared dictionary to store prices and weights
shared_data = {
    "lowest_ask": None,
    "highest_bid": None,
    "weighted_average_ask": None,
    "weighted_average_bid": None
}
data_lock = asyncio.Lock()

setup_logging(log_filename='bitso_price_listener.log')
logger = logging.getLogger(__name__)

async def fetch_prices():
    uri = "wss://ws.bitso.com"
    target_amount_mxn = 50000  # MXN target for weighted average

    def calculate_weighted_average(orders, target_mxn):
        total_mxn = 0
        total_amount = 0

        for price, amount in orders:
            price = float(price)
            amount = float(amount)
            value = price * amount

            mxn_to_add = min(value, target_mxn - total_mxn)
            amount_to_add = mxn_to_add / price

            total_mxn += mxn_to_add
            total_amount += amount_to_add

            if total_mxn >= target_mxn:
                break

        return total_mxn / total_amount if total_amount > 0 else 0

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
                            # Extract asks and bids
                            asks = [(float(order['r']), float(order['a'])) for order in payload['asks']]
                            bids = [(float(order['r']), float(order['a'])) for order in payload['bids']]
                            
                            lowest_ask = min([ask[0] for ask in asks], default=None)
                            highest_bid = max([bid[0] for bid in bids], default=None)
                            
                            # Calculate weighted average for lowest ask and highest bid
                            weighted_average_ask = calculate_weighted_average(asks, target_amount_mxn)
                            weighted_average_bid = calculate_weighted_average(bids, target_amount_mxn)

                            # Update shared data
                            async with data_lock:
                                shared_data["lowest_ask"] = lowest_ask
                                shared_data["highest_bid"] = highest_bid
                                shared_data["weighted_average_ask"] = weighted_average_ask
                                shared_data["weighted_average_bid"] = weighted_average_bid
                            logger.debug(f"Lowest ask price: {lowest_ask}, Weighted average ask price: {weighted_average_ask}")
                            logger.debug(f"Highest bid price: {highest_bid}, Weighted average bid price: {weighted_average_bid}")

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
    asyncio.get_event_loop().run_until_complete(fetch_prices())
