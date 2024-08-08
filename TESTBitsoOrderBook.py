import asyncio
import json
import aiohttp
import websockets
import requests
from collections import deque
import logging
import TESTbitso_order_book_cache 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitsoOrderBook:
    def __init__(self, book):
        self.book = book
        self.order_book = {"bids": {}, "asks": {}}
        self.message_queue = deque()
        self.websocket_url = "wss://ws.bitso.com"
        self.rest_url = f"https://api.bitso.com/v3/order_book/?book={self.book}"
        self.sequence = None

    async def start(self):
        await self.connect_websocket()
        await self.get_initial_order_book()
        await self.process_queued_messages()
        await asyncio.gather(
            self.handle_real_time_messages(),
            self.log_order_book_periodically()
        )
        
    async def check_server_health(self):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.rest_url, timeout=5) as response:
                    if response.status == 200:
                        return True
                    else:
                        logger.warning(f"Server health check failed. Status: {response.status}")
                        return False
            except aiohttp.ClientError as e:
                logger.error(f"Error during server health check: {str(e)}")
                return False

    async def connect_websocket(self):
        if await self.check_server_health():
            max_retries = 5
            retry_delay = 1
            for attempt in range(max_retries):
                try:
                    self.websocket = await asyncio.wait_for(
                        websockets.connect(self.websocket_url),
                        timeout=10  # 10 seconds timeout
                    )
                    await self.subscribe_to_diff_orders()
                    return  # Successfully connected
                except (TimeoutError, websockets.exceptions.WebSocketException) as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to connect after {max_retries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
        else:
            raise Exception("Server is not healthy. Aborting WebSocket connection.")

    async def subscribe_to_diff_orders(self):
        subscribe_message = {
            "action": "subscribe",
            "book": self.book,
            "type": "diff-orders"
        }
        await self.websocket.send(json.dumps(subscribe_message))
        response = await self.websocket.recv()
        logger.debug(f"Subscription response: {response}")

    async def get_initial_order_book(self):
        try:
            response = requests.get(self.rest_url)
            data = response.json()
            if data['success']:
                self.sequence = int(data['payload']['sequence'])
                self.order_book['bids'] = {bid['price']: bid for bid in data['payload']['bids']}
                self.order_book['asks'] = {ask['price']: ask for ask in data['payload']['asks']}
                logger.debug(f"Initial order book loaded. Sequence: {self.sequence}")
                await self.log_reference_prices()
            else:
                raise ValueError(f"Failed to get initial order book: {data['error']}")
        except Exception as e:
            logger.error(f"Error getting initial order book: {str(e)}")
            raise

    async def process_queued_messages(self):
        while self.message_queue:
            message = self.message_queue.popleft()
            if message['sequence'] > self.sequence:
                await self.apply_order_update(message)  # Await the coroutine
                self.sequence = max(self.sequence, message['sequence'])

    async def apply_order_update(self, update):
        try:
            price = update['r']
            amount = update.get('a', '0')
            status = update['s']
            side = 'bids' if update['t'] == 0 else 'asks'

            if status == 'cancelled' or float(amount) == 0:
                self.order_book[side].pop(price, None)
                logger.debug(f"Removed order: {side} {price}")
            else:
                self.order_book[side][price] = {
                    'book': self.book,
                    'price': price,
                    'amount': amount
                }
                logger.debug(f"Updated order: {side} {price} {amount}")
            await self.log_reference_prices()
        except Exception as e:
            logger.error(f"Error applying order update: {e}", exc_info=True)
            logger.error(f"Problematic update: {update}")

    async def handle_real_time_messages(self):
        while True:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                logger.debug(f"Received message: {data}")
                
                if data['type'] == 'ka':
                    logger.debug("Received keep-alive message")
                    continue
                
                if data['type'] == 'diff-orders':
                    sequence = int(data['sequence'])
                    if sequence > self.sequence:
                        logger.debug(f"Processing message with sequence {sequence}")
                        for update in data['payload']:
                            await self.apply_order_update(update)  # Await the coroutine
                        self.sequence = sequence
                        logger.debug(f"Processed message. New sequence: {self.sequence}")
                    else:
                        logger.debug(f"Skipping message with old sequence {sequence}")
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed. Reconnecting...")
                await self.connect_websocket()
            except json.JSONDecodeError:
                logger.error("Failed to parse message")
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}", exc_info=True)


    async def log_order_book_periodically(self):
        while True:
            await asyncio.sleep(10)
            self.log_order_book()

    def log_order_book(self):
        logger.debug("Current Order Book:")
        logger.debug("Bids:")
        for price, order in sorted(self.order_book['bids'].items(), reverse=True)[:5]:
            logger.debug(f"  Price: {price}, Amount: {order.get('amount', 'N/A')}")
        logger.debug("Asks:")
        for price, order in sorted(self.order_book['asks'].items())[:5]:
            logger.debug(f"  Price: {price}, Amount: {order.get('amount', 'N/A')}")

    def calculate_weighted_average(self, side, target_mxn):
        total_mxn = 0
        total_amount = 0
        orders = sorted(self.order_book[side].items(), reverse=(side == 'bids'))
        
        for price, order in orders:
            price = float(price)
            amount = float(order['amount'])
            value = price * amount
            
            mxn_to_add = min(value, target_mxn - total_mxn)
            amount_to_add = mxn_to_add / price
            
            total_mxn += mxn_to_add
            total_amount += amount_to_add
            
            if total_mxn >= target_mxn:
                break
        
        return total_mxn / total_amount if total_amount > 0 else 0

    def get_reference_prices(self):
        highest_bid_wavg = self.calculate_weighted_average('bids', 50000)
        lowest_ask_wavg = self.calculate_weighted_average('asks', 50000)
        return highest_bid_wavg, lowest_ask_wavg

    async def log_reference_prices(self):
        highest_bid_wavg, lowest_ask_wavg = self.get_reference_prices()
        await TESTbitso_order_book_cache.update_reference_prices(highest_bid_wavg, lowest_ask_wavg)

async def start_bitso_order_book():
    order_book = BitsoOrderBook("usdt_mxn")
    await order_book.start()

if __name__ == "__main__":
    asyncio.run(start_bitso_order_book())
