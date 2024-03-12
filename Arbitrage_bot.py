import asyncio
import websockets
import json
import time
from datetime import datetime

class ArbitrageBot:
    def __init__(self):
        self.pairs = [
            'BTCMMXN', 'ETHMMXN', 'XRPMMXN', 'USDTMMXN', 'SOL8MMXN', 'LTCMMXN', 'MATICMMXN',
            'ADAMMXN', 'DOGEMMXN', 'DOTMMXN', 'OPMMXN', 'NEARMMXN', 'ETCMMXN', 'APTMMXN', 'USDCMMXN', 'AVAXMMXN'
        ]
        self.order_book = {pair: {'bids': [], 'asks': []} for pair in self.pairs}
        self.connected = False

    async def send_custom_ping(self, ws):
        while self.connected:
            await asyncio.sleep(300)
            ping_message = {
                "ping": int(time.time() * 1000)  
            }
            await ws.send(json.dumps(ping_message))

    async def connect_diff_depth_stream(self):
        uri = "wss://ws.trubit.com/openapi/quote/ws/v1"
        subscription_message = {
            "symbol": ','.join(self.pairs),
            "topic": "diffDepth",  # Change the topic to "diffDepth"
            "event": "sub",
            "params": {
                "binary": False
            }
        }

        while True:
            try:
                async with websockets.connect(uri) as ws:
                    self.connected = True
                    asyncio.create_task(self.send_custom_ping(ws))
                    await ws.send(json.dumps(subscription_message))

                    async for message in ws:
                        data = json.loads(message)
                        await self.handle_diff_depth_data(data)

            except websockets.ConnectionClosedError:
                self.connected = False
                print(f"[{datetime.now()}] Connection closed, trying to reconnect in 5 seconds...")
                await asyncio.sleep(5)

    async def connect_trade_stream(self):
        uri = "wss://ws.trubit.com/openapi/quote/ws/v1"
        subscription_message = {
            "symbol": ','.join(self.pairs),
            "topic": "trade",
            "event": "sub",
            "params": {
                "binary": False
            }
        }

        while True:
            try:
                async with websockets.connect(uri) as ws:
                    self.connected = True
                    asyncio.create_task(self.send_custom_ping(ws))
                    await ws.send(json.dumps(subscription_message))

                    async for message in ws:
                        data = json.loads(message)
                        await self.handle_trade_data(data)

            except websockets.ConnectionClosedError:
                self.connected = False
                print(f"[{datetime.now()}] Connection to Trade stream closed, trying to reconnect in 5 seconds...")
                await asyncio.sleep(5)

    async def handle_diff_depth_data(self, data):
        if 'pong' in data:
            return

        if 'symbol' not in data:
            print(f"Unexpected data received: {data}")
            return

        symbol = data['symbol']
    
        if 'data' not in data:
            print(f"Unexpected data format received: {data}")
            return

        # Extract the bid and ask data from the 'data' list
        data_list = data['data']
        if len(data_list) == 0:
            print(f"No data received for {symbol}")
            return

        bids = data_list[0]['b']  # Bid data
        asks = data_list[0]['a']  # Ask data

        if symbol in self.order_book:
            self.order_book[symbol]['bids'] = bids
            self.order_book[symbol]['asks'] = asks

        lowest_ask = min(asks, key=lambda x: float(x[0])) if asks else None
        highest_bid = max(bids, key=lambda x: float(x[0])) if bids else None

        if symbol in ['USDCMMXN', 'USDTMMXN']:
            print(f"For {symbol}, Top Bid: {highest_bid}, Top Ask: {lowest_ask}")
            
    async def handle_trade_data(self, data):
        if 'pong' in data:
            return

        if 'symbol' not in data or 'data' not in data:
            print(f"Unexpected data received: {data}")
            return

        symbol = data['symbol']
        for trade in data['data']:
            trade_price = trade['p']
            trade_qty = trade['q']
            trade_time = datetime.fromtimestamp(trade['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            trade_type = "BUY" if trade['m'] else "SELL"

            # Display the trade details
            if symbol in ['USDCMMXN', 'USDTMMXN']:
                print(f"[{trade_time}] {symbol} {trade_type} Trade: Price {trade_price}, Quantity {trade_qty}")


async def main():
    bot = ArbitrageBot()
    await asyncio.gather(bot.connect_diff_depth_stream(), bot.connect_trade_stream())

if __name__ == "__main__":
    asyncio.run(main())