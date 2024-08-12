import asyncio
import websockets
import json
from datetime import datetime

class BinancePriceListener:
    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.last_price = None
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@aggTrade"
        self.reconnect_interval = 5 

    async def start(self):
        await self.run_forever()

    async def process_msg_stream(self, message):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] Received Message: {message}")  # Debug print
        msg = json.loads(message)
        if 'p' in msg:
            self.last_price = float(msg['p'])
            print(f"Updated Price: {self.last_price}")
        else:
            print("Message format is unexpected, no price found.")

    async def run_forever(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{current_time}] WebSocket connection opened.")
                    try:
                        while True:
                            message = await ws.recv()
                            await self.process_msg_stream(message)
                    except websockets.ConnectionClosed as e:
                        print(f"WebSocket connection closed: {e}")
                    except Exception as e:
                        print(f"An error occurred while receiving message: {e}")
            except Exception as e:
                print(f"Failed to connect to WebSocket: {e}")

            print("Reconnecting in 5 seconds.")
            await asyncio.sleep(self.reconnect_interval)

    def get_current_price(self):
        return self.last_price

if __name__ == "__main__":
    async def main():
        try:
            price_listener = BinancePriceListener('USDTMXN')
            await price_listener.start()
        except KeyboardInterrupt:
            print("Stopping the listener.")

    asyncio.run(main())
