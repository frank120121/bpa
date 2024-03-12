import asyncio
import websockets
import json

async def subscribe_to_bitso_websocket():
    async with websockets.connect("wss://ws.bitso.com") as websocket:
        # Define a list of trading pairs to subscribe to
        trading_pairs = ["usdt_mxn"]
        
        # Subscribe to the channels for each trading pair
        for pair in trading_pairs:
            await websocket.send(json.dumps({"action": "subscribe", "book": pair, "type": "trades"}))
            await websocket.send(json.dumps({"action": "subscribe", "book": pair, "type": "diff-orders"}))
            await websocket.send(json.dumps({"action": "subscribe", "book": pair, "type": "orders"}))
        
        # Listen for messages
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            
            if "type" in data:
                if data["type"] == "trades" and "payload" in data:
                    # Handle trades data
                    trades_payload = data["payload"]
                    # Process trades_payload as needed
                    print("Received trades data:", trades_payload)
                    
                elif data["type"] == "diff-orders" and "payload" in data:
                    # Handle diff-orders data
                    diff_orders_payload = data["payload"]
                    # Process diff_orders_payload as needed
                    print("Received diff-orders data:", diff_orders_payload)
                    
                elif data["type"] == "orders" and "payload" in data:
                    # Handle orders data
                    orders_payload = data["payload"]
                    # Process orders_payload as needed
                    print("Received orders data:", orders_payload)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(subscribe_to_bitso_websocket())
