import asyncio
import binance_db
import binance_db_get
import random

def generate_random_order_no():
    """Generate a random order number for testing purposes."""
    return f"TEST_{random.randint(1000, 9999)}"

async def test_order_insertion():
    # 1. Setup database
    conn = await binance_db.create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
    if not conn:
        print("Failed to connect to the database.")
        return

    # 2. Create a new order and add it to the database
    order_details = {
        'data': {
            'sellerName': 'TestSeller',
            'buyerName': 'TestBuyer',
            'orderNumber': generate_random_order_no(),
            'tradeType': 'TestTrade',
            'orderStatus': 1,
            'totalPrice': 1234.56,
            'fiatUnit': 'USD'
        }
    }

    await binance_db.insert_or_update_order(conn, order_details)

    # 3. Fetch the order from the database to verify its presence
    order_data = await binance_db_get.get_order_details(conn, order_details['data']['orderNumber'])

    if order_data:
        print("Order was added successfully!")
        print(order_data)
    else:
        print("Failed to add the order.")

    await conn.close()

if __name__ == '__main__':
    asyncio.run(test_order_insertion())
