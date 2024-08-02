# TESTbitso_order_book_cache.py
import asyncio

# Global cache for storing reference prices
reference_prices = {
    'highest_bid': None,
    'lowest_ask': None
}
price_lock = asyncio.Lock()

async def update_reference_prices(highest_bid, lowest_ask):
    global reference_prices
    async with price_lock:
        reference_prices['highest_bid'] = highest_bid
        reference_prices['lowest_ask'] = lowest_ask

async def get_reference_prices():
    async with price_lock:
        return reference_prices.copy()
