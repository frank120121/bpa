#TESTbitso_order_book_cache.py
# Global cache for storing reference prices
reference_prices = {
    'highest_bid': None,
    'lowest_ask': None
}

def update_reference_prices(highest_bid, lowest_ask):
    global reference_prices
    reference_prices['highest_bid'] = highest_bid
    reference_prices['lowest_ask'] = lowest_ask

def get_reference_prices():
    return reference_prices
