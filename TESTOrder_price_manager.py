import asyncio
import logging
from common_utils_db import create_connection, DB_FILE

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class USDTOrderManager:
    def __init__(self):
        self.orders = []
        self.lock = asyncio.Lock()
    
    async def load_orders_from_db(self):
        pass  # Implementation not required for this example
    
    async def save_orders_to_db(self):
        pass  # Implementation not required for this example
    
    async def add_order(self, usdt_amount, mxn_amount, exchange_rate):
        async with self.lock:
            self.orders.append({'usdt_amount': usdt_amount, 'mxn_amount': mxn_amount, 'exchange_rate': exchange_rate})
            logger.info(f"Order added: USDT Amount: {usdt_amount}, MXN Amount: {mxn_amount}, Exchange Rate: {exchange_rate}")
    
    async def remove_order(self, order_id):
        async with self.lock:
            self.orders = [order for order in self.orders if order['id'] != order_id]
            logger.info(f"Order with ID {order_id} removed.")
    
    async def get_best_exchange_rate(self, mxn_amount, target_exchange_rate):
        async with self.lock:
            logger.info(f"Calculating best exchange rate for MXN Amount: {mxn_amount} and Target Exchange Rate: {target_exchange_rate}")
            orders_sorted = sorted(self.orders, key=lambda x: x['exchange_rate'])
            total_mxn = 0
            total_usdt = 0
            for order in orders_sorted:
                if total_mxn + order['mxn_amount'] <= mxn_amount:
                    total_mxn += order['mxn_amount']
                    total_usdt += order['usdt_amount']
                    logger.info(f"Including full order: {order}")
                else:
                    remaining_mxn = mxn_amount - total_mxn
                    usdt_fraction = (remaining_mxn / order['mxn_amount']) * order['usdt_amount']
                    total_mxn += remaining_mxn
                    total_usdt += usdt_fraction
                    logger.info(f"Including partial order: {order}, Remaining MXN: {remaining_mxn}, USDT Fraction: {usdt_fraction}")
                    break

            weighted_exchange_rate = total_mxn / total_usdt if total_usdt != 0 else float('inf')
            logger.info(f"Weighted Exchange Rate: {weighted_exchange_rate}")
            return min(weighted_exchange_rate, target_exchange_rate)

async def fetch_orders(conn, target_mxn):
    logger.info("Fetching orders from the database.")
    cursor = await conn.cursor()
    await cursor.execute("""
        SELECT id, total_price, amount 
        FROM orders 
        WHERE trade_type = 'BUY' AND order_status = 8 AND fiat_unit = 'MXN' AND asset = 'USDT'
        ORDER BY order_date DESC
    """)
    rows = await cursor.fetchall()
    await cursor.close()

    orders = []
    total_mxn = 0

    for row in rows:
        order_id, total_price, amount = row
        if total_mxn + total_price <= target_mxn:
            total_mxn += total_price
            orders.append({'id': order_id, 'total_price': total_price, 'amount': amount})
            logger.info(f"Full order included: ID: {order_id}, Total Price: {total_price}, Amount: {amount}")
        else:
            remaining_mxn = target_mxn - total_mxn
            partial_usdt = (remaining_mxn / total_price) * amount
            orders.append({'id': order_id, 'total_price': remaining_mxn, 'amount': partial_usdt})
            logger.info(f"Partial order included: ID: {order_id}, Remaining MXN: {remaining_mxn}, Partial USDT: {partial_usdt}")
            break

    logger.info(f"Fetched {len(orders)} orders.")
    return orders

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        try:
            target_mxn = 50000  # The target MXN amount
            orders = await fetch_orders(conn, target_mxn)

            manager = USDTOrderManager()
            for order in orders:
                exchange_rate = order['total_price'] / order['amount']
                await manager.add_order(order['amount'], order['total_price'], exchange_rate)

            # Example usage of the get_best_exchange_rate function
            target_exchange_rate = 20.5
            best_rate = await manager.get_best_exchange_rate(100000, target_exchange_rate)
            logger.info(f"The best exchange rate is: {best_rate}")

        finally:
            await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == "__main__":
    asyncio.run(main())
