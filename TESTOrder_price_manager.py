import asyncio
import math
from collections import defaultdict
import logging
from common_utils_db import create_connection, DB_FILE, execute_and_commit, clear_table, print_table_contents, print_table_schema

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExchangeRateManager:
    def __init__(self):
        self.orders = []
        self.lock = asyncio.Lock()

    async def load_orders_from_db(self, conn):
        async with self.lock:
            cursor = await conn.cursor()
            await cursor.execute("SELECT * FROM usd_price_manager")
            rows = await cursor.fetchall()
            self.orders = [{'id': row[0], 'trade_type': row[1], 'exchange_rate_ratio': row[2], 'mxn_amount': row[3]} for row in rows]
            logger.debug(f"Loaded {len(self.orders)} orders from usd_price_manager.")

    async def save_orders_to_db(self, conn):
        async with self.lock:
            await execute_and_commit(conn, "DELETE FROM usd_price_manager")  # Clear the table before saving
            for order in self.orders:
                insert_sql = """
                INSERT INTO usd_price_manager (trade_type, exchange_rate_ratio, mxn_amount)
                VALUES (?, ?, ?)
                """
                params = (order['trade_type'], order['exchange_rate_ratio'], order['mxn_amount'])
                await execute_and_commit(conn, insert_sql, params)
                logger.debug(f"Saved order to usd_price_manager: {order}")
            logger.debug(f"Saved {len(self.orders)} orders to usd_price_manager.")

    async def add_order(self, trade_type, exchange_rate_ratio, mxn_amount):
        try:
            async def add_order_with_lock():
                async with self.lock:
                    order_id = len(self.orders) + 1
                    self.orders.append({'id': order_id, 'trade_type': trade_type, 'exchange_rate_ratio': exchange_rate_ratio, 'mxn_amount': mxn_amount})

            # Use wait_for instead of timeout
            await asyncio.wait_for(add_order_with_lock(), timeout=10.0)  # 10 seconds timeout
        except asyncio.TimeoutError:
            logger.error(f"Timeout while trying to add order: Trade Type: {trade_type}, Exchange Rate Ratio: {exchange_rate_ratio}, MXN Amount: {mxn_amount}")
        except Exception as e:
            logger.error(f"Error while adding order: {e}")

    async def remove_order(self, order_id):
        async with self.lock:
            self.orders = [order for order in self.orders if order['id'] != order_id]
            logger.debug(f"Order with ID {order_id} removed.")

    async def get_best_exchange_rate(self, trade_type, mxn_amount, exchange_rate_1, exchange_rate_2):
        async with self.lock:
            logger.info(f"Calculating best exchange rate for MXN Amount: {mxn_amount}")
            orders_sorted = sorted(self.orders, key=lambda x: x['exchange_rate_ratio'], reverse=True)
            #the orders with trade_type given
            orders_sorted = [order for order in orders_sorted if order['trade_type'] == trade_type]
            # sum the orders with exchange_rate_ratio above exchange_rate_2
            if trade_type == 'SELL':
                fee_1 = 0.9986
                fee_2 = 0.987
            else:
                fee_1 = 1.0014
                fee_2 = 1.013
            rate = 0
            logger.info(f"Orders sorted: {orders_sorted}")
            total_mxn = 0
            for order in orders_sorted:
                if ((order['exchange_rate_ratio'] > exchange_rate_2) if trade_type == 'SELL' else (order['exchange_rate_ratio'] < exchange_rate_2)):
                    total_mxn += order['mxn_amount']
                    if total_mxn >= mxn_amount:
                        break
            if total_mxn >= mxn_amount:
                rate = exchange_rate_1 * fee_1
                logger.info(f"Have enough orders to cover: {total_mxn}")
                logger.info(f"Best exchange rate: {rate}")
                return rate
            else:
                total_mxn = 0
                rate = 0
                for order in orders_sorted:
                    if ((order['exchange_rate_ratio'] < exchange_rate_2) if trade_type == 'SELL' else (order['exchange_rate_ratio'] > exchange_rate_2)):
                        total_mxn += order['mxn_amount']
                        rate = order['exchange_rate_ratio']
                        logger.info(f"Total MXN: {total_mxn}, Best exchange rate so far: {rate}")
                        if total_mxn >= mxn_amount:
                            break
                if total_mxn >= mxn_amount:
                    rate *= fee_2
                    logger.info(f"Not enough orders sold above {exchange_rate_2} to cover {mxn_amount}. But we can do: {rate}")
                    return rate
                else:
                    rate = exchange_rate_1 * fee_2
                    logger.info(f"Not enough orders sold. Best we can do: {(rate)}")
                    return rate        

    async def populate_usd_price_manager(self, conn):
        logger.debug("Starting to populate usd_price_manager")
        cursor = await conn.cursor()
        await cursor.execute("""
            SELECT trade_type, total_price, currency_rate, priceFloatingRatio
            FROM orders
            WHERE fiat_unit = 'MXN' 
            AND priceFloatingRatio IS NOT NULL 
            AND priceFloatingRatio != 0
            AND order_status IN (4, 8)
        """)
        rows = await cursor.fetchall()
        logger.debug(f"Populating usd_price_manager with {len(rows)} orders.")

        aggregated_orders = defaultdict(float)  # Using a defaultdict to sum mxn_amount

        for row in rows:
            trade_type, total_price, currency_rate, priceFloatingRatio = row
            exchange_rate_ratio = (currency_rate * (priceFloatingRatio / 100))
            exchange_rate_ratio_rounded = round_up_to_nearest_05(exchange_rate_ratio)
            aggregated_orders[(trade_type, exchange_rate_ratio_rounded)] += total_price


        for (trade_type, exchange_rate_ratio), total_price in aggregated_orders.items():
            try:
                await self.add_order(trade_type, exchange_rate_ratio, total_price)
            except Exception as e:
                logger.error(f"Failed to add order: {e}")

        logger.debug(f"usd_price_manager populated with {len(self.orders)} orders.")


def round_up_to_nearest_05(value):
    # Multiply by 20, take the ceiling, then divide by 20
    return math.ceil(value * 20) / 20

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        try:
            manager = ExchangeRateManager()
            await print_table_contents(conn, 'usd_price_manager')
            await clear_table(conn, 'usd_price_manager')
            await print_table_contents(conn, 'usd_price_manager')
            await manager.populate_usd_price_manager(conn)
            ratio = await manager.get_best_exchange_rate('BUY', 10000, 19.16, 18.91)
            print(ratio)
            await manager.save_orders_to_db(conn)
            await print_table_contents(conn, 'usd_price_manager')
        finally:
            await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == "__main__":
    asyncio.run(main())





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