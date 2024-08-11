import asyncio
from common_vars import DB_FILE
from common_utils_db import create_connection, execute_and_commit, print_table_contents
import logging

# Configure logging to show info level messages on the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def count_and_sum_orders_by_total_price(conn, table, trade_type, order_status, total_price, fiat_unit, order_date=None):
    async with conn.cursor() as cursor:
        # Construct the SQL query
        sql = f"SELECT total_price FROM {table} WHERE trade_type = ? AND order_status = ? AND fiat_unit = ?"
        params = [trade_type, order_status, fiat_unit]

        if order_date:
            sql += " AND order_date < ?"
            params.append(order_date)
        
        logger.info(f"Executing SQL: {sql} with params: {params}")
        await cursor.execute(sql, params)
        rows = await cursor.fetchall()
        
        greater_or_equal_count = sum(1 for row in rows if row[0] >= total_price)
        less_count = sum(1 for row in rows if row[0] < total_price)

        greater_or_equal_sum = sum(row[0] for row in rows if row[0] >= total_price)
        less_sum = sum(row[0] for row in rows if row[0] < total_price)

        logger.info(f"Orders with total_price >= {total_price}: {greater_or_equal_count}, Sum: {greater_or_equal_sum}")
        logger.info(f"Orders with total_price < {total_price}: {less_count}, Sum: {less_sum}")

        return (greater_or_equal_count, greater_or_equal_sum), (less_count, less_sum)

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        logger.info("Database connection established")
        
        # Example usage of the count_and_sum_orders_by_total_price function
        table = 'orders'
        trade_type = 'SELL'  # Check the correct trade_type
        order_status = 4  # Ensure this matches your table
        total_price = 55000.0
        fiat_unit = 'MXN'  # Check the correct fiat_unit
        order_date = None  # Example date, can be None

        (greater_or_equal_count, greater_or_equal_sum), (less_count, less_sum) = await count_and_sum_orders_by_total_price(conn, table, trade_type, order_status, total_price, fiat_unit, order_date)

        logger.info(f"Orders with total_price >= {total_price}: Count = {greater_or_equal_count}, Sum = {greater_or_equal_sum}")
        logger.info(f"Orders with total_price < {total_price}: Count = {less_count}, Sum = {less_sum}")
        
        await conn.close()
        logger.info("Database connection closed")
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == '__main__':
    asyncio.run(main())
