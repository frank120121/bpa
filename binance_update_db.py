import asyncio
from common_vars import DB_FILE
from common_utils_db import create_connection, execute_and_commit, print_table_contents
import logging

# Configure logging to show info level messages on the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def update_merchant_ids_for_existing_orders(conn):
    async with conn.cursor() as cursor:
        # Fetch all orders where merchant_id is NULL
        await cursor.execute("SELECT order_no, seller_name FROM orders WHERE merchant_id IS NULL")
        rows = await cursor.fetchall()
        
        if not rows:
            logger.info("No orders found with NULL merchant_id")
            return

        for row in rows:
            order_no, seller_name = row
            merchant_id = None
            
            if seller_name == "GUERRERO LOPEZ MARTHA":
                merchant_id = 2
            elif seller_name == "MUÑOZ PEREA MARIA FERNANDA":
                merchant_id = 1
            
            if merchant_id is not None:
                sql = "UPDATE orders SET merchant_id = ? WHERE order_no = ?"
                params = (merchant_id, order_no)
                await execute_and_commit(conn, sql, params)
                logger.info(f"Successfully updated merchant_id for order_no {order_no} to {merchant_id}")
            else:
                logger.info(f"No merchant_id update needed for order_no {order_no}")

async def delete_orders_with_null_merchant_id(conn):
    async with conn.cursor() as cursor:
        # Delete all orders where merchant_id is NULL
        sql = "DELETE FROM orders WHERE merchant_id IS NULL"
        await execute_and_commit(conn, sql)
        logger.info("Deleted orders with NULL merchant_id")

async def delete_orders_with_specific_status(conn):
    async with conn.cursor() as cursor:
        # Delete all orders where order_status is not 1, 4, or 2
        sql = "DELETE FROM orders WHERE order_status NOT IN (4, 8, 1, 2)"
        await execute_and_commit(conn, sql)
        logger.info("Deleted orders with order_status not in (4, 8, 1, 2)")

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        logger.info("Database connection established")
        # Call the function to update merchant_ids for existing orders
        # await update_merchant_ids_for_existing_orders(conn)
        # await delete_orders_with_specific_status(conn)
        
        # # Call the function to delete orders with null merchant_id
        # await delete_orders_with_null_merchant_id(conn)

        # Print table contents for verification
        # await print_table_contents(conn, 'orders')
        await conn.close()
        logger.info("Database connection closed")
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == '__main__':
    asyncio.run(main())
