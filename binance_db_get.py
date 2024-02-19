import aiosqlite
import asyncio
from common_vars import DB_FILE
from common_utils_db import create_connection, execute_and_commit, handle_error, print_table_contents, clear_table, create_table, remove_from_table, add_column_if_not_exists
import logging
logger = logging.getLogger(__name__)


async def get_buyer_bank(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT buyer_bank FROM orders WHERE order_no=?", (order_no,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
async def get_account_number(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT account_number FROM orders WHERE order_no=?", (order_no,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
    
async def get_order_details(conn, order_no):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM orders WHERE order_no=?", (order_no,))
            row = await cursor.fetchone()
            if row:
                column_names = [desc[0] for desc in cursor.description]
                return {column_names[i]: row[i] for i in range(len(row))}
            else:
                return None
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

async def order_exists(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT id FROM orders WHERE order_no = ?", (order_no,))
        row = await cursor.fetchone()
        return bool(row)
    
async def fetch_merchant_credentials(merchant_id):
    async with aiosqlite.connect(DB_FILE) as conn:  # Use your actual database connection here
        async with conn.execute("SELECT api_key, api_secret FROM merchants WHERE id = ?", (merchant_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    'KEY': row[0],  # Assuming the first column is the API key
                    'SECRET': row[1]  # Assuming the second column is the API secret
                }
            return None

async def calculate_crypto_sold_30d(conn, buyer_name):
    try:
        sql = """
            SELECT SUM(amount)
            FROM orders
            WHERE buyer_name = ? 
                AND order_status = 4
                AND order_date >= datetime('now', '-30 day')
        """
        params = (buyer_name,)
        total_crypto_sold_30d = await execute_and_fetchone(conn, sql, params)
        return total_crypto_sold_30d[0] if total_crypto_sold_30d else 0
    except Exception as e:
        logger.error(f"Error calculating crypto sold in the last 30 days: {e}")
        return 0

async def get_kyc_status(conn, name):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT kyc_status FROM users WHERE name=?", (name,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None

async def get_anti_fraud_stage(conn, name):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT anti_fraud_stage FROM users WHERE name=?", (name,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
    
async def is_menu_presented(conn, order_no):
    """
    Checks if the menu has been presented for a specific order.

    Args:
    - conn (sqlite3.Connection): SQLite database connection.
    - order_no (str): The order number to check.

    Returns:
    - bool: True if the menu has been presented, False otherwise.
    """
    async with conn.cursor() as cursor:
        await cursor.execute("""
            SELECT menu_presented
            FROM orders
            WHERE order_no = ?;
        """, (order_no,))
    
        result = await cursor.fetchone()
    
    if result:
        return result[0] == 1  # SQLite uses 1 for TRUE and 0 for FALSE.
    else:
        # Order doesn't exist or some other unexpected error.
        raise ValueError(f"No order found with order_no {order_no}")

async def execute_and_fetchone(conn, sql, params=None):
    """
    Execute a SQL query and fetch one result.

    Parameters:
    - conn: a database connection object
    - sql: a string containing a SQL query
    - params: a tuple with parameters to substitute into the SQL query

    Returns:
    - A single query result
    """
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, params)
            return await cursor.fetchone()
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

async def get_buyer_bank(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT buyer_bank FROM orders WHERE order_no=?", (order_no,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
async def get_account_number(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT account_number FROM orders WHERE order_no=?", (order_no,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
    
