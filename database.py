
import aiosqlite
import asyncio
from prettytable import PrettyTable
import logging
from logging_config import setup_logging
setup_logging(log_filename='Database_logger.log')
logger = logging.getLogger(__name__)
async def create_connection(db_file, num_retries=3, delay_seconds=5):
    logger.debug("Inside async_create_connection function")
    conn = None
    retries = 0
    while retries < num_retries:
        try:
            conn = await aiosqlite.connect(db_file)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}. Retrying in {delay_seconds} seconds.")
            await asyncio.sleep(delay_seconds)
            retries += 1
    logger.error("Max retries reached. Could not establish the database connection.")
    return None
def handle_error(e, message_prefix):
    if isinstance(e, Exception):
        logger.error(f"Database error: {e}")
    else:
        logger.error(f"{message_prefix}: {e}")
async def execute_and_commit(conn, sql, params=None):
    try:
        cursor = await conn.execute(sql, params)
        await conn.commit()
        await cursor.close()
    except Exception as e:
        handle_error(e, "Exception in execute_and_commit")
async def update_order_status(conn, order_no, order_status):
    sql = "UPDATE orders SET order_status = ? WHERE order_no = ?"
    params = (order_status, order_no)
    await execute_and_commit(conn, sql, params)

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
async def create_table(conn, create_table_sql):
    async with conn.cursor() as cursor:
        await cursor.execute(create_table_sql)
async def order_exists(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT id FROM orders WHERE order_no = ?", (order_no,))
        row = await cursor.fetchone()
        return bool(row)
async def find_or_insert_merchant(conn, sellerName):
    if not sellerName: 
        logger.error(f"Provided sellerName is invalid: {sellerName}")
        return None
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT id FROM merchants WHERE sellerName = ?", (sellerName,))
        row = await cursor.fetchone()
        if row:
            return row[0]
        else:
            await cursor.execute("INSERT INTO merchants (sellerName) VALUES (?)", (sellerName,))
            return cursor.lastrowid
async def find_or_insert_buyer(conn, buyer_name):
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            INSERT OR IGNORE INTO users 
            (name, kyc_status, total_crypto_sold_lifetime) 
            VALUES (?, 0, 0.0)
            """, 
            (buyer_name,)
        )
        await cursor.execute(
            "SELECT id FROM users WHERE name = ?", 
            (buyer_name,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None
async def update_total_spent(conn, order_no):
    try:
        order_sql = """
            SELECT buyer_name, seller_name, total_price, order_date
            FROM orders
            WHERE order_no = ?
        """
        async with conn.cursor() as cursor:
            await cursor.execute(order_sql, (order_no,))
            order_details = await cursor.fetchone()
            if not order_details:
                print(f"No order found with order_no: {order_no}")
                return
            
            buyer_name, seller_name, total_price, order_date = order_details
        update_user_sql = """
            UPDATE users 
            SET total_crypto_sold_lifetime = total_crypto_sold_lifetime + ?
            WHERE name = ?
        """
        await execute_and_commit(conn, update_user_sql, (total_price, buyer_name))
        await insert_transaction(conn, buyer_name, seller_name, total_price, order_date)
    except Exception as e:
        print(f"An error occurred in update_total_spent: {e}")

async def insert_transaction(conn, buyer_name, seller_name, total_price, order_date):
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            INSERT OR IGNORE INTO transactions 
            (buyer_name, seller_name, total_price, order_date) 
            VALUES (?, ?, ?, ?)
            """, 
            (buyer_name, seller_name, total_price, order_date)
        )
async def insert_order(conn, order_tuple):
    async with conn.cursor() as cursor:
        await cursor.execute('''INSERT INTO orders(order_no, buyer_name, seller_name, trade_type, order_status, total_price, fiat_unit, asset, amount)
                                VALUES(?,?,?,?,?,?,?,?,?)''', order_tuple)
        logger.debug(f"Inserted new order: {order_tuple[0]}")
        return cursor.lastrowid
async def insert_or_update_order(conn, order_details):
    try:
        logger.debug(f"Order Details Received in db:")
        data = order_details.get('data', {})
        seller_name = data.get('sellerName') or data.get('sellerNickname')
        buyer_name = data.get('buyerName')
        order_no = data.get('orderNumber')
        trade_type = data.get('tradeType')
        order_status = data.get('orderStatus')
        total_price = data.get('totalPrice')
        fiat_unit = data.get('fiatUnit')
        asset = data.get('asset')
        amount = data.get('amount')
        if None in (seller_name, buyer_name, order_no, trade_type, order_status, total_price, fiat_unit):
            logger.error("One or more required fields are None. Aborting operation.")
            return
        if await order_exists(conn, order_no):
            logger.debug("Updating existing order...")
            sql = """
                UPDATE orders
                SET order_status = ?
                WHERE order_no = ?
            """
            params = (order_status, order_no)
            await execute_and_commit(conn, sql, params)
        else:
            logger.debug("Inserting new order...")
            await find_or_insert_merchant(conn, seller_name)
            await find_or_insert_buyer(conn, buyer_name)
            await insert_order(conn, (order_no, buyer_name, seller_name, trade_type, order_status, total_price, fiat_unit, asset, amount))

    except Exception as e:
        logger.error(f"Error in insert_or_update_order: {e}")
        print(f"Exception details: {e}")

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
async def update_kyc_status(conn, name, new_kyc_status):
    try:
        sql = "UPDATE users SET kyc_status = ? WHERE name = ?"
        params = (new_kyc_status, name)
        await execute_and_commit(conn, sql, params)
        logger.debug(f"Updated KYC status for user {name} to {new_kyc_status}")
    except Exception as e:
        logger.error(f"Error updating KYC status for user {name}: {e}")
async def get_anti_fraud_stage(conn, name):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT anti_fraud_stage FROM users WHERE name=?", (name,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        return None
async def update_anti_fraud_stage(conn, buyer_name, new_stage):
    async with conn.cursor() as cursor:
        await cursor.execute("UPDATE users SET anti_fraud_stage = ? WHERE name = ?", (new_stage, buyer_name))
        await conn.commit()
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
async def set_menu_presented(conn, order_no, value):
    """
    Set the menu_presented field in the orders table to either True or False.

    Parameters:
    - conn: a database connection object
    - order_no: the order number
    - value: boolean indicating if the menu was presented

    Returns:
    - None
    """
    try:
        sql = "UPDATE orders SET menu_presented = ? WHERE order_no = ?"
        params = (1 if value else 0, order_no)  # Convert to SQLite's BOOLEAN representation
        await execute_and_commit(conn, sql, params)
    except Exception as e:
        logger.error(f"Error setting menu_presented for order_no {order_no}: {e}")

async def print_table_contents(conn, table_name):
    async with conn.cursor() as cursor:
        try:
            await cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = await cursor.fetchall()
            column_names = [column[1] for column in columns_info]
            await cursor.execute(f"SELECT * FROM {table_name}")
            rows = await cursor.fetchall()
            table = PrettyTable()
            table.field_names = column_names
            for row in rows:
                table.add_row(row)
            print(f"\nContents of {table_name}:")
            print(table)
        except Exception as e:
            print(f"Error reading from table {table_name}: {e}")
async def add_column_if_not_exists(conn, table_name, column_definition):
    async with conn.cursor() as cursor:
        await cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = await cursor.fetchall()
        column_names = [column[1] for column in columns_info]

        if column_definition.split()[0] not in column_names:
            alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_definition}"
            await cursor.execute(alter_table_sql)

async def main():
    database = "C:/Users/p7016/Documents/bpa/orders_data.db"
    sql_create_merchants_table = """CREATE TABLE IF NOT EXISTS merchants (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                sellerName TEXT NOT NULL UNIQUE
                                );"""
    sql_create_users_table = """CREATE TABLE IF NOT EXISTS users (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    name TEXT NOT NULL UNIQUE,
                                    kyc_status INTEGER DEFAULT 0,
                                    total_crypto_sold_lifetime REAL,
                                    anti_fraud_stage INTEGER DEFAULT 0,
                                    rfc TEXT NULL,  -- RFC can be NULL
                                    codigo_postal TEXT NULL  -- Codigo Postal can be NULL
                                    );"""

    sql_create_transactions_table = """CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        buyer_name TEXT,
        seller_name TEXT,
        total_price REAL,
        order_date TIMESTAMP
    );"""
    sql_create_orders_table = """CREATE TABLE IF NOT EXISTS orders (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              order_no TEXT NOT NULL UNIQUE,
                              buyer_name TEXT,
                              seller_name TEXT,
                              trade_type TEXT,
                              order_status INTEGER,
                              total_price REAL,
                              fiat_unit TEXT,
                              asset TEXT,
                              amount REAL,
                              account_number TEXT,
                              menu_presented BOOLEAN DEFAULT FALSE,
                              order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              );"""


    conn = await create_connection(database)
    if conn is not None:
        await create_table(conn, sql_create_merchants_table)
        await create_table(conn, sql_create_users_table)
        await create_table(conn, sql_create_transactions_table)
        await create_table(conn, sql_create_orders_table)
        #await add_column_if_not_exists(conn, 'orders', 'account_number TEXT')
        # Print table contents for verification
        await print_table_contents(conn, 'orders')

        await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == '__main__':
    asyncio.run(main())