
import asyncio
from common_vars import DB_FILE
from common_utils_db import create_connection, execute_and_commit, print_table_contents, print_table_schema
import logging
logger = logging.getLogger(__name__)

async def order_exists(conn, order_no):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT id FROM orders WHERE order_no = ?", (order_no,))
        row = await cursor.fetchone()
        return bool(row)


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

async def insert_or_update_order(conn, order_details):
    try:
        logger.debug("Order Details Received in db:")
        data = order_details.get('data', {})

       # Extracting order details
        seller_name = data.get('sellerName') or data.get('sellerNickname', '')
        buyer_name = data.get('buyerName', '')
        order_no = data.get('orderNumber', '')
        trade_type = data.get('tradeType', '')
        order_status = data.get('orderStatus', None)
        total_price = float(data.get('totalPrice', 0))
        fiat_unit = data.get('fiatUnit', '')
        asset = data.get('asset', '')
        amount = float(data.get('amount', 0))
        currency_rate = float(data.get('currencyRate', 0))

        if None in (seller_name, buyer_name, order_no, trade_type, order_status, total_price, fiat_unit):
            logger.error("One or more required fields are None. Aborting operation.")
            return

        # Check if order exists and update or insert accordingly
        if await order_exists(conn, order_no):
            logger.debug("Updating existing order...")
            # Here we update all provided fields, ensuring that any changes are applied
            sql = """
                UPDATE orders
                SET order_status = ?, seller_name = ?, buyer_name = ?, trade_type = ?, total_price = ?, fiat_unit = ?, asset = ?, amount = ?, currency_rate = ?
                WHERE order_no = ?
            """
            params = (order_status, seller_name, buyer_name, trade_type, total_price, fiat_unit, asset, amount, currency_rate, order_no)
            await execute_and_commit(conn, sql, params)
        else:
            logger.debug("Inserting new order...")
            # Before inserting a new order, ensure the buyer exists or is inserted into the database
            await find_or_insert_buyer(conn, buyer_name)
            # Now insert the new order
            sql = """
                INSERT INTO orders (order_no, buyer_name, seller_name, trade_type, order_status, total_price, fiat_unit, asset, amount, currency_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (order_no, buyer_name, seller_name, trade_type, order_status, total_price, fiat_unit, asset, amount, currency_rate)
            await execute_and_commit(conn, sql, params)

        # Extract and insert bank identifiers for the order
        pay_methods = data.get('payMethods', [])
        for method in pay_methods:
            bank_identifier = method.get('identifier')
            # Ensure there's an identifier to insert
            if bank_identifier:  
                sql = """
                    INSERT INTO order_bank_identifiers (order_no, bank_identifier)
                    VALUES (?, ?)
                """
                params = (order_no, bank_identifier)
                await execute_and_commit(conn, sql, params)

    except Exception as e:
        logger.error(f"Error in insert_or_update_order: {e}")
        print(f"Exception details: {e}")

async def remove(conn, order_no):
    await conn.execute("DELETE FROM orders WHERE order_no = ?", (order_no,))
    await conn.commit()
async def remove_user(conn, name):
    await conn.execute("DELETE FROM users WHERE name = ?", (name,))
    await conn.commit()
async def get_orders_by_total_price(conn, total_price):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM orders WHERE total_price = ?", (total_price,))
            rows = await cursor.fetchall()
            return rows
    except Exception as e:
        logger.error(f"Error retrieving orders with total price {total_price}: {e}")
        return []

#async function to log orders with a given order status
async def log_orders_by_status(conn, order_status):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM orders WHERE order_status = ?", (order_status,))
            rows = await cursor.fetchall()
            return rows
    except Exception as e:
        logger.error(f"Error retrieving orders with order status {order_status}: {e}")
        return []

async def main():  
    sql_create_merchants_table = """CREATE TABLE IF NOT EXISTS merchants (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                sellerName TEXT NOT NULL UNIQUE,
                                api_key TEXT,  -- Encrypted API key
                                api_secret TEXT,  -- Encrypted API secret
                                email TEXT,
                                password_hash TEXT,
                                phone_num TEXT
                                );"""
    sql_create_users_table = """CREATE TABLE IF NOT EXISTS users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL UNIQUE,
                            kyc_status INTEGER DEFAULT 0,
                            total_crypto_sold_lifetime REAL,
                            anti_fraud_stage INTEGER DEFAULT 0,
                            rfc TEXT NULL,  -- RFC can be NULL
                            codigo_postal TEXT NULL,  -- Codigo Postal can be NULL
                            user_bank TEXT NULL,  -- Name of the user's bank
                            );"""

    sql_create_transactions_table = """CREATE TABLE IF NOT EXISTS transactions (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    buyer_name TEXT,
                                    seller_name TEXT,
                                    total_price REAL,
                                    order_date TIMESTAMP,
                                    merchant_id INTEGER REFERENCES merchants(id) 
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
                            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            buyer_bank TEXT, 
                            seller_bank_account TEXT,
                            merchant_id INTEGER REFERENCES merchants(id),
                            currency_rate REAL  
                            );"""
    sql_create_order_bank_identifiers_table = """CREATE TABLE IF NOT EXISTS order_bank_identifiers (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            order_no TEXT NOT NULL,
                            bank_identifier TEXT NOT NULL,
                            FOREIGN KEY (order_no) REFERENCES orders(order_no)
                            );"""


    conn = await create_connection(DB_FILE)
    if conn is not None:
        # await create_table(conn, sql_create_merchants_table)
        # await create_table(conn, sql_create_users_table)
        # await create_table(conn, sql_create_transactions_table)
        # await create_table(conn, sql_create_orders_table)
        # await create_table(conn, sql_create_order_bank_identifiers_table)

        # Print table contents for verification
        # await remove(conn, '20624872107382546432')
        # await remove_user(conn, 'LOPEZ GUERRERO FRANCISCO JAVIER')
        # await add_column_if_not_exists(conn, 'users', 'user_bank', 'TEXT', 'NULL')
        #log orders with a given order status
        order_status = 8
        orders = await log_orders_by_status(conn, order_status)
        for order in orders:
            print(order)
        # total_price = 10921.00  # Example total price to search for
        # orders = await get_orders_by_total_price(conn, total_price)
        # for order in orders:
        #     print(order)
        await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")
    
    
            
if __name__ == '__main__':
    asyncio.run(main())