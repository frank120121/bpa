from common_utils_db import execute_and_commit, handle_error
import logging
logger = logging.getLogger(__name__)

async def update_order_details(conn, order_no, account_number):
        # Prepare the SQL statement for updating the order
    sql = '''
        UPDATE orders
        SET account_number = ?
        WHERE order_no = ?
    '''

    # Execute the SQL statement with the provided account number and order number
    await conn.execute(sql, (account_number, order_no))

    # Commit the changes to the database
    await conn.commit()

async def update_buyer_bank(conn, buyer_name, new_buyer_bank):
    """
    Update the user_bank field in the orders table for a given order_no.

    Args:
    - conn (aiosqlite.Connection): The database connection.
    - buyer_name (str): The name to update.
    - new_buyer_bank (str): The new name of the buyer's bank.

    Returns:
    - None
    """
    update_query = "UPDATE users SET user_bank = ? WHERE name = ?"
    params = (new_buyer_bank, buyer_name)

    try:
        await execute_and_commit(conn, update_query, params)
        logger.debug(f"Updated user_bank for user {buyer_name} to {new_buyer_bank}")
    except Exception as e:
        handle_error(e, f"Failed to update user_bank for user {buyer_name}")

async def update_order_status(conn, order_no, order_status):
    sql = "UPDATE orders SET order_status = ? WHERE order_no = ?"
    params = (order_status, order_no)
    await execute_and_commit(conn, sql, params)

async def register_merchant(conn, sellerName):
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
    
async def update_kyc_status(conn, name, new_kyc_status):
    try:
        sql = "UPDATE users SET kyc_status = ? WHERE name = ?"
        params = (new_kyc_status, name)
        await execute_and_commit(conn, sql, params)
        logger.debug(f"Updated KYC status for user {name} to {new_kyc_status}")
    except Exception as e:
        logger.error(f"Error updating KYC status for user {name}: {e}")

async def update_anti_fraud_stage(conn, buyer_name, new_stage):
    async with conn.cursor() as cursor:
        await cursor.execute("UPDATE users SET anti_fraud_stage = ? WHERE name = ?", (new_stage, buyer_name))
        await conn.commit()
        logger.debug(f"Updated anti_fraud_stage for user {buyer_name} to {new_stage}")

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



async def update_order_details(conn, order_no, account_number):
        # Prepare the SQL statement for updating the order
    sql = '''
        UPDATE orders
        SET account_number = ?
        WHERE order_no = ?
    '''

    # Execute the SQL statement with the provided account number and order number
    await conn.execute(sql, (account_number, order_no))

    # Commit the changes to the database
    await conn.commit()

