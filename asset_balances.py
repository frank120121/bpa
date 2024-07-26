import sqlite3
import asyncio
import aiosqlite
import logging
from common_utils_db import print_table_contents, create_connection
logger = logging.getLogger(__name__)

DATABASE_PATH = 'C:/Users/p7016/Documents/bpa/asset_balances.db'

def setup_bank_accounts_db():
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bank_accounts (
                id INTEGER PRIMARY KEY,
                account_number TEXT UNIQUE,
                account_name TEXT,
                bank_name TEXT,
                account_balance FLOAT DEFAULT 0
            )
        ''')
        connection.commit()
    except sqlite3.Error as e:
        logger.error(f"Failed to create bank_accounts table, Error: {str(e)}")
    finally:
        if connection:
            connection.close()

def add_bank_account(account_number, account_name, bank_name):
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO bank_accounts (account_number, account_name, bank_name)
            VALUES (?, ?, ?)
        ''', (account_number, account_name, bank_name))

        connection.commit()
    except sqlite3.IntegrityError:
        logger.error(f"Account number {account_number} already exists.")
    except sqlite3.Error as e:
        logger.error(f"Failed to add bank account {account_number}, Error: {str(e)}")
    finally:
        if connection:
            connection.close()

def setup_total_balances_db():
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS total_balances (
                asset TEXT PRIMARY KEY,
                total_balance FLOAT
            )
        ''')
        connection.commit()
    except sqlite3.Error as e:
        logger.error(f"Failed to create total_balances table, Error: {str(e)}")
    finally:
        if connection:
            connection.close()


def setup_db():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS balances (
            id INTEGER PRIMARY KEY,
            exchange_id INTEGER,
            account TEXT,
            asset TEXT,
            balance FLOAT,
            UNIQUE(exchange_id, account, asset)
        )
    ''')
    connection.commit()
    connection.close()


def update_total_balances():
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()

        # Get the total balances from balances table
        cursor.execute('''
            SELECT asset, SUM(balance)
            FROM balances
            GROUP BY asset
        ''')
        aggregated_balances = cursor.fetchall()

        # Update the total_balances table
        for asset, total_balance in aggregated_balances:
            cursor.execute('''
                INSERT OR REPLACE INTO total_balances (asset, total_balance)
                VALUES (?, ?)
            ''', (asset, total_balance))
        
        connection.commit()
    except sqlite3.Error as e:
        logger.error(f"Failed to update total_balances table, Error: {str(e)}")
    finally:
        if connection:
            connection.close()

def update_balance(exchange_id, account, combined_balances):
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    
    # First, get all assets currently in the database for this account
    cursor.execute('''
        SELECT asset FROM balances 
        WHERE exchange_id = ? AND account = ?
    ''', (exchange_id, account))
    existing_assets = set(row[0] for row in cursor.fetchall())

    # Update or insert balances
    for asset, balance in combined_balances.items():
        logging.debug(f"Updating {account} - {asset}: {balance}")
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO balances (exchange_id, account, asset, balance)
                VALUES (?, ?, ?, ?)
            ''', (exchange_id, account, asset, balance))
            existing_assets.discard(asset)  # Remove from set as it's been updated
            logging.debug(f"Successfully updated {account} - {asset}: {balance}")
        except sqlite3.Error as e:
            logging.error(f"Failed to update {account} - {asset}: {balance}, Error: {str(e)}")

    # Set balance to zero for assets not in combined_balances but in the database
    for asset in existing_assets:
        logging.debug(f"Setting zero balance for {account} - {asset}")
        try:
            cursor.execute('''
                UPDATE balances SET balance = 0
                WHERE exchange_id = ? AND account = ? AND asset = ?
            ''', (exchange_id, account, asset))
            logging.debug(f"Successfully set zero balance for {account} - {asset}")
        except sqlite3.Error as e:
            logging.error(f"Failed to set zero balance for {account} - {asset}, Error: {str(e)}")

    connection.commit()
    connection.close()

def get_balance(exchange_id, account):
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT asset, balance 
            FROM balances 
            WHERE exchange_id = ? AND account = ?
        ''', (exchange_id, account))
        balances = {asset: balance for asset, balance in cursor.fetchall()}
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return {}
    finally:
        if connection:
            connection.close()
    return balances
def get_all_balances():
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT exchange_id, account, asset, balance 
            FROM balances
        ''')
        balances_data = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if connection:
            connection.close()
    return balances_data

def get_total_asset_balances():
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT asset, SUM(balance)
            FROM balances
            GROUP BY asset
        ''')
        total_balances = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if connection:
            connection.close()
    return total_balances

async def total_usd():
    try:
        async with aiosqlite.connect(DATABASE_PATH) as connection:
            async with connection.cursor() as cursor:
                # Query for USD-related assets
                await cursor.execute('''
                    SELECT SUM(balance)
                    FROM balances
                    WHERE asset IN ('USD', 'USDC', 'USDT', 'TUSD')
                ''')
                total_usd = await cursor.fetchone()
                total_usd = total_usd[0] if total_usd[0] is not None else 0

        logger.debug(f"Total USD (including USDC, USDT, TUSD): {total_usd}")
        return total_usd
    except Exception as e:
        logger.error(f"Database error: {e}")
        return 0

async def main():
    usd_balance = await total_usd()
    print(f"Total USD balance: {usd_balance}")
    #print total asset balances
    total_balances = get_total_asset_balances()
    print(f"Total asset balances:{total_balances}")
    conn = await create_connection(DATABASE_PATH)
    await print_table_contents(conn, 'balances')
if __name__ == "__main__":
    asyncio.run(main())
