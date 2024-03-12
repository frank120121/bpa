import sqlite3
import asyncio
import aiosqlite
import logging
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
    
    for asset, balance in combined_balances.items():
        logging.info(f"Updating {account} - {asset}: {balance}")
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO balances (exchange_id, account, asset, balance)
                VALUES (?, ?, ?, ?)
            ''', (exchange_id, account, asset, balance))
            logging.info(f"Successfully updated {account} - {asset}: {balance}")
        except sqlite3.Error as e:
            logging.error(f"Failed to update {account} - {asset}: {balance}, Error: {str(e)}")
        
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

                # Query for MXN
                await cursor.execute('''
                    SELECT SUM(balance)
                    FROM balances
                    WHERE asset = 'MXN'
                ''')
                total_mxn = await cursor.fetchone()
                total_mxn = total_mxn[0] if total_mxn[0] is not None else 0

        print(f"Total USD (including USDC, USDT, TUSD): {total_usd}")
        print(f"Total MXN: {total_mxn}")
    except Exception as e:
        print(f"Database error: {e}")

async def main():
    await total_usd()
if __name__ == "__main__":
    asyncio.run(main())
