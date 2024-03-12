import datetime
import asyncio
from common_vars import bank_accounts, DB_FILE
from common_utils_db import print_table_contents, create_connection
import logging

logger = logging.getLogger(__name__)

async def initialize_database(conn):
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS mxn_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            account_number TEXT,
            amount_deposited REAL,
            deposit_from TEXT DEFAULT NULL,
            year INTEGER DEFAULT NULL,
            month INTEGER DEFAULT NULL,
            merchant_id INTEGER REFERENCES merchants(id) 
        )
    ''')

    await conn.execute('''
        CREATE TABLE IF NOT EXISTS mxn_bank_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_bank_name TEXT,
            account_beneficiary TEXT,
            account_number TEXT UNIQUE,
            account_daily_limit REAL,
            account_monthly_limit REAL,
            account_balance REAL DEFAULT 0,
            last_used_timestamp DATETIME DEFAULT NULL,
            merchant_id INTEGER REFERENCES merchants(id)
        )
    ''')
    for account in bank_accounts:
        # Check if the account number already exists
        cursor = await conn.execute("SELECT 1 FROM mxn_bank_accounts WHERE account_number = ?", (account['account_number'],))
        if not await cursor.fetchone():
            await conn.execute(
                'INSERT INTO mxn_bank_accounts (account_bank_name, account_beneficiary, account_number, account_daily_limit, account_monthly_limit, account_balance) VALUES (?, ?, ?, ?, ?, ?)',
                (account['bank_name'], account['beneficiary'], account['account_number'], account['account_daily_limit'], account['account_monthly_limit'], 0))
        else:
            logger.debug(f"Account number {account['account_number']} already exists. Skipping insertion.")
    await conn.commit()

async def add_bank_account(conn, bank_name, beneficiary, account_number, account_daily_limit, account_monthly_limit, account_balance=0):
    try:
        await conn.execute(
            'INSERT INTO mxn_bank_accounts (account_bank_name, account_beneficiary, account_number, account_daily_limit, account_monthly_limit, account_balance) VALUES (?, ?, ?, ?, ?, ?)',
            (bank_name, beneficiary, account_number, account_daily_limit, account_monthly_limit, account_balance))
        await conn.commit()
        logger.debug(f"Added new bank account: {account_number}")
    except Exception as e:
        logger.error(f"Error adding bank account: {e}")
        raise

async def remove_bank_account(conn, account_number):
    try:
        await conn.execute('DELETE FROM mxn_bank_accounts WHERE account_number = ?', (account_number,))
        await conn.commit()
        logger.debug(f"Removed bank account: {account_number}")
    except Exception as e:
        logger.error(f"Error removing bank account: {e}")
        raise

# Create an async function that updates the account balance
async def update_account_balance(conn, account_number, amount):
    try:
        await conn.execute('UPDATE mxn_bank_accounts SET account_balance = ? WHERE account_number = ?', (amount, account_number))
        await conn.commit()
        logger.debug(f"Updated account balance for account: {account_number}")
    except Exception as e:
        logger.error(f"Error updating account balance: {e}")
        raise

async def update_last_used_timestamp(conn, account_number):
    try:
        # Format the current timestamp for SQLite DATETIME
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Execute the SQL command to update the last used timestamp
        await conn.execute(
            'UPDATE mxn_bank_accounts SET last_used_timestamp = ? WHERE account_number = ?',
            (current_timestamp, account_number)
        )

        # Commit the changes to the database
        await conn.commit()

        # Log the successful update
        logger.debug(f"Updated last_used_timestamp for account: {account_number} to {current_timestamp}")
    except Exception as e:
        # Log the error and re-raise it to maintain the behavior of update_account_balance
        logger.error(f"Error updating last_used_timestamp for account: {account_number}: {e}")
        raise

async def log_deposit(conn, deposit_from, bank_account_number, amount_deposited):
    timestamp = datetime.datetime.now()
    year, month = timestamp.year, timestamp.month
    await conn.execute('INSERT INTO mxn_deposits (timestamp, account_number, amount_deposited, deposit_from, year, month) VALUES (?, ?, ?, ?, ?, ?)',
                       (timestamp, bank_account_number, amount_deposited, deposit_from, year, month))
    await conn.execute('UPDATE mxn_bank_accounts SET account_balance = account_balance + ? WHERE account_number = ?', (amount_deposited, bank_account_number))
    await conn.commit()
    logger.debug(f"Logged deposit of {amount_deposited} from {deposit_from} to account {bank_account_number}")


async def sum_recent_deposits(account_number):
    conn = await create_connection(DB_FILE)
    
    # Calculate the timestamp 24 hours ago from now
    twenty_four_hours_ago = datetime.datetime.now() - datetime.timedelta(days=1)
    
    try:
        await initialize_database(conn)  # Assuming this function initializes your DB schemas
        
        # Query to find the sum of deposits for the given account in the last 24 hours
        async with conn.execute('''
            SELECT SUM(amount_deposited) FROM mxn_deposits
            WHERE account_number = ? AND timestamp > ?
        ''', (account_number, twenty_four_hours_ago,)) as cursor:
            sum_deposits = await cursor.fetchone()
            sum_deposits = sum_deposits[0] if sum_deposits[0] is not None else 0
        
        # Log the sum of the deposits
        logger.info(f"Total deposits for account {account_number} in the last 24 hours: MXN {sum_deposits}")
        
    except Exception as e:
        logger.error(f"Error calculating sum of recent deposits: {e}")
    finally:
        await conn.close()

async def clear_accounts(conn):
    await conn.execute("DELETE FROM mxn_bank_accounts")
    await conn.commit()

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        # Initialize the database (create tables and insert initial data)
        await clear_accounts(conn)
        await initialize_database(conn)
        # Print table contents for verification
        # await remove_bank_account(conn, '0482424657')
        # await remove_bank_account(conn, '012778015323351288')
        # await remove_bank_account(conn, '012778015939990486')
        # await remove_bank_account(conn, '646180146006124571')



        # #FRANCISCO JAVIER LOPEZ GUqERRERO
        FNVIO = 81304.26
        FSTP = 39834.45
        FBBVA = 283745.21
        FHEY = 39933.57
        
        # await update_account_balance(conn, '710969000007300927', FNVIO)    #NVIO
        # await update_account_balance(conn, '058597000056476091', FHEY)    #HEY
        # await update_account_balance(conn, '646180146006124571', FSTP)    #STP
        # await update_account_balance(conn, '1532335128', FBBVA)  #BBVA

        # # #MARIA FERNANDA MUNOZ PEREA
        MNVIO = 55577.36
        MBBVA1 = 256981.96
        MBBVA2 = 165759.44

        # await update_account_balance(conn, '710969000016348705', MNVIO)    #NVIO
        # await update_account_balance(conn, '1593999048', MBBVA1)   #BBVA
        # await update_account_balance(conn, '0482424657', MBBVA2)    #BBVA

        # # # MARTHA GUERRERO LOPEZ
        MGNVIO = 197938.79
        MGHEY = 28578.52
        MGSANTANDER = 134879.01

        # await update_account_balance(conn, '710969000015306104', MGNVIO)    #NVIO
        # await update_account_balance(conn, '014761655091416464', MGSANTANDER)    #SANTANDER
        # await update_account_balance(conn, '058597000054265356', MGHEY)  #HEY

        # # #ANBER CAP DE MEXICO
        ASTP = 22518.72
        # await update_account_balance(conn, '646180204200033494', ASTP)    #STP
        # await remove_bank_account(conn, '0482424657')
        await print_table_contents(conn, 'mxn_bank_accounts')

        # await sum_recent_deposits('1532335128')
        await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == '__main__':
    asyncio.run(main())
    # asyncio.run(sum_recent_deposits('1532335128'))