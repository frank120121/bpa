import datetime
import asyncio
import aiosqlite
from common_vars import bank_accounts, DB_FILE, OXXO_DEBIT_CARDS
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
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS oxxo_debit_cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_bank_name TEXT,
            account_beneficiary TEXT,
            card_number TEXT UNIQUE,
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
    # Insert OXXO debit card data into the database
    for card in OXXO_DEBIT_CARDS:
        cursor = await conn.execute("SELECT 1 FROM oxxo_debit_cards WHERE card_number = ?", (card['card_no'],))
        if not await cursor.fetchone():
            await conn.execute(
                'INSERT INTO oxxo_debit_cards (account_bank_name, account_beneficiary, card_number, account_daily_limit, account_monthly_limit) VALUES (?, ?, ?, ?, ?)',
                (card['bank_name'], card['beneficiary'], card['card_no'], card['daily_limit'], card['monthly_limit']))
        else:
            logger.debug(f"Card number {card['card_no']} already exists. Skipping insertion.")

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
    try:
        await conn.execute('DELETE FROM mxn_bank_accounts')
        await conn.commit()
        logger.debug(f"Removed all bank accounts")
    except Exception as e:
        logger.error(f"Error removing bank accounts: {e}")
        raise

async def count_transactions(DB_FILE):
    # SQL to count transactions based on amount_deposited
    sql = """
    SELECT
        SUM(CASE WHEN amount_deposited < 2500 THEN 1 ELSE 0 END) AS under_5000,
        SUM(CASE WHEN amount_deposited > 2500 THEN 1 ELSE 0 END) AS over_5000
    FROM mxn_deposits
    """

    # Connect to your SQLite database
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(sql)
        result = await cursor.fetchone()

    # Extracting the counts
    under_5000, over_5000 = result if result else (0, 0)

    print(f"Transactions under 5000.00: {under_5000}")
    print(f"Transactions over 5000.00: {over_5000}")

async def sum_deposits_by_day_and_week(db_path, year, month):
    # SQL to sum amount_deposited for each day and each week of a specified month and year
    sql = """
    SELECT
        strftime('%d', timestamp) AS day,
        (strftime('%d', timestamp) - 1) / 7 + 1 AS week, -- Calculating week of the month
        SUM(amount_deposited) AS total_deposited
    FROM mxn_deposits
    WHERE
        strftime('%Y', timestamp) = ? AND
        strftime('%m', timestamp) = ?
    GROUP BY week, strftime('%Y-%m-%d', timestamp)
    ORDER BY week, day
    """

    # Connect to your SQLite database
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(sql, (str(year), str(month).zfill(2)))
        results = await cursor.fetchall()

    # Initializing variables to track the current week and its total deposits
    current_week = 0
    weekly_total = 0
    for day, week, total_deposited in results:
        if week != current_week:
            # When we move to a new week, print the total for the previous week and reset the total
            if current_week > 0:
                print(f"Week {current_week}: {weekly_total:.2f}")
            weekly_total = 0  # Resetting weekly total for the new week
            current_week = week
        weekly_total += total_deposited  # Accumulating weekly total
        print(f"  Day {day}: {total_deposited:.2f}")  # Print daily total within the week
    
    # Don't forget to print the total for the last week
    if current_week > 0:
        print(f"Week {current_week}: {weekly_total:.2f}")

async def get_monthly_deposit_sum(conn, account_number: str, year: int, month: int) -> float:
    """
    Fetches the total sum of deposits for a given account number, year, and month.
    
    Args:
        conn: The database connection object.
        account_number (str): The account number to query.
        year (int): The year for the deposits.
        month (int): The month for the deposits.
    
    Returns:
        float: The total sum of deposits.
    """
    # Ensure month and year are integers and within valid ranges
    if not 1 <= month <= 12:
        logger.error("Invalid month: %s. Month must be between 1 and 12.", month)
        return 0.0
    if year < 1900 or year > datetime.datetime.now().year:
        logger.error("Invalid year: %s. Year must be between 1900 and the current year.", year)
        return 0.0
    
    try:
        await conn.execute('PRAGMA foreign_keys = ON')  # Ensure foreign key constraints are enforced
        async with conn.execute(
            'SELECT SUM(amount_deposited) FROM mxn_deposits WHERE account_number = ? AND year = ? AND month = ?',
            (account_number, year, month,)
        ) as cursor:
            result = await cursor.fetchone()
            total_sum = result[0] if result[0] is not None else 0.0
            return total_sum
    except Exception as e:
        logger.exception("Failed to fetch monthly deposit sum due to an error: %s", e)
        return 0.0


async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        # Initialize the database (create tables and insert initial data)
        # await clear_accounts(conn)
        await initialize_database(conn)
        # Print table contents for verification
        await print_table_contents(conn, 'mxn_bank_accounts')
        await print_table_contents(conn, 'oxxo_debit_cards')
        # await remove_bank_account(conn, '0482424657')
        # await remove_bank_account(conn, '012778015323351288')
        # await remove_bank_account(conn, '012778015939990486')



        # #FRANCISCO JAVIER LOPEZ GUqERRERO
        FNVIO = 50497.91
        FSTP = 11100.00
        FBBVA = 194680.4
        FHEY = 41435.07
        
        # await update_account_balance(conn, '710969000007300927', FNVIO)    #NVIO
        # await update_account_balance(conn, '058597000056476091', FHEY)    #HEY
        # await update_account_balance(conn, '646180146099983826', FSTP)    #STP
        # await update_account_balance(conn, '1532335128', FBBVA)  #BBVA

        # # #MARIA FERNANDA MUNOZ PEREA
        MNVIO = 0.00
        MBBVA1 = 100816.94
        MBBVA2 = 310006.43

        # await update_account_balance(conn, '710969000016348705', MNVIO)    #NVIO
        # await update_account_balance(conn, '1593999048', MBBVA1)   #BBVA
        # await update_account_balance(conn, '0482424657', MBBVA2)    #BBVA

        # # # MARTHA GUERRERO LOPEZ
        MGNVIO = 102687.96
        MGHEY = 32336.87
        MGSANTANDER = 175662.45

        # await update_account_balance(conn, '710969000015306104', MGNVIO)    #NVIO
        # await update_account_balance(conn, '014761655091416464', MGSANTANDER)    #SANTANDER
        # await update_account_balance(conn, '058597000054265356', MGHEY)  #HEY

        # # #ANBER CAP DE MEXICO
        ASTP = 14482.91
        ACMBBVA = 50859.89
        # await update_account_balance(conn, '646180204200033494', ASTP)    #STP
        # await update_account_balance(conn, '0122819805', ACMBBVA)  #BBVA

        # await print_table_contents(conn, 'mxn_deposits')
        # await count_transactions(DB_FILE)
        # await sum_deposits_by_day_and_week(DB_FILE, 2024, 2)
        # deposit_sum = await get_monthly_deposit_sum(conn, '1593999048', 2024, 3)
        # print(f"Total deposit sum for account '1593999048' in March 2024: {deposit_sum}")
    
        # await sum_recent_deposits('1532335128')
        await conn.close()
    else:
        logger.error("Error! Cannot create the database connection.")

if __name__ == '__main__':
    asyncio.run(main())
    # asyncio.run(sum_recent_deposits('1532335128'))