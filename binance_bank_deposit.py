import datetime
import logging
import os
from binance_db_get import get_buyer_bank, get_order_amount, get_buyer_name
from binance_db_set import update_order_details, update_buyer_bank
from binance_bank_deposit_db import update_last_used_timestamp
from common_vars import BBVA_BANKS, DB_FILE
from common_utils_db import create_connection, print_table_contents
from asyncio import Lock

bank_accounts_lock = Lock()

# Configuration Management
MONTHLY_LIMIT = float(os.getenv('MONTHLY_LIMIT', '71000.00'))

bank_accounts = {
    'nvio': [],
    'bbva': [],
    'banregio': [],
    'santander': []
}

# Set up logging with different levels
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def check_deposit_limit(conn, account_number, order_no, buyer_name):
    """Checks if the deposit for the given account number and order number exceeds the buyer's monthly limit."""
    try:
        # Use of Time Zones
        current_date = datetime.datetime.now(datetime.timezone.utc)
        current_month = current_date.month
        current_year = current_date.year

        # Get the amount to deposit from the current order
        amount_to_deposit = await get_order_amount(conn, order_no)

        # SQL Injection Protection: Using parameterized queries
        query = '''
            SELECT IFNULL(SUM(amount_deposited), 0)
            FROM mxn_deposits
            WHERE account_number = ? AND deposit_from = ? AND year = ? AND month = ?
        '''

        cursor = await conn.execute(query, (account_number, buyer_name, current_year, current_month))
        total_deposited_this_month_row = await cursor.fetchone()
        total_deposited_this_month = total_deposited_this_month_row[0]
        # Calculate the total after the proposed deposit
        total_after_deposit = total_deposited_this_month + amount_to_deposit
        logger.debug(f"Total after deposit for buyer '{buyer_name}' on account {account_number}: {total_after_deposit}")

        # Check if adding the new deposit exceeds the monthly limit for the buyer to this account
        if total_after_deposit <= MONTHLY_LIMIT:
            logger.debug(f"The deposit does not exceed the buyer's monthly limit of {MONTHLY_LIMIT:.2f}.")
            return True
        else:
            logger.warning(f"Deposit exceeds the monthly limit of {MONTHLY_LIMIT:.2f} for buyer '{buyer_name}' on account {account_number}.")
            return False
    except Exception as e:
        logger.error(f"Error checking deposit limit: {e}")
        raise

async def find_suitable_account(conn, order_no, buyer_name, buyer_bank):
    """Finds suitable accounts for the given order number and buyer details based on buyer's bank preference."""
    try:
        # Decide table and columns based on bank name
        if buyer_bank and buyer_bank.lower() == 'oxxo':
            buyer_bank = 'banregio'
            await update_buyer_bank(conn, buyer_name, 'banregio')

        if buyer_bank.lower() not in bank_accounts or buyer_bank is None:
            logger.warning(f"Bank '{buyer_bank}' not supported. Defaulting to 'nvio'.")
            buyer_bank = 'nvio'
        table_name = 'mxn_bank_accounts'
        account_column = 'account_number'
        query = f'''
            SELECT {account_column}, account_bank_name, account_beneficiary, account_daily_limit, account_monthly_limit, account_balance
            FROM {table_name}
            WHERE LOWER(account_bank_name) = ?
            ORDER BY account_balance ASC
        '''


        cursor = await conn.execute(query, (buyer_bank.lower(),))

        accounts = await cursor.fetchall()
        logger.debug(f"Found {len(accounts)} suitable accounts for buyer bank '{buyer_bank}' in {table_name}")

        # Create a list of dictionaries to store account details
        account_details = [{
            'account_number': acc[0],
            'bank_name': acc[1],
            'beneficiary': acc[2],
            'daily_limit': acc[3],
            'monthly_limit': acc[4],
            'balance': acc[5]
        } for acc in accounts]

        return account_details
    except Exception as e:
        logger.error(f"Error finding suitable account: {e}")
        raise

async def get_payment_details(conn, order_no, buyer_name):
    try:
        await bank_accounts_lock.acquire()
        logger.debug("Checking if an account is already assigned to the order.")
        cursor = await conn.execute('SELECT account_number FROM orders WHERE order_no = ?', (order_no,))
        result = await cursor.fetchone()
        assigned_account_number = result[0] if result else None

        if assigned_account_number:
            # Return details if already assigned
            return await get_account_details(conn, assigned_account_number, buyer_name)
        
        buyer_bank = (await get_buyer_bank(conn, buyer_name) or 'nvio').lower()

        # Load or refresh account details if empty
        if not bank_accounts[buyer_bank]:
            logger.debug(f"No cached accounts for {buyer_bank}. Fetching from database.")
            bank_accounts[buyer_bank] = await find_suitable_account(conn, None, None, buyer_bank)

        # Check each account for deposit limits before assigning
        for account in bank_accounts[buyer_bank][:]:  # Create a copy of the list for safe iteration
            if await check_deposit_limit(conn, account['account_number'], order_no, buyer_name):
                assigned_account_number = account['account_number']
                await update_order_details(conn, order_no, assigned_account_number)
                await update_last_used_timestamp(conn, assigned_account_number)

                # Remove the assigned account from the dictionary to prevent reuse
                bank_accounts[buyer_bank].remove(account)
                logger.debug(f"Account {assigned_account_number} has been assigned and removed from cache.")

                return await get_account_details(conn, assigned_account_number, buyer_name, buyer_bank)
            else:
                logger.debug(f"Account {account['account_number']} exceeded the deposit limit for this month.")

        logger.warning("No suitable account found or all accounts exceed the limit.")
        return "Un momento por favor."
    finally:
        bank_accounts_lock.release()


async def get_account_details(conn, account_number, buyer_name, buyer_bank=None):
    """Retrieves account details for the given account number."""
    try:
        if buyer_bank is None:
            buyer_bank = await get_buyer_bank(conn, buyer_name)
            if buyer_bank == 'oxxo':
                buyer_bank = 'banregio'
                await update_buyer_bank(conn, buyer_name, 'banregio')

        logger.debug(f"Retrieving details for account {account_number} with buyer bank preference {buyer_bank}")

        query = '''
            SELECT account_bank_name, account_beneficiary, account_number
            FROM mxn_bank_accounts
            WHERE account_number = ?
        '''
        cursor = await conn.execute(query, (account_number,))
        account_details = await cursor.fetchone()
        if account_details:
            logger.debug(f"Details retrieved for account {account_number}")
            # Decide the account label after fetching the details if not OXXO
            if buyer_bank.lower() not in ['oxxo', 'banregio']:
                account_label = "Número de cuenta" if account_details[0].lower() == buyer_bank.lower() else "Número de CLABE"
            else:
                account_label = "Número de tarjeta"

            return (
                f"Los detalles para el pago son:\n\n"
                f"Nombre de banco: {account_details[0]}\n"
                f"Nombre del beneficiario: {account_details[1]}\n"
                f"{account_label}: {account_details[2]}\n"
            )
        else:
            logger.warning(f"No details found for account {account_number}")
            return None
    except Exception as e:
        logger.error(f"Error retrieving account details: {e}")
        raise
async def initialize_account_cache(conn):
    for bank in ['nvio', 'bbva', 'banregio', 'santander']:  # Include all banks you need
        bank_accounts[bank] = await find_suitable_account(conn, None, None, bank)

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        try:
            order_no = 1  # Example order number
            buyer_name = 'John Doe'  # Example buyer name
            buyer_bank = 'beanregio'  # Explicitly testing 'oxxo'

            # Testing the find_suitable_account function directly for 'oxxo'
            suitable_accounts = await find_suitable_account(conn, order_no, buyer_name, buyer_bank)
            print(f"Suitable accounts for 'oxxo': {suitable_accounts}")
            # await print_table_contents(conn, 'mxn_bank_accounts')
            await print_table_contents(conn, 'oxxo_debit_cards')
        except Exception as e:
            logger.error(f"Error in main: {e}")
        finally:
            await conn.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())