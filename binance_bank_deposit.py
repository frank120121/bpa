import datetime
import logging
import os
from binance_db_get import get_buyer_bank, get_order_amount, get_buyer_name
from binance_db_set import update_order_details
from binance_bank_deposit_db import update_last_used_timestamp
from common_vars import BBVA_BANKS

from asyncio import Lock

bank_accounts_lock = Lock()

# Configuration Management
MONTHLY_LIMIT = float(os.getenv('MONTHLY_LIMIT', '70000.00'))

bank_accounts = {
    'nvio': [],
    'bbva': [],
    'oxxo': []
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
        logger.info(f"Total after deposit for buyer '{buyer_name}' on account {account_number}: {total_after_deposit}")

        # Check if adding the new deposit exceeds the monthly limit for the buyer to this account
        if total_after_deposit <= MONTHLY_LIMIT:
            logger.info(f"The deposit does not exceed the buyer's monthly limit of {MONTHLY_LIMIT:.2f}.")
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
        current_date = datetime.datetime.now(datetime.timezone.utc).date()
        current_month_str = datetime.datetime.now(datetime.timezone.utc).strftime("%m")
        current_date_str = current_date.strftime('%Y-%m-%d')
        amount_to_deposit = await get_order_amount(conn, order_no)
        logger.info(f"Amount to deposit: {amount_to_deposit}")

        # Decide table based on bank name and set columns
        if buyer_bank.lower() == 'oxxo':
            table_name = 'oxxo_debit_cards'
            account_column = 'card_number'
        else:
            table_name = 'mxn_bank_accounts'
            account_column = 'account_number'

        buyer_bank_condition = f"AND LOWER(a.account_bank_name) = '{buyer_bank.lower()}'"

        query = f'''
            SELECT a.{account_column}, a.account_bank_name, a.account_beneficiary, a.account_daily_limit, a.account_monthly_limit, a.account_balance
            FROM {table_name} a
            LEFT JOIN (
                SELECT account_number, SUM(amount_deposited) AS total_deposited_today
                FROM mxn_deposits
                WHERE DATE(timestamp) = ?
                GROUP BY account_number
            ) d ON a.{account_column} = d.account_number
            LEFT JOIN (
                SELECT account_number, SUM(amount_deposited) AS total_deposited_this_month
                FROM mxn_deposits
                WHERE strftime('%m', timestamp) = ?
                GROUP BY account_number
            ) m ON a.{account_column} = m.account_number
            WHERE (d.total_deposited_today + ? < a.account_daily_limit OR d.total_deposited_today IS NULL)
            AND (m.total_deposited_this_month + ? < a.account_monthly_limit OR m.total_deposited_this_month IS NULL)
            {buyer_bank_condition}
            ORDER BY a.account_balance ASC
        '''

        parameters = [current_date_str, current_month_str, amount_to_deposit, amount_to_deposit]

        logger.debug(f"Executing query: {query}")
        logger.debug(f"With parameters: {parameters}")
        cursor = await conn.execute(query, parameters)
        accounts = await cursor.fetchall()
        logger.info(f"Found {len(accounts)} suitable accounts for order {order_no} with bank {buyer_bank} in {table_name}")

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
    """Retrieves payment details for the given order number and buyer name."""
    global bank_accounts
    try:
        await bank_accounts_lock.acquire()
        # Retrieve the assigned account number if it already exists
        logger.debug("Checking if an account is already assigned to the order.")
        cursor = await conn.execute('SELECT account_number FROM orders WHERE order_no = ?', (order_no,))
        result = await cursor.fetchone()
        assigned_account_number = result[0] if result else None

        if assigned_account_number:
            logger.debug(f"Account {assigned_account_number} already assigned for order {order_no}. Retrieving details.")
            return await get_account_details(conn, assigned_account_number, buyer_name)
        
        buyer_bank = (await get_buyer_bank(conn, buyer_name) or 'nvio').lower()
        logger.debug(f"Buyer bank determined as {buyer_bank}.")


        # Load or refresh account details if empty
        if not bank_accounts.get(buyer_bank):
            logger.info(f"No accounts cached for {buyer_bank}. Fetching from database.")
            bank_accounts[buyer_bank] = await find_suitable_account(conn, order_no, buyer_name, buyer_bank)

        # Check each account for deposit limits before assigning
        for account in bank_accounts[buyer_bank]:
            if await check_deposit_limit(conn, account['account_number'], order_no, buyer_name):
                assigned_account_number = account['account_number']
                await update_order_details(conn, order_no, assigned_account_number)
                await update_last_used_timestamp(conn, assigned_account_number)
                logger.info(f"Assigning account number {assigned_account_number} to order {order_no}.")
                return await get_account_details(conn, assigned_account_number, buyer_name)
            else:
                logger.info(f"Account {account['account_number']} exceeded the deposit limit for this month.")

        # If all accounts exceed limits or are empty
        logger.warning("No suitable account found or all accounts exceed the limit.")
        return "No suitable account found or all accounts exceed the limit"

    except Exception as e:
        logger.error(f"Error getting payment details: {e}")
        raise
    finally:
        bank_accounts_lock.release()

async def get_account_details(conn, account_number, buyer_name, buyer_bank=None):
    """Retrieves account details for the given account number."""
    try:
        if buyer_bank is None:
            buyer_bank = await get_buyer_bank(conn, buyer_name)

        logger.debug(f"Retrieving details for account {account_number} with buyer bank preference {buyer_bank}")

        # Prepare the SQL query based on the buyer_bank
        if buyer_bank.lower() == 'oxxo':
            query = '''
                SELECT account_bank_name, account_beneficiary, card_number AS account_number
                FROM oxxo_debit_cards
                WHERE card_number = ?
            '''
            account_label = "Número de tarjeta"
        else:
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
            if buyer_bank.lower() != 'oxxo':
                account_label = "Número de cuenta" if account_details[0].lower() == buyer_bank.lower() else "Número de CLABE"

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

