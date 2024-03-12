import datetime
import logging
import os
from binance_db_get import get_buyer_bank, get_order_amount, get_buyer_name
from binance_db_set import update_order_details
from binance_bank_deposit_db import update_last_used_timestamp
from common_vars import BBVA_BANKS

# Configuration Management
MONTHLY_LIMIT = float(os.getenv('MONTHLY_LIMIT', '70000.00'))

# Set up logging with different levels
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def check_deposit_limit(conn, account_number, order_no):
    """Checks if the deposit for the given account number and order number exceeds the buyer's monthly limit."""
    try:
        # Use of Time Zones
        current_date = datetime.datetime.now(datetime.timezone.utc)
        current_month = current_date.month
        current_year = current_date.year

        # Get the buyer's name for the order
        buyer_name = await get_buyer_name(conn, order_no)

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

async def find_suitable_account(conn, order_no, buyer_name, buyer_bank, ignore_bank_preference=False):
    """Finds suitable accounts for the given order number and buyer details."""
    try:
        current_date = datetime.datetime.now(datetime.timezone.utc).date()
        current_month_str = datetime.datetime.now(datetime.timezone.utc).strftime("%m")
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Get the amount to deposit from the current order
        amount_to_deposit = await get_order_amount(conn, order_no)
        logger.info(f"Amount to deposit: {amount_to_deposit}")
        # Construct the buyer bank condition with SQL Injection Protection
        buyer_bank_condition = "AND LOWER(a.account_bank_name) NOT LIKE '%bbva%'" if ignore_bank_preference or buyer_bank is None else "AND LOWER(a.account_bank_name) = ?"
        logger.info(f"Buyer bank condition: {buyer_bank_condition}")
        # Parameterized query to avoid SQL Injection
        query = f'''
            WITH LastAccount AS (
                SELECT account_number
                FROM mxn_deposits
                WHERE deposit_from = ? 
                ORDER BY timestamp DESC
                LIMIT 1
            ), MostRecentlyUsedAccount AS (
                SELECT account_number
                FROM mxn_bank_accounts
                ORDER BY last_used_timestamp DESC
                LIMIT 1
            )
            SELECT a.account_number, a.account_bank_name
            FROM mxn_bank_accounts a
            LEFT JOIN (
                SELECT account_number, SUM(amount_deposited) AS total_deposited_today
                FROM mxn_deposits
                WHERE DATE(timestamp) = ?
                GROUP BY account_number
            ) d ON a.account_number = d.account_number
            LEFT JOIN (
                SELECT account_number, SUM(amount_deposited) AS total_deposited_this_month
                FROM mxn_deposits
                WHERE strftime('%m', timestamp) = ?
                GROUP BY account_number
            ) m ON a.account_number = m.account_number
            WHERE (d.total_deposited_today + ? < a.account_daily_limit OR d.total_deposited_today IS NULL)
            AND (m.total_deposited_this_month + ? < a.account_monthly_limit OR m.total_deposited_this_month IS NULL)
            AND a.account_number NOT IN (SELECT account_number FROM LastAccount)
            AND a.account_number NOT IN (SELECT account_number FROM MostRecentlyUsedAccount)
            {buyer_bank_condition}
            ORDER BY a.account_balance ASC
        '''

        parameters = [buyer_name, current_date_str, current_month_str, amount_to_deposit, amount_to_deposit]
        # Adjust the logic to append buyer_bank.lower() only when necessary
        if not ignore_bank_preference and buyer_bank is not None:
            parameters.append(buyer_bank.lower())

        cursor = await conn.execute(query, parameters)
        accounts = await cursor.fetchall()
        logger.info(f"Found {len(accounts)} suitable accounts for order {order_no}")
        logger.info(f"Suitable accounts: {accounts}")
        return [acc[0] for acc in accounts]
    except Exception as e:
        logger.error(f"Error finding suitable account: {e}")
        raise

async def get_payment_details(conn, order_no, buyer_name):
    """Retrieves payment details for the given order number and buyer name."""
    try:
        # Check if a bank account has already been assigned to the order_no
        cursor = await conn.execute('SELECT account_number FROM orders WHERE order_no = ?', (order_no,))
        result = await cursor.fetchone()
        assigned_account_number = result[0] if result else None

        if assigned_account_number:
            logger.debug(f"Account already assigned for order {order_no}.")
            return await get_account_details(conn, assigned_account_number)

        buyer_bank = await get_buyer_bank(conn, order_no)
        suitable_accounts = await find_suitable_account(conn, order_no, buyer_name, buyer_bank, ignore_bank_preference=False)

        # If no account matches the buyer's bank preference or buyer_bank is None, search without bank preference
        if not suitable_accounts:
            suitable_accounts = await find_suitable_account(conn, order_no, buyer_name, buyer_bank, ignore_bank_preference=True)

        for account_number in suitable_accounts:
            if await check_deposit_limit(conn, account_number, order_no):
                await update_order_details(conn, order_no, account_number)
                await update_last_used_timestamp(conn, account_number)
                return await get_account_details(conn, account_number)
            else:
                logger.info(f"Account {account_number} exceeded the monthly deposit limit for the buyer.")

        return "Sorry, no bank accounts available at this time or all suitable accounts exceed the monthly limit."
    except Exception as e:
        logger.error(f"Error getting payment details: {e}")
        raise

async def get_account_details(conn, account_number):
    """Retrieves account details for the given account number."""
    try:
        logger.debug(f"Retrieving details for account {account_number}")
        cursor = await conn.execute('SELECT account_bank_name, account_beneficiary, account_number FROM mxn_bank_accounts WHERE account_number = ?', (account_number,))
        account_details = await cursor.fetchone()
        if account_details:
            logger.debug(f"Details retrieved for account {account_number}")
            account_label = "Número de cuenta" if account_details[0].lower() == "bbva" else "Número de CLABE"
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
