import datetime
import logging
import os
from binance_db_get import get_buyer_bank, get_order_amount, get_buyer_name
from binance_db_set import update_order_details, update_buyer_bank
from binance_bank_deposit_db import update_last_used_timestamp, sum_recent_deposits
from common_vars import BBVA_BANKS, DB_FILE
from common_utils_db import create_connection, print_table_contents
from asyncio import Lock

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


bank_accounts_lock = Lock()

# Configuration Management
MONTHLY_LIMIT = float(os.getenv('MONTHLY_LIMIT', '71000.00'))

bank_accounts = {
    'nvio': [],
    # 'bbva': [],
    'banregio': [],
    'santander': [],
    # 'spin by oxxo': []
}

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
            SELECT {account_column}, account_bank_name, account_beneficiary, account_daily_limit, account_monthly_limit
            FROM {table_name}
            WHERE LOWER(account_bank_name) = ?
            ORDER BY account_balance ASC
        '''

        cursor = await conn.execute(query, (buyer_bank.lower(),))
        accounts = await cursor.fetchall()
        logger.debug(f"Found {len(accounts)} suitable accounts for buyer bank '{buyer_bank}' in {table_name}")

        # Create a list of dictionaries to store account details
        account_details = []
        for acc in accounts:
            daily_balance = await sum_recent_deposits(conn, acc[0])
            print(f"Daily balance for account {acc[0]}: MXN {daily_balance}")
            if daily_balance <= acc[3]:  # Check if daily deposits are within the daily limit
                account_details.append({
                    'account_number': acc[0],
                    'bank_name': acc[1],
                    'beneficiary': acc[2],
                    'daily_limit': acc[3],
                    'monthly_limit': acc[4],
                    'balance': daily_balance  # Replacing balance with daily deposit total
                })

        logger.debug(f"Filtered accounts that are within the daily limit: {len(account_details)} accounts found.")
        return account_details
    except Exception as e:
        logger.error(f"Error finding suitable account: {e}")
        raise



async def get_payment_details(conn, order_no, buyer_name, oxxo_used=False):
    try:
        await bank_accounts_lock.acquire()
        logger.debug("Checking if an account is already assigned to the order.")
        cursor = await conn.execute('SELECT account_number FROM orders WHERE order_no = ?', (order_no,))
        result = await cursor.fetchone()
        assigned_account_number = result[0] if result else None

        if assigned_account_number:
            # Return details if already assigned
            return await get_account_details(conn, assigned_account_number, buyer_name, order_no)
        
        buyer_bank = await get_buyer_bank(conn, buyer_name)
        
        if buyer_bank not in bank_accounts or buyer_bank is None:
            logger.warning(f"Bank '{buyer_bank}' not supported.")
            if buyer_bank == 'oxxo' or oxxo_used:
                buyer_bank = 'banregio'
                logger.debug(f"Updating buyer bank to 'banregio' for OXXO payment.")
                await update_buyer_bank(conn, buyer_name, 'banregio')
            else:
                buyer_bank = 'nvio'

        async def assign_account():
            nonlocal assigned_account_number
            amount_to_deposit = await get_order_amount(conn, order_no)
            accounts_copy = sorted(bank_accounts[buyer_bank], key=lambda x: x['balance'])
            
            for account in accounts_copy:
                if await check_deposit_limit(conn, account['account_number'], order_no, buyer_name):
                    assigned_account_number = account['account_number']
                    await update_order_details(conn, order_no, assigned_account_number)
                    await update_last_used_timestamp(conn, assigned_account_number)

                    # Update the balance of the original cached account
                    for original_account in bank_accounts[buyer_bank]:
                        if original_account['account_number'] == assigned_account_number:
                            original_account['balance'] += amount_to_deposit
                            if original_account['balance'] > original_account['daily_limit']:
                                #remove the account from the list
                                bank_accounts[buyer_bank].remove(original_account)
                            logger.info(f"New balance for account {assigned_account_number}: MXN {original_account['balance']}")
                            break

                    return await get_account_details(conn, assigned_account_number, buyer_name, order_no, buyer_bank)
                else:
                    logger.info(f"Account {account['account_number']} exceeded the deposit limit for this month.")

            return None

        # Load or refresh account details if empty
        if not bank_accounts[buyer_bank]:
            logger.info(f"No cached accounts for {buyer_bank}. Fetching from database.")
            bank_accounts[buyer_bank] = await find_suitable_account(conn, None, None, buyer_bank)

        # Attempt to assign an account
        account_details = await assign_account()

        if account_details is not None:
            return account_details

        # If no suitable account found, reload the accounts and try again
        logger.info("No suitable account found, reloading accounts and retrying.")
        bank_accounts[buyer_bank] = await find_suitable_account(conn, None, None, buyer_bank)
        account_details = await assign_account()

        if account_details is not None:
            return account_details

        # Additional condition for third retry with 'nvio' bank
        if not oxxo_used and buyer_bank not in ['oxxo', 'banregio']:
            logger.info("Second retry unsuccessful, loading accounts from 'nvio' bank and assigning first account without limit checks.")
            bank_accounts['nvio'] = await find_suitable_account(conn, None, None, 'nvio')
            if bank_accounts['nvio']:
                assigned_account_number = bank_accounts['nvio'][0]['account_number']
                await update_order_details(conn, order_no, assigned_account_number)
                await update_last_used_timestamp(conn, assigned_account_number)
                logger.debug(f"Assigned first 'nvio' account {assigned_account_number} without limit checks.")

                return await get_account_details(conn, assigned_account_number, buyer_name, order_no, 'nvio')

        logger.warning("No suitable account found or all accounts exceed the limit.")
        return "Un momento por favor."
    finally:
        bank_accounts_lock.release()




async def get_account_details(conn, account_number, buyer_name, order_no, buyer_bank=None):
    """Retrieves account details for the given account number."""
    try:
        if buyer_bank is None:
            buyer_bank = await get_buyer_bank(conn, buyer_name)
            if buyer_bank == 'oxxo':
                buyer_bank = 'banregio'
                await update_buyer_bank(conn, buyer_name, 'banregio')
            if buyer_bank not in ['bbva', 'santander', 'banregio', 'nvio']:
                buyer_bank = 'nvio'
                await update_buyer_bank(conn, buyer_name, 'nvio')

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

            if buyer_bank.lower() in ['oxxo', 'banregio',]:
                bank_name = "NO SE ACEPTA TRANSFERENCIA SOLO DEPOSITO EN EFECTIVO"
                beneficiary_name = "NO SE ACEPTA TRANSFERENCIA SOLO DEPOSITO EN EFECTIVO"
                account_label = "Número de tarjeta de debito"
                return (
                    f"Muestra el numero de tarjeta de debito junto con el efectivo que vas a depositar:\n\n"
                    f"Recuerda que solo se acepta deposito en efectivo.\n\n"
                    f"{account_label}: {account_details[2]}\n"
                )
            else:
                bank_name = account_details[0]
                beneficiary_name = account_details[1]
                account_label = "Número de cuenta" if account_details[0].lower() == buyer_bank.lower() and account_details[0].lower() != 'nvio' else "Número de CLABE"
                return (
                    f"Los detalles para el pago son:\n\n"
                    f"Nombre de banco: {bank_name}\n"
                    f"Nombre del beneficiario: {beneficiary_name}\n"
                    f"{account_label}: {account_details[2]}\n"
                    f"Concepto: {order_no}\n\n"
                    f"Por favor, incluye el concepto de arriba en tu pago.\n"
                    f"(Solo copea y pega el concepto en el area de concepto de pago dentro de tu app bancaria o banca enlinea)\n"
                )
        else:
            logger.warning(f"No details found for account {account_number}")
            return None
    except Exception as e:
        logger.error(f"Error retrieving account details: {e}")
        raise

async def initialize_account_cache(conn):
    for bank in ['nvio','banregio', 'santander', 'spin by oxxo']:  # Include all banks you need
        bank_accounts[bank] = await find_suitable_account(conn, None, None, bank)
        #log the balance for each account
        for account in bank_accounts[bank]:
            logger.info(f"Balance for account {account['account_number']}: MXN {account['balance']}, daily limit: MXN {account['daily_limit']}")

async def main():
    conn = await create_connection(DB_FILE)
    if conn is not None:
        try:
            order_no = 1  # Example order number
            buyer_name = 'John Doe'  # Example buyer name
            buyer_bank = 'banregio'  # Explicitly testing 'oxxo'

            # Testing the find_suitable_account function directly for 'oxxo'
            suitable_accounts = await find_suitable_account(conn, order_no, buyer_name, buyer_bank)
            print(f"Suitable accounts: {suitable_accounts}")
            # await print_table_contents(conn, 'mxn_bank_accounts')
        except Exception as e:
            logger.error(f"Error in main: {e}")
        finally:
            await conn.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())