#bpa/binance_bank_deposit.py
import asyncio
import datetime
import logging
from typing import Dict, Optional, Any

from binance_db_get import get_buyer_bank, get_order_amount
from binance_db_set import update_order_details
from binance_bank_deposit_db import update_last_used_timestamp, sum_recent_deposits, sum_monthly_deposits
from common_utils_db import create_connection, DB_FILE, execute_and_commit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MONTHLY_LIMIT = 71000.00
NVIO_BANK = 'nvio'
OXXO_BANK = 'oxxo'
SANTANDER_BANK = 'santander'

SUPPORTED_BANKS = [NVIO_BANK, OXXO_BANK, SANTANDER_BANK]

class PaymentManager:
    _instance: Optional['PaymentManager'] = None
    _lock = asyncio.Lock()

    def __init__(self):
        if self.__class__._instance is not None:
            raise RuntimeError("This class is a singleton. Use get_instance() instead.")
        self.bank_accounts: Dict[str, list] = {bank: [] for bank in SUPPORTED_BANKS}
        self.bank_accounts_lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls) -> 'PaymentManager':
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    async def initialize_bank_account_cache(self, conn):
        for bank in self.bank_accounts.keys():
            self.bank_accounts[bank] = await self._populate_bank_account_cache(conn, bank)

    async def get_payment_details(self, conn, order_no: str, buyer_name: str, oxxo_used: bool = False) -> Optional[Dict[str, Any]]:
        async with self.bank_accounts_lock:
            assigned_account_number = await self._get_assigned_account(conn, order_no)
            if assigned_account_number:
                return await self._get_account_details(conn, assigned_account_number, buyer_name, order_no)

            buyer_bank = await get_buyer_bank(conn, buyer_name)
            logger.info(f"Retrieved buyer_bank: {buyer_bank}")  # Add logging
            
            # Default to NVIO_BANK if buyer_bank is None or not supported
            if buyer_bank is None or buyer_bank.lower() not in SUPPORTED_BANKS:
                logger.info(f"Using default bank (NVIO) for buyer {buyer_name}")
                buyer_bank = NVIO_BANK

            account_details = await self._assign_account(conn, buyer_bank, order_no, buyer_name)
            if account_details:
                return account_details

            if not oxxo_used and buyer_bank not in [OXXO_BANK]:
                return await self._fallback_to_nvio(conn, order_no, buyer_name)

            logger.warning("No suitable account found or all accounts exceed the limit.")
            return None
    async def _get_assigned_account(self, conn, order_no: str) -> Optional[str]:
        cursor = await conn.execute('SELECT account_number FROM orders WHERE order_no = ?', (order_no,))
        result = await cursor.fetchone()
        return result[0] if result else None

    async def _assign_account(self, conn, buyer_bank: str, order_no: str, buyer_name: str) -> Optional[Dict[str, Any]]:
        amount_to_deposit = await get_order_amount(conn, order_no)
        accounts_copy = sorted(self.bank_accounts[buyer_bank], key=lambda x: x['balance'])
        
        for account in accounts_copy:
            if await self._check_deposit_limit(conn, account['account_number'], order_no, buyer_name):
                assigned_account_number = account['account_number']
                seller_bank = account['bank_name']
                
                # Single update call for both fields
                await update_order_details(conn, order_no, assigned_account_number, seller_bank)
                
                await update_last_used_timestamp(conn, assigned_account_number)
                self._update_account_balance(buyer_bank, assigned_account_number, amount_to_deposit)

                return await self._get_account_details(conn, assigned_account_number, buyer_name, order_no, buyer_bank)
            else:
                logger.info(f"Account {account['account_number']} exceeded the deposit limit for this month.")

        return None

    def _update_account_balance(self, bank: str, account_number: str, amount: float):
        for account in self.bank_accounts[bank]:
            if account['account_number'] == account_number:
                account['balance'] += amount
                if account['balance'] > account['daily_limit']:
                    self.bank_accounts[bank].remove(account)
                logger.info(f"New balance for account {account_number}: MXN {account['balance']}")
                break

    async def _check_deposit_limit(self, conn, account_number: str, order_no: str, buyer_name: str) -> bool:
        try:
            current_date = datetime.datetime.now(datetime.timezone.utc)
            amount_to_deposit = await get_order_amount(conn, order_no)

            query = '''
                SELECT IFNULL(SUM(amount_deposited), 0)
                FROM mxn_deposits
                WHERE account_number = ? AND deposit_from = ? AND year = ? AND month = ?
            '''
            cursor = await conn.execute(query, (account_number, buyer_name, current_date.year, current_date.month))
            total_deposited_this_month = (await cursor.fetchone())[0]
            total_after_deposit = total_deposited_this_month + amount_to_deposit
            
            logger.info(f"Total deposited this month for account {account_number}: {total_deposited_this_month:.2f}")
            
            if total_after_deposit <= MONTHLY_LIMIT:
                return True
            else:
                logger.warning(f"Deposit exceeds the monthly limit of {MONTHLY_LIMIT:.2f} for buyer '{buyer_name}' on account {account_number}.")
                return False
        except Exception as e:
            logger.error(f"Error checking deposit limit: {e}")
            raise

    async def _populate_bank_account_cache(self, conn, bank: str) -> list:
        try:
            query = '''
                SELECT account_number, account_bank_name, account_beneficiary, account_daily_limit, account_monthly_limit
                FROM mxn_bank_accounts
                WHERE LOWER(account_bank_name) = ?
                ORDER BY account_balance ASC
            '''

            cursor = await conn.execute(query, (bank.lower(),))
            accounts = await cursor.fetchall()
            account_details = []
            for acc in accounts:
                daily_balance = await sum_recent_deposits(conn, acc[0])
                monthly_balance = await sum_monthly_deposits(conn, acc[0])
                logger.info(f"Monthly balance for account {acc[0]}: {monthly_balance:.2f}")
                if daily_balance <= acc[3] and monthly_balance <= acc[4]:  
                    account_details.append({
                        'account_number': acc[0],
                        'bank_name': acc[1],
                        'beneficiary': acc[2],
                        'daily_limit': acc[3],
                        'monthly_limit': acc[4],
                        'balance': daily_balance 
                    })
            return account_details
        except Exception as e:
            logger.error(f"Error finding suitable account: {e}")
            raise

    async def _get_account_details(self, conn, account_number: str, buyer_name: str, order_no: str, buyer_bank: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not buyer_bank:
            logger.info("Buyer bank not provided. Attempting to retrieve from database.")
            buyer_bank = await get_buyer_bank(conn, buyer_name)
        try:
            query = '''
                SELECT account_bank_name, account_beneficiary, account_number
                FROM mxn_bank_accounts
                WHERE account_number = ?
            '''
            cursor = await conn.execute(query, (account_number,))
            account_details = await cursor.fetchone()
            if account_details:
                # Update the seller_bank if it's not set
                sql = "UPDATE orders SET seller_bank = ? WHERE order_no = ? AND (seller_bank IS NULL OR seller_bank = '')"
                await execute_and_commit(conn, sql, (account_details[0], order_no))
                
                return self._format_account_details(account_details, buyer_bank, order_no)
            else:
                logger.warning(f"No details found for account {account_number}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving account details: {e}")
            raise

    def _format_account_details(self, account_details: tuple, buyer_bank: str, order_no: str) -> str:
        buyer_bank = buyer_bank or NVIO_BANK

        if buyer_bank.lower() in [OXXO_BANK]:
            return (
                f"Muestra el numero de tarjeta de debito junto con el efectivo que vas a depositar:\n\n"
                f"Recuerda que solo se acepta deposito en efectivo.\n\n"
                f"Número de tarjeta de debito: {account_details[2]}\n"
            )
        else:
            bank_name = account_details[0]
            beneficiary_name = account_details[1]
            account_label = "Número de cuenta" if account_details[0].lower() == buyer_bank.lower() and account_details[0].lower() != NVIO_BANK else "Número de CLABE"
            return (
                f"Los detalles para el pago son:\n\n"
                f"Nombre de banco: {bank_name}\n"
                f"Nombre del beneficiario: {beneficiary_name}\n"
                f"{account_label}: {account_details[2]}\n"
                f"Concepto: {order_no}\n\n"
                f"Por favor, incluye el concepto de arriba en tu pago.\n"
                f"(Solo copea y pega el concepto en el area de concepto de pago dentro de tu app bancaria o banca enlinea)\n"
            )

    async def _fallback_to_nvio(self, conn, order_no: str, buyer_name: str) -> Optional[Dict[str, Any]]:
        self.bank_accounts[NVIO_BANK] = await self._populate_bank_account_cache(conn, NVIO_BANK)
        if self.bank_accounts[NVIO_BANK]:
            assigned_account_number = self.bank_accounts[NVIO_BANK][0]['account_number']
            # Single update call for both fields
            await update_order_details(conn, order_no, assigned_account_number, NVIO_BANK)
            await update_last_used_timestamp(conn, assigned_account_number)
            return await self._get_account_details(conn, assigned_account_number, buyer_name, order_no, NVIO_BANK)
        return None
    
async def main():
    conn = await create_connection(DB_FILE)
    payment_manager = await PaymentManager.get_instance()
    await payment_manager.initialize_bank_account_cache(conn)
    
    for bank in payment_manager.bank_accounts.keys():
        logger.info(f"Accounts for bank {bank}: {payment_manager.bank_accounts[bank]}")
    
    order_no = '22663322577624915969'
    buyer_name = 'OSUNA ROBLES GABRIEL HIRAM'
    
    # Log current order details before update
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT account_number, seller_bank FROM orders WHERE order_no = ?", (order_no,))
        current_details = await cursor.fetchone()
        logger.info(f"Current order details - Account: {current_details[0] if current_details else 'None'}, Seller Bank: {current_details[1] if current_details else 'None'}")
    
    payment_details = await payment_manager.get_payment_details(conn, order_no, buyer_name)
    
    # Log order details after update
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT account_number, seller_bank FROM orders WHERE order_no = ?", (order_no,))
        updated_details = await cursor.fetchone()
        logger.info(f"Updated order details - Account: {updated_details[0] if updated_details else 'None'}, Seller Bank: {updated_details[1] if updated_details else 'None'}")
    
    if payment_details:
        logger.info(payment_details)
    else:
        logger.warning("No suitable account found.")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())