#bpa/binance_db_set.py
import aiosqlite
from typing import Any, Dict, Union
import logging

from common_utils_db import execute_and_commit

logger = logging.getLogger(__name__)

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
    
async def update_kyc_status(conn, name, new_kyc_status):
    try:
        await update_table_column(conn, "users", "kyc_status", new_kyc_status, "name", name)
    except Exception as e:
        logger.error(f"Error updating KYC status for user {name}: {e}")

async def update_anti_fraud_stage(conn, buyer_name, new_stage):
    try:
        await update_table_column(conn, "users", "anti_fraud_stage", new_stage, "name", buyer_name)
    except Exception as e:
        logger.error(f"Error updating anti-fraud stage for user {buyer_name}: {e}")

async def update_returning_customer_stage(conn, buyer_name, new_stage):
    try:
        await update_table_column(conn, "users", "returning_customer_stage", new_stage, "name", buyer_name)
    except Exception as e:
        logger.error(f"Error updating returning customer stage for user {buyer_name}: {e}")

async def set_menu_presented(conn, order_no, value):
    try:
        await update_table_column(conn, "orders", "menu_presented", value, "order_no", order_no)
    except Exception as e:
        logger.error(f"Error setting menu_presented for order_no {order_no}: {e}")

async def update_order_status(conn, order_no, order_status):
    try:
        await update_table_column(conn, "orders", "order_status", order_status, "order_no", order_no)
    except Exception as e:
        logger.error(f"Error updating order status for order_no {order_no}: {e}")

async def update_order_details(conn, order_no, account_number, seller_bank):
    try:
        sql = "UPDATE orders SET account_number = ?, seller_bank = ? WHERE order_no = ?"
        params = (account_number, seller_bank, order_no)
        await execute_and_commit(conn, sql, params)
    except Exception as e:
        logger.error(f"Error updating order details for order_no {order_no}: {e}")

async def update_buyer_bank(conn, buyer_name, new_buyer_bank):
    try:
        await update_table_column(conn, "users", "user_bank", new_buyer_bank, "name", buyer_name)
    except Exception as e:
        logger.error(f"Error updating user_bank for user {buyer_name}: {e}")


ALLOWED_TABLES: Dict[str, Dict[str, Union[type, tuple]]] = {
    "orders": {
        "order_no": str,
        "buyer_name": str,
        "seller_name": str,
        "trade_type": str,
        "order_status": int,
        "total_price": float,
        "fiat_unit": str,
        "asset": str,
        "amount": float,
        "menu_presented": bool,
        "ignore_count": int,
        "account_number": str,
        "buyer_bank": str,
        "seller_bank_account": str,
        "merchant_id": int,
        "currency_rate": float,
        "priceFloatingRatio": float,
        "advNo": str,
        "payment_screenshoot": bool,
        "payment_image_url": str,
        "paid": bool,
        "clave_de_rastreo": str,
        "seller_bank": str,
        "order_date": str
    },
    "users": {
        "name": str,
        "kyc_status": int,
        "total_crypto_sold_lifetime": float,
        "anti_fraud_stage": int,
        "rfc": str,
        "codigo_postal": str,
        "user_bank": str,
        "returning_customer_stage": int
    },
    "merchants": {
        "sellerName": str,
        "api_key": str,
        "api_secret": str,
        "email": str,
        "password_hash": str,
        "phone_num": str,
        "user_bank": str
    },
    "transactions": {
        "buyer_name": str,
        "seller_name": str,
        "total_price": float,
        "order_date": str,
        "merchant_id": int
    },
    "order_bank_identifiers": {
        "order_no": str,
        "bank_identifier": str
    },
    "usd_price_manager": {
        "trade_type": str,
        "exchange_rate_ratio": float,
        "mxn_amount": float
    },
    "deposits": {
        "timestamp": str,
        "bank_account_id": int,
        "amount_deposited": float
    },
    "bank_accounts": {
        "account_bank_name": str,
        "account_beneficiary": str,
        "account_number": str,
        "account_limit": float,
        "account_balance": float
    },
    "blacklist": {
        "name": str,
        "order_no": str,
        "country": str
    },
    "mxn_deposits": {
        "timestamp": str,
        "account_number": str,
        "amount_deposited": float,
        "deposit_from": str,
        "year": int,
        "month": int,
        "merchant_id": int
    },
    "P2PBlacklist": {
        "name": str,
        "order_no": str,
        "country": str,
        "response": str,
        "anti_fraud_stage": int,
        "merchant_id": int
    },
    "mxn_bank_accounts": {
        "account_bank_name": str,
        "account_beneficiary": str,
        "account_number": str,
        "account_daily_limit": float,
        "account_monthly_limit": float,
        "account_balance": float,
        "last_used_timestamp": str,
        "merchant_id": int
    },
    "oxxo_debit_cards": {
        "account_bank_name": str,
        "account_beneficiary": str,
        "card_number": str,
        "account_daily_limit": float,
        "account_monthly_limit": float,
        "account_balance": float,
        "last_used_timestamp": str,
        "merchant_id": int
    }
}

async def update_table_column(
    conn: aiosqlite.Connection,
    table: str,
    column: str,
    value: Any,
    condition_column: str,
    condition_value: Any
) -> None:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table: {table}")
    
    if column not in ALLOWED_TABLES[table]:
        raise ValueError(f"Invalid column: {column} for table: {table}")
    
    if condition_column not in ALLOWED_TABLES[table]:
        raise ValueError(f"Invalid condition column: {condition_column} for table: {table}")

    expected_type = ALLOWED_TABLES[table][column]
    if not isinstance(value, expected_type):
        raise TypeError(f"Expected {expected_type} for {column}, got {type(value)}")

    if expected_type == bool:
        value = 1 if value else 0

    try:
        sql = f"UPDATE {table} SET {column} = ? WHERE {condition_column} = ?"
        params = (value, condition_value)
        await execute_and_commit(conn, sql, params)
    except Exception as e:
        logger.error(f"Error updating {column} in {table} where {condition_column} = {condition_value}: {e}")
        raise