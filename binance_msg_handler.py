from lang_utils import get_message_by_language, determine_language, get_default_reply, payment_concept, payment_warning, invalid_country
from binance_db_get import get_account_number, is_menu_presented, get_kyc_status, get_anti_fraud_stage, get_buyer_bank, has_specific_bank_identifiers
from binance_db_set import update_total_spent, update_buyer_bank
from binance_bank_deposit import get_payment_details
from binance_bank_deposit_db import log_deposit
from binance_messages import present_menu_based_on_status, handle_menu_response, send_messages
from binance_orders import binance_buy_order
from binance_anti_fraud import handle_anti_fraud
from binance_blacklist import add_to_blacklist
from verify_client_ip import fetch_ip
from common_vars import prohibited_countries
import logging
logger = logging.getLogger(__name__)

COUNTRY_NOT_ALLOWED = "Transaction denied. Seller not from Mexico. Buyer: {buyer_name} added to blacklist."

async def check_order_details(order_details):
    if order_details is None:
        logger.warning("order_details is None.")
        return False
    return True
async def check_and_handle_country_restrictions(connection_manager, conn, order_no, seller_name, buyer_name, fiat):
    if fiat == 'USD':
        country = await fetch_ip(order_no[-4:], seller_name)
        if country in prohibited_countries:
            logger.debug(f"Transaction denied. Seller from prohibited country {country}. Buyer: {buyer_name} added to blacklist.")
            await connection_manager.send_text_message(invalid_country, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, country)
            return

    country = await fetch_ip(order_no[-4:], seller_name)
    if country and country != "MX":
        logger.debug(f"Transaction denied. Seller not from Mexico. Buyer: {buyer_name} added to blacklist.")
        await connection_manager.send_text_message(invalid_country, order_no)
        await add_to_blacklist(conn, buyer_name, order_no, country)
        return True  

    return False

async def handle_order_status_4(connection_manager, conn, order_no, order_details):
    await generic_reply(connection_manager, order_no, order_details, 4)
    asset_type = order_details.get('asset')
    logger.debug(asset_type)
    if asset_type in ['BTC', 'ETH']:
        await binance_buy_order(asset_type)
    await update_total_spent(conn, order_no)
    amount_deposited = order_details.get('total_price')
    bank_account_number = await get_account_number(conn, order_no)
    buyer_name = order_details.get('buyer_name')
    logger.debug(f"Logging deposit for {buyer_name} with bank account {bank_account_number} for {amount_deposited}")
    await log_deposit(conn, buyer_name, bank_account_number, amount_deposited)



async def handle_order_status_1(connection_manager, conn, order_no, order_details):
    seller_name, buyer_name, fiat = order_details.get('seller_name'), order_details.get('buyer_name'), order_details.get('fiat_unit')
    kyc_status = await get_kyc_status(conn, buyer_name)
    if kyc_status == 0 or kyc_status is None:
        anti_fraud_stage = await get_anti_fraud_stage(conn, buyer_name)
        if anti_fraud_stage is None:
            anti_fraud_stage = 0
        await generic_reply(connection_manager, order_no, order_details, 1)
        await handle_anti_fraud(buyer_name, seller_name, conn, anti_fraud_stage, "", order_no, connection_manager)
        if await check_and_handle_country_restrictions(connection_manager, conn, order_no, seller_name, buyer_name, fiat):
            return
    else:
        if await check_and_handle_country_restrictions(connection_manager, conn, order_no, seller_name, buyer_name, fiat):
            return
        oxxo_used = await has_specific_bank_identifiers(conn, order_no, ['OXXO'])
        if oxxo_used:
            await update_buyer_bank(conn, buyer_name, 'banregio')
        payment_details = await get_payment_details(conn, order_no, buyer_name)
        buyer_bank = await get_buyer_bank(conn, buyer_name)
        if buyer_bank and buyer_bank.lower() in ['banregio', 'oxxo']:
            await send_messages(connection_manager, order_no, [payment_details])
        else:
            await send_messages(connection_manager, order_no, [payment_warning, payment_concept, payment_details])

async def generic_reply(connection_manager, order_no, order_details, status_code):
    buyer_name = order_details.get('buyer_name')
    current_language = determine_language(order_details)
    messages_to_send = await get_message_by_language(current_language, status_code, buyer_name)
    if messages_to_send is None:
        logger.warning(f"No messages for language: {current_language}, status_code: {status_code}")
        return
    await send_messages(connection_manager, order_no, messages_to_send)

async def handle_system_notifications(connection_manager, order_no, order_details, conn, order_status):
    if not await check_order_details(order_details):
        return
    logger.debug(f'Order status: {order_status}')
    if order_status == 4:
        await handle_order_status_4(connection_manager, conn, order_no, order_details)
    elif order_status == 1:
        await handle_order_status_1(connection_manager, conn, order_no, order_details)
    else:
        await generic_reply(connection_manager, order_no, order_details, order_status)
        response = await get_default_reply(order_details)
        await connection_manager.send_text_message(response, order_no)

async def handle_text_message(connection_manager, content, order_no, order_details, conn):
    if not await check_order_details(order_details):
        print("check_order_details returned False. Exiting function.")
        return

    order_status = order_details.get('order_status')
    if order_status not in [1, 2]:
        logger.debug("Order not in 1 or 2")
        return

    seller_name, buyer_name = order_details.get('seller_name'), order_details.get('buyer_name')
    kyc_status = await get_kyc_status(conn, buyer_name)
    anti_fraud_stage = await get_anti_fraud_stage(conn, buyer_name)
    if anti_fraud_stage is None:
        anti_fraud_stage = 0

    if kyc_status == 0 or anti_fraud_stage < 5:
        await handle_anti_fraud(buyer_name, seller_name, conn, anti_fraud_stage, content, order_no, connection_manager)
    else:
        logger.debug(f"Handling TEXT: {content}")

        if not await is_menu_presented(conn, order_no) and content in ['ayuda', 'help']:
            await present_menu_based_on_status(connection_manager, order_details, order_no, conn)

        if content.isdigit():
            await handle_menu_response(connection_manager, int(content), order_details, order_no, conn)


async def handle_image_message(connection_manager, order_no, order_details):
    if not await check_order_details(order_details):
        return
    logger.debug("Handling IMAGE")
    order_status = 100
    await generic_reply(connection_manager, order_no, order_details, order_status)
