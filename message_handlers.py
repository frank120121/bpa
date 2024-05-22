from lang_utils import get_message_by_language, determine_language, transaction_denied, get_default_reply, payment_concept
from common_utils import RateLimiter
from database import update_total_spent, get_kyc_status, get_anti_fraud_stage, is_menu_presented
from binance_bank_deposit import get_payment_details, log_deposit
from binance_messages import send_text_message, present_menu_based_on_status, handle_menu_response
from binance_orders import binance_buy_order
from binance_anti_fraud import handle_anti_fraud
from binance_blacklist import add_to_blacklist
from verify_client_ip import fetch_ip
import logging
from logging_config import setup_logging

setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
rate_limiter = RateLimiter(limit_period=10)

async def check_order_details(order_details):
    if order_details is None:
        logger.warning("order_details is None.")
        return False
    return True

async def send_messages(ws, order_no, messages):
    for msg in messages:
        await send_text_message(ws, msg, order_no)

async def handle_order_status_4(ws, conn, order_no, order_details):
    asset_type = order_details.get('asset')
    logger.debug(asset_type)
    if asset_type == 'BTC':
        await binance_buy_order(asset_type)
    await update_total_spent(conn, order_no)
    await generic_reply(ws, order_no, order_details, 4)

async def handle_order_status_1(ws, conn, order_no, order_details):
    seller_name, buyer_name = order_details.get('seller_name'), order_details.get('buyer_name')
    country = await fetch_ip(order_no[-4:], seller_name)
    if country and country != "MX":
        logger.info(f"Transaction denied. Seller not from Mexico. Buyer: {buyer_name} added to blacklist.")
        await send_text_message(ws, transaction_denied, order_no)
        await add_to_blacklist(conn, buyer_name, order_no, country)
        return

    kyc_status = await get_kyc_status(conn, buyer_name)
    if kyc_status == 0:
        anti_fraud_stage = await get_anti_fraud_stage(conn, buyer_name)
        await generic_reply(ws, order_no, order_details, 1)
        await handle_anti_fraud(buyer_name, conn, anti_fraud_stage, "start_pro", order_no, ws)
    else:
        payment_details = await get_payment_details(conn, order_no, buyer_name, seller_name)
        await send_messages(ws, order_no, [payment_details, payment_concept])

async def generic_reply(ws, order_no, order_details, status_code):
    buyer_name = order_details.get('buyer_name')
    current_language = determine_language(order_details)
    messages_to_send = await get_message_by_language(current_language, status_code, buyer_name)
    if messages_to_send is None:
        logger.warning(f"No messages for language: {current_language}, status_code: {status_code}")
        return
    await send_messages(ws, order_no, messages_to_send)

async def handle_system_notifications(ws, order_no, order_details, conn, order_status):
    if not await check_order_details(order_details):
        return
    logger.debug(f'Order status: {order_status}')
    if order_status == 4:
        await handle_order_status_4(ws, conn, order_no, order_details)
    elif order_status == 1:
        await handle_order_status_1(ws, conn, order_no, order_details)
    else:
        await generic_reply(ws, order_no, order_details, order_status)
        response = await get_default_reply(order_details)
        await send_text_message(ws, response, order_no)

async def handle_text_message(ws, content, order_no, order_details, conn):
    if not await check_order_details(order_details):
        print("check_order_details returned False. Exiting function.")
        return

    order_status = order_details.get('order_status')
    if order_status not in [1, 2]:
        logger.debug("Order not in 1 or 2")
        return

    buyer_name = order_details.get('buyer_name')
    kyc_status = await get_kyc_status(conn, buyer_name)
    anti_fraud_stage = await get_anti_fraud_stage(conn, buyer_name)

    if kyc_status == 0 or anti_fraud_stage < 5:
        await handle_anti_fraud(buyer_name, conn, anti_fraud_stage, content, order_no, ws)
    else:
        logger.debug(f"Handling TEXT: {content}")

        if not await is_menu_presented(conn, order_no) and content in ['ayuda', 'help']:
            await present_menu_based_on_status(ws, order_details, order_no, conn)

        if content.isdigit():
            await handle_menu_response(ws, int(content), order_details, order_no)


async def handle_image_message(ws, order_no, order_details):
    if not await check_order_details(order_details):
        return
    logger.debug("Handling IMAGE")
    order_status = 100
    await generic_reply(ws, order_no, order_details, order_status)
