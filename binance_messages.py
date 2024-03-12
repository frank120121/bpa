import asyncio
import logging
from lang_utils import get_response_for_menu_choice, is_valid_choice, get_invalid_choice_reply, determine_language, get_menu_for_order
from binance_db_set import set_menu_presented
from binance_bank_deposit import get_payment_details
logger = logging.getLogger(__name__)

async def send_messages(connection_manager, order_no, messages):
    for msg in messages:
        await connection_manager.send_text_message(msg, order_no)
        await asyncio.sleep(5)

async def present_menu_based_on_status(connection_manager, order_details, order_no, conn):

    menu = await get_menu_for_order(order_details)
    msg = '\n'.join(menu)
    await connection_manager.send_text_message(msg, order_no)
    await set_menu_presented(conn, order_no, True)

async def handle_menu_response(connection_manager, choice, order_details, order_no, conn):
    language = determine_language(order_details)
    order_status = order_details.get('order_status')
    buyer_name = order_details.get('buyer_name')
    if await is_valid_choice(language, order_status, choice):
        if choice == 1:
            payment_details = await get_payment_details(conn, order_no, buyer_name)
            await connection_manager.send_text_message(payment_details, order_no)
        else:
            response = await get_response_for_menu_choice(language, order_status, choice, buyer_name)
            await connection_manager.send_text_message(response, order_no)
    else:
        response = await get_invalid_choice_reply(order_details)
        await connection_manager.send_text_message(response, order_no)