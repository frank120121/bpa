from binance_msg_handler import handle_text_message, handle_system_notifications, handle_image_message
from binance_db_set import update_order_status
from binance_db_get import get_order_details
from binance_order_details import fetch_order_details
from binance_db import insert_or_update_order
import json
from common_vars import status_map
from binance_blacklist import is_blacklisted
from lang_utils import transaction_denied
import traceback
import logging
logger = logging.getLogger(__name__)
class MerchantAccount:
    async def handle_message_by_type(self, connection_manager, KEY, SECRET, msg_json, msg_type, conn):
        order_no = msg_json.get('orderNo', '')
        order_details = await self._fetch_and_update_order_details(KEY, SECRET, conn, order_no)
        if not order_details:
            logger.warning("Failed to fetch order details from the external source.")
            return
        # Check for specific bank identifiers
        if await has_specific_bank_identifiers(conn, order_no, ['OXXO', 'SkrillMoneybookers']):
            logger.info(f"Order {order_no} uses payment method (OXXO or Skrill).")
            return  # Skip further processing for now
        fiat = order_details.get('fiat_unit')
        if fiat == 'USD':
            return # Skip further processing for now
        buyer_name = order_details.get('buyer_name')
        if buyer_name in ['LOPEZ GUERRERO FRANCISCO JAVIER', 'GUERRERO LOPEZ MARTHA', 'MUNOZ PEREA MARIA FERNANDA']:
            return
        if msg_type == 'system':
            await self._handle_system_type(connection_manager, msg_json, conn, order_no, order_details, buyer_name)
        else:
            await self._handle_other_types(connection_manager, msg_json, msg_type, conn, order_no, order_details, buyer_name)
    async def _handle_system_type(self, connection_manager, msg_json, conn, order_no, order_details, buyer_name):
        try:
            content = msg_json.get('content', '').lower()
            content_dict = json.loads(content)
            system_type_str = content_dict.get('type', '')
            if system_type_str not in status_map:
                logger.debug("System type not in status_map")
                return
            order_status = status_map[system_type_str]
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from content: {content}")
            return
        if await is_blacklisted(conn, buyer_name):
            await connection_manager.send_text_message(transaction_denied, order_no)
            return
        await update_order_status(conn, order_no, order_status)
        order_details = await get_order_details(conn, order_no)
        await handle_system_notifications(connection_manager, order_no, order_details, conn, order_status)
    async def _handle_other_types(self, connection_manager, msg_json, msg_type, conn, order_no, order_details, buyer_name):
        msg_status = msg_json.get('status')
        if msg_status == 'read':
            return
        uuid = msg_json.get('uuid', '')
        is_self_message = uuid.startswith("self_")

        if is_self_message:
            logger.debug(f"Ignoring self message: {uuid}")
            return
        if await is_blacklisted(conn, buyer_name):
            return
        seller_name = order_details.get('seller_name')
        if seller_name == 'LOPEZ GUERRERO FRANCISCO JAVIER':
            return
        fiat_unit = order_details.get('fiat_unit')
        if fiat_unit == 'USD':
            return
        if msg_type == 'text':
            content =  msg_json.get('content', '').lower()
            await handle_text_message(connection_manager, content, order_no, order_details, conn)
        elif msg_type == 'image':
            await handle_image_message(connection_manager, order_no, order_details)
    async def _fetch_and_update_order_details(self, KEY, SECRET, conn, order_no):
        try:
            order_details = await get_order_details(conn, order_no)
            if not order_details:
                order_details = await fetch_order_details(KEY, SECRET, order_no)
                if order_details:
                    await insert_or_update_order(conn, order_details)
                    order_details = await get_order_details(conn, order_no)
                    return order_details
            return order_details
        except Exception as e:
            logger.error(f"An error occurred: {e}\n{traceback.format_exc()}")
            return None
async def has_specific_bank_identifiers(conn, order_no, identifiers):
    async with conn.cursor() as cursor:
        await cursor.execute("""
            SELECT COUNT(*) FROM order_bank_identifiers
            WHERE order_no = ? AND bank_identifier IN ({})
        """.format(','.join('?'*len(identifiers))), (order_no, *identifiers))
        result = await cursor.fetchone()
        return result[0] > 0  # True if any of the specified identifiers are found