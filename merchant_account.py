from message_handlers import handle_text_message, handle_system_notifications, handle_image_message
from database import insert_or_update_order, get_order_details, update_order_status
from binance_order_details import fetch_order_details
import json
from common_vars import status_map
from binance_blacklist import is_blacklisted
from binance_messages import send_text_message
from lang_utils import transaction_denied
import traceback
import logging
from logging_config import setup_logging
setup_logging(log_filename='Binance_c2c_logger.log')
logger = logging.getLogger(__name__)
class MerchantAccount:
    async def handle_message_by_type(self, ws, KEY, SECRET, msg_json, msg_type, conn):
        order_no = msg_json.get('orderNo', '')
        if not ws.open:
            logger.error("WebSocket connection is not open!")
            return
        order_details = await self._fetch_and_update_order_details(KEY, SECRET, conn, order_no)
        if not order_details:
            logger.warning("Failed to fetch order details from the external source.")
            return
        buyer_name = order_details.get('buyer_name')
        if msg_type == 'system':
            await self._handle_system_type(ws, msg_json, conn, order_no, order_details, buyer_name)
        else:
            await self._handle_other_types(ws, msg_json, msg_type, conn, order_no, order_details, buyer_name)
    async def _handle_system_type(self, ws, msg_json, conn, order_no, order_details, buyer_name):
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
            await send_text_message(ws, transaction_denied, order_no)
            return
        await update_order_status(conn, order_no, order_status)
        order_details = await get_order_details(conn, order_no)
        await handle_system_notifications(ws, order_no, order_details, conn, order_status)
    async def _handle_other_types(self, ws, msg_json, msg_type, conn, order_no, order_details, buyer_name):
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
        if msg_type == 'text':
            content =  msg_json.get('content', '').lower()
            await handle_text_message(ws, content, order_no, order_details, conn)
        elif msg_type == 'image':
            await handle_image_message(ws, order_no, order_details)
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
