import json
from database import create_connection
from merchant_account import MerchantAccount
import logging
from logging_config import setup_logging
setup_logging(log_filename='websocket_handler.log')
logger = logging.getLogger(__name__)

async def on_message(ws, message, KEY, SECRET):
    merchant_account = MerchantAccount()
    try:
        msg_json = json.loads(message)
        is_self = msg_json.get('self', False)
        if is_self:
            logger.debug("message was from self")
            return
        msg_type = msg_json.get('type', '')
        conn = await create_connection("C:/Users/p7016/Documents/bpa/orders_data.db")
        if conn:
            logger.info(message)
            try:
                await merchant_account.handle_message_by_type(ws, KEY, SECRET, msg_json, msg_type, conn)
                await conn.commit()               
            except Exception as e:
                await conn.rollback()
                logger.exception("Database operation failed, rolled back: %s", e)
            finally:
                await conn.close()
        else:
            logger.error("Failed to connect to the database.")
    except Exception as e:
        logger.exception("An exception occurred: %s", e)
async def on_close(ws, close_status_code, close_msg, KEY, SECRET):
    logger.debug(f"### closed ###")