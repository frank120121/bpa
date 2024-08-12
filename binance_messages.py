
import logging
logger = logging.getLogger(__name__)

async def send_messages(connection_manager, account, order_no, messages):
    for msg in messages:
        await connection_manager.send_text_message(account, msg, order_no)