import json
import logging
import traceback
from datetime import date

from dataclasses import dataclass

from binance_db import insert_or_update_order
from binance_db_set import update_order_status, update_total_spent, update_buyer_bank
from lang_utils import get_message_by_language, determine_language, get_default_reply, payment_warning, invalid_country, verified_customer_greeting, transaction_denied
from binance_db_get import get_account_number, is_menu_presented, get_kyc_status, get_anti_fraud_stage, get_buyer_bank, get_order_details, has_specific_bank_identifiers, get_account_number
from binance_bank_deposit_db import log_deposit
from binance_messages import send_messages
from binance_orders import binance_buy_order
from binance_anti_fraud import handle_anti_fraud
from binance_blacklist import add_to_blacklist, is_blacklisted
from verify_client_ip import fetch_ip
from common_vars import prohibited_countries, status_map, ACCEPTED_COUNTRIES_FOR_OXXO
from TEST_binance_cep import extract_clave_de_rastreo, validate_transfer
from lang_utils import get_response_for_menu_choice, is_valid_choice, get_invalid_choice_reply, determine_language, get_menu_for_order
from binance_db_set import set_menu_presented

logger = logging.getLogger(__name__)

@dataclass
class OrderData:
    order_no: str
    buyer_name: str
    seller_name: str
    fiat_unit: str
    total_price: float
    asset: str
    order_status: int
    account_number: str

class MerchantAccount:
    def __init__(self, payment_manager, binance_api):
        self.payment_manager = payment_manager
        self.binance_api = binance_api

    def _extract_order_data(self, order_details: dict, order_no: str) -> OrderData:
        return OrderData(
            order_no=order_no,
            buyer_name=order_details.get('buyer_name', ''),
            seller_name=order_details.get('seller_name', ''),
            fiat_unit=order_details.get('fiat_unit', ''),
            total_price=order_details.get('total_price', 0.0),
            asset=order_details.get('asset', ''),
            order_status=order_details.get('order_status', 0),
            account_number=order_details.get('account_number', '')
        )

    async def handle_message_by_type(self, connection_manager, account, KEY, SECRET, msg_json, conn):
        order_details = await self._fetch_and_update_order_details(KEY, SECRET, conn, msg_json.get('orderNo', ''))
        if not order_details:
            logger.warning("Failed to fetch order details from the external source.")
            return
        order_data = self._extract_order_data(order_details, msg_json.get('orderNo', ''))

        if await has_specific_bank_identifiers(conn, order_data.order_no, ['SkrillMoneybookers']):
            return
         
        if order_data.fiat_unit == 'USD':
            return
        if msg_json.get('type') == 'system':
            await self._handle_system_type(connection_manager, account, msg_json, conn, order_data)
        else:
            await self._handle_other_types(connection_manager, account, msg_json, conn, order_data)

    async def _handle_system_type(self, connection_manager, account, msg_json, conn, order_data: OrderData):
        try:
            content = msg_json.get('content', '').lower()
            content_dict = json.loads(content)
            system_type_str = content_dict.get('type', '')
            if system_type_str not in status_map:
                logger.info(f"System type not in status_map: {system_type_str}.")
                return
            order_status = status_map[system_type_str]
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from content: {content}")
            return

        if await is_blacklisted(conn, order_data.buyer_name):
            await connection_manager.send_text_message(account, transaction_denied, order_data.order_no)
            return
        
        await update_order_status(conn, order_data.order_no, order_status)
        order_data.order_status = order_status
        await self.handle_system_notifications(connection_manager, account, order_data, conn, order_status)

    async def _handle_other_types(self, connection_manager, account, msg_json, conn, order_data: OrderData):
        if msg_json.get('status') == 'read':    
            return
        if msg_json.get('uuid', '').startswith("self_"):
            return
        if await is_blacklisted(conn, order_data.buyer_name):
            return
        if order_data.seller_name == 'LOPEZ GUERRERO FRANCISCO JAVIER':
            return
        if order_data.fiat_unit == 'USD':
            return
        if msg_json.get('type') == 'text':
            content = msg_json.get('content', '').lower()
            await self.handle_text_message(connection_manager, account, content, order_data, conn)
        elif msg_json.get('type') == 'image':
            await self.handle_image_message(connection_manager, account, msg_json, order_data)

    async def _fetch_and_update_order_details(self, KEY, SECRET, conn, order_no):
        try:
            order_details = await get_order_details(conn, order_no)
            if not order_details:
                order_details = await self.binance_api.fetch_order_details(KEY, SECRET, order_no)
                if order_details:
                    await insert_or_update_order(conn, order_details)
                    order_details = await get_order_details(conn, order_no)
                    return order_details
            return order_details
        except Exception as e:
            logger.error(f"An error occurred: {e}\n{traceback.format_exc()}")
            return None
        
    async def handle_system_notifications(self, connection_manager, account, order_data: OrderData, conn, order_status):
        if order_status == 4:
            await self._handle_order_status_4(connection_manager, account, conn, order_data)
        elif order_status == 1:
            await self._handle_order_status_1(connection_manager, account, conn, order_data)
        else:
            await self._generic_reply(connection_manager, account, order_data, order_status)
            response = await get_default_reply(order_data.fiat_unit)
            await connection_manager.send_text_message(account, response, order_data.order_no)

    async def handle_text_message(self, connection_manager, account, content, order_data: OrderData, conn):

        if order_data.order_status not in [1, 2]:
            return
        
        kyc_status = await get_kyc_status(conn, order_data.buyer_name)
        anti_fraud_stage = await get_anti_fraud_stage(conn, order_data.buyer_name)
        if anti_fraud_stage is None:
            anti_fraud_stage = 0

        if kyc_status == 0 or anti_fraud_stage < 5:
            await handle_anti_fraud(order_data.buyer_name, order_data.seller_name, conn, anti_fraud_stage, content, order_data.order_no, connection_manager, account, self.payment_manager)
        else:
            if not await is_menu_presented(conn, order_data.order_no) and content in ['ayuda', 'help']:
                await self.present_menu_based_on_status(connection_manager, account, order_data, conn)
            if content.isdigit():
                await self.handle_menu_response(connection_manager, account, int(content), order_data, conn)

    async def handle_image_message(self, connection_manager, account, msg_json, order_data: OrderData):
        order_status = 100
        await self._generic_reply(connection_manager, account, order_data, order_status)

        if order_data.order_status == 1:
            message = ("Por favor marcar la orden como pagada si ya envio el pago.")
            await connection_manager.send_text_message(account, message, order_data.order_no)

        image_URL = msg_json.get('imageUrl')
        if image_URL is None:
            logger.error(f"No url found. URL: {image_URL}")
            return

        clave_rastreo = await extract_clave_de_rastreo(image_URL, 'BBVA')
        if clave_rastreo is None:
            logger.error("No Clave de Rastreo found.")
            return
        
        fecha = date.today()
        emisor = '40012'
        receptor = '90710'

        validation_successful = await validate_transfer(fecha, clave_rastreo, emisor, receptor, order_data.account_number, order_data.total_price)
        if validation_successful:
            logger.info("Transfer validation and PDF download successful.")
        else:
            logger.error("Transfer validation failed.")

    async def _generic_reply(self, connection_manager, account, order_data: OrderData, status_code):
        current_language = determine_language(order_data.fiat_unit)
        messages_to_send = await get_message_by_language(current_language, status_code, order_data.buyer_name)
        if messages_to_send is None:
            logger.warning(f"No messages for language: {current_language}, status_code: {status_code}")
            return
        await send_messages(connection_manager, account, order_data.order_no, messages_to_send)

    async def _handle_order_status_4(self, connection_manager, account, conn, order_data: OrderData):
        await self._generic_reply(connection_manager, account, order_data, 4)
        asset_type = order_data.asset
        if asset_type in ['BTC', 'ETH']:
            await binance_buy_order(asset_type)
        await update_total_spent(conn, order_data.order_no)
        bank_account_number = await get_account_number(conn, order_data.order_no)
    
        await log_deposit(conn, order_data.buyer_name, bank_account_number, order_data.total_price)

    async def _handle_order_status_1(self, connection_manager, account, conn, order_data: OrderData):
        kyc_status = await get_kyc_status(conn, order_data.buyer_name)
        oxxo_used = await has_specific_bank_identifiers(conn, order_data.order_no, ['OXXO'])
        if oxxo_used:
            await update_buyer_bank(conn, order_data.buyer_name, 'oxxo')

        if kyc_status == 0 or kyc_status is None:
            anti_fraud_stage = await get_anti_fraud_stage(conn, order_data.buyer_name)
            if anti_fraud_stage is None:
                anti_fraud_stage = 0
            await self._generic_reply(connection_manager, account, order_data, 1)
            await handle_anti_fraud(order_data.buyer_name, order_data.seller_name, conn, anti_fraud_stage, "", order_data.order_no, connection_manager, account, self.payment_manager)
            if await self._check_and_handle_country_restrictions(connection_manager, account, conn, order_data, oxxo_used):
                return
        else:
            greeting = await verified_customer_greeting(order_data.buyer_name)
            await connection_manager.send_text_message(account, greeting, order_data.order_no)
            if await self._check_and_handle_country_restrictions(connection_manager, account, conn, order_data, oxxo_used):
                return
            payment_details = await self.payment_manager.get_payment_details(conn, order_data.order_no, order_data.buyer_name)
            buyer_bank = await get_buyer_bank(conn, order_data.buyer_name)
            if buyer_bank and buyer_bank.lower() in ['banregio', 'oxxo']:
                await send_messages(connection_manager, account, order_data.order_no, [payment_details])
            else:
                await send_messages(connection_manager, account, order_data.order_no, [payment_warning, payment_details])

    async def _check_and_handle_country_restrictions(self, connection_manager, account, conn, order_data: OrderData, oxxo_used):
        country = await fetch_ip(order_data.order_no[-4:], order_data.seller_name)
        if order_data.fiat_unit == 'USD':
            if country in prohibited_countries:
                await connection_manager.send_text_message(account, invalid_country, order_data.order_no)
                await add_to_blacklist(conn, order_data.buyer_name, order_data.order_no, country)
                return True
        if oxxo_used and country in ACCEPTED_COUNTRIES_FOR_OXXO and order_data.total_price < 5000:
            return False 
        if country and country != "MX":
            await connection_manager.send_text_message(account, invalid_country, order_data.order_no)
            await add_to_blacklist(conn, order_data.buyer_name, order_data.order_no, country)
            return True  
        return False
    
    async def present_menu_based_on_status(self, connection_manager, account, order_data: OrderData, conn):

        menu = await get_menu_for_order(order_data.fiat_unit, order_data.order_status)
        msg = '\n'.join(menu)
        await connection_manager.send_text_message(account, msg, order_data.order_no)
        await set_menu_presented(conn, order_data.order_no, True)

    async def handle_menu_response(self, connection_manager, account, choice, order_data: OrderData, conn):
        language = determine_language(order_data.fiat_unit)
        if await is_valid_choice(language, order_data.order_status, choice):
            if choice == 1:
                payment_details = await self.payment_manager.get_payment_details(conn, order_data.order_no, order_data.buyer_name)
                await connection_manager.send_text_message(account, payment_details, order_data.order_no)
            else:
                response = await get_response_for_menu_choice(language, order_data.order_status, choice, order_data.buyer_name)
                await connection_manager.send_text_message(account, response, order_data.order_no)
        else:
            response = await get_invalid_choice_reply(order_data.fiat_unit)
            await connection_manager.send_text_message(account, response, order_data.order_no)