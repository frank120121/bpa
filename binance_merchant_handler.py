# bpa/binance_merchant_handler.py
import json
import logging
import traceback
from datetime import date
import asyncio
from typing import Optional, Dict, Any, Tuple
from cep import Transferencia
from dataclasses import dataclass

from queue_based_transfer_validation import TransferValidationQueue, TransferValidator
from binance_db import insert_or_update_order
from binance_db_set import (
    update_order_status, update_total_spent, update_buyer_bank,
    set_menu_presented
)
from lang_utils import (
    get_message_by_language, determine_language, get_default_reply,
    payment_warning, verified_customer_greeting, transaction_denied,
    get_response_for_menu_choice, is_valid_choice,
    get_invalid_choice_reply, get_menu_for_order
)
from binance_db_get import (
    get_account_number, is_menu_presented, get_kyc_status,
    get_anti_fraud_stage, get_buyer_bank, get_order_details,
    has_specific_bank_identifiers, get_returning_customer_stage
)
from binance_bank_deposit_db import log_deposit
from binance_messages import send_messages
from binance_orders import binance_buy_order
from binance_anti_fraud import handle_anti_fraud
from binance_blacklist import is_blacklisted
from common_vars import status_map, BANK_SPEI_CODES
from TEST_binance_cep_2 import extract_clave_de_rastreo, retry_request
from binance_returning_customer import returning_customer

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
    buyer_bank: Optional[str] = None
class MerchantAccount:
    def __init__(self, payment_manager, binance_api):
        self.payment_manager = payment_manager
        self.binance_api = binance_api
        self.validation_queue = TransferValidationQueue()
        self.validator = None

    def initialize_validator(self, connection_manager) -> None:
        """Initialize the transfer validator with the connection manager."""
        self.validator = TransferValidator(self.validation_queue, connection_manager)

    async def start_validation_processor(self):
        """Start the validation processor task."""
        if self.validator is None:
            raise ValueError("Validator not initialized. Call initialize_validator first.")
        return asyncio.create_task(self.validator.process_queue())

    def _extract_order_data(self, order_details: Dict[str, Any], order_no: str) -> OrderData:
        """Extract order data from order details dictionary."""
        return OrderData(
            order_no=order_no,
            buyer_name=order_details.get('buyer_name', ''),
            seller_name=order_details.get('seller_name', ''),
            fiat_unit=order_details.get('fiat_unit', ''),
            total_price=order_details.get('total_price', 0.0),
            asset=order_details.get('asset', ''),
            order_status=order_details.get('order_status', 0),
            account_number=order_details.get('account_number', ''),
            buyer_bank=order_details.get('buyer_bank', None)
        )

    async def _fetch_and_update_order_details(
        self,
        KEY: str,
        SECRET: str,
        conn,
        order_no: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch and update order details."""
        try:
            order_details = await get_order_details(conn, order_no)
            if not order_details:
                order_details = await self.binance_api.fetch_order_details(KEY, SECRET, order_no)
                if order_details:
                    await insert_or_update_order(conn, order_details)
                    return await get_order_details(conn, order_no)
            return order_details
        except Exception as e:
            logger.error(f"Error fetching order details: {str(e)}\n{traceback.format_exc()}")
            return None

    async def handle_message_by_type(
        self,
        connection_manager,
        account: str,
        KEY: str,
        SECRET: str,
        msg_json: Dict[str, Any],
        conn
    ) -> None:
        """Handle incoming messages based on their type."""
        try:
            order_details = await self._fetch_and_update_order_details(
                KEY, SECRET, conn, msg_json.get('orderNo', '')
            )
            if not order_details:
                logger.warning("Failed to fetch order details")
                return

            order_data = self._extract_order_data(order_details, msg_json.get('orderNo', ''))

            # Early returns for specific conditions
            if (await has_specific_bank_identifiers(conn, order_data.order_no, ['SkrillMoneybookers'])
                or order_data.fiat_unit == 'USD'):
                return

            if msg_json.get('type') == 'system':
                await self._handle_system_type(connection_manager, account, msg_json, conn, order_data)
            else:
                await self._handle_other_types(connection_manager, account, msg_json, conn, order_data)

        except Exception as e:
            logger.error(f"Error in message handling: {str(e)}\n{traceback.format_exc()}")

    async def _handle_system_type(
        self,
        connection_manager,
        account: str,
        msg_json: Dict[str, Any],
        conn,
        order_data: OrderData
    ) -> None:
        """Handle system type messages."""
        try:
            content = msg_json.get('content', '').lower()
            content_dict = json.loads(content)
            system_type_str = content_dict.get('type', '')
            
            if system_type_str not in status_map:
                logger.info(f"Unknown system type: {system_type_str}")
                return
                
            if await is_blacklisted(conn, order_data.buyer_name):
                await connection_manager.send_text_message(
                    account,
                    transaction_denied,
                    order_data.order_no
                )
                return

            order_status = status_map[system_type_str]
            await update_order_status(conn, order_data.order_no, order_status)
            order_data.order_status = order_status
            await self.handle_system_notifications(
                connection_manager,
                account,
                order_data,
                conn,
                order_status
            )

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}\nContent: {content}")
        except Exception as e:
            logger.error(f"System type handling error: {str(e)}\n{traceback.format_exc()}")

    async def _handle_other_types(
        self,
        connection_manager,
        account: str,
        msg_json: Dict[str, Any],
        conn,
        order_data: OrderData
    ) -> None:
        """Handle non-system type messages."""
        try:
            # Early return conditions
            if (msg_json.get('status') == 'read'
                or msg_json.get('uuid', '').startswith("self_")
                or await is_blacklisted(conn, order_data.buyer_name)
                or order_data.seller_name == 'LOPEZ GUERRERO FRANCISCO JAVIER'
                or order_data.fiat_unit == 'USD'):
                return

            msg_type = msg_json.get('type')
            if msg_type == 'text':
                content = msg_json.get('content', '').lower()
                await self.handle_text_message(
                    connection_manager,
                    account,
                    content,
                    order_data,
                    conn
                )
            elif msg_type == 'image':
                await self.handle_image_message(
                    connection_manager,
                    account,
                    msg_json,
                    order_data,
                    conn
                )

        except Exception as e:
            logger.error(f"Message handling error: {str(e)}\n{traceback.format_exc()}")

    async def handle_system_notifications(
        self,
        connection_manager,
        account: str,
        order_data: OrderData,
        conn,
        order_status: int
    ) -> None:
        """Handle system notifications based on order status."""
        try:
            if order_status == 4:
                await self._handle_order_status_4(connection_manager, account, conn, order_data)
            elif order_status == 1:
                await self._handle_order_status_1(connection_manager, account, conn, order_data)
            else:
                await self._generic_reply(connection_manager, account, order_data, order_status)
                response = await get_default_reply(order_data.fiat_unit)
                await connection_manager.send_text_message(account, response, order_data.order_no)

        except Exception as e:
            logger.error(f"System notification error: {str(e)}\n{traceback.format_exc()}")

    async def _handle_order_status_4(
        self,
        connection_manager,
        account: str,
        conn,
        order_data: OrderData
    ) -> None:
        """Handle order status 4 (completion)."""
        try:
            await self._generic_reply(connection_manager, account, order_data, 4)
            
            if order_data.asset in ['BTC', 'ETH']:
                await binance_buy_order(order_data.asset)
            
            await update_total_spent(conn, order_data.order_no)
            bank_account_number = await get_account_number(conn, order_data.order_no)
            await log_deposit(
                conn,
                order_data.buyer_name,
                bank_account_number,
                order_data.total_price
            )

        except Exception as e:
            logger.error(f"Error handling order status 4: {str(e)}\n{traceback.format_exc()}")

    async def _handle_order_status_1(
        self,
        connection_manager,
        account: str,
        conn,
        order_data: OrderData
    ) -> None:
        """Handle order status 1 (initial state)."""
        try:
            kyc_status = await get_kyc_status(conn, order_data.buyer_name)
            
            # Handle OXXO case
            if await has_specific_bank_identifiers(conn, order_data.order_no, ['OXXO']):
                await update_buyer_bank(conn, order_data.buyer_name, 'oxxo')

            if kyc_status == 0 or kyc_status is None:
                # Handle new customer flow
                anti_fraud_stage = await get_anti_fraud_stage(conn, order_data.buyer_name) or 0
                await self._generic_reply(connection_manager, account, order_data, 1)
                await handle_anti_fraud(
                    order_data.buyer_name,
                    order_data.seller_name,
                    conn,
                    anti_fraud_stage,
                    "",
                    order_data.order_no,
                    connection_manager,
                    account,
                    self.payment_manager
                )
            else:
                # Handle verified customer flow
                buyer_bank = await get_buyer_bank(conn, order_data.buyer_name)
                greeting = await verified_customer_greeting(order_data.buyer_name)
                await connection_manager.send_text_message(account, greeting, order_data.order_no)
                
                if buyer_bank and buyer_bank.lower() in ['banregio', 'oxxo']:
                    payment_details = await self.payment_manager.get_payment_details(
                        conn,
                        order_data.order_no,
                        order_data.buyer_name
                    )
                    await send_messages(connection_manager, account, order_data.order_no, [payment_details])
                else:
                    returning_customer_stage = await get_returning_customer_stage(conn, order_data.buyer_name)
                    await returning_customer(
                        order_data.buyer_name,
                        conn,
                        returning_customer_stage,
                        "",
                        order_data.order_no,
                        connection_manager,
                        account,
                        self.payment_manager,
                        buyer_bank
                    )

        except Exception as e:
            logger.error(f"Error handling order status 1: {str(e)}\n{traceback.format_exc()}")

    async def handle_text_message(
        self,
        connection_manager,
        account: str,
        content: str,
        order_data: OrderData,
        conn
    ) -> None:
        """Handle text messages."""
        try:
            if order_data.order_status not in [1, 2]:
                return

            await self.process_customer_verification(
                order_data,
                conn,
                connection_manager,
                account,
                content
            )

        except Exception as e:
            logger.error(f"Text message handling error: {str(e)}\n{traceback.format_exc()}")

    async def handle_image_message(
        self,
        connection_manager,
        account: str,
        msg_json: Dict[str, Any],
        order_data: OrderData,
        conn
    ) -> None:
        """Handle image messages."""
        try:
            await self._generic_reply(connection_manager, account, order_data, 100)

            if order_data.order_status == 1:
                await connection_manager.send_text_message(
                    account,
                    "Por favor marcar la orden como pagada si ya envio el pago.",
                    order_data.order_no
                )

            # Validate image URL
            image_URL = msg_json.get('imageUrl')
            if not image_URL:
                logger.error(f"No image URL provided for order {order_data.order_no}")
                return

            # Get and validate buyer's bank
            buyer_bank = await get_buyer_bank(conn, order_data.buyer_name)
            if not buyer_bank:
                logger.error(f"No buyer bank found for {order_data.buyer_name} in order {order_data.order_no}")
                return

            # Get and validate seller's bank
            order_details = await get_order_details(conn, order_data.order_no)
            if not order_details:
                logger.error(f"Could not fetch order details for order {order_data.order_no}")
                return

            seller_bank = order_details.get('seller_bank')
            if not seller_bank:
                logger.error(f"No seller bank found for order {order_data.order_no}")
                return

            # Now we can safely use lower() as we've validated seller_bank is not None
            seller_bank = seller_bank.lower()

            # Proceed with bank validation
            await self.handle_bank_validation(
                order_data,
                buyer_bank,
                seller_bank,
                image_URL,
                conn,
                connection_manager,
                account
            )

        except Exception as e:
            logger.error(
                f"Image message handling error for order {order_data.order_no} - "
                f"Error: {str(e)}\n{traceback.format_exc()}"
            )

    async def _generic_reply(
            self,
            connection_manager,
            account: str,
            order_data: OrderData,
            status_code: int
        ) -> None:
            """Send generic reply based on status code."""
            try:
                current_language = determine_language(order_data.fiat_unit)
                messages_to_send = await get_message_by_language(
                    current_language,
                    status_code,
                    order_data.buyer_name
                )
                
                if messages_to_send is None:
                    logger.warning(
                        f"No messages found - Language: {current_language}, "
                        f"Status: {status_code}, Buyer: {order_data.buyer_name}"
                    )
                    return

                await send_messages(
                    connection_manager,
                    account,
                    order_data.order_no,
                    messages_to_send
                )

            except Exception as e:
                logger.error(f"Error sending generic reply: {str(e)}\n{traceback.format_exc()}")

    async def present_menu_based_on_status(
        self,
        connection_manager,
        account: str,
        order_data: OrderData,
        conn
    ) -> None:
        """Present menu options based on order status."""
        try:
            menu = await get_menu_for_order(
                order_data.fiat_unit,
                order_data.order_status
            )
            msg = '\n'.join(menu)
            await connection_manager.send_text_message(
                account,
                msg,
                order_data.order_no
            )
            await set_menu_presented(conn, order_data.order_no, True)
            logger.info(f"Menu presented for order {order_data.order_no}")

        except Exception as e:
            logger.error(f"Error presenting menu: {str(e)}\n{traceback.format_exc()}")

    async def handle_menu_response(
        self,
        connection_manager,
        account: str,
        choice: int,
        order_data: OrderData,
        conn
    ) -> None:
        """Handle customer's menu selection."""
        try:
            language = determine_language(order_data.fiat_unit)
            
            if await is_valid_choice(language, order_data.order_status, choice):
                if choice == 1:
                    payment_details = await self.payment_manager.get_payment_details(
                        conn,
                        order_data.order_no,
                        order_data.buyer_name
                    )
                    await connection_manager.send_text_message(
                        account,
                        payment_details,
                        order_data.order_no
                    )
                else:
                    response = await get_response_for_menu_choice(
                        language,
                        order_data.order_status,
                        choice,
                        order_data.buyer_name
                    )
                    await connection_manager.send_text_message(
                        account,
                        response,
                        order_data.order_no
                    )
                logger.info(
                    f"Menu choice {choice} processed for order {order_data.order_no}"
                )
            else:
                response = await get_invalid_choice_reply(order_data.fiat_unit)
                await connection_manager.send_text_message(
                    account,
                    response,
                    order_data.order_no
                )
                logger.warning(
                    f"Invalid menu choice {choice} for order {order_data.order_no}"
                )

        except Exception as e:
            logger.error(
                f"Error handling menu response - Order: {order_data.order_no}, "
                f"Choice: {choice}, Error: {str(e)}\n{traceback.format_exc()}"
            )

    async def handle_bank_validation(
        self,
        order_data: OrderData,
        buyer_bank: str,
        seller_bank: str,
        image_URL: str,
        conn,
        connection_manager,
        account: str
    ) -> bool:
        """Handle bank validation process for image messages."""
        try:
            # Validate SPEI codes
            emisor_code = BANK_SPEI_CODES.get(buyer_bank.lower())
            receptor_code = BANK_SPEI_CODES.get(seller_bank) 
            
            if not emisor_code or not receptor_code:
                logger.error(
                    f"SPEI codes not found - Order: {order_data.order_no}, "
                    f"Buyer Bank: {buyer_bank}, Seller Bank: {seller_bank}"
                )
                return False

            # Extract and validate tracking key
            clave_rastreo = await extract_clave_de_rastreo(
                image_URL,
                buyer_bank.upper()
            )
            if not clave_rastreo:
                logger.error(
                    f"No Clave de Rastreo found - Order: {order_data.order_no}, "
                    f"Bank: {buyer_bank}"
                )
                return False
            
            # Perform validation
            fecha = date.today()
            validation_successful = await retry_request(
                lambda: Transferencia.validar(
                    fecha=fecha,
                    clave_rastreo=clave_rastreo,
                    emisor=emisor_code,
                    receptor=receptor_code,
                    cuenta=order_data.account_number,
                    monto=order_data.total_price
                ),
                retries=5,
                delay=2,
                backoff=2
            )

            if validation_successful:
                logger.info(
                    f"Transfer validation successful - Order: {order_data.order_no}, "
                    f"Buyer: {order_data.buyer_name}"
                )
                await connection_manager.send_text_message(
                    account,
                    "Listo procedo a liberar.",
                    order_data.order_no
                )
                return True

            # Log validation failure details
            logger.error(
                f"Transfer validation failed - Order: {order_data.order_no}, "
                f"Buyer: {order_data.buyer_name}, "
                f"Banks: {buyer_bank}->{seller_bank}, "
                f"Amount: {order_data.total_price}, "
                f"Clave: {clave_rastreo}"
            )
            return False

        except Exception as e:
            logger.error(
                f"Bank validation error - Order: {order_data.order_no}, "
                f"Error: {str(e)}\n{traceback.format_exc()}"
            )
            return False

    async def process_customer_verification(
        self,
        order_data: OrderData,
        conn,
        connection_manager,
        account: str,
        content: str
    ) -> None:
        """Handle customer verification process."""
        try:
            kyc_status = await get_kyc_status(conn, order_data.buyer_name)
            anti_fraud_stage = await get_anti_fraud_stage(conn, order_data.buyer_name) or 0

            if kyc_status == 0 or anti_fraud_stage < 5:
                await handle_anti_fraud(
                    order_data.buyer_name,
                    order_data.seller_name,
                    conn,
                    anti_fraud_stage,
                    content,
                    order_data.order_no,
                    connection_manager,
                    account,
                    self.payment_manager
                )
                logger.info(f"Anti-fraud check completed for buyer {order_data.buyer_name}")
                return

            returning_customer_stage = await get_returning_customer_stage(
                conn,
                order_data.buyer_name
            ) or 0

            if returning_customer_stage < 3:
                current_buyer_bank = (
                    order_data.buyer_bank 
                    if order_data.buyer_bank is not None 
                    else await get_buyer_bank(conn, order_data.buyer_name)
                )
                await returning_customer(
                    order_data.buyer_name,
                    conn,
                    returning_customer_stage,
                    content,
                    order_data.order_no,
                    connection_manager,
                    account,
                    self.payment_manager,
                    current_buyer_bank  
                )
            elif content in ['ayuda', 'help']:
                if not await is_menu_presented(conn, order_data.order_no):
                    await self.present_menu_based_on_status(
                        connection_manager,
                        account,
                        order_data,
                        conn
                    )
            elif content.isdigit():
                await self.handle_menu_response(
                    connection_manager,
                    account,
                    int(content),
                    order_data,
                    conn
                )

        except Exception as e:
            logger.error(
                f"Customer verification error - Buyer: {order_data.buyer_name}, "
                f"Error: {str(e)}\n{traceback.format_exc()}"
            )