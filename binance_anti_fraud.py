# bpa/binance_anti_fraud.py
import logging
import asyncio
import string
import unicodedata
import traceback
from typing import List, Tuple, Optional

from binance_messages import send_messages
from binance_blacklist import add_to_blacklist
from lang_utils import (
    payment_warning,
    anti_fraud_stage3,
    anti_fraud_not_valid_response,
    anti_fraud_possible_fraud,
    anti_fraud_user_denied,
    ANTI_FRAUD_MESSAGES
)
from common_vars import NOT_ACCEPTED_BANKS, ACCEPTED_BANKS, normalize_bank_name


logger = logging.getLogger(__name__)

def normalize_string(input_str: str) -> str:
    """Normalize string by removing diacritics, punctuation and converting to lowercase."""
    normalized_str = unicodedata.normalize('NFD', input_str)
    stripped_str = ''.join([c for c in normalized_str if unicodedata.category(c) != 'Mn'])
    return stripped_str.translate(str.maketrans('', '', string.punctuation))

def get_anti_fraud_questions(buyer_name: str, order_no: str) -> Tuple[List[str], List[str]]:
    """
    Generate anti-fraud questions based on customer context.
    
    Args:
        buyer_name: Name of the buyer
        order_no: Order number for the transaction
        
    Returns:
        Tuple of (standard questions, OXXO-specific questions)
    """
    standard_questions = [
        ANTI_FRAUD_MESSAGES["employment_check"],
        ANTI_FRAUD_MESSAGES["pressure_check"],
        ANTI_FRAUD_MESSAGES["refund_agreement"](order_no),
        ANTI_FRAUD_MESSAGES["bank_request"],
        ANTI_FRAUD_MESSAGES["account_ownership"](buyer_name),
    ]
    
    oxxo_questions = [ANTI_FRAUD_MESSAGES["oxxo_cash_payment"]]
    
    return standard_questions, oxxo_questions

async def handle_bank_verification(
    order_cache,
    conn,
    normalized_response: str,
    buyer_name: str,
    account: str,
    order_no: str,
    connection_manager,
    questions: List[str],
    anti_fraud_stage: int  # Added this parameter
) -> Optional[str]:
    """
    Handle bank verification process.
    
    Args:
        conn: Database connection
        normalized_response: Normalized user input for bank name
        buyer_name: Name of the buyer
        account: Account identifier
        order_no: Order number
        connection_manager: Connection manager instance
        questions: List of verification questions
        anti_fraud_stage: Current stage in anti-fraud process
        
    Returns:
        Optional[str]: Verified bank name if successful, None otherwise
    """
    try:
        standard_bank_name = normalize_bank_name(normalized_response)
        
        if standard_bank_name in [normalize_bank_name(bank) for bank in NOT_ACCEPTED_BANKS]:
            await connection_manager.send_text_message(account, anti_fraud_stage3, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, None, standard_bank_name, anti_fraud_stage)
            return None

        if standard_bank_name in [normalize_bank_name(bank) for bank in ACCEPTED_BANKS]:
            # Update cache first
            await order_cache.update_fields(order_no, {
                'buyer_bank': standard_bank_name
            })
            # Then update DB
            await order_cache.sync_to_db(conn, order_no)
            return standard_bank_name
        
        # Handle invalid bank
        accepted_banks_list = ', '.join(ACCEPTED_BANKS)
        await connection_manager.send_text_message(
            account,
            ANTI_FRAUD_MESSAGES["bank_verification_failed"](accepted_banks_list),
            order_no
        )
        await asyncio.sleep(2)
        await connection_manager.send_text_message(account, questions[3], order_no)
        return None

    except Exception as e:
        logger.error(f"Error in bank verification for buyer {buyer_name}: {str(e)}\n{traceback.format_exc()}")
        return None


async def handle_invalid_response(
    connection_manager,
    account: str,
    order_no: str,
    questions: List[str],
    oxxo_questions: List[str],
    anti_fraud_stage: int,
    oxxo_used: bool
) -> None:
    """Handle invalid yes/no responses."""
    await connection_manager.send_text_message(account, anti_fraud_not_valid_response, order_no)
    await asyncio.sleep(2)
    
    if anti_fraud_stage == 4 and oxxo_used:
        await connection_manager.send_text_message(account, oxxo_questions[0], order_no)
    else:
        await connection_manager.send_text_message(account, questions[anti_fraud_stage], order_no)

async def handle_anti_fraud(
    buyer_name: str,
    seller_name: str,
    conn,
    anti_fraud_stage: int,
    response: str,
    order_no: str,
    connection_manager,
    account,
    payment_manager,
    order_cache
) -> None:
    """Handle anti-fraud verification process."""
    try:
        questions, oxxo_questions = get_anti_fraud_questions(buyer_name, order_no)
        normalized_response = normalize_string(response.strip())

        # Get order from cache
        order_data = await order_cache.get_order(order_no)
        if not order_data:
            logger.error(f"Order {order_no} not found in cache")
            return

        # Check for OXXO using cached bank identifiers
        oxxo_used = 'OXXO' in order_data.bank_identifiers

        logger.info(f"Processing anti-fraud stage {anti_fraud_stage} for {buyer_name}")

        # Handle empty initial response
        if anti_fraud_stage == 0 and not normalized_response:
            await connection_manager.send_text_message(account, questions[anti_fraud_stage], order_no)
            return 

        # Validate stage bounds
        if anti_fraud_stage >= len(questions):
            return 

        # First handle yes/no responses for anti-fraud questions
        if anti_fraud_stage in [0, 1, 2] and normalized_response not in ['si', 'no']:
            await handle_invalid_response(
                connection_manager, account, order_no, questions,
                oxxo_questions, anti_fraud_stage, oxxo_used
            )
            return

        # Check for fraud indicators
        fraud_responses = {(0, 'si'), (1, 'si')}
        deny_responses = {(2, 'no')}

        if (anti_fraud_stage, normalized_response) in fraud_responses:
            await connection_manager.send_text_message(account, anti_fraud_possible_fraud, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, anti_fraud_stage)
            return

        if (anti_fraud_stage, normalized_response) in deny_responses:
            await connection_manager.send_text_message(account, anti_fraud_user_denied, order_no)
            await add_to_blacklist(conn, buyer_name, order_no, None, normalized_response, anti_fraud_stage)
            return

        # Handle bank verification stage
        if anti_fraud_stage == 3:
            verified_bank = await handle_bank_verification(
                order_cache, 
                conn,
                normalized_response,
                buyer_name,
                account,
                order_no,
                connection_manager,
                questions,
                anti_fraud_stage
            )
            if not verified_bank:
                return
            
            # Progress to account ownership question
            # Update cache with new stage
            anti_fraud_stage = 4
            await order_cache.update_fields(order_no, {
                'anti_fraud_stage': anti_fraud_stage
            })
            # Sync to DB
            await order_cache.sync_to_db(conn, order_no)

            await connection_manager.send_text_message(
                account,
                questions[anti_fraud_stage],
                order_no
            )
            return

        # Progress to next stage
        anti_fraud_stage += 1

        # Update cache with new stage
        await order_cache.update_fields(order_no, {
            'anti_fraud_stage': anti_fraud_stage
        })

        if anti_fraud_stage == len(questions):
            # Update KYC status in cache
            await order_cache.update_fields(order_no, {
                'kyc_status': 1
            })

            # Get payment details and update cache
            payment_details = await payment_manager.get_payment_details(conn, order_no, buyer_name)
            if payment_details.get('account_number'):
                await order_cache.update_fields(order_no, {
                    'account_number': payment_details['account_number'],
                    'seller_bank': payment_details.get('bank_name')
                })
            
            messages = [payment_warning, payment_details]
            await send_messages(connection_manager, account, order_no, messages)
        else:
            await connection_manager.send_text_message(account, questions[anti_fraud_stage], order_no)

    except Exception as e:
        logger.error(f"Error in anti-fraud process for {buyer_name}: {str(e)}\n{traceback.format_exc()}")